package staged

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func pilotRequest() Request {
	transfer := func(id string) *Transfer {
		return &Transfer{SourceEndpointID: "source-id", DestinationEndpointID: "destination-id", SubmissionID: id, Label: id, Items: []TransferItem{{SourcePath: "/source/file", DestinationPath: "/destination/file"}}}
	}
	return Request{
		WorkflowID: "msk-cardiac-pilot", TaskQueue: "pilot-queue", GlobusTaskQueue: "pilot-queue",
		StageIn: transfer("stage-in"), StageBack: transfer("stage-back"), Publish: transfer("publish"),
		Slurm: &SlurmStage{TaskQueue: "user@slurm.example:22", Job: SlurmJob{Name: "cardiac", Script: "echo done", Config: SlurmOptions{Partition: "RM-shared", Time: "04:00:00", Nodes: 1, Ntasks: 1, CpusPerTask: 32, Mem: "64G"}}},
		GPU:   &GPUStage{TaskQueue: "user@gpu.example:22", Job: GPUJob{Image: "biodepot/cellbender:0.3.2", Cmd: []string{"cellbender", "remove-background", "--cuda"}, UseGPU: true, InputFiles: map[string]string{"/input/matrix": "raw_feature_bc_matrix"}, LocalOutputDir: "/output/gpu"}},
	}
}

func TestPilotRequestContractAndUnsafePaths(t *testing.T) {
	req := pilotRequest()
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Request
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := decoded.Validate(); err != nil {
		t.Fatalf("valid pilot request rejected: %v", err)
	}
	decoded.StageIn.Items[0].SourcePath = "/source/../secret"
	if err := decoded.Validate(); err == nil {
		t.Fatal("parent traversal accepted")
	}
	decoded = req
	decoded.GPU.Job.InputFiles = map[string]string{"/input/matrix": "../../escape"}
	if err := decoded.Validate(); err == nil {
		t.Fatal("GPU relative-path traversal accepted")
	}
	if SafePath("relative/file") || SafePath("/a/../b") {
		t.Fatal("unsafe path accepted")
	}
}

func TestPythonStagedDefaults(t *testing.T) {
	var req Request
	data := `{"workflow_id":"default-test","stage_in":{"source_endpoint_id":"source","destination_endpoint_id":"dest","submission_id":"submission","label":"test","items":[{"source_path":"/src","destination_path":"/dest"}]},"slurm":{"task_queue":"user@host:22","job":{"script":"true","config":{"partition":"normal"}}},"gpu":{"task_queue":"user@gpu:22","job":{"image":"alpine:latest","cmd":["true"],"local_output_dir":"/output"}}}`
	if err := json.Unmarshal([]byte(data), &req); err != nil {
		t.Fatal(err)
	}
	req.Normalize()
	if err := req.Validate(); err != nil {
		t.Fatal(err)
	}
	if req.TaskQueue != "staged-slurm-gpu" || req.GlobusTaskQueue != req.TaskQueue || !req.StageIn.VerifyChecksum || !req.StageIn.PreserveTimestamp || req.GPU.Job.UseGPU || req.Slurm.Job.Name != "slurm-script" {
		t.Fatalf("Python defaults missing: %+v", req)
	}
}

func TestFlatPythonGPUStage(t *testing.T) {
	var stage GPUStage
	if err := json.Unmarshal([]byte(`{"task_queue":"gpu-queue","image":"alpine:latest","cmd":["true"],"local_output_dir":"/output"}`), &stage); err != nil {
		t.Fatal(err)
	}
	if stage.TaskQueue != "gpu-queue" || stage.Job.Image != "alpine:latest" {
		t.Fatalf("flat GPU job not decoded: %+v", stage)
	}
}

func TestGPUContainerIDIsDockerSafe(t *testing.T) {
	id := gpuContainerID("work@site:run.1")
	if id != "rdjob_work_site_run_1" {
		t.Fatalf("unexpected GPU container ID %q", id)
	}
}

func TestGPUTimeoutDefaultsToPythonValue(t *testing.T) {
	if gpuTimeout(0) != 2*time.Hour {
		t.Fatalf("GPU timeout default = %s", gpuTimeout(0))
	}
}

func TestAllowedRootsResolveSymlinks(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "allowed")
	outside := filepath.Join(base, "outside")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(outside, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if withinRoots(filepath.Join(root, "link", "new-output"), []string{root}) {
		t.Fatal("symlink escaped allowed roots")
	}
	if !withinRoots(filepath.Join(root, "new-output"), []string{root}) {
		t.Fatal("legitimate new output rejected")
	}
}

func TestWorkflowRunsAllFiveStagesInOrder(t *testing.T) {
	req := pilotRequest()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	defer env.AssertExpectations(t)
	a := &Activities{}
	for _, entry := range []struct {
		name string
		fn   interface{}
	}{
		{"staged.SubmitTransfer", a.SubmitTransfer}, {"staged.PollTransfer", a.PollTransfer},
		{"staged.SubmitSlurm", a.SubmitSlurm}, {"staged.PollSlurm", a.PollSlurm},
		{"staged.PreflightGPU", a.PreflightGPU}, {"staged.SetupGPU", a.SetupGPU},
		{"staged.UploadGPU", a.UploadGPU}, {"staged.RunGPU", a.RunGPU},
		{"staged.DownloadGPU", a.DownloadGPU}, {"staged.CleanupGPU", a.CleanupGPU},
	} {
		env.RegisterActivityWithOptions(entry.fn, activity.RegisterOptions{Name: entry.name})
	}
	var order []string
	env.OnActivity("staged.SubmitTransfer", mock.Anything, mock.Anything).Return(func(_ context.Context, tr Transfer) (string, error) {
		order = append(order, tr.SubmissionID)
		return "task-" + tr.SubmissionID, nil
	}).Times(3)
	env.OnActivity("staged.PollTransfer", mock.Anything, mock.Anything).Return(func(_ context.Context, id string) (TransferStatus, error) {
		return TransferStatus{TaskID: id, Status: "SUCCEEDED"}, nil
	}).Times(3)
	env.OnActivity("staged.SubmitSlurm", mock.Anything, mock.Anything).Return(func(_ context.Context, _ SlurmJob) (string, error) { order = append(order, "slurm"); return "123", nil }).Once()
	env.OnActivity("staged.PollSlurm", mock.Anything, "123").Return(SlurmStatus{JobID: "123", Status: "COMPLETED"}, nil).Once()
	env.OnActivity("staged.PreflightGPU", mock.Anything, mock.Anything).Return(nil).Once()
	env.OnActivity("staged.SetupGPU", mock.Anything, mock.Anything).Return(nil).Once()
	env.OnActivity("staged.UploadGPU", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	env.OnActivity("staged.RunGPU", mock.Anything, mock.Anything, mock.Anything).Return(func(_ context.Context, _ string, _ GPUJob) (string, error) {
		order = append(order, "gpu")
		return "done", nil
	}).Once()
	env.OnActivity("staged.DownloadGPU", mock.Anything, mock.Anything, mock.Anything).Return(1, nil).Once()
	env.OnActivity("staged.CleanupGPU", mock.Anything, mock.Anything).Return(nil).Once()
	env.ExecuteWorkflow(Run, req)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result Result
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if !result.Success || result.SlurmJobID == nil || *result.SlurmJobID != 123 || result.GPUResult == nil || result.GPUResult.OutputItemCount != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if got := strings.Join(order, ","); got != "stage-in,slurm,stage-back,gpu,publish" {
		t.Fatalf("stage order = %q", got)
	}
}

func TestSlurmFailureRequestsCancellation(t *testing.T) {
	req := pilotRequest()
	req.StageIn, req.StageBack, req.GPU, req.Publish = nil, nil, nil, nil
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	a := &Activities{}
	for _, entry := range []struct {
		name string
		fn   interface{}
	}{
		{"staged.SubmitSlurm", a.SubmitSlurm},
		{"staged.PollSlurm", a.PollSlurm},
		{"staged.CancelSlurm", a.CancelSlurm},
	} {
		env.RegisterActivityWithOptions(entry.fn, activity.RegisterOptions{Name: entry.name})
	}
	env.OnActivity("staged.SubmitSlurm", mock.Anything, mock.Anything).Return("123", nil).Once()
	env.OnActivity("staged.PollSlurm", mock.Anything, "123").Return(SlurmStatus{JobID: "123", Status: "FAILED", ExitCode: 1, Logs: "failed"}, nil).Once()
	env.OnActivity("staged.CancelSlurm", mock.Anything, "123").Return(nil).Once()
	env.ExecuteWorkflow(Run, req)
	if err := env.GetWorkflowError(); err == nil || !strings.Contains(err.Error(), "Slurm job 123 ended FAILED") {
		t.Fatalf("unexpected error: %v", err)
	}
	env.AssertExpectations(t)
}

func TestTransferFailureStopsBeforeSlurm(t *testing.T) {
	req := pilotRequest()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	a := &Activities{}
	env.RegisterActivityWithOptions(a.SubmitTransfer, activity.RegisterOptions{Name: "staged.SubmitTransfer"})
	env.OnActivity("staged.SubmitTransfer", mock.Anything, mock.Anything).Return("", errors.New("endpoint denied")).Times(3)
	env.ExecuteWorkflow(Run, req)
	if err := env.GetWorkflowError(); err == nil || !strings.Contains(err.Error(), "endpoint denied") {
		t.Fatalf("unexpected error: %v", err)
	}
	env.AssertExpectations(t)
}
