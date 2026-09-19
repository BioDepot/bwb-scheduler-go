package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"go-scheduler/staged"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	temporalmocks "go.temporal.io/sdk/mocks"
)

type stagedEncodedStatus struct{ value staged.Status }

func (v stagedEncodedStatus) HasValue() bool            { return true }
func (v stagedEncodedStatus) Get(ptr interface{}) error { *ptr.(*staged.Status) = v.value; return nil }

func stagedRequestForHTTP() staged.Request {
	return staged.Request{
		WorkflowID: "test-staged-1", TaskQueue: "pilot-queue", GlobusTaskQueue: "pilot-queue",
		StageIn: &staged.Transfer{SourceEndpointID: "source-id", DestinationEndpointID: "dest-id", SubmissionID: "submission-1", Label: "test transfer", Items: []staged.TransferItem{{SourcePath: "/source/file", DestinationPath: "/dest/file"}}},
	}
}

func TestStagedStartRejectsInvalidPathBeforeTemporal(t *testing.T) {
	req := stagedRequestForHTTP()
	req.StageIn.Items[0].SourcePath = "/source/../secret"
	data, _ := json.Marshal(req)
	s := &Server{}
	w := httptest.NewRecorder()
	s.handleStartStagedWorkflow(w, httptest.NewRequest(http.MethodPost, "/start_staged_slurm_gpu_workflow", strings.NewReader(string(data))))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestStagedStartPreservesPythonResponseContract(t *testing.T) {
	req := stagedRequestForHTTP()
	data, _ := json.Marshal(req)
	hash := sha256.Sum256(data)
	fingerprint := hex.EncodeToString(hash[:])
	payload, err := converter.GetDefaultDataConverter().ToPayload(fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	clientMock := &temporalmocks.Client{}
	run := &temporalmocks.WorkflowRun{}
	run.On("GetID").Return(req.WorkflowID).Maybe()
	run.On("GetRunID").Return("run-1").Maybe()
	clientMock.On("ExecuteWorkflow", mock.Anything, mock.Anything, mock.Anything, req).Return(run, nil).Once()
	clientMock.On("DescribeWorkflowExecution", mock.Anything, req.WorkflowID, "run-1").Return(&workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: req.WorkflowID, RunId: "run-1"}, Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{"staged_request_fingerprint": payload}}},
	}, nil).Once()
	s := &Server{temporalClient: clientMock}
	w := httptest.NewRecorder()
	s.handleStartStagedWorkflow(w, httptest.NewRequest(http.MethodPost, "/start_staged_slurm_gpu_workflow", strings.NewReader(string(data))))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var response map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response["workflow_id"] != req.WorkflowID || response["run_id"] != "run-1" || response["task_queue"] != req.TaskQueue {
		t.Fatalf("unexpected response: %v", response)
	}
	clientMock.AssertExpectations(t)
}

func TestStagedStatusUsesPythonNames(t *testing.T) {
	clientMock := &temporalmocks.Client{}
	clientMock.On("DescribeWorkflowExecution", mock.Anything, "test-staged-1", "run-1").Return(&workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: "test-staged-1", RunId: "run-1"}, Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
	}, nil).Once()
	query := stagedEncodedStatus{value: staged.Status{WorkflowStatus: "Started", CurrentStage: "stage_in", Stages: map[string]staged.StageState{"stage_in": {State: "RUNNING", TaskID: "task-1"}}, Artifacts: map[string]string{"stage_in_task_id": "task-1"}}}
	clientMock.On("QueryWorkflow", mock.Anything, "test-staged-1", "run-1", "get_status").Return(query, nil).Once()
	s := &Server{temporalClient: clientMock}
	w := httptest.NewRecorder()
	s.handleStagedWorkflowStatus(w, httptest.NewRequest(http.MethodPost, "/staged_slurm_gpu_workflow_status", strings.NewReader(`{"workflow_id":"test-staged-1","run_id":"run-1"}`)))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response["workflow_status"] != "Started" || response["current_stage"] != "stage_in" || response["workflow_type"] != "StagedSlurmGpuWorkflow" {
		t.Fatalf("unexpected status: %v", response)
	}
	clientMock.AssertExpectations(t)
}
