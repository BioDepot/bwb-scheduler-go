package workflow

import (
	"errors"
	"fmt"
	"go-scheduler/parsing"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// startActivityMatcher returns a mock.MatchedBy predicate that accepts any
// call to StartRemoteSlurmJobActivity whose first two arguments match the
// given cmd and config. The remaining arguments (fs, slurmDir, imageDir) are
// not inspected because their values are derived from SlurmState internals
// that are irrelevant to the workflow-level tests.
func startActivityMatcher(expCmd parsing.CmdTemplate, expConfig parsing.SlurmJobConfig) interface{} {
	return mock.MatchedBy(func(args []interface{}) bool {
		if len(args) < 2 {
			return false
		}
		cmd, ok1 := args[0].(parsing.CmdTemplate)
		cfg, ok2 := args[1].(parsing.SlurmJobConfig)
		return ok1 && ok2 && cmd.Id == expCmd.Id && cfg.Mem == expConfig.Mem
	})
}

func TestCancelSlurmJobsRecoversManifestAndLeavesForeignJobsAlone(t *testing.T) {
	active := map[string]string{
		"123": "morphic-requested",
		"456": "morphic-recovered",
		"999": "someone-else",
	}
	var cancelCommands []string
	runCmd := func(cmd string) (CmdOut, error) {
		switch {
		case strings.Contains(cmd, "submissions.tsv"):
			return CmdOut{StdOut: strings.Join([]string{
				"morphic-requested\t123\thash-a\trequest-a\tworkflow-a\trun-a\texecutor-a\tprofile-a\t2026-09-18T00:00:00Z",
				"morphic-recovered\t456\thash-b\trequest-b\tworkflow-b\trun-b\texecutor-b\tprofile-b\t2026-09-18T00:00:01Z",
			}, "\n") + "\n"}, nil
		case strings.HasPrefix(cmd, "squeue "):
			var lines []string
			for jobID, name := range active {
				lines = append(lines, jobID+"|"+name)
			}
			sort.Strings(lines)
			return CmdOut{StdOut: strings.Join(lines, "\n") + "\n"}, nil
		case strings.HasPrefix(cmd, "scancel "):
			cancelCommands = append(cancelCommands, cmd)
			for jobID := range active {
				if strings.Contains(cmd, "'"+jobID+"'") {
					delete(active, jobID)
				}
			}
			return CmdOut{}, nil
		default:
			return CmdOut{}, fmt.Errorf("unexpected command %q", cmd)
		}
	}

	evidence, err := cancelSlurmJobs(SlurmCancellationRequest{
		JobIDs:          []string{"123"},
		CorrelationKeys: []string{"morphic-requested", "morphic-recovered"},
		ManifestPath:    "/srv/slurm_mnt/slurm/submissions.tsv",
		User:            "tutorial",
	}, runCmd, func(time.Duration) {}, 5)
	if err != nil {
		t.Fatalf("cancelSlurmJobs returned error: %v", err)
	}
	if !evidence.Verified || evidence.CleanupStatus != "verified" {
		t.Fatalf("unexpected cancellation evidence: %#v", evidence)
	}
	require.Equal(t, []string{"456"}, evidence.RecoveredJobIDs)
	require.Equal(t, []string{"123", "456"}, evidence.VerifiedTerminalIDs)
	if _, ok := active["999"]; !ok {
		t.Fatal("foreign Slurm job was canceled")
	}
	for _, cmd := range cancelCommands {
		if strings.Contains(cmd, "999") {
			t.Fatalf("foreign Slurm job appeared in scancel command %q", cmd)
		}
	}
}

func TestCancelSlurmJobsRejectsInvalidJobIDBeforeRemoteCommand(t *testing.T) {
	called := false
	_, err := cancelSlurmJobs(SlurmCancellationRequest{
		JobIDs: []string{"123; scancel 999"},
		User:   "tutorial",
	}, func(string) (CmdOut, error) {
		called = true
		return CmdOut{}, nil
	}, func(time.Duration) {}, 1)
	if err == nil || !strings.Contains(err.Error(), "invalid Slurm job ID") {
		t.Fatalf("expected invalid job ID error, got %v", err)
	}
	if called {
		t.Fatal("remote command ran before Slurm job ID validation")
	}
}

func TestSlurmPollerCancellationRunsDisconnectedCleanup(t *testing.T) {
	var a SlurmActivity
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterActivity(a.CancelRemoteSlurmJobsActivity)
	expectedEvidence := SlurmCancellationEvidence{
		RequestedJobIDs:     []string{"123"},
		CorrelationKeys:     []string{"morphic-test"},
		VerifiedTerminalIDs: []string{"123"},
		CleanupStatus:       "verified",
		Verified:            true,
	}
	env.OnActivity(
		a.CancelRemoteSlurmJobsActivity,
		mock.MatchedBy(func(request SlurmCancellationRequest) bool {
			return request.User == "tutorial" &&
				reflect.DeepEqual([]string{"123"}, request.JobIDs) &&
				reflect.DeepEqual([]string{"morphic-test"}, request.CorrelationKeys)
		}),
	).Return(expectedEvidence, nil).Once()
	env.RegisterDelayedCallback(env.CancelWorkflow, time.Second)
	env.ExecuteWorkflow(SlurmPollerWorkflow, SlurmState{
		ParentWfId: "parent",
		SlurmConfig: parsing.SshConfig{
			User:     "tutorial",
			SchedDir: "/srv/slurm_mnt",
		},
		RunningJobs: map[string]SlurmJob{
			"123": {JobId: "123", CorrelationKey: "morphic-test"},
		},
		Requests: map[int]SlurmRequest{
			1: {CorrelationKey: "morphic-test"},
		},
	})

	err := env.GetWorkflowError()
	if !temporal.IsCanceledError(err) {
		t.Fatalf("workflow did not close as canceled: %v", err)
	}
	var canceledErr *temporal.CanceledError
	if !errors.As(err, &canceledErr) || !canceledErr.HasDetails() {
		t.Fatalf("cancellation evidence was not attached: %v", err)
	}
	var actual SlurmCancellationEvidence
	if err := canceledErr.Details(&actual); err != nil {
		t.Fatalf("failed decoding cancellation evidence: %v", err)
	}
	require.Equal(t, expectedEvidence.RequestedJobIDs, actual.RequestedJobIDs)
	require.Equal(t, expectedEvidence.CorrelationKeys, actual.CorrelationKeys)
	require.Equal(t, expectedEvidence.VerifiedTerminalIDs, actual.VerifiedTerminalIDs)
	require.Equal(t, expectedEvidence.CleanupStatus, actual.CleanupStatus)
	require.Equal(t, expectedEvidence.Verified, actual.Verified)
	env.AssertExpectations(t)
}

// Test the lifecycle of a successful SLURM job.
//  1. The parent workflow submits a job to the slurm poller child WF.
//  2. The child poller WF submits that job to the SLURM cluster.
//  3. The child poller WF polls SLURM.
//  4. The child poller WF gets the job outputs *only after* the job
//     registers as COMPLETED (not when it is RUNNING or PENDING).
//  5. The child poller waits 1 cycle to verify the job still lists as
//     COMPLETED; this is a bug in some SLURM systems where a job briefly
//     lists as COMPLETED right after starting before showing up as RUNNING.
//  6. The child poller WF signals the outputs to the caller WF.
func TestSlurmResponse(t *testing.T) {
	var a SlurmActivity
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterActivity(a.StartRemoteSlurmJobActivity)
	env.RegisterActivity(a.PollRemoteSlurmActivity)
	env.RegisterActivity(a.GetRemoteSlurmJobOutputsActivity)

	jobId := "2718281828459"
	cmdId := 1

	memStr := "40G"
	expConfig := parsing.SlurmJobConfig{Mem: &memStr}
	expCmd := parsing.CmdRunParams{Cmd: parsing.CmdTemplate{Id: cmdId}}
	expSlurmJob := SlurmJob{CmdId: cmdId, JobId: jobId}

	// StartRemoteSlurmJobActivity receives cmd, jobConfig, correlation key,
	// filesystem, Slurm directory, image directory, and execution identity.
	env.OnActivity(
		a.StartRemoteSlurmJobActivity,
		mock.Anything, expCmd, expConfig, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return(expSlurmJob, nil).Once()

	// Handle workflow polling before request, which should be empty.
	env.OnActivity(a.PollRemoteSlurmActivity, []string{}).Return(nil, nil)

	stateBeforePoll := ""
	stateAfterPoll := "PENDING"
	env.OnActivity(a.PollRemoteSlurmActivity, []string{jobId}).
		Return(func(jobIds []string) (map[string]SacctResult, error) {
			stateBeforePoll = stateAfterPoll
			switch stateAfterPoll {
			case "PENDING":
				stateAfterPoll = "RUNNING"
			case "RUNNING":
				stateAfterPoll = "COMPLETED"
			case "COMPLETED":
				stateAfterPoll = "COMPLETED"
			}
			return map[string]SacctResult{jobId: {JobId: jobId, State: stateBeforePoll}}, nil
		})

	expCmdOutput := CmdOutput{Id: cmdId, StdOut: "stdout", StdErr: "stderr"}
	env.OnActivity(a.GetRemoteSlurmJobOutputsActivity, []SlurmJob{expSlurmJob}).
		Return([]CmdOutput{expCmdOutput}, nil)

	expParentWfId := "parentId"
	expParentWfRunId := "parentRunId"
	env.OnSignalExternalWorkflow(
		mock.Anything, expParentWfId, "", "slurm-response",
		mock.MatchedBy(func(arg interface{}) bool {
			require.True(t, stateBeforePoll == "COMPLETED")
			resp, ok := arg.(SlurmResponse)
			return ok && resp.Result.Id == expCmdOutput.Id &&
				resp.Result.StdOut == expCmdOutput.StdOut &&
				resp.Result.StdErr == expCmdOutput.StdErr
		}),
	).Return(nil).Once()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("slurm-request", SlurmRequest{Cmd: expCmd, Config: expConfig})
	}, 0)
	env.RegisterDelayedCallback(func() {
		env.SetCurrentHistoryLength(9500)
	}, 30*time.Second)
	env.ExecuteWorkflow(SlurmPollerWorkflow, SlurmState{
		ParentWfId:    expParentWfId,
		ParentWfRunId: expParentWfRunId,
	})

	env.AssertExpectations(t)
	require.True(t, env.IsWorkflowCompleted())
}

// Test that, after workflow continues-as-new, it retains its
// list of outstanding jobs and continues polling for them,
// eventually sending signal back to calling workflow.
func TestSlurmContinueAsNewStateMaintenance(t *testing.T) {
	var a SlurmActivity
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterActivity(a.StartRemoteSlurmJobActivity)
	env.RegisterActivity(a.PollRemoteSlurmActivity)
	env.RegisterActivity(a.GetRemoteSlurmJobOutputsActivity)

	jobId := "2718281828459"
	cmdId := 1

	memStr := "40G"
	expConfig := parsing.SlurmJobConfig{Mem: &memStr}
	expCmd := parsing.CmdRunParams{Cmd: parsing.CmdTemplate{Id: cmdId}}
	expSlurmJob := SlurmJob{CmdId: cmdId, JobId: jobId}

	env.OnActivity(
		a.StartRemoteSlurmJobActivity,
		mock.Anything, expCmd, expConfig, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return(expSlurmJob, nil).Once()

	// Handle workflow polling before request, which should be empty.
	env.OnActivity(a.PollRemoteSlurmActivity, []string{}).Return(nil, nil).Maybe()

	env.OnActivity(a.PollRemoteSlurmActivity, []string{jobId}).
		Return(func(jobIds []string) (map[string]SacctResult, error) {
			return map[string]SacctResult{jobId: {JobId: jobId, State: "RUNNING"}}, nil
		})

	expParentWfId := "parentId"

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("slurm-request", SlurmRequest{Cmd: expCmd, Config: expConfig})
	}, 0)
	env.RegisterDelayedCallback(func() {
		env.SetCurrentHistoryLength(9500)
	}, 30*time.Second)
	env.ExecuteWorkflow(SlurmPollerWorkflow, SlurmState{
		ParentWfId:    expParentWfId,
		ParentWfRunId: "",
	})
	env.AssertExpectations(t)
	err := env.GetWorkflowError()

	var caeErr *workflow.ContinueAsNewError
	if ok := errors.As(err, &caeErr); !ok {
		t.Fatalf("failed to gen continue-as-new error")
	}

	var continuedWfState SlurmState
	if err := converter.GetDefaultDataConverter().FromPayloads(caeErr.Input, &continuedWfState); err != nil {
		t.Fatalf("failed to convert continue-as-new input")
	}

	contEnv := testSuite.NewTestWorkflowEnvironment()
	contEnv.OnActivity(a.PollRemoteSlurmActivity, []string{jobId}).
		Return(func(jobIds []string) (map[string]SacctResult, error) {
			return map[string]SacctResult{jobId: {JobId: jobId, State: "COMPLETED"}}, nil
		})

	expCmdOutput := CmdOutput{Id: cmdId, StdOut: "stdout", StdErr: "stderr"}
	contEnv.OnActivity(a.PollRemoteSlurmActivity, []string{}).Return(nil, nil)
	contEnv.OnActivity(a.GetRemoteSlurmJobOutputsActivity, []SlurmJob{expSlurmJob}).
		Return([]CmdOutput{expCmdOutput}, nil)

	contEnv.OnSignalExternalWorkflow(
		mock.Anything, expParentWfId, "", "slurm-response",
		mock.MatchedBy(func(arg interface{}) bool {
			resp, ok := arg.(SlurmResponse)
			return ok && resp.Result.Id == expCmdOutput.Id &&
				resp.Result.StdOut == expCmdOutput.StdOut &&
				resp.Result.StdErr == expCmdOutput.StdErr
		}),
	).Return(nil).Once()
	contEnv.RegisterDelayedCallback(func() {
		contEnv.SetCurrentHistoryLength(9500)
	}, 30*time.Second)
	contEnv.ExecuteWorkflow(SlurmPollerWorkflow, continuedWfState)

	contEnv.AssertExpectations(t)
	require.True(t, env.IsWorkflowCompleted())
}

func TestSlurmContinueAsNewCancellationUsesOriginalJob(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	first := testSuite.NewTestWorkflowEnvironment()
	initial := SlurmState{
		ParentWfId:         "parent",
		ContinueAsNewAfter: 1,
		SlurmConfig: parsing.SshConfig{
			User:     "tutorial",
			SchedDir: "/srv/slurm_mnt",
		},
		RunningJobs: map[string]SlurmJob{
			"123": {JobId: "123", CorrelationKey: "morphic-original"},
		},
		Requests: map[int]SlurmRequest{
			1: {CorrelationKey: "morphic-original", Sequence: 1},
		},
		SubmissionSequence: 1,
		JobEvidence: map[string]SlurmJobEvidence{
			"123": {JobID: "123", CommandID: 1, Attempt: 1, CorrelationKey: "morphic-original"},
		},
	}
	first.SetCurrentHistoryLength(2)
	first.ExecuteWorkflow(SlurmPollerWorkflow, initial)
	var continuedErr *workflow.ContinueAsNewError
	if !errors.As(first.GetWorkflowError(), &continuedErr) {
		t.Fatalf("first run did not continue as new: %v", first.GetWorkflowError())
	}
	var continued SlurmState
	if err := converter.GetDefaultDataConverter().FromPayloads(continuedErr.Input, &continued); err != nil {
		t.Fatal(err)
	}
	if continued.SubmissionSequence != 1 || continued.RunningJobs["123"].JobId != "123" {
		t.Fatalf("active job state did not survive continue-as-new: %#v", continued)
	}

	var a SlurmActivity
	second := testSuite.NewTestWorkflowEnvironment()
	second.RegisterActivity(a.CancelRemoteSlurmJobsActivity)
	second.OnActivity(
		a.CancelRemoteSlurmJobsActivity,
		mock.MatchedBy(func(request SlurmCancellationRequest) bool {
			return reflect.DeepEqual(request.JobIDs, []string{"123"}) &&
				strings.Contains(strings.Join(request.CorrelationKeys, ","), "morphic-original")
		}),
	).Return(SlurmCancellationEvidence{
		RequestedJobIDs: []string{"123"}, VerifiedTerminalIDs: []string{"123"},
		CleanupStatus: "verified", Verified: true,
	}, nil).Once()
	second.RegisterDelayedCallback(second.CancelWorkflow, time.Second)
	continued.ContinueAsNewAfter = 9000
	second.ExecuteWorkflow(SlurmPollerWorkflow, continued)
	if !temporal.IsCanceledError(second.GetWorkflowError()) {
		t.Fatalf("continued run did not cancel cleanly: %v", second.GetWorkflowError())
	}
	second.AssertExpectations(t)
}

// If a slurm job fails fatally (i.e. FAILED or CANCELLED), it
// should immediately send notice of the job failure to the
// calling workflow without any retries.
func TestSlurmJobFatalFailure(t *testing.T) {
	var a SlurmActivity
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterActivity(a.StartRemoteSlurmJobActivity)
	env.RegisterActivity(a.PollRemoteSlurmActivity)
	env.RegisterActivity(a.GetRemoteSlurmJobOutputsActivity)

	jobId := "2718281828459"
	cmdId := 1

	memStr := "40G"
	maxRetries := 2
	expConfig := parsing.SlurmJobConfig{Mem: &memStr, MaxRetries: &maxRetries}
	expCmd := parsing.CmdRunParams{Cmd: parsing.CmdTemplate{Id: cmdId}}
	expSlurmJob := SlurmJob{CmdId: cmdId, JobId: jobId}

	env.OnActivity(
		a.StartRemoteSlurmJobActivity,
		mock.Anything, expCmd, expConfig, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return(expSlurmJob, nil).Once()

	// Handle workflow polling before request, which should be empty.
	env.OnActivity(a.PollRemoteSlurmActivity, []string{}).Return(nil, nil).Maybe()

	env.OnActivity(a.PollRemoteSlurmActivity, []string{jobId}).
		Return(map[string]SacctResult{jobId: {JobId: jobId, State: "FAILED"}}, nil)

	expCmdOutput := CmdOutput{Id: cmdId, StdOut: "stdout", StdErr: "stderr"}
	env.OnActivity(a.GetRemoteSlurmJobOutputsActivity, []SlurmJob{expSlurmJob}).
		Return([]CmdOutput{expCmdOutput}, nil)

	expParentWfId := "parentId"
	expParentWfRunId := "parentRunId"
	env.OnSignalExternalWorkflow(
		mock.Anything, expParentWfId, "", "slurm-response",
		mock.MatchedBy(func(arg interface{}) bool {
			resp, ok := arg.(SlurmResponse)
			return ok && resp.Error != nil && resp.Result.Id == expCmdOutput.Id &&
				resp.Result.StdOut == expCmdOutput.StdOut &&
				resp.Result.StdErr == expCmdOutput.StdErr
		}),
	).Return(nil).Once()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("slurm-request", SlurmRequest{Cmd: expCmd, Config: expConfig})
	}, 0)
	env.RegisterDelayedCallback(func() {
		env.SetCurrentHistoryLength(9500)
	}, 90*time.Second)
	env.ExecuteWorkflow(SlurmPollerWorkflow, SlurmState{
		ParentWfId:    expParentWfId,
		ParentWfRunId: expParentWfRunId,
	})

	env.AssertExpectations(t)
	require.True(t, env.IsWorkflowCompleted())
}

// If a SLURM job fails more than the max number of retries, even
// if those failures are non-fatal, it should send a signal to the
// calling workflow indicating failure.
func TestSlurmJobNonFatalFailure(t *testing.T) {
	var a SlurmActivity
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterActivity(a.StartRemoteSlurmJobActivity)
	env.RegisterActivity(a.PollRemoteSlurmActivity)
	env.RegisterActivity(a.GetRemoteSlurmJobOutputsActivity)

	jobId := "2718281828459"
	cmdId := 1

	memStr := "40G"
	maxRetries := 2
	expConfig := parsing.SlurmJobConfig{Mem: &memStr, MaxRetries: &maxRetries}
	expCmd := parsing.CmdRunParams{Cmd: parsing.CmdTemplate{Id: cmdId}}
	expSlurmJob := SlurmJob{CmdId: cmdId, JobId: jobId}

	env.OnActivity(
		a.StartRemoteSlurmJobActivity,
		mock.Anything, expCmd, expConfig, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return(expSlurmJob, nil).Times(maxRetries + 1)

	// Handle workflow polling before request, which should be empty.
	env.OnActivity(a.PollRemoteSlurmActivity, []string{}).Return(nil, nil).Maybe()

	env.OnActivity(a.PollRemoteSlurmActivity, []string{jobId}).
		Return(map[string]SacctResult{jobId: {JobId: jobId, State: "PREEMPTED"}}, nil)

	expCmdOutput := CmdOutput{Id: cmdId, StdOut: "stdout", StdErr: "stderr"}
	env.OnActivity(a.GetRemoteSlurmJobOutputsActivity, []SlurmJob{expSlurmJob}).
		Return([]CmdOutput{expCmdOutput}, nil)

	expParentWfId := "parentId"
	expParentWfRunId := "parentRunId"
	env.OnSignalExternalWorkflow(
		mock.Anything, expParentWfId, "", "slurm-response",
		mock.MatchedBy(func(arg interface{}) bool {
			resp, ok := arg.(SlurmResponse)
			return ok && resp.Error != nil && resp.Result.Id == expCmdOutput.Id &&
				resp.Result.StdOut == expCmdOutput.StdOut &&
				resp.Result.StdErr == expCmdOutput.StdErr
		}),
	).Return(nil).Once()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("slurm-request", SlurmRequest{Cmd: expCmd, Config: expConfig})
	}, 0)
	env.RegisterDelayedCallback(func() {
		env.SetCurrentHistoryLength(9500)
	}, 90*time.Second)
	env.ExecuteWorkflow(SlurmPollerWorkflow, SlurmState{
		ParentWfId:    expParentWfId,
		ParentWfRunId: expParentWfRunId,
	})

	env.AssertExpectations(t)
	require.True(t, env.IsWorkflowCompleted())
}

// If a SLURM job fails repeatedly but succeeds after retry,
// it should send a successful completion message to the caller
// workflow.
func TestSlurmJobRetryOnNonFatalErr(t *testing.T) {
	var a SlurmActivity
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	env.RegisterActivity(a.StartRemoteSlurmJobActivity)
	env.RegisterActivity(a.PollRemoteSlurmActivity)
	env.RegisterActivity(a.GetRemoteSlurmJobOutputsActivity)

	jobId := "2718281828459"
	cmdId := 1

	memStr := "40G"
	maxRetries := 2
	expConfig := parsing.SlurmJobConfig{Mem: &memStr, MaxRetries: &maxRetries}
	expCmd := parsing.CmdRunParams{Cmd: parsing.CmdTemplate{Id: cmdId}}
	expSlurmJob := SlurmJob{CmdId: cmdId, JobId: jobId}

	env.OnActivity(
		a.StartRemoteSlurmJobActivity,
		mock.Anything, expCmd, expConfig, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return(expSlurmJob, nil).Times(maxRetries + 1)

	// Handle workflow polling before request, which should be empty.
	env.OnActivity(a.PollRemoteSlurmActivity, []string{}).Return(nil, nil).Maybe()

	currTries := 0
	env.OnActivity(a.PollRemoteSlurmActivity, []string{jobId}).
		Return(func(jobIds []string) (map[string]SacctResult, error) {
			if currTries == maxRetries {
				return map[string]SacctResult{jobId: {JobId: jobId, State: "COMPLETED"}}, nil
			}
			currTries++
			return map[string]SacctResult{jobId: {JobId: jobId, State: "PREEMPTED"}}, nil
		})

	expCmdOutput := CmdOutput{Id: cmdId, StdOut: "stdout", StdErr: "stderr"}
	env.OnActivity(a.GetRemoteSlurmJobOutputsActivity, []SlurmJob{expSlurmJob}).
		Return([]CmdOutput{expCmdOutput}, nil)

	expParentWfId := "parentId"
	expParentWfRunId := "parentRunId"
	env.OnSignalExternalWorkflow(
		mock.Anything, expParentWfId, "", "slurm-response",
		mock.MatchedBy(func(arg interface{}) bool {
			resp, ok := arg.(SlurmResponse)
			return ok && resp.Error == nil && resp.Result.Id == expCmdOutput.Id &&
				resp.Result.StdOut == expCmdOutput.StdOut &&
				resp.Result.StdErr == expCmdOutput.StdErr
		}),
	).Return(nil).Once()

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("slurm-request", SlurmRequest{Cmd: expCmd, Config: expConfig})
	}, 0)
	env.RegisterDelayedCallback(func() {
		env.SetCurrentHistoryLength(9500)
	}, 90*time.Second)
	env.ExecuteWorkflow(SlurmPollerWorkflow, SlurmState{
		ParentWfId:    expParentWfId,
		ParentWfRunId: expParentWfRunId,
	})

	env.AssertExpectations(t)
	require.True(t, env.IsWorkflowCompleted())
}

// simulateScriptOutput produces the stdout that the gather script would emit
// for the given jobs and canned data, using the specified token. This lets
// tests exercise parseGatherOutput without needing a real SSH connection.
// data[i] corresponds to jobs[i].
type fakeJobData struct {
	stdout        string
	stderr        string
	outputFiles   map[string]string
	stdoutMissing bool
	stderrMissing bool
}

func simulateScriptOutput(jobs []SlurmJob, data []fakeJobData, token string) string {
	var b strings.Builder
	for i, job := range jobs {
		_ = job
		d := data[i]

		fmt.Fprintf(&b, "\n%s:%d:%s:\n", token, i, sectionStdout)
		if d.stdoutMissing {
			b.WriteString(missingFileMarker)
		} else {
			b.WriteString(d.stdout)
		}

		fmt.Fprintf(&b, "\n%s:%d:%s:\n", token, i, sectionStderr)
		if d.stderrMissing {
			b.WriteString(missingFileMarker)
		} else {
			b.WriteString(d.stderr)
		}

		for fname, content := range d.outputFiles {
			fmt.Fprintf(&b, "\n%s:%d:%s:%s\n", token, i, sectionOutputFile, fname)
			b.WriteString(content)
		}
	}
	return b.String()
}

func TestShellQuote_SimplePath(t *testing.T) {
	got := shellQuote("/ocean/projects/sched/job1.out")
	want := "'/ocean/projects/sched/job1.out'"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestShellQuote_PathWithSpaces(t *testing.T) {
	got := shellQuote("/path with spaces/file")
	want := "'/path with spaces/file'"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestShellQuote_SingleQuoteInPath(t *testing.T) {
	got := shellQuote("/path/it's/here")
	want := "'/path/it'\\''s/here'"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestShellQuote_MultipleSingleQuotes(t *testing.T) {
	got := shellQuote("a'b'c")
	want := "'a'\\''b'\\''c'"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestShellQuote_EmptyString(t *testing.T) {
	got := shellQuote("")
	want := "''"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestBuildGatherScript_TokenEmbedded(t *testing.T) {
	token := "cafebabe12345678"
	jobs := []SlurmJob{{
		CmdId: 0, JobId: "j0",
		OutPath: "/s/j0.out", ErrPath: "/s/j0.err",
		TmpOutputHostPath: "/s/j0", SbatchPath: "/s/j0.sbatch",
	}}
	script := buildGatherScript(jobs, token)
	if !strings.Contains(script, token) {
		t.Error("token not embedded in script")
	}
}

func TestBuildGatherScript_AllPathsPresent(t *testing.T) {
	token := "tok"
	job := SlurmJob{
		CmdId: 1, JobId: "j1",
		OutPath: "/s/j1.out", ErrPath: "/s/j1.err",
		TmpOutputHostPath: "/s/j1tmp", SbatchPath: "/s/j1.sbatch",
	}
	script := buildGatherScript([]SlurmJob{job}, token)
	for _, path := range []string{
		job.OutPath, job.ErrPath, job.TmpOutputHostPath, job.SbatchPath,
	} {
		if !strings.Contains(script, path) {
			t.Errorf("script missing path %s", path)
		}
	}
}

func TestBuildGatherScript_CleanupRmRfPresent(t *testing.T) {
	token := "tok"
	job := SlurmJob{
		CmdId: 0, JobId: "j0",
		OutPath: "/s/j0.out", ErrPath: "/s/j0.err",
		TmpOutputHostPath: "/s/j0tmp", SbatchPath: "/s/j0.sbatch",
	}
	script := buildGatherScript([]SlurmJob{job}, token)
	if !strings.Contains(script, "rm -rf") {
		t.Error("script does not contain rm -rf for cleanup")
	}
	// All four artefacts must appear after rm -rf on the same logical line.
	rmIdx := strings.Index(script, "rm -rf")
	rmLine := script[rmIdx:]
	if nl := strings.IndexByte(rmLine, '\n'); nl >= 0 {
		rmLine = rmLine[:nl]
	}
	for _, path := range []string{job.OutPath, job.ErrPath, job.TmpOutputHostPath, job.SbatchPath} {
		if !strings.Contains(rmLine, path) {
			t.Errorf("rm -rf line missing %s; line: %s", path, rmLine)
		}
	}
}

func TestBuildGatherScript_CleanupAfterReads(t *testing.T) {
	// The rm -rf must come after the await_and_cat calls for the same job,
	// so output is already captured before deletion.
	token := "tok"
	job := SlurmJob{
		CmdId: 0, JobId: "j0",
		OutPath: "/s/j0.out", ErrPath: "/s/j0.err",
		TmpOutputHostPath: "/s/j0tmp", SbatchPath: "/s/j0.sbatch",
	}
	script := buildGatherScript([]SlurmJob{job}, token)
	lastCatIdx := strings.LastIndex(script, "await_and_cat")
	rmIdx := strings.Index(script, "rm -rf")
	if rmIdx < lastCatIdx {
		t.Error("rm -rf appears before the last await_and_cat; cleanup would delete files before reading them")
	}
}

func TestBuildGatherScript_MultipleJobs_AllPathsPresent(t *testing.T) {
	token := "tok"
	jobs := []SlurmJob{
		{CmdId: 0, OutPath: "/s/0.out", ErrPath: "/s/0.err", TmpOutputHostPath: "/s/0", SbatchPath: "/s/0.sbatch"},
		{CmdId: 1, OutPath: "/s/1.out", ErrPath: "/s/1.err", TmpOutputHostPath: "/s/1", SbatchPath: "/s/1.sbatch"},
		{CmdId: 2, OutPath: "/s/2.out", ErrPath: "/s/2.err", TmpOutputHostPath: "/s/2", SbatchPath: "/s/2.sbatch"},
	}
	script := buildGatherScript(jobs, token)
	for _, j := range jobs {
		for _, path := range []string{j.OutPath, j.ErrPath, j.TmpOutputHostPath, j.SbatchPath} {
			if !strings.Contains(script, path) {
				t.Errorf("script missing path %s", path)
			}
		}
	}
	if count := strings.Count(script, "rm -rf"); count != 3 {
		t.Errorf("expected 3 rm -rf lines, got %d", count)
	}
}

func TestBuildGatherScript_SingleQuoteInPath_Escaped(t *testing.T) {
	token := "tok"
	job := SlurmJob{
		CmdId: 0, JobId: "j0",
		OutPath: "/it's/j0.out", ErrPath: "/it's/j0.err",
		TmpOutputHostPath: "/it's/j0", SbatchPath: "/it's/j0.sbatch",
	}
	script := buildGatherScript([]SlurmJob{job}, token)
	if !strings.Contains(script, "'\\''") {
		t.Error("single quotes in paths were not escaped")
	}
}

func TestBuildGatherScript_AwaitAndCatDefined(t *testing.T) {
	script := buildGatherScript([]SlurmJob{{OutPath: "/a", ErrPath: "/b", TmpOutputHostPath: "/c", SbatchPath: "/d"}}, "tok")
	if !strings.Contains(script, "await_and_cat()") {
		t.Error("await_and_cat function not defined in script")
	}
	if !strings.Contains(script, "sleep 5") {
		t.Error("grace-period sleep not present in await_and_cat")
	}
}

func TestBuildGatherScript_SectionMarkersPresent(t *testing.T) {
	token := "tok"
	job := SlurmJob{OutPath: "/a.out", ErrPath: "/a.err", TmpOutputHostPath: "/tmp/a", SbatchPath: "/a.sbatch"}
	script := buildGatherScript([]SlurmJob{job}, token)
	for _, section := range []string{sectionStdout, sectionStderr, sectionOutputFile} {
		if !strings.Contains(script, section) {
			t.Errorf("section marker %q missing from script", section)
		}
	}
}

// --- parseGatherOutput ---

func TestParseGatherOutput_SingleJob_BasicContents(t *testing.T) {
	token := "aaaa000000000000"
	jobs := []SlurmJob{{CmdId: 5, JobId: "j5", OutPath: "/s/j5.out", ErrPath: "/s/j5.err", TmpOutputHostPath: "/s/j5"}}
	data := []fakeJobData{{
		stdout:      "hello stdout\n",
		stderr:      "hello stderr\n",
		outputFiles: map[string]string{"result.txt": "result content\n"},
	}}
	raw := simulateScriptOutput(jobs, data, token)
	results, err := parseGatherOutput(raw, jobs, token)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	entry := results[5]
	if entry.stdOut != "hello stdout\n" {
		t.Errorf("stdOut = %q, want %q", entry.stdOut, "hello stdout\n")
	}
	if entry.stdErr != "hello stderr\n" {
		t.Errorf("stdErr = %q, want %q", entry.stdErr, "hello stderr\n")
	}
	if entry.outputFiles["result.txt"] != "result content\n" {
		t.Errorf("result.txt = %q, want %q", entry.outputFiles["result.txt"], "result content\n")
	}
}

func TestParseGatherOutput_SingleJob_NoOutputFiles(t *testing.T) {
	token := "bbbb111111111111"
	jobs := []SlurmJob{{CmdId: 1, JobId: "j1", OutPath: "/s/j1.out", ErrPath: "/s/j1.err", TmpOutputHostPath: "/s/j1"}}
	data := []fakeJobData{{stdout: "out\n", stderr: "err\n", outputFiles: map[string]string{}}}
	raw := simulateScriptOutput(jobs, data, token)
	results, err := parseGatherOutput(raw, jobs, token)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results[1].outputFiles) != 0 {
		t.Errorf("expected no output files, got %v", results[1].outputFiles)
	}
}

func TestParseGatherOutput_SingleJob_MultipleOutputFiles(t *testing.T) {
	token := "cccc222222222222"
	jobs := []SlurmJob{{CmdId: 2, JobId: "j2", OutPath: "/s/j2.out", ErrPath: "/s/j2.err", TmpOutputHostPath: "/s/j2"}}
	data := []fakeJobData{{
		stdout: "out\n",
		stderr: "err\n",
		outputFiles: map[string]string{
			"a.txt": "aaa",
			"b.txt": "bbb",
			"c.txt": "ccc",
		},
	}}
	raw := simulateScriptOutput(jobs, data, token)
	results, err := parseGatherOutput(raw, jobs, token)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for fname, want := range data[0].outputFiles {
		if got := results[2].outputFiles[fname]; got != want {
			t.Errorf("outputFiles[%q] = %q, want %q", fname, got, want)
		}
	}
}

func TestParseGatherOutput_MultipleJobs(t *testing.T) {
	token := "dddd333333333333"
	jobs := []SlurmJob{
		{CmdId: 10, JobId: "ja", OutPath: "/s/ja.out", ErrPath: "/s/ja.err", TmpOutputHostPath: "/s/ja"},
		{CmdId: 20, JobId: "jb", OutPath: "/s/jb.out", ErrPath: "/s/jb.err", TmpOutputHostPath: "/s/jb"},
		{CmdId: 30, JobId: "jc", OutPath: "/s/jc.out", ErrPath: "/s/jc.err", TmpOutputHostPath: "/s/jc"},
	}
	data := []fakeJobData{
		{stdout: "out A\n", stderr: "err A\n", outputFiles: map[string]string{"a.txt": "A"}},
		{stdout: "out B\n", stderr: "err B\n", outputFiles: map[string]string{"b1.txt": "B1", "b2.txt": "B2"}},
		{stdout: "out C\n", stderr: "err C\n", outputFiles: map[string]string{}},
	}
	raw := simulateScriptOutput(jobs, data, token)
	results, err := parseGatherOutput(raw, jobs, token)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if results[10].stdOut != "out A\n" {
		t.Errorf("job A stdOut = %q", results[10].stdOut)
	}
	if results[20].stdOut != "out B\n" {
		t.Errorf("job B stdOut = %q", results[20].stdOut)
	}
	if results[30].stdOut != "out C\n" {
		t.Errorf("job C stdOut = %q", results[30].stdOut)
	}
	if results[10].outputFiles["a.txt"] != "A" {
		t.Errorf("job A a.txt = %q", results[10].outputFiles["a.txt"])
	}
	if results[20].outputFiles["b1.txt"] != "B1" {
		t.Errorf("job B b1.txt = %q", results[20].outputFiles["b1.txt"])
	}
	if results[20].outputFiles["b2.txt"] != "B2" {
		t.Errorf("job B b2.txt = %q", results[20].outputFiles["b2.txt"])
	}
	if len(results[30].outputFiles) != 0 {
		t.Errorf("job C should have no output files")
	}
}

func TestParseGatherOutput_MultilineContent(t *testing.T) {
	token := "eeee444444444444"
	multiline := "line1\nline2\nline3\nline4\n"
	jobs := []SlurmJob{{CmdId: 7, JobId: "j7", OutPath: "/s/j7.out", ErrPath: "/s/j7.err", TmpOutputHostPath: "/s/j7"}}
	data := []fakeJobData{{
		stdout:      multiline,
		stderr:      multiline,
		outputFiles: map[string]string{"out.txt": multiline},
	}}
	raw := simulateScriptOutput(jobs, data, token)
	results, err := parseGatherOutput(raw, jobs, token)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if results[7].stdOut != multiline {
		t.Errorf("stdOut = %q, want %q", results[7].stdOut, multiline)
	}
	if results[7].outputFiles["out.txt"] != multiline {
		t.Errorf("out.txt = %q, want %q", results[7].outputFiles["out.txt"], multiline)
	}
}

func TestParseGatherOutput_ContentContainingWrongToken(t *testing.T) {
	// File content that looks like a delimiter but uses a different token value.
	// The parser must not split on it.
	realToken := "realtoken1234567"
	fakeToken := "faketoken9999999"
	jobs := []SlurmJob{{CmdId: 3, JobId: "j3", OutPath: "/s/j3.out", ErrPath: "/s/j3.err", TmpOutputHostPath: "/s/j3"}}
	data := []fakeJobData{{
		stdout:      fmt.Sprintf("before\n%s:0:STDOUT:\nafter\n", fakeToken),
		stderr:      "stderr\n",
		outputFiles: map[string]string{},
	}}
	raw := simulateScriptOutput(jobs, data, realToken)
	results, err := parseGatherOutput(raw, jobs, realToken)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(results[3].stdOut, fakeToken) {
		t.Error("parser incorrectly split on a non-matching token inside file content")
	}
	if !strings.Contains(results[3].stdOut, "before") || !strings.Contains(results[3].stdOut, "after") {
		t.Error("content around fake token was dropped")
	}
}

func TestParseGatherOutput_ContentContainingRealTokenMidLine(t *testing.T) {
	// If the real token appears in the middle of a line (not at line start
	// after the leading-newline split prefix), the parser must not split on it.
	token := "mytoken000000001"
	jobs := []SlurmJob{{CmdId: 4, JobId: "j4", OutPath: "/s/j4.out", ErrPath: "/s/j4.err", TmpOutputHostPath: "/s/j4"}}
	// Embed the token mid-line in the content: "prefix_<token>:0:STDOUT:_suffix"
	// This does NOT start after a newline, so the split on "\n<token>:" won't fire.
	embeddedContent := fmt.Sprintf("prefix_%s:0:STDOUT:_suffix\n", token)
	data := []fakeJobData{{
		stdout:      embeddedContent,
		stderr:      "stderr\n",
		outputFiles: map[string]string{},
	}}
	raw := simulateScriptOutput(jobs, data, token)
	results, err := parseGatherOutput(raw, jobs, token)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(results[4].stdOut, "prefix_") {
		t.Error("mid-line token occurrence was incorrectly treated as a section delimiter")
	}
}

func TestParseGatherOutput_OutputFilenameWithColons(t *testing.T) {
	// Filenames with colons must be preserved intact (SplitN limit of 3).
	token := "colontesttoken01"
	jobs := []SlurmJob{{CmdId: 8, JobId: "j8", OutPath: "/s/j8.out", ErrPath: "/s/j8.err", TmpOutputHostPath: "/s/j8"}}
	data := []fakeJobData{{
		stdout:      "out\n",
		stderr:      "err\n",
		outputFiles: map[string]string{"file:with:colons.txt": "colon content"},
	}}
	raw := simulateScriptOutput(jobs, data, token)
	results, err := parseGatherOutput(raw, jobs, token)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if results[8].outputFiles["file:with:colons.txt"] != "colon content" {
		t.Errorf("filename with colons was mangled; outputFiles = %v", results[8].outputFiles)
	}
}

func TestParseGatherOutput_EmptyStdoutAndStderr(t *testing.T) {
	token := "emptystdiotoken0"
	jobs := []SlurmJob{{CmdId: 6, JobId: "j6", OutPath: "/s/j6.out", ErrPath: "/s/j6.err", TmpOutputHostPath: "/s/j6"}}
	data := []fakeJobData{{stdout: "", stderr: "", outputFiles: map[string]string{}}}
	raw := simulateScriptOutput(jobs, data, token)
	results, err := parseGatherOutput(raw, jobs, token)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if results[6].stdOut != "" {
		t.Errorf("expected empty stdOut, got %q", results[6].stdOut)
	}
	if results[6].stdErr != "" {
		t.Errorf("expected empty stdErr, got %q", results[6].stdErr)
	}
}

func TestParseGatherOutput_LargeContentInOutputFile(t *testing.T) {
	token := "largefiletoken00"
	jobs := []SlurmJob{{CmdId: 9, JobId: "j9", OutPath: "/s/j9.out", ErrPath: "/s/j9.err", TmpOutputHostPath: "/s/j9"}}
	// Build a large string with many lines including lines that contain the token string
	// as a substring (but not after a newline, so they should not split).
	var sb strings.Builder
	for i := 0; i < 1000; i++ {
		fmt.Fprintf(&sb, "line %d: data data data\n", i)
	}
	bigContent := sb.String()
	data := []fakeJobData{{
		stdout:      bigContent,
		stderr:      "err\n",
		outputFiles: map[string]string{"big.txt": bigContent},
	}}
	raw := simulateScriptOutput(jobs, data, token)
	results, err := parseGatherOutput(raw, jobs, token)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if results[9].stdOut != bigContent {
		t.Errorf("large content was not preserved intact in stdOut")
	}
	if results[9].outputFiles["big.txt"] != bigContent {
		t.Errorf("large content was not preserved intact in output file")
	}
}

// --- parseGatherOutput error cases ---

func TestParseGatherOutput_MissingStdout_ReturnsError(t *testing.T) {
	token := "missstdouttoken0"
	jobs := []SlurmJob{{CmdId: 1, JobId: "j1", OutPath: "/s/j1.out", ErrPath: "/s/j1.err", TmpOutputHostPath: "/s/j1"}}
	data := []fakeJobData{{stdoutMissing: true, stderr: "err\n"}}
	raw := simulateScriptOutput(jobs, data, token)
	_, err := parseGatherOutput(raw, jobs, token)
	if err == nil {
		t.Fatal("expected error for missing stdout, got nil")
	}
	if !strings.Contains(err.Error(), "stdout file missing") {
		t.Errorf("error should mention 'stdout file missing', got: %v", err)
	}
	if !strings.Contains(err.Error(), "j1") {
		t.Errorf("error should mention the job ID, got: %v", err)
	}
}

func TestParseGatherOutput_MissingStderr_ReturnsError(t *testing.T) {
	token := "missstderrtoken0"
	jobs := []SlurmJob{{CmdId: 2, JobId: "j2", OutPath: "/s/j2.out", ErrPath: "/s/j2.err", TmpOutputHostPath: "/s/j2"}}
	data := []fakeJobData{{stdout: "out\n", stderrMissing: true}}
	raw := simulateScriptOutput(jobs, data, token)
	_, err := parseGatherOutput(raw, jobs, token)
	if err == nil {
		t.Fatal("expected error for missing stderr, got nil")
	}
	if !strings.Contains(err.Error(), "stderr file missing") {
		t.Errorf("error should mention 'stderr file missing', got: %v", err)
	}
}

func TestParseGatherOutput_MissingOutputFile_IsSkipped(t *testing.T) {
	// A missing output file (missingFileMarker in an OUTPUT_FILE section)
	// is non-fatal: the file is simply absent from the result map.
	token := "missoutfiletoken"
	jobs := []SlurmJob{{CmdId: 3, JobId: "j3", OutPath: "/s/j3.out", ErrPath: "/s/j3.err", TmpOutputHostPath: "/s/j3"}}
	// Manually craft raw output with a missing output file marker.
	raw := fmt.Sprintf(
		"\n%s:0:%s:\nstdout\n\n%s:0:%s:\nstderr\n\n%s:0:%s:ghost.txt\n%s",
		token, sectionStdout,
		token, sectionStderr,
		token, sectionOutputFile,
		missingFileMarker,
	)
	results, err := parseGatherOutput(raw, jobs, token)
	if err != nil {
		t.Fatalf("unexpected error for missing output file: %v", err)
	}
	if _, present := results[3].outputFiles["ghost.txt"]; present {
		t.Error("missing output file should not appear in results map")
	}
}

func TestParseGatherOutput_MalformedHeader_TooFewColons(t *testing.T) {
	token := "malformedtokenn1"
	jobs := []SlurmJob{{CmdId: 1}}
	// A section header with only one colon (index, no section type).
	raw := fmt.Sprintf("\n%s:0\nsome content\n", token)
	_, err := parseGatherOutput(raw, jobs, token)
	if err == nil {
		t.Fatal("expected error for malformed header, got nil")
	}
	if !strings.Contains(err.Error(), "malformed section header") {
		t.Errorf("error should mention malformed header, got: %v", err)
	}
}

func TestParseGatherOutput_NonIntegerJobIndex(t *testing.T) {
	token := "noninttoken00000"
	jobs := []SlurmJob{{CmdId: 1}}
	raw := fmt.Sprintf("\n%s:abc:%s:\nsome content\n", token, sectionStdout)
	_, err := parseGatherOutput(raw, jobs, token)
	if err == nil {
		t.Fatal("expected error for non-integer job index, got nil")
	}
	if !strings.Contains(err.Error(), "non-integer job index") {
		t.Errorf("error should mention non-integer job index, got: %v", err)
	}
}

func TestParseGatherOutput_OutOfRangeJobIndex(t *testing.T) {
	token := "outofrangetoken0"
	jobs := []SlurmJob{{CmdId: 1}}
	// Index 5 when there is only 1 job.
	raw := fmt.Sprintf("\n%s:5:%s:\nsome content\n", token, sectionStdout)
	_, err := parseGatherOutput(raw, jobs, token)
	if err == nil {
		t.Fatal("expected error for out-of-range job index, got nil")
	}
	if !strings.Contains(err.Error(), "out-of-range") {
		t.Errorf("error should mention out-of-range, got: %v", err)
	}
}

func TestParseGatherOutput_UnknownSectionType(t *testing.T) {
	token := "unknownsecttoken"
	jobs := []SlurmJob{{CmdId: 1}}
	raw := fmt.Sprintf("\n%s:0:BOGUS:\nsome content\n", token)
	_, err := parseGatherOutput(raw, jobs, token)
	if err == nil {
		t.Fatal("expected error for unknown section type, got nil")
	}
	if !strings.Contains(err.Error(), "unknown section type") {
		t.Errorf("error should mention unknown section type, got: %v", err)
	}
}

func TestParseGatherOutput_OutputFileSectionWithEmptyFilename(t *testing.T) {
	token := "emptyfilenametok"
	jobs := []SlurmJob{{CmdId: 1, JobId: "j1"}}
	// OUTPUT_FILE section where extra (the filename) is empty.
	raw := fmt.Sprintf("\n%s:0:%s:\nstdout\n\n%s:0:%s:\nstderr\n\n%s:0:%s:\ncontent\n",
		token, sectionStdout,
		token, sectionStderr,
		token, sectionOutputFile,
	)
	_, err := parseGatherOutput(raw, jobs, token)
	if err == nil {
		t.Fatal("expected error for OUTPUT_FILE section with empty filename, got nil")
	}
	if !strings.Contains(err.Error(), "empty filename") {
		t.Errorf("error should mention empty filename, got: %v", err)
	}
}

func TestParseGatherOutput_EmptyJobsList(t *testing.T) {
	results, err := parseGatherOutput("", []SlurmJob{}, "anytoken")
	if err != nil {
		t.Fatalf("unexpected error for empty job list: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected empty results for empty job list, got %v", results)
	}
}

func TestParseGatherOutput_AllJobsKeyedByCmdId(t *testing.T) {
	// Verify that CmdId is used as the map key, not the slice index.
	token := "cmdidkeytoken000"
	jobs := []SlurmJob{
		{CmdId: 100, JobId: "ja", OutPath: "/s/a.out", ErrPath: "/s/a.err", TmpOutputHostPath: "/s/a"},
		{CmdId: 200, JobId: "jb", OutPath: "/s/b.out", ErrPath: "/s/b.err", TmpOutputHostPath: "/s/b"},
	}
	data := []fakeJobData{
		{stdout: "A out", stderr: "A err", outputFiles: map[string]string{}},
		{stdout: "B out", stderr: "B err", outputFiles: map[string]string{}},
	}
	raw := simulateScriptOutput(jobs, data, token)
	results, err := parseGatherOutput(raw, jobs, token)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := results[100]; !ok {
		t.Error("CmdId 100 missing from results")
	}
	if _, ok := results[200]; !ok {
		t.Error("CmdId 200 missing from results")
	}
	if results[100].stdOut != "A out" {
		t.Errorf("CmdId 100 stdOut = %q, want %q", results[100].stdOut, "A out")
	}
	if results[200].stdOut != "B out" {
		t.Errorf("CmdId 200 stdOut = %q, want %q", results[200].stdOut, "B out")
	}
}

// --- fetchAllJobOutputs ---

func TestFetchAllJobOutputs_EmptyJobList_NoSSHCall(t *testing.T) {
	called := false
	runCmd := func(_ string) (CmdOut, error) {
		called = true
		return CmdOut{}, nil
	}
	results, err := fetchAllJobOutputs([]SlurmJob{}, runCmd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected empty results, got %v", results)
	}
	if called {
		t.Error("runCmd should not be called for an empty job list")
	}
}

func TestFetchAllJobOutputs_SingleJob_RoundTrip(t *testing.T) {
	job := SlurmJob{
		CmdId: 42, JobId: "j42",
		OutPath: "/s/j42.out", ErrPath: "/s/j42.err",
		TmpOutputHostPath: "/s/j42", SbatchPath: "/s/j42.sbatch",
	}
	runCmd := makeSimulatingRunCmd([]SlurmJob{job}, []fakeJobData{
		{stdout: "out42\n", stderr: "err42\n", outputFiles: map[string]string{"r.txt": "result"}},
	})
	results, err := fetchAllJobOutputs([]SlurmJob{job}, runCmd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	entry, ok := results[42]
	if !ok {
		t.Fatal("CmdId 42 missing from results")
	}
	if entry.stdOut != "out42\n" {
		t.Errorf("stdOut = %q, want %q", entry.stdOut, "out42\n")
	}
	if entry.outputFiles["r.txt"] != "result" {
		t.Errorf("r.txt = %q, want %q", entry.outputFiles["r.txt"], "result")
	}
}

func TestFetchAllJobOutputs_MultipleJobs_RoundTrip(t *testing.T) {
	jobs := []SlurmJob{
		{CmdId: 1, JobId: "j1", OutPath: "/s/j1.out", ErrPath: "/s/j1.err", TmpOutputHostPath: "/s/j1", SbatchPath: "/s/j1.sbatch"},
		{CmdId: 2, JobId: "j2", OutPath: "/s/j2.out", ErrPath: "/s/j2.err", TmpOutputHostPath: "/s/j2", SbatchPath: "/s/j2.sbatch"},
	}
	data := []fakeJobData{
		{stdout: "out1\n", stderr: "err1\n", outputFiles: map[string]string{"f1.txt": "v1"}},
		{stdout: "out2\n", stderr: "err2\n", outputFiles: map[string]string{"f2.txt": "v2"}},
	}
	runCmd := makeSimulatingRunCmd(jobs, data)
	results, err := fetchAllJobOutputs(jobs, runCmd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if results[1].stdOut != "out1\n" {
		t.Errorf("job 1 stdOut = %q", results[1].stdOut)
	}
	if results[2].stdOut != "out2\n" {
		t.Errorf("job 2 stdOut = %q", results[2].stdOut)
	}
	if results[1].outputFiles["f1.txt"] != "v1" {
		t.Errorf("job 1 f1.txt = %q", results[1].outputFiles["f1.txt"])
	}
	if results[2].outputFiles["f2.txt"] != "v2" {
		t.Errorf("job 2 f2.txt = %q", results[2].outputFiles["f2.txt"])
	}
}

func TestFetchAllJobOutputs_OnlyOneSSHCall(t *testing.T) {
	jobs := []SlurmJob{
		{CmdId: 1, JobId: "j1", OutPath: "/s/j1.out", ErrPath: "/s/j1.err", TmpOutputHostPath: "/s/j1", SbatchPath: "/s/j1.sbatch"},
		{CmdId: 2, JobId: "j2", OutPath: "/s/j2.out", ErrPath: "/s/j2.err", TmpOutputHostPath: "/s/j2", SbatchPath: "/s/j2.sbatch"},
		{CmdId: 3, JobId: "j3", OutPath: "/s/j3.out", ErrPath: "/s/j3.err", TmpOutputHostPath: "/s/j3", SbatchPath: "/s/j3.sbatch"},
	}
	data := []fakeJobData{
		{stdout: "o1", stderr: "e1", outputFiles: map[string]string{}},
		{stdout: "o2", stderr: "e2", outputFiles: map[string]string{}},
		{stdout: "o3", stderr: "e3", outputFiles: map[string]string{}},
	}
	callCount := 0
	inner := makeSimulatingRunCmd(jobs, data)
	runCmd := func(cmd string) (CmdOut, error) {
		callCount++
		return inner(cmd)
	}
	_, err := fetchAllJobOutputs(jobs, runCmd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if callCount != 1 {
		t.Errorf("expected exactly 1 SSH call for %d jobs, got %d", len(jobs), callCount)
	}
}

func TestFetchAllJobOutputs_ScriptFailure_ReturnsError(t *testing.T) {
	job := SlurmJob{CmdId: 1, JobId: "jfail"}
	runCmd := func(_ string) (CmdOut, error) {
		return CmdOut{ExitCode: 1, StdErr: "permission denied"}, fmt.Errorf("exit status 1")
	}
	_, err := fetchAllJobOutputs([]SlurmJob{job}, runCmd)
	if err == nil {
		t.Error("expected error on script failure, got nil")
	}
}

// makeSimulatingRunCmd returns a CmdRunner that intercepts the gather script,
// extracts the embedded token, and returns simulated output without any real
// SSH connection.
func makeSimulatingRunCmd(jobs []SlurmJob, data []fakeJobData) CmdRunner {
	return func(cmd string) (CmdOut, error) {
		token := extractTokenFromScript(cmd)
		if token == "" {
			return CmdOut{}, fmt.Errorf("could not extract token from script")
		}
		simulated := simulateScriptOutput(jobs, data, token)
		return CmdOut{ExitCode: 0, StdOut: simulated}, nil
	}
}

// extractTokenFromScript parses the emit_sep function definition line to
// recover the randomly generated token that buildGatherScript embedded.
func extractTokenFromScript(script string) string {
	for _, line := range strings.Split(script, "\n") {
		// The definition line looks like:
		// emit_sep() { printf '\n<TOKEN>:%s:%s:%s\n' "$1" "$2" "$3"; }
		if !strings.HasPrefix(line, "emit_sep()") {
			continue
		}
		// The token sits between '\n and the first colon after it.
		marker := "'\\n"
		start := strings.Index(line, marker)
		if start < 0 {
			return ""
		}
		start += len(marker)
		end := strings.Index(line[start:], ":")
		if end < 0 {
			return ""
		}
		return line[start : start+end]
	}
	return ""
}

func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
