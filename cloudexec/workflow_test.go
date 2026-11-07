package cloudexec

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestStageWorkflowPublishesBeforeCleanup(t *testing.T) {
	for _, failPublication := range []bool{false, true} {
		t.Run(fmt.Sprint(failPublication), func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.RegisterActivityWithOptions(func(context.Context, string, StageResult) error { return nil }, activity.RegisterOptions{Name: "cloud.SaveStageResult"})
			var order []string
			env.RegisterActivityWithOptions(func(context.Context, Request) (Probe, error) { return Probe{Ready: true}, nil }, activity.RegisterOptions{Name: "cloud.Probe"})
			env.RegisterActivityWithOptions(func(context.Context, Request) (State, error) {
				order = append(order, "acquire")
				return State{Phase: "preparing_storage"}, nil
			}, activity.RegisterOptions{Name: "cloud.Acquire"})
			env.RegisterActivityWithOptions(func(context.Context, string) (State, error) { return State{Phase: "staging"}, nil }, activity.RegisterOptions{Name: "cloud.PrepareStorage"})
			env.RegisterActivityWithOptions(func(_ context.Context, _ string, r RunnerRequest) (string, error) {
				order = append(order, r.Action+":"+r.CommandID)
				return r.OperationID, nil
			}, activity.RegisterOptions{Name: "cloud.DispatchRunner"})
			env.RegisterActivityWithOptions(func(_ context.Context, _ string, op string) (RunnerResult, error) {
				state := "succeeded"
				if op == "prepare" {
					state = "prepared"
				}
				if op == "runtime" {
					state = "ready"
				}
				if op == "cleanup.unmount" {
					state = "unmounted"
				}
				if failPublication && strings.HasPrefix(op, "publish") {
					state = "failed"
				}
				return RunnerResult{DeliveryState: "Success", ProcessState: state, ProcessStopped: true}, nil
			}, activity.RegisterOptions{Name: "cloud.PollRunner"})
			env.RegisterActivityWithOptions(func(_ context.Context, _ string, compute, publication string, stopped bool) error {
				if compute != "succeeded" || !stopped {
					t.Errorf("wrong outcomes %s %s %v", compute, publication, stopped)
				}
				return nil
			}, activity.RegisterOptions{Name: "cloud.RecordOutcome"})
			env.RegisterActivityWithOptions(func(context.Context, string) (State, error) {
				order = append(order, "cleanup")
				outcome := "complete"
				if failPublication {
					outcome = "retained"
				}
				return State{CleanupOutcome: outcome}, nil
			}, activity.RegisterOptions{Name: "cloud.Cleanup"})
			req := StageRequest{Identity: Request{JobID: "job"}, Queue: "aws", BootstrapSeconds: 60, JobSeconds: 60, CollectionSeconds: 60, Command: RunnerRequest{CommandID: "science"}, Outputs: []RunnerRequest{{CommandID: "publish", OperationID: "publish", Action: "start"}}}
			env.ExecuteWorkflow(StageWorkflow, req)
			if !env.IsWorkflowCompleted() {
				t.Fatal("workflow incomplete")
			}
			if failPublication != (env.GetWorkflowError() != nil) {
				t.Fatal(env.GetWorkflowError())
			}
			if !failPublication {
				var result StageResult
				if e := env.GetWorkflowResult(&result); e != nil || result.Cleanup != "complete" || result.Publication != "succeeded" {
					t.Fatal(result, e)
				}
			}
			cleanup, publish := -1, -1
			for i, s := range order {
				if s == "cleanup" {
					cleanup = i
				}
				if s == "start:publish" {
					publish = i
				}
			}
			if cleanup <= publish || publish < 0 {
				t.Fatal(order)
			}
		})
	}
}
func TestStageCancellationUsesDisconnectedCleanup(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(func(context.Context, string, StageResult) error { return nil }, activity.RegisterOptions{Name: "cloud.SaveStageResult"})
	env.RegisterActivityWithOptions(func(context.Context, Request) (Probe, error) { return Probe{Ready: true}, nil }, activity.RegisterOptions{Name: "cloud.Probe"})
	env.RegisterActivityWithOptions(func(context.Context, Request) (State, error) { return State{Phase: "acquiring_compute"}, nil }, activity.RegisterOptions{Name: "cloud.Acquire"})
	env.RegisterActivityWithOptions(func(context.Context, string, RunnerRequest) (string, error) { return "cleanup-command", nil }, activity.RegisterOptions{Name: "cloud.DispatchRunner"})
	env.RegisterActivityWithOptions(func(context.Context, string, string) (RunnerResult, error) {
		return RunnerResult{DeliveryState: "Success", ProcessState: "unmounted", ProcessStopped: true}, nil
	}, activity.RegisterOptions{Name: "cloud.PollRunner"})
	env.RegisterActivityWithOptions(func(context.Context, string, string, string, bool) error { return nil }, activity.RegisterOptions{Name: "cloud.RecordOutcome"})
	env.RegisterActivityWithOptions(func(context.Context, string) (State, error) { return State{CleanupOutcome: "retained"}, nil }, activity.RegisterOptions{Name: "cloud.Cleanup"})
	env.OnActivity("cloud.Cleanup", mock.Anything, "job").Return(State{CleanupOutcome: "retained"}, nil).Once()
	env.RegisterDelayedCallback(func() { env.CancelWorkflow() }, time.Second)
	env.ExecuteWorkflow(StageWorkflow, StageRequest{Identity: Request{JobID: "job"}, Queue: "aws", BootstrapSeconds: 60, JobSeconds: 60, CollectionSeconds: 60, Command: RunnerRequest{CommandID: "science"}})
	if env.GetWorkflowError() == nil {
		t.Fatal("cancellation ignored")
	}
	env.AssertExpectations(t)
}

func TestCollectionDoesNotRecomputeOrProvision(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	req := StageRequest{CollectionID: "collect-1", Identity: Request{JobID: "job"}, Queue: "aws", BootstrapSeconds: 60, JobSeconds: 60, CollectionSeconds: 60, Command: RunnerRequest{CommandID: "science"}, Outputs: []RunnerRequest{{CommandID: "publish", OperationID: "publish", Action: "start"}}}
	env.RegisterActivityWithOptions(func(context.Context, string, string) (State, error) {
		return State{Request: req.Identity, ComputeOutcome: "succeeded", CleanupOutcome: "retained", ProcessStopped: true, RetainUntil: time.Now().Add(time.Hour).UTC().Format(time.RFC3339)}, nil
	}, activity.RegisterOptions{Name: "cloud.BeginCollection"})
	env.RegisterActivityWithOptions(func(_ context.Context, _ string, r RunnerRequest) (string, error) {
		if r.Action == "start" && r.CommandID != "publish.collect-1" {
			t.Fatalf("collection started computation: %#v", r)
		}
		return r.OperationID, nil
	}, activity.RegisterOptions{Name: "cloud.DispatchRunner"})
	env.RegisterActivityWithOptions(func(context.Context, string, string) (RunnerResult, error) {
		return RunnerResult{DeliveryState: "Success", ProcessState: "succeeded", ProcessStopped: true}, nil
	}, activity.RegisterOptions{Name: "cloud.PollRunner"})
	env.RegisterActivityWithOptions(func(context.Context, string, string, string, bool) error { return nil }, activity.RegisterOptions{Name: "cloud.RecordOutcome"})
	env.RegisterActivityWithOptions(func(context.Context, string) (State, error) { return State{CleanupOutcome: "complete"}, nil }, activity.RegisterOptions{Name: "cloud.Cleanup"})
	env.RegisterActivityWithOptions(func(context.Context, string, StageResult) error { return nil }, activity.RegisterOptions{Name: "cloud.SaveStageResult"})
	env.ExecuteWorkflow(StageWorkflow, req)
	if e := env.GetWorkflowError(); e != nil {
		t.Fatal(e)
	}
	var result StageResult
	if e := env.GetWorkflowResult(&result); e != nil || result.Compute != "succeeded" || result.Publication != "succeeded" {
		t.Fatal(result, e)
	}
}
