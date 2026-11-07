package cloudexec

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestRecordIsolatedStageHistories(t *testing.T) {
	if os.Getenv("BWB_RECORD_HYBRID_HISTORY") != "1" {
		t.Skip("explicit local Temporal fixture recording required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	c, e := client.Dial(client.Options{HostPort: "127.0.0.1:7233", Namespace: "workbench-hybrid-offline-v1"})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	for _, scenario := range []string{"success", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			queue := "ec2-fixture-only-" + scenario + "-v1"
			w := worker.New(c, queue, worker.Options{})
			w.RegisterWorkflow(StageWorkflow)
			started := make(chan struct{}, 1)
			registrations := []struct {
				name string
				fn   any
			}{
				{"Probe", func(context.Context, Request) (Probe, error) { return Probe{Ready: true}, nil }},
				{"Acquire", func(context.Context, Request) (State, error) { return State{Phase: "preparing_storage"}, nil }},
				{"PrepareStorage", func(context.Context, string) (State, error) { return State{Phase: "staging"}, nil }},
				{"DispatchRunner", func(_ context.Context, _ string, r RunnerRequest) (string, error) { return r.OperationID, nil }},
				{"PollRunner", func(_ context.Context, _ string, op string) (RunnerResult, error) {
					result := RunnerResult{DeliveryState: "Success", ProcessState: "succeeded", ProcessStopped: true, Data: json.RawMessage(`{}`)}
					if scenario == "cancel" && op == "science.start" {
						result.ProcessState = "running"
						result.ProcessStopped = false
						select {
						case started <- struct{}{}:
						default:
						}
					}
					return result, nil
				}},
				{"RecordOutcome", func(context.Context, string, string, string, bool) error { return nil }},
				{"Cleanup", func(context.Context, string) (State, error) { return State{CleanupOutcome: "complete"}, nil }},
				{"SaveStageResult", func(context.Context, string, StageResult) error { return nil }},
			}
			for _, r := range registrations {
				w.RegisterActivityWithOptions(r.fn, activity.RegisterOptions{Name: "cloud." + r.name})
			}
			if e = w.Start(); e != nil {
				t.Fatal(e)
			}
			defer w.Stop()
			req := StageRequest{Identity: Request{JobID: "fixture-job"}, Queue: queue, BootstrapSeconds: 60, JobSeconds: 60, CollectionSeconds: 60, Command: RunnerRequest{CommandID: "science"}, Outputs: []RunnerRequest{{CommandID: "publication", OperationID: "publication", Action: "start"}}}
			id := "ec2-replay-" + scenario + "-v1"
			run, e := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskQueue: queue, WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE}, StageWorkflow, req)
			if e != nil {
				t.Fatal(e)
			}
			if scenario == "cancel" {
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if e = c.CancelWorkflow(ctx, id, run.GetRunID()); e != nil {
					t.Fatal(e)
				}
			}
			var result StageResult
			e = run.Get(ctx, &result)
			if (scenario == "cancel") != (e != nil) {
				t.Fatal(e)
			}
			history := &historypb.History{}
			it := c.GetWorkflowHistory(ctx, id, run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
			for it.HasNext() {
				event, e := it.Next()
				if e != nil {
					t.Fatal(e)
				}
				history.Events = append(history.Events, event)
			}
			data, e := protojson.MarshalOptions{Indent: "  "}.Marshal(history)
			if e != nil {
				t.Fatal(e)
			}
			os.MkdirAll("testdata", 0700)
			if e = os.WriteFile("testdata/history_ec2_"+scenario+"_v1.json", data, 0600); e != nil {
				t.Fatal(e)
			}
		})
	}
}
func TestReplayStageHistories(t *testing.T) {
	for _, scenario := range []string{"success", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			f, e := os.Open("testdata/history_ec2_" + scenario + "_v1.json")
			if e != nil {
				t.Fatal(e)
			}
			defer f.Close()
			history, e := client.HistoryFromJSON(f, client.HistoryJSONOptions{})
			if e != nil {
				t.Fatal(e)
			}
			replayer := worker.NewWorkflowReplayer()
			replayer.RegisterWorkflow(StageWorkflow)
			if e = replayer.ReplayWorkflowHistoryWithOptions(nil, history, worker.ReplayWorkflowHistoryOptions{OriginalExecution: workflow.Execution{ID: "ec2-replay-" + scenario + "-v1"}}); e != nil {
				t.Fatal(e)
			}
		})
	}
}
