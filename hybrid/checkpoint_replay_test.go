package hybrid

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"go-scheduler/transfer"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestRecordCheckpointRestartHistory(t *testing.T) {
	if os.Getenv("BWB_RECORD_CHECKPOINT_HISTORY") != "1" {
		t.Skip("explicit isolated Temporal fixture opt-in required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	c, e := client.Dial(client.Options{HostPort: "127.0.0.1:7233", Namespace: "workbench-hybrid-offline-v1"})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	p := checkpointFixture(t, "manual")
	p.Plan.Identity.WorkflowID = "hybrid-checkpoint-replay-v1"
	const queue = "hybrid-checkpoint-fixture-only-v1"
	start := func() worker.Worker {
		w := worker.New(c, queue, worker.Options{})
		w.RegisterWorkflow(Run)
		w.RegisterWorkflowWithOptions(func(ctx workflow.Context, s Stage) (NodeResult, error) {
			artifacts := map[string]transfer.Artifact{}
			for _, out := range s.Outputs {
				artifacts[out.Key] = transfer.Artifact{URI: out.Destination, SHA256: "sha256:" + strings.Repeat("a", 64), Size: 1}
			}
			return NodeResult{State: "succeeded", Compute: "succeeded", Publication: "succeeded", Artifacts: artifacts}, nil
		}, workflow.RegisterOptions{Name: "NodeWorkflow"})
		w.RegisterActivityWithOptions(func(context.Context, string, Result) error { return nil }, activity.RegisterOptions{Name: "hybrid.PersistResult"})
		if e := w.Start(); e != nil {
			t.Fatal(e)
		}
		return w
	}
	w := start()
	stopped := false
	defer func() {
		if !stopped {
			w.Stop()
		}
	}()
	run, e := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: p.Plan.Identity.WorkflowID, TaskQueue: queue, WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE}, Run, p)
	if e != nil {
		t.Fatal(e)
	}
	var gate CheckpointResult
	for ctx.Err() == nil {
		encoded, e := c.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), "status")
		if e == nil {
			var r Result
			if e = encoded.Get(&r); e != nil {
				t.Fatal(e)
			}
			gate = r.Checkpoints["pilot-review"]
			if gate.State == "waiting_for_approval" {
				if len(r.Nodes) != 1 {
					t.Fatalf("full job started before approval: %+v", r)
				}
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if gate.State != "waiting_for_approval" {
		t.Fatal("checkpoint never waited", ctx.Err())
	}
	w.Stop()
	stopped = true
	decision := CheckpointDecision{ID: "pilot-review", StateHash: gate.StateHash, DecisionID: "offline-human", Action: "approve", Actor: "fixture-human", ReceiptHash: "sha256:" + strings.Repeat("b", 64)}
	if e = c.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), CheckpointSignal, decision); e != nil {
		t.Fatal(e)
	}
	resumed := start()
	defer resumed.Stop()
	var result Result
	if e = run.Get(ctx, &result); e != nil {
		t.Fatal(e)
	}
	if result.State != "succeeded" || result.Checkpoints["pilot-review"].State != "approved" {
		t.Fatal(result)
	}
	history := &historypb.History{}
	it := c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for it.HasNext() {
		event, e := it.Next()
		if e != nil {
			t.Fatal(e)
		}
		history.Events = append(history.Events, event)
	}
	raw, e := protojson.MarshalOptions{Indent: "  "}.Marshal(history)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile("testdata/history_checkpoint_v1.json", raw, 0600); e != nil {
		t.Fatal(e)
	}
}

func TestReplayCheckpointHistory(t *testing.T) {
	f, e := os.Open("testdata/history_checkpoint_v1.json")
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	history, e := client.HistoryFromJSON(f, client.HistoryJSONOptions{})
	if e != nil {
		t.Fatal(e)
	}
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflow(Run)
	if e = replayer.ReplayWorkflowHistoryWithOptions(nil, history, worker.ReplayWorkflowHistoryOptions{OriginalExecution: workflow.Execution{ID: "hybrid-checkpoint-replay-v1"}}); e != nil {
		t.Fatal(e)
	}
}
