package hybrid

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go-scheduler/transfer"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestRecordIsolatedHybridHistory(t *testing.T) {
	if os.Getenv("BWB_RECORD_HYBRID_HISTORY") != "1" {
		t.Skip("explicit local Temporal fixture recording opt-in required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const namespace = "workbench-hybrid-offline-v1"
	ns, e := client.NewNamespaceClient(client.Options{HostPort: "127.0.0.1:7233"})
	if e != nil {
		t.Fatal(e)
	}
	defer ns.Close()
	e = ns.Register(ctx, &workflowservice.RegisterNamespaceRequest{Namespace: namespace, Description: "Isolated fake-cloud replay fixture recording", WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour)})
	var exists *serviceerror.NamespaceAlreadyExists
	if e != nil && !errors.As(e, &exists) {
		t.Fatal(e)
	}
	c, e := client.Dial(client.Options{HostPort: "127.0.0.1:7233", Namespace: namespace})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	req, cfg := fixture(t)
	cfg.JournalRoot = "/test-evidence"
	p, e := Prepare(req, cfg)
	if e != nil {
		t.Fatal(e)
	}
	const queue = "hybrid-fixture-only-v1"
	p.Plan.Identity.WorkflowID = "hybrid-replay-fixture-v1"
	w := worker.New(c, queue, worker.Options{})
	w.RegisterWorkflow(Run)
	w.RegisterWorkflowWithOptions(func(ctx workflow.Context, s Stage) (NodeResult, error) {
		artifacts := map[string]transfer.Artifact{}
		for _, out := range s.Outputs {
			artifacts[out.Key] = transfer.Artifact{URI: out.Destination, SHA256: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Size: 1}
		}
		return NodeResult{State: "succeeded", Compute: "succeeded", Publication: "succeeded", Cleanup: "complete", Artifacts: artifacts}, nil
	}, workflow.RegisterOptions{Name: "NodeWorkflow"})
	w.RegisterActivityWithOptions(func(context.Context, string, Result) error { return nil }, activity.RegisterOptions{Name: "hybrid.PersistResult"})
	if e = w.Start(); e != nil {
		t.Fatal(e)
	}
	defer w.Stop()
	run, e := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: p.Plan.Identity.WorkflowID, TaskQueue: queue, WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE}, Run, p)
	if e != nil {
		t.Fatal(e)
	}
	var result Result
	if e = run.Get(ctx, &result); e != nil {
		t.Fatal(e)
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
	data, e := protojson.MarshalOptions{Indent: "  "}.Marshal(history)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.MkdirAll("testdata", 0700); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join("testdata", "history_hybrid_v1.json"), data, 0600); e != nil {
		t.Fatal(e)
	}
}
func TestReplayHybridHistory(t *testing.T) {
	f, e := os.Open("testdata/history_hybrid_v1.json")
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
	if e = replayer.ReplayWorkflowHistoryWithOptions(nil, history, worker.ReplayWorkflowHistoryOptions{OriginalExecution: workflow.Execution{ID: "hybrid-replay-fixture-v1"}}); e != nil {
		t.Fatal(e)
	}
}
