package hybrid

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"go-scheduler/executioncontract"
	"go-scheduler/transfer"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func checkpointFixture(t *testing.T, mode string) Prepared {
	req, cfg := fixture(t)
	p, e := executioncontract.Validate(req.Plan)
	if e != nil {
		t.Fatal(e)
	}
	gate := map[string]any{"checkpoint_id": "pilot-review", "description": "Pilot and QC must succeed before the full run", "after_nodes": []any{"prepare"}, "before_nodes": []any{"align"}, "mode": mode}
	if mode == "manual" {
		gate["review_timeout_seconds"] = json.Number("60")
	}
	p["checkpoints"] = []any{gate}
	p["review"].(map[string]any)["state_hash"], _ = executioncontract.ExecutionFingerprint(p)
	req.Plan, _ = json.Marshal(p)
	prepared, e := Prepare(req, cfg)
	if e != nil {
		t.Fatal(e)
	}
	return prepared
}

func TestCheckpointContinuation(t *testing.T) {
	for _, scenario := range []string{"automatic", "approve", "decline", "expire", "cancel", "failed_qc", "stale_then_approve"} {
		t.Run(scenario, func(t *testing.T) {
			mode := "manual"
			if scenario == "automatic" {
				mode = "automatic"
			}
			p := checkpointFixture(t, mode)
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			visited := []string{}
			var persisted Result
			env.RegisterWorkflowWithOptions(func(ctx workflow.Context, s Stage) (NodeResult, error) {
				visited = append(visited, s.Node.ID)
				if scenario == "failed_qc" && s.Node.ID == "prepare" {
					return NodeResult{State: "failed", Compute: "failed"}, nil
				}
				artifacts := map[string]transfer.Artifact{}
				for _, out := range s.Outputs {
					artifacts[out.Key] = transfer.Artifact{URI: out.Destination, SHA256: "sha256:" + strings.Repeat("a", 64), Size: 1}
				}
				return NodeResult{State: "succeeded", Compute: "succeeded", Publication: "succeeded", Artifacts: artifacts}, nil
			}, workflow.RegisterOptions{Name: "NodeWorkflow"})
			env.RegisterActivityWithOptions(func(_ context.Context, _ string, r Result) error { persisted = r; return nil }, activity.RegisterOptions{Name: "hybrid.PersistResult"})
			read := func() Result {
				encoded, e := env.QueryWorkflow("status")
				if e != nil {
					t.Fatal(e)
				}
				var r Result
				if e = encoded.Get(&r); e != nil {
					t.Fatal(e)
				}
				return r
			}
			if scenario == "approve" || scenario == "decline" || scenario == "stale_then_approve" || scenario == "cancel" {
				env.RegisterDelayedCallback(func() {
					r := read()
					gate := r.Checkpoints["pilot-review"]
					if r.State != "waiting_for_approval" || gate.State != "waiting_for_approval" || len(visited) != 1 {
						t.Errorf("full computation started before review: %+v %v", r, visited)
					}
					if len(gate.Evidence["prepare"].Artifacts) == 0 {
						t.Error("review missing publication evidence")
					}
					if scenario == "cancel" {
						env.CancelWorkflow()
						return
					}
					d := CheckpointDecision{ID: "pilot-review", StateHash: gate.StateHash, DecisionID: "human-1", Action: "approve", Actor: "alice", ReceiptHash: "sha256:" + strings.Repeat("b", 64)}
					if scenario == "decline" {
						d.Action = "decline"
					}
					if scenario == "stale_then_approve" {
						stale := d
						stale.StateHash = "sha256:" + strings.Repeat("c", 64)
						env.SignalWorkflow(CheckpointSignal, stale)
						env.RegisterDelayedCallback(func() {
							if len(visited) != 1 {
								t.Error("stale review released compute")
							}
							env.SignalWorkflow(CheckpointSignal, d)
						}, time.Second)
					} else {
						env.SignalWorkflow(CheckpointSignal, d)
						env.SignalWorkflow(CheckpointSignal, d) // delivery retry
					}
				}, time.Second)
			}
			env.ExecuteWorkflow(Run, p)
			succeeds := scenario == "automatic" || scenario == "approve" || scenario == "stale_then_approve"
			if succeeds {
				if e := env.GetWorkflowError(); e != nil {
					t.Fatal(e)
				}
				var r Result
				if e := env.GetWorkflowResult(&r); e != nil {
					t.Fatal(e)
				}
				if r.State != "succeeded" || len(visited) != 4 {
					t.Fatal(r, visited)
				}
				if scenario != "automatic" && r.Checkpoints["pilot-review"].Decision == nil {
					t.Error("human decision not persisted")
				}
			} else {
				if env.GetWorkflowError() == nil || len(visited) != 1 {
					t.Fatal("blocked continuation executed", visited, env.GetWorkflowError())
				}
				wanted := map[string]string{"decline": "declined", "expire": "expired", "failed_qc": "blocked"}[scenario]
				if wanted != "" && persisted.Checkpoints["pilot-review"].State != wanted {
					t.Fatalf("wrong final checkpoint %+v", persisted)
				}
				if scenario == "cancel" && persisted.State != "cancelled" {
					t.Fatal(persisted)
				}
			}
		})
	}
}

func TestCheckpointCannotApproveBeforeEvidenceOrReapprove(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(func(ctx workflow.Context) (bool, error) {
		r := Result{Checkpoints: map[string]CheckpointResult{}}
		d := CheckpointDecision{ID: "pilot", StateHash: "hash", DecisionID: "first", Action: "approve", Actor: "alice", ReceiptHash: "sha256:" + strings.Repeat("a", 64)}
		acceptCheckpointDecision(ctx, &r, d)
		if len(r.Checkpoints) != 0 {
			t.Error("early decision accepted")
		}
		r.Checkpoints["pilot"] = CheckpointResult{State: "waiting_for_approval", StateHash: "hash", ExpiresAt: workflow.Now(ctx).Add(time.Minute)}
		acceptCheckpointDecision(ctx, &r, d)
		d.Action = "decline"
		d.DecisionID = "second"
		acceptCheckpointDecision(ctx, &r, d)
		return r.Checkpoints["pilot"].State == "approved" && r.Checkpoints["pilot"].Decision.DecisionID == "first", nil
	})
	var ok bool
	if err := env.GetWorkflowResult(&ok); err != nil || !ok {
		t.Fatal(err, ok)
	}
}
