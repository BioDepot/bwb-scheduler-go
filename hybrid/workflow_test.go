package hybrid

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"go-scheduler/cloudexec"
	"go-scheduler/executioncontract"
	"go-scheduler/transfer"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func fixture(t *testing.T) (Request, Config) {
	t.Helper()
	raw, e := os.ReadFile("../executioncontract/testdata/contracts-v1.json")
	if e != nil {
		t.Fatal(e)
	}
	var corpus struct {
		Valid map[string]json.RawMessage `json:"valid"`
	}
	if e = json.Unmarshal(raw, &corpus); e != nil {
		t.Fatal(e)
	}
	cfg := Config{Queue: "hybrid-test", Region: "us-west-2", Policy: corpus.Valid["policy"], JournalRoot: t.TempDir(), Sites: map[string]Site{}, Profiles: map[string]cloudexec.Profile{}, TransferQueues: map[string][]string{"transfer-a": {"s3", "local-a", "ec2-managed", "ec2-existing", "slurm-a"}}}
	var plan Plan
	_ = json.Unmarshal(corpus.Valid["hybrid"], &plan)
	commands := map[string]Command{}
	for _, node := range plan.Placement.Nodes {
		key := node.Key + "@1"
		cfg.Sites[node.Key] = Site{Kind: node.Kind, Queue: node.Key, Root: "/work/jobs", ProfileKey: key, MaxJobs: 2}
		commands[node.ID] = Command{Argv: []string{"/bin/true"}, TimeoutSeconds: 60}
		if node.Kind == "aws_ec2" {
			name := "managed"
			if node.Key == "ec2-existing" {
				name = "existing"
			}
			var p cloudexec.Profile
			_ = json.Unmarshal(corpus.Valid[name], &p)
			cfg.Profiles[key] = p
		}
	}
	p, e := executioncontract.Validate(corpus.Valid["hybrid"])
	if e != nil {
		t.Fatal(e)
	}
	p["graph_hash"], _ = objectHash(commands)
	p["review"].(map[string]any)["state_hash"], _ = executioncontract.ExecutionFingerprint(p)
	planRaw, _ := json.Marshal(p)
	policyHash, _ := objectHash(cfg.Policy)
	return Request{PolicyHash: policyHash, Schema: "biodepot.hybrid_workflow/v1", Plan: planRaw, Commands: commands}, cfg
}
func TestPolicyConfigurationAndCommandPins(t *testing.T) {
	req, cfg := fixture(t)
	if e := ValidateConfig(cfg); e != nil {
		t.Fatal(e)
	}
	p, e := Prepare(req, cfg)
	if e != nil {
		t.Fatal(e)
	}
	if p.Stages["align"].JobID == p.Stages["count"].JobID {
		t.Fatal("EC2 stages share identity")
	}
	cfg.TransferQueues["transfer-a"] = []string{"s3", "local-a"}
	if _, e = Prepare(req, cfg); e == nil {
		t.Fatal("unsupported route accepted before provisioning")
	}
	req, cfg = fixture(t)
	req.Commands["align"] = Command{Argv: []string{"changed"}, TimeoutSeconds: 60}
	if _, e = Prepare(req, cfg); e == nil {
		t.Fatal("unreviewed command accepted")
	}
}
func TestHybridDAGPublicationReadiness(t *testing.T) {
	req, cfg := fixture(t)
	prepared, e := Prepare(req, cfg)
	if e != nil {
		t.Fatal(e)
	}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	visited := map[string]bool{}
	env.RegisterWorkflowWithOptions(func(ctx workflow.Context, s Stage) (NodeResult, error) {
		for _, dep := range s.Node.Depends {
			if !visited[dep] {
				t.Errorf("consumer %s started before %s publication", s.Node.ID, dep)
			}
		}
		if s.Node.ID != "prepare" && len(s.Inputs) != 1 {
			t.Errorf("missing staged input for %s", s.Node.ID)
		}
		artifacts := map[string]transfer.Artifact{}
		for _, out := range s.Outputs {
			artifacts[out.Key] = transfer.Artifact{URI: out.Destination, SHA256: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Size: 1}
		}
		visited[s.Node.ID] = true
		return NodeResult{State: "succeeded", Compute: "succeeded", Publication: "succeeded", Cleanup: "complete", Artifacts: artifacts}, nil
	}, workflow.RegisterOptions{Name: "NodeWorkflow"})
	env.RegisterActivityWithOptions(func(_ context.Context, _ string, r Result) error {
		if r.State != "succeeded" {
			t.Errorf("bad persisted result %s", r.State)
		}
		return nil
	}, activity.RegisterOptions{Name: "hybrid.PersistResult"})
	env.ExecuteWorkflow(Run, prepared)
	if e = env.GetWorkflowError(); e != nil {
		t.Fatal(e)
	}
	var result Result
	if e = env.GetWorkflowResult(&result); e != nil || result.State != "succeeded" || len(visited) != 4 {
		t.Fatal(result, e)
	}
}
func TestFailedPublicationBlocksConsumer(t *testing.T) {
	req, cfg := fixture(t)
	prepared, e := Prepare(req, cfg)
	if e != nil {
		t.Fatal(e)
	}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	calls := 0
	env.RegisterWorkflowWithOptions(func(workflow.Context, Stage) (NodeResult, error) {
		calls++
		return NodeResult{State: "collection_failed", Compute: "succeeded", Publication: "failed"}, nil
	}, workflow.RegisterOptions{Name: "NodeWorkflow"})
	env.RegisterActivityWithOptions(func(context.Context, string, Result) error { return nil }, activity.RegisterOptions{Name: "hybrid.PersistResult"})
	env.ExecuteWorkflow(Run, prepared)
	if env.GetWorkflowError() == nil || calls != 1 {
		t.Fatal("consumer ran without publication", calls, env.GetWorkflowError())
	}
}

func TestParallelFanInAndReturnToLocalWorker(t *testing.T) {
	req, cfg := fixture(t)
	plan, e := executioncontract.Validate(req.Plan)
	if e != nil {
		t.Fatal(e)
	}
	placement := plan["placement"].(map[string]any)
	nodes := placement["nodes"].([]any)
	nodes[2].(map[string]any)["depends_on"] = []any{"prepare"}
	nodes[3].(map[string]any)["depends_on"] = []any{"align", "count"}
	finish := map[string]any{}
	for k, v := range nodes[0].(map[string]any) {
		finish[k] = v
	}
	finish["node_id"] = "finish"
	finish["depends_on"] = []any{"report"}
	placement["nodes"] = append(nodes, finish)
	storage := plan["storage"].(map[string]any)
	for _, key := range []string{"artifacts", "routes", "transfers", "publications"} {
		storage[key] = []any{}
	}
	req.Commands["finish"] = Command{Argv: []string{"/bin/true"}, TimeoutSeconds: 60}
	plan["graph_hash"], _ = objectHash(req.Commands)
	plan["review"].(map[string]any)["state_hash"], _ = executioncontract.ExecutionFingerprint(plan)
	req.Plan, _ = json.Marshal(plan)
	prepared, e := Prepare(req, cfg)
	if e != nil {
		t.Fatal(e)
	}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	visited := map[string]bool{}
	active, maxActive := 0, 0
	env.RegisterWorkflowWithOptions(func(ctx workflow.Context, s Stage) (NodeResult, error) {
		for _, dep := range s.Node.Depends {
			if !visited[dep] {
				t.Errorf("%s ran before %s publication", s.Node.ID, dep)
			}
		}
		active++
		if active > maxActive {
			maxActive = active
		}
		if e := workflow.Sleep(ctx, time.Second); e != nil {
			return NodeResult{}, e
		}
		active--
		visited[s.Node.ID] = true
		return NodeResult{State: "succeeded", Compute: "succeeded", Publication: "succeeded", Cleanup: "complete"}, nil
	}, workflow.RegisterOptions{Name: "NodeWorkflow"})
	env.RegisterActivityWithOptions(func(context.Context, string, Result) error { return nil }, activity.RegisterOptions{Name: "hybrid.PersistResult"})
	env.ExecuteWorkflow(Run, prepared)
	if e := env.GetWorkflowError(); e != nil {
		t.Fatal(e)
	}
	if len(visited) != 5 || maxActive != 2 || prepared.Stages["finish"].Node.Kind != "temporal_worker" {
		t.Fatal(visited, maxActive)
	}
}
