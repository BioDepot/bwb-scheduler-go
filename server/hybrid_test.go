package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
	"go-scheduler/cloudexec"
	"go-scheduler/executioncontract"
	"go-scheduler/hybrid"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	temporalmocks "go.temporal.io/sdk/mocks"
)

func localHybridFixture(t *testing.T) ([]byte, hybrid.Config) {
	p := hybridPlan(t)
	placement := p["placement"].(map[string]any)
	placement["nodes"] = placement["nodes"].([]any)[:1]
	storage := p["storage"].(map[string]any)
	for _, key := range []string{"artifacts", "routes", "transfers", "publications"} {
		storage[key] = []any{}
	}
	commands := map[string]hybrid.Command{"prepare": {Argv: []string{"true"}, TimeoutSeconds: 60}}
	b, _ := json.Marshal(commands)
	v, _ := executioncontract.Decode(b)
	p["graph_hash"], _ = executioncontract.CanonicalHash(v)
	p["review"].(map[string]any)["state_hash"], _ = executioncontract.ExecutionFingerprint(p)
	b, _ = json.Marshal(p)
	req := hybrid.Request{Schema: "biodepot.hybrid_workflow/v1", Plan: b, Commands: commands}
	raw, e := os.ReadFile("../executioncontract/testdata/contracts-v1.json")
	if e != nil {
		t.Fatal(e)
	}
	var corpus struct {
		Valid map[string]json.RawMessage `json:"valid"`
	}
	json.Unmarshal(raw, &corpus)
	cfg := hybrid.Config{Queue: "hybrid-api-tests", Policy: corpus.Valid["policy"], JournalRoot: t.TempDir(), Profiles: map[string]cloudexec.Profile{}, Sites: map[string]hybrid.Site{"local-a": {Kind: "temporal_worker", Queue: "local-tests", Root: "/work/jobs", ProfileKey: "local-a@1", MaxJobs: 1}}}
	req.ConfigurationHash = hybrid.ConfigurationHash(cfg)
	policy, _ := executioncontract.Decode(cfg.Policy)
	req.PolicyHash, _ = executioncontract.CanonicalHash(policy)
	if _, e = hybrid.Prepare(req, cfg); e != nil {
		t.Fatal(e)
	}
	body, _ := json.Marshal(req)
	return body, cfg
}
func TestHybridSubmissionLostResponseAndOwnerIsolation(t *testing.T) {
	body, cfg := localHybridFixture(t)
	value, _ := executioncontract.Decode(body)
	hash, _ := executioncontract.CanonicalHash(value)
	var req hybrid.Request
	json.Unmarshal(body, &req)
	var plan hybrid.Plan
	json.Unmarshal(req.Plan, &plan)
	c := &temporalmocks.Client{}
	c.On("ExecuteWorkflow", mock.Anything, mock.Anything, hybrid.WorkflowName, mock.Anything).Return(nil, fmt.Errorf("lost response after acceptance")).Once()
	c.On("ExecuteWorkflow", mock.Anything, mock.Anything, hybrid.WorkflowName, mock.Anything).Return(nil, &serviceerror.WorkflowExecutionAlreadyStarted{}).Once()
	payload, _ := converter.GetDefaultDataConverter().ToPayload(hash)
	c.On("DescribeWorkflowExecution", mock.Anything, plan.Identity.WorkflowID, "").Return(&workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: plan.Identity.WorkflowID, RunId: "accepted-run"}, Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{"hybrid_fingerprint": payload}}}}, nil).Once()
	s := &Server{temporalClient: c, bearerToken: "test", hybridConfig: &cfg}
	post := func(body []byte) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		s.handleStartHybrid(w, httptest.NewRequest("POST", "/start_hybrid_workflow", strings.NewReader(string(body))))
		return w
	}
	if w := post(body); w.Code != 502 {
		t.Fatal(w.Code, w.Body.String())
	}
	// A lost-response retry recovers the original policy even after new routing is removed.
	cfg.Sites = map[string]hybrid.Site{}
	if w := post(body); w.Code != 200 || !strings.Contains(w.Body.String(), "accepted-run") {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := post(body); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var changed map[string]any
	json.Unmarshal(body, &changed)
	changed["commands"].(map[string]any)["prepare"].(map[string]any)["argv"] = []string{"false"}
	different, _ := json.Marshal(changed)
	if w := post(different); w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
	w := httptest.NewRecorder()
	s.handleHybridStatus(w, httptest.NewRequest(http.MethodGet, "/hybrid_status?workflow_id="+plan.Identity.WorkflowID+"&owner_id=intruder", nil))
	if w.Code != 404 {
		t.Fatal(w.Code)
	}
	w = httptest.NewRecorder()
	s.handleHybridArtifacts(w, httptest.NewRequest(http.MethodGet, "/hybrid_artifact_access?workflow_id="+plan.Identity.WorkflowID+"&owner_id=intruder", nil))
	if w.Code != 404 {
		t.Fatal(w.Code)
	}
	c.AssertExpectations(t)
}
