package server

import (
	"encoding/json"
	"go-scheduler/executioncontract"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func hybridPlan(t *testing.T) map[string]any {
	t.Helper()
	b, err := os.ReadFile("../executioncontract/testdata/contracts-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	v, err := executioncontract.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	return v.(map[string]any)["valid"].(map[string]any)["hybrid"].(map[string]any)
}

func TestPlacementPinnedInStartFingerprint(t *testing.T) {
	p := hybridPlan(t)
	b, _ := json.Marshal(p)
	req := StartWorkflowRequest{Schema: "biodepot.resolved_workflow/v1", ResolvedWorkflow: json.RawMessage(`{"nodes":{}}`),
		ExecutionPlan: b, RequestID: "request-1", WorkflowID: "workflow-1", WorkbenchRunID: "run-1"}
	first, err := startRequestFingerprint(req)
	if err != nil {
		t.Fatal(err)
	}
	p["bundle_hash"] = "sha256:" + strings.Repeat("a", 64)
	req.ExecutionPlan, _ = json.Marshal(p)
	if _, err := startRequestFingerprint(req); err == nil {
		t.Fatal("stale review accepted")
	}
	h, _ := executioncontract.ExecutionFingerprint(p)
	p["review"].(map[string]any)["state_hash"] = h
	req.ExecutionPlan, _ = json.Marshal(p)
	second, err := startRequestFingerprint(req)
	if err != nil || first == second {
		t.Fatal("plan omitted from fingerprint", err)
	}
	req.WorkbenchRunID = "other"
	if _, err := startRequestFingerprint(req); err == nil {
		t.Fatal("mismatched identity accepted")
	}
}

func TestLegacyEndpointRejectsHybridRatherThanIgnoringPlacement(t *testing.T) {
	b, _ := json.Marshal(StartWorkflowRequest{ExecutionPlan: json.RawMessage(`{}`)})
	r := httptest.NewRequest(http.MethodPost, "/start_workflow", strings.NewReader(string(b)))
	w := httptest.NewRecorder()
	// A nil client proves rejection occurs before any Temporal side effect.
	(&Server{}).handleStartWorkflow(w, r)
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "contract-only") {
		t.Fatal(w.Code, w.Body.String())
	}
}
