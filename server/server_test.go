package server

import (
	"encoding/json"
	"errors"
	"go-scheduler/parsing"
	"go-scheduler/workflow"
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

func TestParseRequestJobConfigUsesPublicSiteProfileContract(t *testing.T) {
	raw := json.RawMessage(`{
        "executors": {
            "slurm": {
                "ip_addr": "10.159.4.53:22",
                "user": "lhhung",
                "transfer_addr": "10.159.4.53",
                "sched_dir": "/srv/slurm_mnt/data",
                "cmd_prefix": ""
            }
        },
        "configs": {
            "bulk_rna": {
                "executor": "slurm",
                "annotations": {
                    "partition": "normal",
                    "mem": "4G",
                    "cpus_per_task": 2
                }
            }
        },
        "node_configs": {"100": "bulk_rna"}
    }`)

	config, err := parseRequestJobConfig(raw, &parsing.ResolvedWorkflow{})
	if err != nil {
		t.Fatalf("parseRequestJobConfig returned error: %v", err)
	}
	if config.SlurmExecutor.IpAddr != "10.159.4.53:22" {
		t.Fatalf("unexpected Slurm endpoint: %q", config.SlurmExecutor.IpAddr)
	}
	if got := config.ExecTypeByNode[100]; got != parsing.EXEC_SLURM {
		t.Fatalf("node 100 executor = %v, want EXEC_SLURM", got)
	}
	nodeConfig := config.SlurmConfigsByNode[100]
	if nodeConfig.Partition == nil || *nodeConfig.Partition != "normal" {
		t.Fatalf("node 100 partition was not parsed: %#v", nodeConfig.Partition)
	}
	if nodeConfig.Mem == nil || *nodeConfig.Mem != "4G" {
		t.Fatalf("node 100 memory was not parsed: %#v", nodeConfig.Mem)
	}
	if nodeConfig.CpusPerTask == nil || *nodeConfig.CpusPerTask != 2 {
		t.Fatalf("node 100 CPUs were not parsed: %#v", nodeConfig.CpusPerTask)
	}
}

func TestParseRequestJobConfigRejectsInvalidSiteProfile(t *testing.T) {
	raw := json.RawMessage(`{
        "executors": {"slurm": {}},
        "configs": {},
        "node_configs": {}
    }`)

	if _, err := parseRequestJobConfig(raw, &parsing.ResolvedWorkflow{}); err == nil {
		t.Fatal("parseRequestJobConfig accepted an invalid Slurm site profile")
	}
}

func TestStopWorkflowRequestsCooperativeCancellation(t *testing.T) {
	temporalClient := &temporalmocks.Client{}
	temporalClient.On(
		"DescribeWorkflowExecution", mock.Anything, "workflow-1", "run-1",
	).Return(&workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
	}, nil).Once()
	temporalClient.On(
		"CancelWorkflow", mock.Anything, "workflow-1", "run-1",
	).Return(nil).Once()
	server := &Server{temporalClient: temporalClient}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodPost,
		"/stop_workflow",
		strings.NewReader(`{"workflow_id":"workflow-1","run_id":"run-1"}`),
	)

	server.handleStopWorkflow(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected stop status %d: %s", recorder.Code, recorder.Body.String())
	}
	var response StopWorkflowResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed decoding stop response: %v", err)
	}
	if response.WorkflowStatus != "CANCEL_REQUESTED" {
		t.Fatalf("unexpected stop response: %#v", response)
	}
	temporalClient.AssertExpectations(t)
}

func TestStopWorkflowReturnsExistingTerminalStatus(t *testing.T) {
	temporalClient := &temporalmocks.Client{}
	temporalClient.On(
		"DescribeWorkflowExecution", mock.Anything, "workflow-1", "",
	).Return(&workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		},
	}, nil).Once()
	server := &Server{temporalClient: temporalClient}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodPost,
		"/stop_workflow",
		strings.NewReader(`{"workflow_id":"workflow-1"}`),
	)

	server.handleStopWorkflow(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected stop status %d: %s", recorder.Code, recorder.Body.String())
	}
	var response StopWorkflowResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed decoding stop response: %v", err)
	}
	if response.WorkflowStatus != "CANCELED" {
		t.Fatalf("unexpected stop response: %#v", response)
	}
	temporalClient.AssertExpectations(t)
}

func TestRegisteredRoutesRequireConfiguredBearerToken(t *testing.T) {
	server := &Server{bearerToken: "test-secret"}
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	unauthorized := httptest.NewRecorder()
	mux.ServeHTTP(unauthorized, httptest.NewRequest(
		http.MethodPost, "/workflow_status", strings.NewReader(`{}`),
	))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("request without token returned %d", unauthorized.Code)
	}

	rawTokenRequest := httptest.NewRequest(
		http.MethodPost, "/workflow_status", strings.NewReader(`{}`),
	)
	rawTokenRequest.Header.Set("Authorization", "test-secret")
	rawToken := httptest.NewRecorder()
	mux.ServeHTTP(rawToken, rawTokenRequest)
	if rawToken.Code != http.StatusUnauthorized {
		t.Fatalf("request without Bearer scheme returned %d", rawToken.Code)
	}

	authorizedRequest := httptest.NewRequest(
		http.MethodPost, "/workflow_status", strings.NewReader(`{}`),
	)
	authorizedRequest.Header.Set("Authorization", "Bearer test-secret")
	authorized := httptest.NewRecorder()
	mux.ServeHTTP(authorized, authorizedRequest)
	if authorized.Code != http.StatusBadRequest {
		t.Fatalf("authorized request did not reach handler: %d", authorized.Code)
	}
}

func TestAdminReconciliationRequiresSeparateToken(t *testing.T) {
	server := &Server{bearerToken: "ordinary", adminToken: "administrator"}
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	ordinaryRequest := httptest.NewRequest(
		http.MethodPost, "/admin/reconcile_slurm", strings.NewReader(`{"workflow_id":"workflow-1"}`),
	)
	ordinaryRequest.Header.Set("Authorization", "Bearer ordinary")
	ordinary := httptest.NewRecorder()
	mux.ServeHTTP(ordinary, ordinaryRequest)
	if ordinary.Code != http.StatusUnauthorized {
		t.Fatalf("ordinary bearer reached administrator endpoint: %d", ordinary.Code)
	}

	adminRequest := httptest.NewRequest(
		http.MethodPost, "/admin/reconcile_slurm", strings.NewReader(`{"workflow_id":"workflow-1"}`),
	)
	adminRequest.Header.Set("Authorization", "Bearer administrator")
	admin := httptest.NewRecorder()
	mux.ServeHTTP(admin, adminRequest)
	if admin.Code != http.StatusServiceUnavailable {
		t.Fatalf("administrator token did not reach handler: %d", admin.Code)
	}
}

func TestWorkflowStatusUsesDurableFallback(t *testing.T) {
	root := t.TempDir()
	identity := parsing.ExecutionIdentity{
		RequestID: "request-1", WorkflowID: "workflow-1",
		WorkbenchRunID: "workbench-1", ExecutorID: "executor-1", SiteProfileID: "site-1",
	}
	if err := workflow.WriteDurableWorkflowRecord(root, workflow.DurableWorkflowRecord{
		Identity: identity,
		TerminalEvidence: &workflow.WorkflowTerminalEvidence{
			WorkflowStatus: "CANCELED",
			NodeStatuses:   map[int]string{10: "CANCELED"},
			Identity:       identity,
		},
	}); err != nil {
		t.Fatal(err)
	}
	temporalClient := &temporalmocks.Client{}
	temporalClient.On(
		"DescribeWorkflowExecution", mock.Anything, "workflow-1", "",
	).Return((*workflowservice.DescribeWorkflowExecutionResponse)(nil), errors.New("not retained")).Once()
	server := &Server{temporalClient: temporalClient, evidenceDir: root}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodPost, "/workflow_status", strings.NewReader(`{"workflow_id":"workflow-1"}`),
	)
	server.handleWorkflowStatus(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("durable fallback returned %d: %s", recorder.Code, recorder.Body.String())
	}
	var response WorkflowStatusResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.WorkflowStatus != "CANCELED" || response.NodeStatuses[10] != "CANCELED" || response.SiteProfileID != "site-1" {
		t.Fatalf("unexpected fallback response: %#v", response)
	}
	temporalClient.AssertExpectations(t)
}

func TestTerminalEvidenceMemoRoundTrip(t *testing.T) {
	expected := workflow.WorkflowTerminalEvidence{
		WorkflowStatus: "CANCELED",
		NodeStatuses:   map[int]string{10: "RUNNING"},
		SlurmCancellation: &workflow.SlurmCancellationEvidence{
			RequestedJobIDs:     []string{"123"},
			VerifiedTerminalIDs: []string{"123"},
			CleanupStatus:       "failed",
			Verified:            false,
			Error:               "scheduler unreachable",
		},
	}
	payload, err := converter.GetDefaultDataConverter().ToPayload(expected)
	if err != nil {
		t.Fatalf("failed encoding evidence: %v", err)
	}
	actual, err := terminalEvidenceFromMemo(map[string]*commonpb.Payload{
		workflow.TerminalEvidenceMemoKey: payload,
	})
	if err != nil {
		t.Fatalf("failed decoding evidence: %v", err)
	}
	if actual == nil || actual.WorkflowStatus != expected.WorkflowStatus ||
		actual.NodeStatuses[10] != "RUNNING" || actual.SlurmCancellation == nil ||
		actual.SlurmCancellation.Verified ||
		actual.SlurmCancellation.Error != "scheduler unreachable" {
		t.Fatalf("unexpected terminal evidence: %#v", actual)
	}
}

func TestStartRequestFingerprintDetectsMaterialChanges(t *testing.T) {
	request := StartWorkflowRequest{
		Schema:           "biodepot.resolved_workflow/v1",
		ResolvedWorkflow: json.RawMessage(`{"nodes":{}}`),
		RequestID:        "request-1",
		WorkflowID:       "workflow-1",
		WorkbenchRunID:   "workbench-1",
		ExecutorID:       "executor-1",
		SiteProfileID:    "site-1",
	}
	first, err := startRequestFingerprint(request)
	if err != nil {
		t.Fatalf("failed fingerprinting request: %v", err)
	}
	second, _ := startRequestFingerprint(request)
	if second != first {
		t.Fatalf("request fingerprint is not deterministic: %q != %q", second, first)
	}
	request.SiteProfileID = "other-site"
	changed, _ := startRequestFingerprint(request)
	if changed == first {
		t.Fatal("materially different request retained the same fingerprint")
	}
}

func TestStartRequestFingerprintCanonicalizesJSON(t *testing.T) {
	first := StartWorkflowRequest{
		Schema:           "biodepot.resolved_workflow/v1",
		ResolvedWorkflow: json.RawMessage(`{"nodes":{"2":{"name":"b"},"1":{"name":"a"}}}`),
		Config:           json.RawMessage(`{"b":2,"a":1}`),
		RequestID:        "request-1",
		WorkflowID:       "workflow-1",
		WorkbenchRunID:   "workbench-1",
		ExecutorID:       "executor-1",
		SiteProfileID:    "site-1",
	}
	second := first
	second.ResolvedWorkflow = json.RawMessage(`{
		"nodes": {"1": {"name": "a"}, "2": {"name": "b"}}
	}`)
	second.Config = json.RawMessage(`{ "a": 1, "b": 2 }`)

	firstFingerprint, err := startRequestFingerprint(first)
	if err != nil {
		t.Fatalf("failed fingerprinting first request: %v", err)
	}
	secondFingerprint, err := startRequestFingerprint(second)
	if err != nil {
		t.Fatalf("failed fingerprinting second request: %v", err)
	}
	if firstFingerprint != secondFingerprint {
		t.Fatalf("semantically equivalent JSON produced different fingerprints: %q != %q", firstFingerprint, secondFingerprint)
	}
}

func TestValidatedExecutionIdentityRequiresAllFields(t *testing.T) {
	valid := StartWorkflowRequest{
		RequestID:      "request-1",
		WorkflowID:     "workflow-1",
		WorkbenchRunID: "workbench-1",
		ExecutorID:     "executor-1",
		SiteProfileID:  "site-1",
	}
	if _, err := validatedExecutionIdentity(valid); err != nil {
		t.Fatalf("valid identity rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*StartWorkflowRequest)
		want   string
	}{
		{"request", func(r *StartWorkflowRequest) { r.RequestID = "" }, "request_id is required"},
		{"workflow", func(r *StartWorkflowRequest) { r.WorkflowID = "" }, "workflow_id is required"},
		{"workbench", func(r *StartWorkflowRequest) { r.WorkbenchRunID = "" }, "workbench_run_id is required"},
		{"executor", func(r *StartWorkflowRequest) { r.ExecutorID = "" }, "executor_id is required"},
		{"site", func(r *StartWorkflowRequest) { r.SiteProfileID = "" }, "site_profile_id is required"},
		{"malformed", func(r *StartWorkflowRequest) { r.ExecutorID = "bad value" }, `invalid executor_id "bad value"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := valid
			tc.mutate(&request)
			if _, err := validatedExecutionIdentity(request); err == nil || err.Error() != tc.want {
				t.Fatalf("validation error = %v, want %q", err, tc.want)
			}
		})
	}
}
