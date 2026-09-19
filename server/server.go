package server

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go-scheduler/fs"
	"go-scheduler/parsing"
	"go-scheduler/workflow"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	temporalLog "go.temporal.io/sdk/log"
)

type StartWorkflowRequest struct {
	Schema           string              `json:"schema"`
	ResolvedWorkflow json.RawMessage     `json:"resolved_workflow"`
	WorkerInfo       workflow.WorkerInfo `json:"worker_info"`
	Config           json.RawMessage     `json:"config,omitempty"`
	RequestID        string              `json:"request_id,omitempty"`
	WorkflowID       string              `json:"workflow_id,omitempty"`
	WorkbenchRunID   string              `json:"workbench_run_id,omitempty"`
	ExecutorID       string              `json:"executor_id,omitempty"`
	SiteProfileID    string              `json:"site_profile_id,omitempty"`
}

type StartWorkflowResponse struct {
	WorkflowID     string `json:"workflow_id"`
	RunID          string `json:"run_id"`
	RequestID      string `json:"request_id"`
	WorkbenchRunID string `json:"workbench_run_id"`
	ExecutorID     string `json:"executor_id"`
	SiteProfileID  string `json:"site_profile_id"`
}

type StopWorkflowRequest struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id,omitempty"`
}

type StopWorkflowResponse struct {
	Message        string `json:"message"`
	WorkflowStatus string `json:"workflow_status"`
}

type WorkflowStatusRequest struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id,omitempty"`
}

type WorkflowStatusResponse struct {
	WorkflowStatus       string                               `json:"workflow_status"`
	RunID                string                               `json:"run_id,omitempty"`
	NodeStatuses         map[int]string                       `json:"node_statuses"`
	NodeAttempts         map[int]int                          `json:"node_attempts,omitempty"`
	SlurmCancellation    *workflow.SlurmCancellationEvidence  `json:"slurm_cancellation,omitempty"`
	RequestID            string                               `json:"request_id,omitempty"`
	WorkflowID           string                               `json:"workflow_id,omitempty"`
	WorkbenchRunID       string                               `json:"workbench_run_id,omitempty"`
	ExecutorID           string                               `json:"executor_id,omitempty"`
	SiteProfileID        string                               `json:"site_profile_id,omitempty"`
	SlurmReconciliations []workflow.SlurmCancellationEvidence `json:"slurm_reconciliations,omitempty"`
	ErrorCategory        string                               `json:"error_category,omitempty"`
	ErrorDetail          string                               `json:"error_detail,omitempty"`
	DeclaredArtifacts    []string                             `json:"declared_artifacts,omitempty"`
	SlurmJobs            []workflow.SlurmJobEvidence          `json:"slurm_jobs,omitempty"`
}

type ReconcileSlurmRequest struct {
	WorkflowID string `json:"workflow_id"`
	Apply      bool   `json:"apply,omitempty"`
}

type ReconcileSlurmResponse struct {
	WorkflowID string                             `json:"workflow_id"`
	Apply      bool                               `json:"apply"`
	Evidence   workflow.SlurmCancellationEvidence `json:"evidence"`
}

type Server struct {
	temporalClient client.Client
	logger         *slog.Logger
	bearerToken    string
	adminToken     string
	evidenceDir    string
}

func NewServer(logger *slog.Logger) (*Server, error) {
	hostPort := strings.TrimSpace(os.Getenv("BWB_TEMPORAL_ADDRESS"))
	if hostPort == "" {
		hostPort = "localhost:7233"
	}
	namespace := strings.TrimSpace(os.Getenv("BWB_TEMPORAL_NAMESPACE"))
	if namespace == "" {
		namespace = "default"
	}
	c, err := client.NewLazyClient(client.Options{
		HostPort:  hostPort,
		Namespace: namespace,
		Logger:    temporalLog.NewStructuredLogger(logger),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create Temporal client: %w", err)
	}
	evidenceDir := strings.TrimSpace(os.Getenv("BWB_EVIDENCE_DIR"))
	if evidenceDir == "" {
		if schedDir := strings.TrimSpace(os.Getenv("BWB_SCHED_DIR")); schedDir != "" {
			evidenceDir = filepath.Join(schedDir, "workflow-evidence")
		}
	}
	return &Server{
		temporalClient: c,
		logger:         logger,
		bearerToken:    strings.TrimSpace(os.Getenv("BWB_API_BEARER_TOKEN")),
		adminToken:     strings.TrimSpace(os.Getenv("BWB_ADMIN_BEARER_TOKEN")),
		evidenceDir:    evidenceDir,
	}, nil
}

func (s *Server) Close() {
	s.temporalClient.Close()
}

func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/start_workflow", s.authenticated(s.handleStartWorkflow))
	mux.HandleFunc("/start_staged_slurm_gpu_workflow", s.authenticated(s.handleStartStagedWorkflow))
	mux.HandleFunc("/staged_slurm_gpu_workflow_status", s.authenticated(s.handleStagedWorkflowStatus))
	mux.HandleFunc("/stop_workflow", s.authenticated(s.handleStopWorkflow))
	mux.HandleFunc("/workflow_status", s.authenticated(s.handleWorkflowStatus))
	mux.HandleFunc("/admin/reconcile_slurm", s.adminAuthenticated(s.handleReconcileSlurm))
}

func (s *Server) authenticated(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.bearerToken != "" {
			const bearerPrefix = "Bearer "
			header := r.Header.Get("Authorization")
			if !strings.HasPrefix(header, bearerPrefix) {
				writeError(w, http.StatusUnauthorized, "missing or invalid bearer token")
				return
			}
			supplied := strings.TrimPrefix(header, bearerPrefix)
			if len(supplied) != len(s.bearerToken) || subtle.ConstantTimeCompare(
				[]byte(supplied), []byte(s.bearerToken),
			) != 1 {
				writeError(w, http.StatusUnauthorized, "missing or invalid bearer token")
				return
			}
		}
		next(w, r)
	}
}

func bearerTokenMatches(r *http.Request, expected string) bool {
	const bearerPrefix = "Bearer "
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, bearerPrefix) {
		return false
	}
	supplied := strings.TrimPrefix(header, bearerPrefix)
	return len(supplied) == len(expected) && subtle.ConstantTimeCompare(
		[]byte(supplied), []byte(expected),
	) == 1
}

func (s *Server) adminAuthenticated(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.adminToken == "" {
			writeError(w, http.StatusServiceUnavailable, "administrator reconciliation is not configured")
			return
		}
		if !bearerTokenMatches(r, s.adminToken) {
			writeError(w, http.StatusUnauthorized, "missing or invalid administrator bearer token")
			return
		}
		next(w, r)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func parseRequestJobConfig(
	raw json.RawMessage, bwbWorkflow *parsing.ResolvedWorkflow,
) (parsing.JobConfig, error) {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return parsing.GetDefaultConfig(bwbWorkflow, false), nil
	}

	var jobConfig parsing.JobConfig
	if err := parsing.ParseJobConfig(raw, &jobConfig); err != nil {
		return parsing.JobConfig{}, err
	}
	return jobConfig, nil
}

var executionIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$`)

func startRequestFingerprint(req StartWorkflowRequest) (string, error) {
	canonicalValue := func(name string, raw json.RawMessage, required bool) (any, error) {
		if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			if required {
				return nil, fmt.Errorf("missing %s", name)
			}
			return nil, nil
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var value any
		if err := decoder.Decode(&value); err != nil {
			return nil, fmt.Errorf("invalid %s: %w", name, err)
		}
		return value, nil
	}
	resolvedWorkflow, err := canonicalValue("resolved_workflow", req.ResolvedWorkflow, true)
	if err != nil {
		return "", err
	}
	config, err := canonicalValue("config", req.Config, false)
	if err != nil {
		return "", err
	}
	payload, err := json.Marshal(struct {
		Schema           string              `json:"schema"`
		ResolvedWorkflow any                 `json:"resolved_workflow"`
		WorkerInfo       workflow.WorkerInfo `json:"worker_info"`
		Config           any                 `json:"config"`
		RequestID        string              `json:"request_id"`
		WorkflowID       string              `json:"workflow_id"`
		WorkbenchRunID   string              `json:"workbench_run_id"`
		ExecutorID       string              `json:"executor_id"`
		SiteProfileID    string              `json:"site_profile_id"`
	}{
		Schema:           req.Schema,
		ResolvedWorkflow: resolvedWorkflow,
		WorkerInfo:       req.WorkerInfo,
		Config:           config,
		RequestID:        req.RequestID,
		WorkflowID:       req.WorkflowID,
		WorkbenchRunID:   req.WorkbenchRunID,
		ExecutorID:       req.ExecutorID,
		SiteProfileID:    req.SiteProfileID,
	})
	if err != nil {
		return "", fmt.Errorf("unable to encode start request: %w", err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func executionIdentityFromMemo(
	fields map[string]*commonpb.Payload,
) (parsing.ExecutionIdentity, error) {
	keys := []struct {
		memoKey string
		target  *string
	}{
		{"bwb_request_id", nil},
		{"bwb_workflow_id", nil},
		{"bwb_workbench_run_id", nil},
		{"bwb_executor_id", nil},
		{"bwb_site_profile_id", nil},
	}
	identity := parsing.ExecutionIdentity{}
	keys[0].target = &identity.RequestID
	keys[1].target = &identity.WorkflowID
	keys[2].target = &identity.WorkbenchRunID
	keys[3].target = &identity.ExecutorID
	keys[4].target = &identity.SiteProfileID
	for _, item := range keys {
		value, err := memoString(fields, item.memoKey)
		if err != nil {
			return parsing.ExecutionIdentity{}, err
		}
		*item.target = value
	}
	return identity, nil
}

func applyIdentityToStatus(response *WorkflowStatusResponse, identity parsing.ExecutionIdentity) {
	response.RequestID = identity.RequestID
	response.WorkflowID = identity.WorkflowID
	response.WorkbenchRunID = identity.WorkbenchRunID
	response.ExecutorID = identity.ExecutorID
	response.SiteProfileID = identity.SiteProfileID
}

func validatedExecutionIdentity(req StartWorkflowRequest) (parsing.ExecutionIdentity, error) {
	identity := parsing.ExecutionIdentity{
		RequestID:      req.RequestID,
		WorkflowID:     req.WorkflowID,
		WorkbenchRunID: req.WorkbenchRunID,
		ExecutorID:     req.ExecutorID,
		SiteProfileID:  req.SiteProfileID,
	}
	fields := []struct {
		name  string
		value string
	}{
		{"request_id", identity.RequestID},
		{"workflow_id", identity.WorkflowID},
		{"workbench_run_id", identity.WorkbenchRunID},
		{"executor_id", identity.ExecutorID},
		{"site_profile_id", identity.SiteProfileID},
	}
	for _, field := range fields {
		if field.value == "" {
			return parsing.ExecutionIdentity{}, fmt.Errorf("%s is required", field.name)
		}
		if !executionIDPattern.MatchString(field.value) {
			return parsing.ExecutionIdentity{}, fmt.Errorf(
				"invalid %s %q", field.name, field.value,
			)
		}
	}
	return identity, nil
}

func memoString(fields map[string]*commonpb.Payload, key string) (string, error) {
	payload, ok := fields[key]
	if !ok {
		return "", nil
	}
	var value string
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &value); err != nil {
		return "", err
	}
	return value, nil
}

func terminalEvidenceFromMemo(
	fields map[string]*commonpb.Payload,
) (*workflow.WorkflowTerminalEvidence, error) {
	payload, ok := fields[workflow.TerminalEvidenceMemoKey]
	if !ok {
		return nil, nil
	}
	var evidence workflow.WorkflowTerminalEvidence
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &evidence); err != nil {
		return nil, err
	}
	return &evidence, nil
}

func (s *Server) handleStartWorkflow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req StartWorkflowRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %s", err))
		return
	}

	if req.Schema != "biodepot.resolved_workflow/v1" {
		writeError(w, http.StatusBadRequest,
			fmt.Sprintf("unsupported schema %q: only \"biodepot.resolved_workflow/v1\" is accepted", req.Schema))
		return
	}

	if len(req.ResolvedWorkflow) == 0 {
		writeError(w, http.StatusBadRequest, "missing resolved_workflow field")
		return
	}

	var bwbWorkflow parsing.ResolvedWorkflow
	if err := json.Unmarshal(req.ResolvedWorkflow, &bwbWorkflow); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid resolved_workflow: %s", err))
		return
	}

	index, err := parsing.ParseAndValidateWorkflow(&bwbWorkflow)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("workflow validation error: %s", err))
		return
	}

	jobConfig, err := parseRequestJobConfig(req.Config, &bwbWorkflow)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid config: %s", err))
		return
	}

	identity, err := validatedExecutionIdentity(req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	workflowID := identity.WorkflowID
	fingerprint, err := startRequestFingerprint(req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	jobConfig.ExecutionIdentity = identity
	jobConfig.EvidenceDir = s.evidenceDir

	workflowOptions := client.StartWorkflowOptions{
		ID:                                       workflowID,
		TaskQueue:                                "bwb_worker",
		WorkflowIDConflictPolicy:                 enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		WorkflowIDReusePolicy:                    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowExecutionErrorWhenAlreadyStarted: false,
		Memo: map[string]interface{}{
			"bwb_request_id":          req.RequestID,
			"bwb_workflow_id":         workflowID,
			"bwb_request_fingerprint": fingerprint,
			"bwb_workbench_run_id":    req.WorkbenchRunID,
			"bwb_executor_id":         req.ExecutorID,
			"bwb_site_profile_id":     req.SiteProfileID,
		},
	}

	workers := map[string]workflow.WorkerInfo{
		req.WorkerInfo.QueueId: req.WorkerInfo,
	}

	sched_dir := os.Getenv("BWB_SCHED_DIR")
	masterFS := fs.LocalFS{
		RootDir: sched_dir,
	}
	we, err := s.temporalClient.ExecuteWorkflow(
		r.Context(), workflowOptions,
		workflow.RunBwbWorkflowV1, "", jobConfig,
		bwbWorkflow, index, workers, masterFS, false,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to start workflow: %s", err))
		return
	}
	if req.RequestID != "" {
		desc, describeErr := s.temporalClient.DescribeWorkflowExecution(
			r.Context(), we.GetID(), we.GetRunID(),
		)
		if describeErr != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf(
				"failed to verify execution identity: %s", describeErr,
			))
			return
		}
		fields := desc.WorkflowExecutionInfo.Memo.GetFields()
		existingRequestID, decodeErr := memoString(fields, "bwb_request_id")
		if decodeErr != nil {
			writeError(w, http.StatusInternalServerError, "failed to decode existing request identity")
			return
		}
		existingFingerprint, decodeErr := memoString(fields, "bwb_request_fingerprint")
		if decodeErr != nil {
			writeError(w, http.StatusInternalServerError, "failed to decode existing request fingerprint")
			return
		}
		if existingRequestID != req.RequestID || existingFingerprint != fingerprint {
			writeError(w, http.StatusConflict, fmt.Sprintf(
				"workflow_id %q already belongs to a different request", we.GetID(),
			))
			return
		}
	}
	if s.evidenceDir != "" {
		if err := workflow.WriteDurableWorkflowRecord(s.evidenceDir, workflow.DurableWorkflowRecord{
			Identity:      identity,
			TemporalRunID: we.GetRunID(),
		}); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf(
				"failed to record Temporal run identity: %s", err,
			))
			return
		}
	}

	writeJSON(w, http.StatusOK, StartWorkflowResponse{
		WorkflowID:     we.GetID(),
		RunID:          we.GetRunID(),
		RequestID:      req.RequestID,
		WorkbenchRunID: req.WorkbenchRunID,
		ExecutorID:     req.ExecutorID,
		SiteProfileID:  req.SiteProfileID,
	})
}

func (s *Server) handleStopWorkflow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req StopWorkflowRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %s", err))
		return
	}

	if req.WorkflowID == "" {
		writeError(w, http.StatusBadRequest, "workflow_id is required")
		return
	}
	desc, err := s.temporalClient.DescribeWorkflowExecution(
		r.Context(), req.WorkflowID, req.RunID,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to describe workflow before cancellation: %s", err))
		return
	}
	if status, terminal := temporalStatusString(desc.WorkflowExecutionInfo.Status); terminal {
		writeJSON(w, http.StatusOK, StopWorkflowResponse{
			Message:        fmt.Sprintf("workflow %s is already %s", req.WorkflowID, status),
			WorkflowStatus: status,
		})
		return
	}

	err = s.temporalClient.CancelWorkflow(r.Context(), req.WorkflowID, req.RunID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to request workflow cancellation: %s", err))
		return
	}

	writeJSON(w, http.StatusOK, StopWorkflowResponse{
		Message:        fmt.Sprintf("workflow %s cancellation requested", req.WorkflowID),
		WorkflowStatus: "CANCEL_REQUESTED",
	})
}

// temporalStatusString maps the Temporal proto enum to the status strings
// used by this API.
func temporalStatusString(s enumspb.WorkflowExecutionStatus) (string, bool) {
	switch s {
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		return "FINISHED", true
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		return "FAILED", true
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		return "CANCELED", true
	case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		return "TERMINATED", true
	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return "TIMED_OUT", true
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		// CONTINUED_AS_NEW means there is a newer run; treat as still running.
		return "RUNNING", false
	default:
		return "RUNNING", false
	}
}

func terminalStatusResponse(
	defaultStatus string, evidence *workflow.WorkflowTerminalEvidence,
	reconciliations []workflow.SlurmCancellationEvidence,
) WorkflowStatusResponse {
	response := WorkflowStatusResponse{WorkflowStatus: defaultStatus}
	if evidence != nil {
		applyIdentityToStatus(&response, evidence.Identity)
		response.NodeStatuses = evidence.NodeStatuses
		response.NodeAttempts = evidence.NodeAttempts
		response.SlurmCancellation = evidence.SlurmCancellation
		response.ErrorCategory = evidence.ErrorCategory
		response.ErrorDetail = evidence.ErrorDetail
		response.DeclaredArtifacts = evidence.DeclaredArtifacts
		response.SlurmJobs = evidence.SlurmJobs
		if evidence.WorkflowStatus != "" {
			response.WorkflowStatus = evidence.WorkflowStatus
		}
	}
	response.SlurmReconciliations = reconciliations
	for i := len(reconciliations) - 1; i >= 0; i-- {
		recovery := reconciliations[i]
		if recovery.DryRun {
			continue
		}
		if recovery.Verified {
			response.WorkflowStatus = "CANCELED"
			response.SlurmCancellation = &recovery
			response.ErrorCategory = ""
			response.ErrorDetail = ""
		}
		break
	}
	return response
}

func (s *Server) handleWorkflowStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req WorkflowStatusRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %s", err))
		return
	}

	if req.WorkflowID == "" {
		writeError(w, http.StatusBadRequest, "workflow_id is required")
		return
	}

	// Always pass "" as runID to DescribeWorkflow so that we follow
	// continues-as-new chains to the latest run automatically.
	desc, err := s.temporalClient.DescribeWorkflowExecution(
		r.Context(), req.WorkflowID, "",
	)
	if err != nil {
		if s.evidenceDir != "" {
			record, recordErr := workflow.ReadDurableWorkflowRecord(s.evidenceDir, req.WorkflowID)
			if recordErr == nil && record.TerminalEvidence != nil {
				response := terminalStatusResponse(
					record.TerminalEvidence.WorkflowStatus,
					record.TerminalEvidence,
					record.Reconciliations,
				)
				response.RunID = record.TemporalRunID
				writeJSON(w, http.StatusOK, response)
				return
			}
		}
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to describe workflow: %s", err))
		return
	}

	execStatus := desc.WorkflowExecutionInfo.Status
	statusStr, isTerminal := temporalStatusString(execStatus)
	identity, identityErr := executionIdentityFromMemo(desc.WorkflowExecutionInfo.Memo.GetFields())
	if identityErr != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf(
			"failed to decode workflow identity: %s", identityErr,
		))
		return
	}

	// Terminal workflows can't answer queries. Their final node and Slurm
	// cleanup evidence is persisted in workflow memo before close.
	if isTerminal {
		evidence, evidenceErr := terminalEvidenceFromMemo(
			desc.WorkflowExecutionInfo.Memo.GetFields(),
		)
		if evidenceErr != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf(
				"failed to decode terminal workflow evidence: %s", evidenceErr,
			))
			return
		}
		var reconciliations []workflow.SlurmCancellationEvidence
		if s.evidenceDir != "" {
			if record, recordErr := workflow.ReadDurableWorkflowRecord(s.evidenceDir, req.WorkflowID); recordErr == nil {
				reconciliations = record.Reconciliations
			}
		}
		response := terminalStatusResponse(statusStr, evidence, reconciliations)
		if response.WorkflowID == "" {
			applyIdentityToStatus(&response, identity)
		}
		response.RunID = desc.WorkflowExecutionInfo.Execution.GetRunId()
		writeJSON(w, http.StatusOK, response)
		return
	}

	// Workflow is running — query for live node statuses.
	// Use "" runID here too so the query goes to the latest run.
	qresp, err := s.temporalClient.QueryWorkflow(
		r.Context(), req.WorkflowID, "", "getNodeStatuses",
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to query workflow: %s", err))
		return
	}

	var nodeStatuses map[int]string
	if err := qresp.Get(&nodeStatuses); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to decode node statuses: %s", err))
		return
	}

	response := WorkflowStatusResponse{
		WorkflowStatus: statusStr,
		NodeStatuses:   nodeStatuses,
		RunID:          desc.WorkflowExecutionInfo.Execution.GetRunId(),
	}
	applyIdentityToStatus(&response, identity)
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleReconcileSlurm(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req ReconcileSlurmRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %s", err))
		return
	}
	if !executionIDPattern.MatchString(req.WorkflowID) {
		writeError(w, http.StatusBadRequest, "a valid workflow_id is required")
		return
	}
	if s.evidenceDir == "" {
		writeError(w, http.StatusServiceUnavailable, "durable workflow evidence is not configured")
		return
	}
	record, err := workflow.ReadDurableWorkflowRecord(s.evidenceDir, req.WorkflowID)
	if err != nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("workflow evidence not found: %s", err))
		return
	}
	if record.SlurmTarget == nil {
		writeError(w, http.StatusConflict, "workflow has no Slurm reconciliation target")
		return
	}
	if record.TerminalEvidence == nil || record.TerminalEvidence.WorkflowStatus != "CANCEL_CLEANUP_FAILED" {
		writeError(w, http.StatusConflict, "workflow does not require Slurm cleanup reconciliation")
		return
	}
	if req.Apply {
		for i := len(record.Reconciliations) - 1; i >= 0; i-- {
			previous := record.Reconciliations[i]
			if previous.DryRun {
				continue
			}
			if previous.Verified {
				writeJSON(w, http.StatusOK, ReconcileSlurmResponse{
					WorkflowID: req.WorkflowID,
					Apply:      true,
					Evidence:   previous,
				})
				return
			}
			break
		}
	}
	reconcileNumber := len(record.Reconciliations) + 1
	reconcileIDSum := sha256.Sum256([]byte(fmt.Sprintf(
		"%s\x00%t\x00%d", req.WorkflowID, req.Apply, reconcileNumber,
	)))
	reconcileID := "slurm-reconcile-" + hex.EncodeToString(reconcileIDSum[:12])
	run, err := s.temporalClient.ExecuteWorkflow(
		r.Context(), client.StartWorkflowOptions{
			ID:                    reconcileID,
			TaskQueue:             workflow.SCHEDULER_QUEUE,
			WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
		workflow.SlurmReconciliationWorkflow,
		record.SlurmTarget.Config,
		record.SlurmTarget.Request,
		req.Apply,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to start reconciliation: %s", err))
		return
	}
	var evidence workflow.SlurmCancellationEvidence
	if err := run.Get(r.Context(), &evidence); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("reconciliation workflow failed: %s", err))
		return
	}
	if _, err := workflow.AppendReconciliationEvidence(
		s.evidenceDir, req.WorkflowID, evidence,
	); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to append reconciliation evidence: %s", err))
		return
	}
	writeJSON(w, http.StatusOK, ReconcileSlurmResponse{
		WorkflowID: req.WorkflowID,
		Apply:      req.Apply,
		Evidence:   evidence,
	})
}

func ListenAndServe(addr string, logger *slog.Logger) error {
	srv, err := NewServer(logger)
	if err != nil {
		return err
	}
	defer srv.Close()

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	logger.Info("starting HTTP server", "addr", addr)
	return http.ListenAndServe(addr, mux)
}
