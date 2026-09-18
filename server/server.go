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
	RequestID      string `json:"request_id,omitempty"`
	WorkbenchRunID string `json:"workbench_run_id,omitempty"`
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
	WorkflowStatus    string                              `json:"workflow_status"`
	NodeStatuses      map[int]string                      `json:"node_statuses"`
	SlurmCancellation *workflow.SlurmCancellationEvidence `json:"slurm_cancellation,omitempty"`
}

type Server struct {
	temporalClient client.Client
	logger         *slog.Logger
	bearerToken    string
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
	return &Server{
		temporalClient: c,
		logger:         logger,
		bearerToken:    strings.TrimSpace(os.Getenv("BWB_API_BEARER_TOKEN")),
	}, nil
}

func (s *Server) Close() {
	s.temporalClient.Close()
}

func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/start_workflow", s.authenticated(s.handleStartWorkflow))
	mux.HandleFunc("/stop_workflow", s.authenticated(s.handleStopWorkflow))
	mux.HandleFunc("/workflow_status", s.authenticated(s.handleWorkflowStatus))
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
	payload, err := json.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("unable to encode start request: %w", err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
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

	fingerprint, err := startRequestFingerprint(req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	workflowID := req.WorkflowID
	if workflowID == "" && req.RequestID != "" {
		workflowID = "morphic-" + req.RequestID
	}
	for field, value := range map[string]string{
		"request_id":  req.RequestID,
		"workflow_id": workflowID,
	} {
		if value != "" && !executionIDPattern.MatchString(value) {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid %s %q", field, value))
			return
		}
	}

	workflowOptions := client.StartWorkflowOptions{
		ID:                                       workflowID,
		TaskQueue:                                "bwb_worker",
		WorkflowIDConflictPolicy:                 enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		WorkflowIDReusePolicy:                    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowExecutionErrorWhenAlreadyStarted: false,
		Memo: map[string]interface{}{
			"bwb_request_id":          req.RequestID,
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

	writeJSON(w, http.StatusOK, StartWorkflowResponse{
		WorkflowID:     we.GetID(),
		RunID:          we.GetRunID(),
		RequestID:      req.RequestID,
		WorkbenchRunID: req.WorkbenchRunID,
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
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to describe workflow: %s", err))
		return
	}

	execStatus := desc.WorkflowExecutionInfo.Status
	statusStr, isTerminal := temporalStatusString(execStatus)

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
		response := WorkflowStatusResponse{WorkflowStatus: statusStr}
		if evidence != nil {
			response.NodeStatuses = evidence.NodeStatuses
			response.SlurmCancellation = evidence.SlurmCancellation
			if evidence.WorkflowStatus == "CANCEL_CLEANUP_FAILED" {
				response.WorkflowStatus = evidence.WorkflowStatus
			}
		}
		writeJSON(w, http.StatusOK, WorkflowStatusResponse{
			WorkflowStatus:    response.WorkflowStatus,
			NodeStatuses:      response.NodeStatuses,
			SlurmCancellation: response.SlurmCancellation,
		})
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

	writeJSON(w, http.StatusOK, WorkflowStatusResponse{
		WorkflowStatus: statusStr,
		NodeStatuses:   nodeStatuses,
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
