package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go-scheduler/fs"
	"go-scheduler/parsing"
	"go-scheduler/workflow"
	"log/slog"
	"net/http"
	"os"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	temporalLog "go.temporal.io/sdk/log"
)

type StartWorkflowRequest struct {
	Schema           string              `json:"schema"`
	ResolvedWorkflow json.RawMessage     `json:"resolved_workflow"`
	WorkerInfo       workflow.WorkerInfo `json:"worker_info"`
	Config           json.RawMessage     `json:"config,omitempty"`
}

type StartWorkflowResponse struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
}

type StopWorkflowRequest struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id,omitempty"`
}

type StopWorkflowResponse struct {
	Message string `json:"message"`
}

type WorkflowStatusRequest struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id,omitempty"`
}

type WorkflowStatusResponse struct {
	WorkflowStatus string         `json:"workflow_status"`
	NodeStatuses   map[int]string `json:"node_statuses"`
}

type Server struct {
	temporalClient client.Client
	logger         *slog.Logger
}

func NewServer(logger *slog.Logger) (*Server, error) {
	c, err := client.NewLazyClient(client.Options{
		HostPort: "localhost:7233",
		Logger:   temporalLog.NewStructuredLogger(logger),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create Temporal client: %w", err)
	}
	return &Server{temporalClient: c, logger: logger}, nil
}

func (s *Server) Close() {
	s.temporalClient.Close()
}

func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/start_workflow", s.handleStartWorkflow)
	mux.HandleFunc("/stop_workflow", s.handleStopWorkflow)
	mux.HandleFunc("/workflow_status", s.handleWorkflowStatus)
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

	workflowOptions := client.StartWorkflowOptions{
		TaskQueue: "bwb_worker",
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

	writeJSON(w, http.StatusOK, StartWorkflowResponse{
		WorkflowID: we.GetID(),
		RunID:      we.GetRunID(),
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

	err := s.temporalClient.TerminateWorkflow(
		r.Context(), req.WorkflowID, req.RunID, "terminated via API",
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to terminate workflow: %s", err))
		return
	}

	writeJSON(w, http.StatusOK, StopWorkflowResponse{
		Message: fmt.Sprintf("workflow %s terminated", req.WorkflowID),
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

	// Terminal workflows can't answer queries; return early with no node detail.
	if isTerminal {
		writeJSON(w, http.StatusOK, WorkflowStatusResponse{
			WorkflowStatus: statusStr,
			NodeStatuses:   nil,
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
