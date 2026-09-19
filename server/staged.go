package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go-scheduler/staged"
	"net/http"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

func (s *Server) handleStartStagedWorkflow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req staged.Request
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid staged request: %v", err))
		return
	}
	req.Normalize()
	if req.WorkflowID == "" {
		req.WorkflowID = "staged-slurm-gpu-" + uuid.NewString()
	}
	if err := req.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	canonical, err := json.Marshal(req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	hash := sha256.Sum256(canonical)
	fingerprint := hex.EncodeToString(hash[:])
	run, err := s.temporalClient.ExecuteWorkflow(r.Context(), client.StartWorkflowOptions{
		ID:                       req.WorkflowID,
		TaskQueue:                req.TaskQueue,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		Memo:                     map[string]interface{}{"staged_request_fingerprint": fingerprint},
	}, staged.Run, req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to start staged workflow: %v", err))
		return
	}
	desc, err := s.temporalClient.DescribeWorkflowExecution(r.Context(), run.GetID(), run.GetRunID())
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to verify staged workflow identity: %v", err))
		return
	}
	existing, err := memoString(desc.WorkflowExecutionInfo.Memo.GetFields(), "staged_request_fingerprint")
	if err != nil || existing != fingerprint {
		writeError(w, http.StatusConflict, "workflow_id already belongs to a different staged request")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"workflow_id": run.GetID(), "run_id": run.GetRunID(), "task_queue": req.TaskQueue})
}

func (s *Server) handleStagedWorkflowStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req WorkflowStatusRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || !executionIDPattern.MatchString(req.WorkflowID) {
		writeError(w, http.StatusBadRequest, "valid workflow_id required")
		return
	}
	desc, err := s.temporalClient.DescribeWorkflowExecution(r.Context(), req.WorkflowID, req.RunID)
	if err != nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("workflow not found: %v", err))
		return
	}
	status := "Running"
	switch desc.WorkflowExecutionInfo.Status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		status = "Finished"
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		status = "Failed"
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		status = "Canceled"
	case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		status = "Terminated"
	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		status = "TimedOut"
	}
	response := map[string]interface{}{"workflow_id": req.WorkflowID, "run_id": desc.WorkflowExecutionInfo.Execution.GetRunId(), "workflow_type": "StagedSlurmGpuWorkflow", "workflow_status": status, "node_statuses": map[string]string{}, "condition_results": map[string]string{}, "status_source": "describe"}
	if status == "Running" {
		query, err := s.temporalClient.QueryWorkflow(r.Context(), req.WorkflowID, req.RunID, "get_status")
		if err == nil {
			var stagedStatus staged.Status
			if err := query.Get(&stagedStatus); err == nil {
				response["workflow_status"] = stagedStatus.WorkflowStatus
				response["current_stage"] = stagedStatus.CurrentStage
				response["stages"] = stagedStatus.Stages
				response["artifacts"] = stagedStatus.Artifacts
			}
		}
	} else if status == "Finished" {
		var result staged.Result
		if err := s.temporalClient.GetWorkflow(r.Context(), req.WorkflowID, req.RunID).Get(r.Context(), &result); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to read staged result: %v", err))
			return
		}
		response["result"] = result
	}
	writeJSON(w, http.StatusOK, response)
}
