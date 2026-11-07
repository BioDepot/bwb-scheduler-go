package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"

	"go-scheduler/cloudexec"
	"go-scheduler/durable"
	"go-scheduler/executioncontract"
	"go-scheduler/hybrid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

type hybridSubmission struct {
	ActiveCollection string          `json:"active_collection,omitempty"`
	CollectionRunID  string          `json:"collection_run_id,omitempty"`
	Owner            string          `json:"owner_id"`
	Fingerprint      string          `json:"fingerprint"`
	Prepared         hybrid.Prepared `json:"prepared"`
	RunID            string          `json:"temporal_run_id,omitempty"`
}
type hybridIdentity struct {
	WorkflowID string `json:"workflow_id"`
	Owner      string `json:"owner_id"`
}

func (s *Server) hybridEnabled(w http.ResponseWriter) bool {
	if s.hybridConfig == nil || s.bearerToken == "" {
		writeError(w, http.StatusServiceUnavailable, "hybrid execution requires configured policy and authenticated service transport")
		return false
	}
	return true
}
func (s *Server) handleCapabilities(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]any{"schema": "biodepot.scheduler_capabilities/v1", "legacy": true, "hybrid": s.hybridConfig != nil && s.bearerToken != "", "hybrid_schema": "biodepot.hybrid_workflow/v1", "artifact_transport": "worker_to_s3", "review_authority": "workbench", "checkpoints": "reviewed_qc_and_manual/v1"})
}

// This endpoint uses only read APIs. Runner readiness is checked inside the
// stage after allocation; a passing control-plane probe cannot attest runtime.
func (s *Server) handleValidateHybrid(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, 405, "POST required")
		return
	}
	if !s.hybridEnabled(w) {
		return
	}
	raw, e := io.ReadAll(http.MaxBytesReader(w, r.Body, 2*1024*1024))
	if e != nil {
		writeError(w, 400, "invalid request")
		return
	}
	req, e := hybrid.DecodeRequest(raw)
	if e != nil {
		writeError(w, 400, e.Error())
		return
	}
	prepared, e := hybrid.Prepare(req, *s.hybridConfig)
	if e != nil {
		writeError(w, 400, e.Error())
		return
	}
	c := cloudexec.Controller{Profiles: s.hybridConfig.Profiles, Clients: cloudexec.SDKClients}
	probes := map[string]cloudexec.Probe{}
	ready := true
	for _, id := range prepared.Order {
		stage := prepared.Stages[id]
		if stage.Cloud == nil {
			continue
		}
		probe, err := c.Probe(r.Context(), stage.Cloud.Identity)
		if err != nil {
			probe.Blockers = append(probe.Blockers, err.Error())
			probe.Ready = false
		}
		probes[id] = probe
		ready = ready && probe.Ready
	}
	policy, _ := executioncontract.Decode(s.hybridConfig.Policy)
	policyHash, _ := executioncontract.CanonicalHash(policy)
	json.NewEncoder(w).Encode(map[string]any{"ready": ready, "target_runtime_verified": false, "probes": probes, "policy_hash": policyHash, "configuration_hash": hybrid.ConfigurationHash(*s.hybridConfig), "sites": s.hybridConfig.Sites})
}
func (s *Server) handleStartHybrid(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	if !s.hybridEnabled(w) {
		return
	}
	raw, e := io.ReadAll(http.MaxBytesReader(w, r.Body, 4*1024*1024))
	if e != nil {
		writeError(w, http.StatusBadRequest, "request too large")
		return
	}
	req, e := hybrid.DecodeRequest(raw)
	if e != nil {
		writeError(w, http.StatusBadRequest, e.Error())
		return
	}
	var requested hybrid.Plan
	if e = json.Unmarshal(req.Plan, &requested); e != nil {
		writeError(w, 400, "invalid plan")
		return
	}
	var prior hybridSubmission
	var prepared hybrid.Prepared
	if err := (durable.Store{Root: s.hybridConfig.JournalRoot}).Read(r.Context(), "hybrid-submission/"+requested.Identity.WorkflowID, &prior); err == nil {
		prepared = prior.Prepared
	} else if !os.IsNotExist(err) {
		writeError(w, 503, "submission journal unavailable")
		return
	} else {
		if req.ConfigurationHash == "" {
			writeError(w, 400, "reviewed executor configuration hash required")
			return
		}
		prepared, e = hybrid.Prepare(req, *s.hybridConfig)
		if e != nil {
			writeError(w, 400, e.Error())
			return
		}
	}
	value, e := executioncontract.Decode(raw)
	if e != nil {
		writeError(w, http.StatusBadRequest, e.Error())
		return
	}
	fingerprint, e := executioncontract.CanonicalHash(value)
	if e != nil {
		writeError(w, http.StatusBadRequest, e.Error())
		return
	}
	store := durable.Store{Root: s.hybridConfig.JournalRoot}
	var submission hybridSubmission
	conflict := false
	e = store.With(r.Context(), "hybrid-submission/"+prepared.Plan.Identity.WorkflowID, func(entry *durable.Entry) error {
		err := entry.Read(&submission)
		if err == nil {
			if submission.Fingerprint != fingerprint || submission.Owner != prepared.Plan.Identity.Owner {
				conflict = true
				return fmt.Errorf("workflow identity conflicts with recorded request")
			}
			if submission.RunID != "" {
				return nil
			}
		} else if !os.IsNotExist(err) {
			return err
		}
		submission = hybridSubmission{Owner: prepared.Plan.Identity.Owner, Fingerprint: fingerprint, Prepared: prepared}
		if err = entry.Write(submission); err != nil {
			return err
		}
		run, err := s.temporalClient.ExecuteWorkflow(r.Context(), client.StartWorkflowOptions{ID: prepared.Plan.Identity.WorkflowID, TaskQueue: s.hybridConfig.Queue, WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE, Memo: map[string]any{"hybrid_fingerprint": fingerprint, "owner_id": submission.Owner}}, hybrid.WorkflowName, prepared)
		if err != nil {
			var already *serviceerror.WorkflowExecutionAlreadyStarted
			if !errors.As(err, &already) {
				return err
			}
			description, err := s.temporalClient.DescribeWorkflowExecution(r.Context(), prepared.Plan.Identity.WorkflowID, "")
			if err != nil {
				return err
			}
			prior, err := memoString(description.WorkflowExecutionInfo.Memo.GetFields(), "hybrid_fingerprint")
			if err != nil {
				return err
			}
			if prior != fingerprint {
				conflict = true
				return fmt.Errorf("Temporal workflow content conflicts")
			}
			submission.RunID = description.WorkflowExecutionInfo.Execution.RunId
		} else {
			submission.RunID = run.GetRunID()
		}
		return entry.Write(submission)
	})
	if e != nil {
		code := http.StatusBadGateway
		if conflict {
			code = http.StatusConflict
		}
		writeError(w, code, e.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"schema": "biodepot.hybrid_submission/v1", "workflow_id": prepared.Plan.Identity.WorkflowID, "run_id": submission.RunID, "request_id": prepared.Plan.Identity.RequestID, "workbench_run_id": prepared.Plan.Identity.RunID, "fingerprint": fingerprint})
}
func (s *Server) ownedHybrid(ctx context.Context, id hybridIdentity) (hybridSubmission, error) {
	var record hybridSubmission
	if id.Owner == "" || id.WorkflowID == "" {
		return record, fmt.Errorf("owner and workflow IDs required")
	}
	e := (durable.Store{Root: s.hybridConfig.JournalRoot}).Read(ctx, "hybrid-submission/"+id.WorkflowID, &record)
	if e != nil || record.Owner != id.Owner {
		return hybridSubmission{}, fmt.Errorf("execution not found for owner")
	}
	return record, nil
}
func (s *Server) handleHybridStatus(w http.ResponseWriter, r *http.Request) {
	if !s.hybridEnabled(w) {
		return
	}
	id := hybridIdentity{WorkflowID: r.URL.Query().Get("workflow_id"), Owner: r.URL.Query().Get("owner_id")}
	record, e := s.ownedHybrid(r.Context(), id)
	if e != nil {
		writeError(w, http.StatusNotFound, e.Error())
		return
	}
	workflowID, runID := id.WorkflowID, record.RunID
	if record.ActiveCollection != "" {
		workflowID, runID = record.ActiveCollection, record.CollectionRunID
	}
	encoded, e := s.temporalClient.QueryWorkflow(r.Context(), workflowID, runID, "status")
	var result hybrid.Result
	if e == nil {
		e = encoded.Get(&result)
	}
	if e != nil {
		e = (durable.Store{Root: s.hybridConfig.JournalRoot}).Read(r.Context(), "hybrid-result/"+id.WorkflowID, &result)
	}
	if e != nil {
		writeError(w, http.StatusServiceUnavailable, "execution status unknown; retry reconciliation")
		return
	}
	json.NewEncoder(w).Encode(result)
}
func (s *Server) handleHybridCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	if !s.hybridEnabled(w) {
		return
	}
	var id hybridIdentity
	if e := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&id); e != nil {
		writeError(w, http.StatusBadRequest, "invalid identity")
		return
	}
	record, e := s.ownedHybrid(r.Context(), id)
	if e != nil {
		writeError(w, http.StatusNotFound, e.Error())
		return
	}
	workflowID, runID := id.WorkflowID, record.RunID
	if record.ActiveCollection != "" {
		workflowID, runID = record.ActiveCollection, record.CollectionRunID
	}
	if e = s.temporalClient.CancelWorkflow(r.Context(), workflowID, runID); e != nil {
		writeError(w, http.StatusBadGateway, "cancellation request failed")
		return
	}
	json.NewEncoder(w).Encode(map[string]any{"state": "cancellation_requested", "workflow_id": id.WorkflowID})
}
