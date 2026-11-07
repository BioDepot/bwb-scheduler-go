package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"regexp"
	"time"

	"go-scheduler/durable"
	"go-scheduler/hybrid"
)

type checkpointRequest struct {
	hybridIdentity
	Decision hybrid.CheckpointDecision `json:"decision"`
}
type checkpointDelivery struct {
	Request checkpointRequest `json:"request"`
	Sent    bool              `json:"sent"`
}

// The authenticated service transport is the Workbench trust boundary. User
// approval is minted only by its trusted host/native elicitation adapter.
func (s *Server) handleCheckpointDecision(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, 405, "POST required")
		return
	}
	if !s.hybridEnabled(w) {
		return
	}
	var req checkpointRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8192))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		writeError(w, 400, "invalid checkpoint decision")
		return
	}
	id := regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)
	hash := regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
	d := req.Decision
	if !id.MatchString(d.ID) || !id.MatchString(d.DecisionID) || !hash.MatchString(d.StateHash) || !hash.MatchString(d.ReceiptHash) || d.Actor == "" || len(d.Actor) > 1024 || (d.Action != "approve" && d.Action != "decline") {
		writeError(w, 400, "invalid checkpoint decision fields")
		return
	}
	record, err := s.ownedHybrid(r.Context(), req.hybridIdentity)
	if err != nil {
		writeError(w, 404, err.Error())
		return
	}
	workflowID, runID := req.WorkflowID, record.RunID
	if record.ActiveCollection != "" {
		workflowID, runID = record.ActiveCollection, record.CollectionRunID
	}
	known := false
	for _, gate := range record.Prepared.Plan.Checkpoints {
		if gate.ID == d.ID && gate.Mode == "manual" {
			known = true
		}
	}
	if !known {
		writeError(w, 400, "checkpoint is not a reviewed manual gate")
		return
	}
	conflict := false
	err = (durable.Store{Root: s.hybridConfig.JournalRoot}).With(r.Context(), "hybrid-checkpoint/"+req.WorkflowID+"/"+d.ID, func(entry *durable.Entry) error {
		var delivery checkpointDelivery
		prior := entry.Read(&delivery)
		if prior == nil {
			if !reflect.DeepEqual(delivery.Request, req) {
				conflict = true
				return fmt.Errorf("checkpoint already has a different recorded decision")
			}
			if delivery.Sent {
				return nil
			}
		} else if !os.IsNotExist(prior) {
			return prior
		}
		encoded, e := s.temporalClient.QueryWorkflow(r.Context(), workflowID, runID, "status")
		if e != nil {
			return e
		}
		var result hybrid.Result
		if e = encoded.Get(&result); e != nil {
			return e
		}
		gate, ok := result.Checkpoints[d.ID]
		// Recover a response lost after Temporal consumed this exact signal.
		if ok && gate.Decision != nil && reflect.DeepEqual(*gate.Decision, d) {
			return entry.Write(checkpointDelivery{Request: req, Sent: true})
		}
		if !ok || gate.State != "waiting_for_approval" || gate.StateHash != d.StateHash || !time.Now().Before(gate.ExpiresAt) {
			conflict = true
			return fmt.Errorf("checkpoint is not waiting for this exact evidence review")
		}
		if e = entry.Write(checkpointDelivery{Request: req}); e != nil {
			return e
		}
		if e = s.temporalClient.SignalWorkflow(r.Context(), workflowID, runID, hybrid.CheckpointSignal, d); e != nil {
			return e
		}
		return entry.Write(checkpointDelivery{Request: req, Sent: true})
	})
	if err != nil {
		code := 503
		if conflict {
			code = 409
		}
		writeError(w, code, err.Error())
		return
	}
	json.NewEncoder(w).Encode(map[string]any{"state": "decision_delivered", "workflow_id": req.WorkflowID, "checkpoint_id": d.ID, "decision_id": d.DecisionID})
}
