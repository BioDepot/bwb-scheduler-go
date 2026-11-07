package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go-scheduler/durable"
	"go-scheduler/hybrid"
	temporalmocks "go.temporal.io/sdk/mocks"
)

type checkpointEncoded struct{ Result hybrid.Result }

func (v checkpointEncoded) HasValue() bool { return true }
func (v checkpointEncoded) Get(dst interface{}) error {
	raw, _ := json.Marshal(v.Result)
	return json.Unmarshal(raw, dst)
}

func TestCheckpointDecisionOwnerConflictAndLostSignalResponse(t *testing.T) {
	_, cfg := localHybridFixture(t)
	c := &temporalmocks.Client{}
	s := &Server{temporalClient: c, bearerToken: "test", hybridConfig: &cfg}
	original := hybridSubmission{Owner: "alice", RunID: "run-1", Prepared: hybrid.Prepared{Plan: hybrid.Plan{Checkpoints: []hybrid.Checkpoint{{ID: "pilot", Mode: "manual"}}}}}
	if e := (durable.Store{Root: cfg.JournalRoot}).Write(context.Background(), "hybrid-submission/wf", original); e != nil {
		t.Fatal(e)
	}
	decision := hybrid.CheckpointDecision{ID: "pilot", StateHash: "sha256:" + strings.Repeat("a", 64), DecisionID: "human-1", Action: "approve", Actor: "alice", ReceiptHash: "sha256:" + strings.Repeat("b", 64)}
	waiting := checkpointEncoded{Result: hybrid.Result{State: "waiting_for_approval", Checkpoints: map[string]hybrid.CheckpointResult{"pilot": {State: "waiting_for_approval", StateHash: decision.StateHash, ExpiresAt: time.Now().Add(time.Hour)}}}}
	c.On("QueryWorkflow", mock.Anything, "wf", "run-1", "status").Return(waiting, nil).Once()
	c.On("SignalWorkflow", mock.Anything, "wf", "run-1", hybrid.CheckpointSignal, decision).Return(fmt.Errorf("response lost after delivery")).Once()
	accepted := waiting
	accepted.Result.Checkpoints = map[string]hybrid.CheckpointResult{"pilot": {State: "approved", Decision: &decision}}
	c.On("QueryWorkflow", mock.Anything, "wf", "run-1", "status").Return(accepted, nil).Once()
	post := func(owner string, d hybrid.CheckpointDecision) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(checkpointRequest{hybridIdentity: hybridIdentity{WorkflowID: "wf", Owner: owner}, Decision: d})
		w := httptest.NewRecorder()
		s.handleCheckpointDecision(w, httptest.NewRequest("POST", "/hybrid_checkpoint_decision", strings.NewReader(string(raw))))
		return w
	}
	if w := post("bob", decision); w.Code != 404 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := post("alice", decision); w.Code != 503 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := post("alice", decision); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := post("alice", decision); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	changed := decision
	changed.Action = "decline"
	changed.DecisionID = "other"
	if w := post("alice", changed); w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
	c.AssertExpectations(t)
}

func TestCheckpointRejectsUnreadyStaleAndExpired(t *testing.T) {
	for _, state := range []string{"missing", "stale", "expired", "declined"} {
		t.Run(state, func(t *testing.T) {
			_, cfg := localHybridFixture(t)
			c := &temporalmocks.Client{}
			s := &Server{temporalClient: c, bearerToken: "test", hybridConfig: &cfg}
			record := hybridSubmission{Owner: "alice", RunID: "run-1", Prepared: hybrid.Prepared{Plan: hybrid.Plan{Checkpoints: []hybrid.Checkpoint{{ID: "pilot", Mode: "manual"}}}}}
			if e := (durable.Store{Root: cfg.JournalRoot}).Write(context.Background(), "hybrid-submission/wf", record); e != nil {
				t.Fatal(e)
			}
			d := hybrid.CheckpointDecision{ID: "pilot", StateHash: "sha256:" + strings.Repeat("a", 64), DecisionID: "human-1", Action: "approve", Actor: "alice", ReceiptHash: "sha256:" + strings.Repeat("b", 64)}
			gate := hybrid.CheckpointResult{State: "waiting_for_approval", StateHash: d.StateHash, ExpiresAt: time.Now().Add(time.Hour)}
			if state == "stale" {
				gate.StateHash = "sha256:" + strings.Repeat("c", 64)
			}
			if state == "expired" {
				gate.ExpiresAt = time.Now().Add(-time.Hour)
			}
			if state == "declined" {
				gate.State = "declined"
			}
			result := hybrid.Result{Checkpoints: map[string]hybrid.CheckpointResult{"pilot": gate}}
			if state == "missing" {
				result.Checkpoints = nil
			}
			c.On("QueryWorkflow", mock.Anything, "wf", "run-1", "status").Return(checkpointEncoded{Result: result}, nil).Once()
			raw, _ := json.Marshal(checkpointRequest{hybridIdentity: hybridIdentity{WorkflowID: "wf", Owner: "alice"}, Decision: d})
			w := httptest.NewRecorder()
			s.handleCheckpointDecision(w, httptest.NewRequest("POST", "/hybrid_checkpoint_decision", strings.NewReader(string(raw))))
			if w.Code != 409 {
				t.Fatal(w.Code, w.Body.String())
			}
			c.AssertExpectations(t)
		})
	}
}
