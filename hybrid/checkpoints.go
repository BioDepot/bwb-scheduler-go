package hybrid

import (
	"fmt"
	"sort"
	"time"

	"go.temporal.io/sdk/workflow"
)

const CheckpointSignal = "hybrid.checkpoint_decision.v1"

// Automatic conditions are explicit, reviewed QC commands in after_nodes.
// They must exit successfully and publish their evidence before continuation.
type Checkpoint struct {
	ID            string   `json:"checkpoint_id"`
	Description   string   `json:"description"`
	After         []string `json:"after_nodes"`
	Before        []string `json:"before_nodes"`
	Mode          string   `json:"mode"`
	ReviewSeconds int      `json:"review_timeout_seconds,omitempty"`
}
type CheckpointDecision struct {
	ID          string `json:"checkpoint_id"`
	StateHash   string `json:"state_hash"`
	DecisionID  string `json:"decision_id"`
	Action      string `json:"action"`
	Actor       string `json:"actor_id"`
	ReceiptHash string `json:"receipt_hash"`
}
type CheckpointResult struct {
	State       string                `json:"state"`
	StateHash   string                `json:"state_hash,omitempty"`
	Description string                `json:"description"`
	Evidence    map[string]NodeResult `json:"evidence,omitempty"`
	Before      []string              `json:"before_nodes"`
	ExpiresAt   time.Time             `json:"expires_at,omitempty"`
	Decision    *CheckpointDecision   `json:"decision,omitempty"`
}

func checkpointBlocks(p Prepared, result Result, id string) bool {
	// Descendants are blocked by their normal dependency edges. Protect the
	// explicit roots here; validation requires every milestone to be an ancestor.
	for _, gate := range p.Plan.Checkpoints {
		state := result.Checkpoints[gate.ID].State
		if state == "passed" || state == "approved" {
			continue
		}
		for _, root := range gate.Before {
			if root == id {
				return true
			}
		}
	}
	return false
}

func advanceCheckpoints(ctx workflow.Context, p Prepared, result *Result) (changed bool) {
	for _, gate := range p.Plan.Checkpoints {
		current := result.Checkpoints[gate.ID]
		if current.State == "waiting_for_approval" && !workflow.Now(ctx).Before(current.ExpiresAt) {
			current.State = "expired"
			result.Checkpoints[gate.ID] = current
			changed = true
		}
		if current.State != "" {
			continue
		}
		evidence := map[string]NodeResult{}
		ready := true
		for _, id := range gate.After {
			node, ok := result.Nodes[id]
			if !ok {
				ready = false
				break
			}
			if node.State != "succeeded" || node.Compute != "succeeded" || node.Publication != "succeeded" {
				current.State = "blocked"
				ready = false
				break
			}
			evidence[id] = node
		}
		if current.State == "blocked" {
			current.Description, current.Before = gate.Description, gate.Before
			result.Checkpoints[gate.ID] = current
			changed = true
			continue
		}
		if !ready {
			continue
		}
		stateHash, err := objectHash(map[string]any{"plan": p.Request.Plan, "configuration_hash": p.Request.ConfigurationHash, "checkpoint": gate, "evidence": evidence})
		current = CheckpointResult{State: "passed", StateHash: stateHash, Description: gate.Description, Evidence: evidence, Before: gate.Before}
		if err != nil {
			current.State = "blocked"
		}
		if err == nil && gate.Mode == "manual" {
			current.State = "waiting_for_approval"
			current.ExpiresAt = workflow.Now(ctx).Add(time.Duration(gate.ReviewSeconds) * time.Second)
		}
		result.Checkpoints[gate.ID] = current
		changed = true
	}
	return changed
}

func checkpointFailure(result Result) error {
	ids := make([]string, 0, len(result.Checkpoints))
	for id := range result.Checkpoints {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		gate := result.Checkpoints[id]
		if gate.State == "declined" || gate.State == "expired" || gate.State == "blocked" {
			return fmt.Errorf("checkpoint %s is %s; continuation was not released", id, gate.State)
		}
	}
	return nil
}

func acceptCheckpointDecision(ctx workflow.Context, result *Result, decision CheckpointDecision) {
	gate, ok := result.Checkpoints[decision.ID]
	if !ok || gate.State != "waiting_for_approval" || !workflow.Now(ctx).Before(gate.ExpiresAt) ||
		gate.StateHash != decision.StateHash || decision.DecisionID == "" || decision.Actor == "" ||
		len(decision.ReceiptHash) != 71 || (decision.Action != "approve" && decision.Action != "decline") {
		return
	}
	gate.State = "declined"
	if decision.Action == "approve" {
		gate.State = "approved"
	}
	gate.Decision = &decision
	result.Checkpoints[decision.ID] = gate
}
