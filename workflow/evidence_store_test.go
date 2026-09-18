package workflow

import (
	"testing"

	"go-scheduler/parsing"
)

func TestDurableWorkflowRecordRoundTripAndRecoveryAppend(t *testing.T) {
	root := t.TempDir()
	identity := parsing.ExecutionIdentity{
		RequestID: "request-1", WorkflowID: "workflow-1",
		WorkbenchRunID: "workbench-1", ExecutorID: "executor-1", SiteProfileID: "site-1",
	}
	record := DurableWorkflowRecord{
		Identity:      identity,
		TemporalRunID: "run-1",
		TerminalEvidence: &WorkflowTerminalEvidence{
			WorkflowStatus: "CANCEL_CLEANUP_FAILED",
			Identity:       identity,
		},
	}
	if err := WriteDurableWorkflowRecord(root, record); err != nil {
		t.Fatal(err)
	}
	actual, err := ReadDurableWorkflowRecord(root, identity.WorkflowID)
	if err != nil {
		t.Fatal(err)
	}
	if actual.Schema != "biodepot.scheduler_evidence/v1" || actual.TemporalRunID != "run-1" {
		t.Fatalf("unexpected durable record: %#v", actual)
	}

	recovery := SlurmCancellationEvidence{CleanupStatus: "verified", Verified: true}
	actual, err = AppendReconciliationEvidence(root, identity.WorkflowID, recovery)
	if err != nil {
		t.Fatal(err)
	}
	if len(actual.Reconciliations) != 1 || !actual.Reconciliations[0].Verified {
		t.Fatalf("recovery evidence was not appended: %#v", actual.Reconciliations)
	}
	actual, err = AppendReconciliationEvidence(root, identity.WorkflowID, recovery)
	if err != nil {
		t.Fatal(err)
	}
	if len(actual.Reconciliations) != 1 {
		t.Fatalf("verified recovery was not idempotent: %#v", actual.Reconciliations)
	}
}

func TestDurableWorkflowRecordRejectsUnsafeWorkflowID(t *testing.T) {
	err := WriteDurableWorkflowRecord(t.TempDir(), DurableWorkflowRecord{
		Identity: parsing.ExecutionIdentity{WorkflowID: "../escape"},
	})
	if err == nil {
		t.Fatal("unsafe workflow ID was accepted")
	}
}

func TestDryRunDoesNotSuppressAppliedReconciliation(t *testing.T) {
	root := t.TempDir()
	workflowID := "workflow-dry-then-apply"
	if err := WriteDurableWorkflowRecord(root, DurableWorkflowRecord{
		Identity: parsing.ExecutionIdentity{WorkflowID: workflowID},
	}); err != nil {
		t.Fatal(err)
	}
	dryRun := SlurmCancellationEvidence{
		CleanupStatus: "dry_run", Verified: true, DryRun: true,
	}
	if _, err := AppendReconciliationEvidence(root, workflowID, dryRun); err != nil {
		t.Fatal(err)
	}
	applied := SlurmCancellationEvidence{CleanupStatus: "verified", Verified: true}
	record, err := AppendReconciliationEvidence(root, workflowID, applied)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Reconciliations) != 2 || record.Reconciliations[1].DryRun {
		t.Fatalf("applied reconciliation was not persisted after dry run: %#v", record.Reconciliations)
	}
}
