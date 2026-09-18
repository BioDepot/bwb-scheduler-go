package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"go-scheduler/parsing"
)

type SlurmReconciliationTarget struct {
	Config  parsing.SshConfig        `json:"config"`
	Request SlurmCancellationRequest `json:"request"`
}

type DurableWorkflowRecord struct {
	Schema            string                      `json:"schema"`
	Identity          parsing.ExecutionIdentity   `json:"identity"`
	TemporalRunID     string                      `json:"temporal_run_id,omitempty"`
	SlurmTarget       *SlurmReconciliationTarget  `json:"slurm_reconciliation_target,omitempty"`
	TerminalEvidence  *WorkflowTerminalEvidence   `json:"terminal_evidence,omitempty"`
	Reconciliations   []SlurmCancellationEvidence `json:"reconciliations,omitempty"`
	DeclaredArtifacts []string                    `json:"declared_artifacts,omitempty"`
	UpdatedAt         string                      `json:"updated_at"`
}

var (
	durableRecordIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$`)
	durableRecordMu        sync.Mutex
)

func durableRecordPath(root, workflowID string) (string, error) {
	if root == "" {
		return "", fmt.Errorf("durable evidence directory is not configured")
	}
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return "", fmt.Errorf("durable evidence directory must be an absolute canonical path")
	}
	if !durableRecordIDPattern.MatchString(workflowID) {
		return "", fmt.Errorf("invalid workflow ID %q", workflowID)
	}
	return filepath.Join(root, workflowID+".json"), nil
}

func ReadDurableWorkflowRecord(root, workflowID string) (DurableWorkflowRecord, error) {
	path, err := durableRecordPath(root, workflowID)
	if err != nil {
		return DurableWorkflowRecord{}, err
	}
	return readDurableWorkflowRecordPath(path)
}

func readDurableWorkflowRecordPath(path string) (DurableWorkflowRecord, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return DurableWorkflowRecord{}, err
	}
	var record DurableWorkflowRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return DurableWorkflowRecord{}, fmt.Errorf("decode durable workflow record: %w", err)
	}
	return record, nil
}

func writeDurableWorkflowRecordPath(
	root, path string, record DurableWorkflowRecord,
) (DurableWorkflowRecord, error) {
	if record.Schema == "" {
		record.Schema = "biodepot.scheduler_evidence/v1"
	}
	record.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if err := os.MkdirAll(root, 0700); err != nil {
		return DurableWorkflowRecord{}, err
	}
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return DurableWorkflowRecord{}, err
	}
	tmp, err := os.CreateTemp(root, ".workflow-evidence-*.tmp")
	if err != nil {
		return DurableWorkflowRecord{}, err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0600); err != nil {
		tmp.Close()
		return DurableWorkflowRecord{}, err
	}
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		tmp.Close()
		return DurableWorkflowRecord{}, err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return DurableWorkflowRecord{}, err
	}
	if err := tmp.Close(); err != nil {
		return DurableWorkflowRecord{}, err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return DurableWorkflowRecord{}, err
	}
	return record, nil
}

func WriteDurableWorkflowRecord(root string, update DurableWorkflowRecord) error {
	path, err := durableRecordPath(root, update.Identity.WorkflowID)
	if err != nil {
		return err
	}
	durableRecordMu.Lock()
	defer durableRecordMu.Unlock()

	record := DurableWorkflowRecord{}
	if data, readErr := os.ReadFile(path); readErr == nil {
		if err := json.Unmarshal(data, &record); err != nil {
			return fmt.Errorf("decode existing durable workflow record: %w", err)
		}
	} else if !os.IsNotExist(readErr) {
		return readErr
	}
	if update.Schema != "" {
		record.Schema = update.Schema
	}
	record.Identity = update.Identity
	if update.TemporalRunID != "" {
		record.TemporalRunID = update.TemporalRunID
	}
	if update.SlurmTarget != nil {
		record.SlurmTarget = update.SlurmTarget
	}
	if update.TerminalEvidence != nil {
		record.TerminalEvidence = update.TerminalEvidence
	}
	if update.Reconciliations != nil {
		record.Reconciliations = update.Reconciliations
	}
	if update.DeclaredArtifacts != nil {
		record.DeclaredArtifacts = update.DeclaredArtifacts
	}
	_, err = writeDurableWorkflowRecordPath(root, path, record)
	return err
}

func AppendReconciliationEvidence(
	root, workflowID string, evidence SlurmCancellationEvidence,
) (DurableWorkflowRecord, error) {
	path, err := durableRecordPath(root, workflowID)
	if err != nil {
		return DurableWorkflowRecord{}, err
	}
	durableRecordMu.Lock()
	defer durableRecordMu.Unlock()
	record, err := readDurableWorkflowRecordPath(path)
	if err != nil {
		return DurableWorkflowRecord{}, err
	}
	if len(record.Reconciliations) > 0 {
		last := record.Reconciliations[len(record.Reconciliations)-1]
		if last.Verified && evidence.Verified && last.DryRun == evidence.DryRun {
			return record, nil
		}
	}
	record.Reconciliations = append(record.Reconciliations, evidence)
	return writeDurableWorkflowRecordPath(root, path, record)
}

func PersistDurableWorkflowRecordActivity(
	_ context.Context, root string, record DurableWorkflowRecord,
) error {
	return WriteDurableWorkflowRecord(root, record)
}
