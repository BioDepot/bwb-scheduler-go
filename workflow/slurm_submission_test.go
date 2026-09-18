package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"go-scheduler/parsing"
)

func writeExecutable(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body+"\n"), 0755); err != nil {
		t.Fatalf("write executable %s: %v", path, err)
	}
}

func submissionFixture(t *testing.T, queueRows, accountRows string) (slurmSubmissionTransaction, []string) {
	t.Helper()
	root := t.TempDir()
	binDir := filepath.Join(root, "bin")
	if err := os.Mkdir(binDir, 0755); err != nil {
		t.Fatal(err)
	}
	countPath := filepath.Join(root, "sbatch.count")
	writeExecutable(t, filepath.Join(binDir, "squeue"), "printf '%s' "+shellQuote(queueRows))
	writeExecutable(t, filepath.Join(binDir, "sacct"), "printf '%s' "+shellQuote(accountRows))
	writeExecutable(t, filepath.Join(binDir, "sbatch"),
		"printf x >> "+shellQuote(countPath)+"; printf '123;cluster\\n'")

	batch := []byte("#!/bin/bash\necho test\n")
	sum := sha256.Sum256(batch)
	hash := hex.EncodeToString(sum[:])
	sbatchPath := filepath.Join(root, "job.sbatch")
	candidatePath := sbatchPath + ".candidate." + hash
	if err := os.WriteFile(candidatePath, batch, 0600); err != nil {
		t.Fatal(err)
	}
	tx := slurmSubmissionTransaction{
		CorrelationKey: "morphic-test",
		SubmittingUser: "tester",
		ManifestPath:   filepath.Join(root, "submissions.tsv"),
		LockPath:       filepath.Join(root, "submissions.tsv.lock"),
		SbatchPath:     sbatchPath,
		CandidatePath:  candidatePath,
		BatchSHA256:    hash,
		Identity: parsing.ExecutionIdentity{
			RequestID:      "request-1",
			WorkflowID:     "workflow-1",
			WorkbenchRunID: "workbench-1",
			ExecutorID:     "executor-1",
			SiteProfileID:  "site-1",
		},
		ReconciliationWindow: "now-1day",
	}
	return tx, []string{binDir, countPath}
}

func runSubmissionScript(t *testing.T, tx slurmSubmissionTransaction, binDir string) (string, error) {
	t.Helper()
	cmd := exec.Command("bash", "-c", buildSubmissionTransactionScript(tx))
	cmd.Env = append(os.Environ(), "PATH="+binDir+":"+os.Getenv("PATH"))
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func TestSubmissionTransactionIsIdempotent(t *testing.T) {
	tx, fixture := submissionFixture(t, "", "")
	first, err := runSubmissionScript(t, tx, fixture[0])
	if err != nil {
		t.Fatalf("first submission failed: %v: %s", err, first)
	}
	if !strings.Contains(first, "123\tsubmitted\t") {
		t.Fatalf("unexpected first result %q", first)
	}

	batch, err := os.ReadFile(tx.SbatchPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tx.CandidatePath, batch, 0600); err != nil {
		t.Fatal(err)
	}
	second, err := runSubmissionScript(t, tx, fixture[0])
	if err != nil {
		t.Fatalf("replayed submission failed: %v: %s", err, second)
	}
	if !strings.Contains(second, "123\tmanifest\t") {
		t.Fatalf("replay did not use manifest: %q", second)
	}
	count, err := os.ReadFile(fixture[1])
	if err != nil {
		t.Fatal(err)
	}
	if string(count) != "x" {
		t.Fatalf("sbatch invoked %d times, want 1", len(count))
	}
	manifest, err := os.ReadFile(tx.ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Split(strings.TrimSpace(string(manifest)), "\t")
	if len(fields) != 9 || fields[2] != tx.BatchSHA256 || fields[4] != tx.Identity.WorkflowID {
		t.Fatalf("unexpected durable manifest record: %q", manifest)
	}
}

func TestSubmissionTransactionRecoversLostResultFromScheduler(t *testing.T) {
	tx, fixture := submissionFixture(t, "456|morphic-test|tester\n", "")
	out, err := runSubmissionScript(t, tx, fixture[0])
	if err != nil {
		t.Fatalf("reconciliation failed: %v: %s", err, out)
	}
	if !strings.Contains(out, "456\trecovered\t") {
		t.Fatalf("scheduler job was not recovered: %q", out)
	}
	if _, err := os.Stat(fixture[1]); !os.IsNotExist(err) {
		t.Fatalf("sbatch was invoked while recovering existing job: %v", err)
	}
}

func TestSubmissionTransactionRejectsDuplicateSchedulerMatches(t *testing.T) {
	tx, fixture := submissionFixture(
		t, "456|morphic-test|tester\n", "789|morphic-test|tester\n",
	)
	out, err := runSubmissionScript(t, tx, fixture[0])
	if err == nil || !strings.Contains(out, "multiple Slurm jobs match correlation key") {
		t.Fatalf("duplicate scheduler jobs were not rejected: err=%v output=%q", err, out)
	}
}

func TestPauseAfterDurableSubmissionRequiresAbsolutePath(t *testing.T) {
	t.Setenv("BWB_TEST_PAUSE_AFTER_SLURM_SUBMIT_FILE", "relative")
	err := pauseAfterDurableSubmission(context.Background(), "morphic-test", "123", "submitted")
	if err == nil || !strings.Contains(err.Error(), "must be absolute") {
		t.Fatalf("relative fault-hook path was accepted: %v", err)
	}
}

func TestPauseAfterDurableSubmissionPublishesReadyEvidence(t *testing.T) {
	hook := filepath.Join(t.TempDir(), "pause")
	t.Setenv("BWB_TEST_PAUSE_AFTER_SLURM_SUBMIT_FILE", hook)
	if err := os.WriteFile(hook+".release", nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := pauseAfterDurableSubmission(context.Background(), "morphic-test", "123", "submitted"); err != nil {
		t.Fatal(err)
	}
	ready, err := os.ReadFile(hook + ".ready")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(ready), `"job_id":"123"`) {
		t.Fatalf("unexpected fault-hook evidence: %s", ready)
	}
}
