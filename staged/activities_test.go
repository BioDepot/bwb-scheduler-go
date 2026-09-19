package staged

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testExecutable(t *testing.T, dir, name, script string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+script), 0700); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestGlobusActivityUsesSubmissionIDAndBatchPaths(t *testing.T) {
	dir := t.TempDir()
	capture := filepath.Join(dir, "batch.txt")
	t.Setenv("CAPTURE", capture)
	cli := testExecutable(t, dir, "globus", `
if [ "$1" = "transfer" ]; then
  cat > "$CAPTURE"
  printf '%s\n' '{"task_id":"task-123"}'
else
  printf '%s\n' '{"status":"SUCCEEDED","files":2,"files_transferred":2,"bytes_transferred":100}'
fi
`)
	a := &Activities{}
	a.Config.Executors.Globus.CLIPath = cli
	a.Config.Executors.Globus.AllowedEndpointIDs = []string{"source", "dest"}
	transfer := Transfer{SourceEndpointID: "source", DestinationEndpointID: "dest", Label: "test", SubmissionID: "submission-1", VerifyChecksum: true, Items: []TransferItem{{SourcePath: "/source/quoted'file", DestinationPath: "/dest/file", Recursive: true}}}
	id, err := a.SubmitTransfer(context.Background(), transfer)
	if err != nil || id != "task-123" {
		t.Fatalf("submit=%q, %v", id, err)
	}
	batch, err := os.ReadFile(capture)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(batch); !strings.HasPrefix(got, "--recursive '") || !strings.Contains(got, `'/source/quoted'"'"'file' '/dest/file'`) {
		t.Fatalf("unexpected Globus batch: %q", got)
	}
	status, err := a.PollTransfer(context.Background(), id)
	if err != nil || status.Status != "SUCCEEDED" || status.BytesTransferred != 100 {
		t.Fatalf("poll=%+v, %v", status, err)
	}
	transfer.DestinationEndpointID = "unapproved"
	if _, err := a.SubmitTransfer(context.Background(), transfer); err == nil {
		t.Fatal("unapproved endpoint accepted")
	}
}

func TestSlurmActivitySubmitsScriptAndReadsTerminalState(t *testing.T) {
	dir := t.TempDir()
	capture := filepath.Join(dir, "script.txt")
	t.Setenv("CAPTURE", capture)
	testExecutable(t, dir, "ssh", `
for arg in "$@"; do command="$arg"; done
case "$command" in
  *sbatch*) cat > "$CAPTURE"; printf '123\n' ;;
  *sacct*) printf '123|COMPLETED|0:0\n' ;;
  *tail*) printf 'job complete\n' ;;
  *mkdir*|*scancel*) exit 0 ;;
  *) exit 1 ;;
esac
`)
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	a := &Activities{}
	a.Config.Executors.Slurm = Site{IPAddr: "slurm.example", User: "tester", Port: 22, StorageDir: "/remote/scheduler"}
	job := SlurmJob{Name: "test-job", Script: "echo done", Resources: SlurmResources{Cpus: 4, MemMB: 2048}, Config: SlurmOptions{Partition: "normal", Modules: []string{"gcc/12"}}}
	id, err := a.SubmitSlurm(context.Background(), job)
	if err != nil || id != "123" {
		t.Fatalf("submit=%q, %v", id, err)
	}
	script, err := os.ReadFile(capture)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(script), "module load gcc/12\n") || !strings.Contains(string(script), "echo done\n") {
		t.Fatalf("unexpected sbatch script: %q", script)
	}
	status, err := a.PollSlurm(context.Background(), id)
	if err != nil || status.Status != "COMPLETED" || status.ExitCode != 0 || !strings.Contains(status.Logs, "job complete") {
		t.Fatalf("poll=%+v, %v", status, err)
	}
	if err := a.CancelSlurm(context.Background(), id); err != nil {
		t.Fatal(err)
	}
}
