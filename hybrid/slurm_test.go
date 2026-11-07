package hybrid

import (
	"context"
	"crypto/sha256"
	"fmt"
	"go-scheduler/durable"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func slurmFixture(t *testing.T) (*Activities, Stage, string) {
	t.Helper()
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	os.Mkdir(bin, 0700)
	runner, e := filepath.Abs("../cloudexec/runner/runner.py")
	if e != nil {
		t.Fatal(e)
	}
	raw, e := os.ReadFile(runner)
	if e != nil {
		t.Fatal(e)
	}
	stage := Stage{JobID: "slurm-fixture", Node: Node{ID: "science", Key: "slurm"}, Site: Site{Kind: "slurm", Root: root, Queue: "fake-slurm", ProfileKey: "fixture@1", MaxJobs: 1, RunnerPath: runner, RunnerHash: fmt.Sprintf("sha256:%x", sha256.Sum256(raw))}, WorkRoot: filepath.Join(root, "slurm-fixture"), Command: Command{Argv: []string{"/bin/sh", "-c", "printf success > result.txt"}, TimeoutSeconds: 30}}
	journal := filepath.Join(root, "journal")
	a := &Activities{Config: Config{Sites: map[string]Site{"slurm": stage.Site}, JournalRoot: journal}, Store: durable.Store{Root: journal}}
	if e = a.PrepareWorkspace(context.Background(), stage); e != nil {
		t.Fatal(e)
	}
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
	t.Setenv("FAKE_SLURM_ROOT", root)
	commands := map[string]string{
		"sbatch":  "#!/bin/sh\ncat > \"$FAKE_SLURM_ROOT/script\"\necho submitted >> \"$FAKE_SLURM_ROOT/submissions\"\nsh \"$FAKE_SLURM_ROOT/script\" > \"$FAKE_SLURM_ROOT/batch.log\" 2>&1 < /dev/null &\nif [ -f \"$FAKE_SLURM_ROOT/lose-response\" ]; then rm \"$FAKE_SLURM_ROOT/lose-response\"; exit 1; fi\nprintf '12345\\n'\n",
		"sacct":   "#!/bin/sh\ncase \"$*\" in *--name*) printf '12345|slurm-fixture\\n';; *) if [ -f \"$FAKE_SLURM_ROOT/cancelled\" ]; then printf '12345|CANCELLED\\n'; else printf '12345|RUNNING\\n'; fi;; esac\n",
		"scancel": "#!/bin/sh\ntouch \"$FAKE_SLURM_ROOT/cancelled\"\ntouch \"$FAKE_SLURM_ROOT/journal/runner/slurm-fixture/science/cancel\"\n",
	}
	for name, data := range commands {
		if e = os.WriteFile(filepath.Join(bin, name), []byte(data), 0700); e != nil {
			t.Fatal(e)
		}
	}
	return a, stage, root
}
func TestSlurmLostSubmissionRecoversExactJob(t *testing.T) {
	a, stage, root := slurmFixture(t)
	os.WriteFile(filepath.Join(root, "lose-response"), nil, 0600)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, e := a.RunSlurm(ctx, stage); e == nil {
		t.Fatal("lost sbatch receipt was ignored")
	}
	result, e := a.RunSlurm(ctx, stage)
	if e != nil || !result.Stopped || result.State != "succeeded" {
		t.Fatal(result, e)
	}
	data, _ := os.ReadFile(filepath.Join(root, "submissions"))
	if strings.Count(string(data), "submitted") != 1 {
		t.Fatal("job was resubmitted", string(data))
	}
}
func TestSlurmCancellationWaitsForProcessEvidence(t *testing.T) {
	a, stage, _ := slurmFixture(t)
	stage.Command.Argv = []string{"/bin/sleep", "20"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	time.AfterFunc(time.Second, cancel)
	result, e := a.RunSlurm(ctx, stage)
	if e != nil || !result.Stopped || result.State != "cancelled" {
		t.Fatal(result, e)
	}
}
