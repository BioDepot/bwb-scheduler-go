package parsing

import (
	"path/filepath"
	"testing"
)

func TestV1DataInputPathResolvesAgainstExecutorRoot(t *testing.T) {
	wf := ResolvedWorkflow{
		Nodes: map[int]ResolvedNode{
			100: {
				Id:        100,
				ImageName: "example/image",
				ImageTag:  "latest",
				Inputs: map[string]NodeInput{
					"reads": {
						Kind:   "file",
						Source: InputSource{Type: "path", Path: "/data/input/reads.fastq.gz"},
						Mount:  &InputMount{ContainerPath: "/work/reads.fastq.gz"},
					},
				},
			},
		},
	}
	index, err := ParseAndValidateWorkflow(&wf)
	if err != nil {
		t.Fatalf("failed to validate workflow: %v", err)
	}
	config := JobConfig{ExecTypeByNode: map[int]ExecType{100: EXEC_SLURM}}
	executorRoot := "/ocean/project/SCHED_STORAGE"
	cmdMan := NewCmdManager(
		&wf, index, config, map[ExecType]string{EXEC_SLURM: executorRoot},
	)

	cmds, err := cmdMan.GetInitialCmds(GlobNoOp)
	if err != nil {
		t.Fatalf("failed to create initial commands: %v", err)
	}
	readsVolume := cmds[100][0].Volumes["reads"]
	want := filepath.Join(executorRoot, "/data/input/reads.fastq.gz")
	if readsVolume.HostPath != want {
		t.Fatalf("input host path = %q, want %q", readsVolume.HostPath, want)
	}
	if readsVolume.CntPath != "/work/reads.fastq.gz" {
		t.Fatalf("input container path = %q", readsVolume.CntPath)
	}
}
