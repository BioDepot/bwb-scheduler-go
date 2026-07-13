package workflow

import (
	"bytes"
	"strings"
	"testing"

	"go-scheduler/parsing"
)

func TestWriteSbatchFileExportsSiteEnvironmentDeterministically(t *testing.T) {
	partition := "normal"
	config := parsing.SlurmJobConfig{
		Partition: &partition,
		Environment: map[string]string{
			"APPTAINERENV_CUDA_VISIBLE_DEVICES": "1",
			"TUTORIAL_LABEL":                    "Patrick's GPU",
		},
	}
	cmd := parsing.CmdTemplate{
		Id:        10,
		ImageName: "cellbender.sif",
		ResourceReqs: parsing.ResourceVector{
			Cpus:  1,
			MemMb: 1024,
			Gpus:  1,
		},
	}
	var output bytes.Buffer
	_, _, err := WriteSbatchFile(
		&output,
		cmd,
		map[string]string{},
		parsing.SshConfig{},
		config,
		"/slurm",
		"/images",
		"job-1",
	)
	if err != nil {
		t.Fatalf("WriteSbatchFile returned an error: %v", err)
	}

	text := output.String()
	first := "export APPTAINERENV_CUDA_VISIBLE_DEVICES='1'"
	second := "export TUTORIAL_LABEL='Patrick'\\''s GPU'"
	if !strings.Contains(text, first) || !strings.Contains(text, second) {
		t.Fatalf("missing environment exports in sbatch file:\n%s", text)
	}
	if strings.Index(text, first) > strings.Index(text, second) {
		t.Fatalf("environment exports are not sorted:\n%s", text)
	}
}
