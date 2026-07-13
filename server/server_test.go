package server

import (
	"encoding/json"
	"go-scheduler/parsing"
	"testing"
)

func TestParseRequestJobConfigUsesPublicSiteProfileContract(t *testing.T) {
	raw := json.RawMessage(`{
        "executors": {
            "slurm": {
                "ip_addr": "10.159.4.53:22",
                "user": "lhhung",
                "transfer_addr": "10.159.4.53",
                "sched_dir": "/srv/slurm_mnt/data",
                "cmd_prefix": ""
            }
        },
        "configs": {
            "bulk_rna": {
                "executor": "slurm",
                "annotations": {
                    "partition": "normal",
                    "mem": "4G",
                    "cpus_per_task": 2
                }
            }
        },
        "node_configs": {"100": "bulk_rna"}
    }`)

	config, err := parseRequestJobConfig(raw, &parsing.ResolvedWorkflow{})
	if err != nil {
		t.Fatalf("parseRequestJobConfig returned error: %v", err)
	}
	if config.SlurmExecutor.IpAddr != "10.159.4.53:22" {
		t.Fatalf("unexpected Slurm endpoint: %q", config.SlurmExecutor.IpAddr)
	}
	if got := config.ExecTypeByNode[100]; got != parsing.EXEC_SLURM {
		t.Fatalf("node 100 executor = %v, want EXEC_SLURM", got)
	}
	nodeConfig := config.SlurmConfigsByNode[100]
	if nodeConfig.Partition == nil || *nodeConfig.Partition != "normal" {
		t.Fatalf("node 100 partition was not parsed: %#v", nodeConfig.Partition)
	}
	if nodeConfig.Mem == nil || *nodeConfig.Mem != "4G" {
		t.Fatalf("node 100 memory was not parsed: %#v", nodeConfig.Mem)
	}
	if nodeConfig.CpusPerTask == nil || *nodeConfig.CpusPerTask != 2 {
		t.Fatalf("node 100 CPUs were not parsed: %#v", nodeConfig.CpusPerTask)
	}
}

func TestParseRequestJobConfigRejectsInvalidSiteProfile(t *testing.T) {
	raw := json.RawMessage(`{
        "executors": {"slurm": {}},
        "configs": {},
        "node_configs": {}
    }`)

	if _, err := parseRequestJobConfig(raw, &parsing.ResolvedWorkflow{}); err == nil {
		t.Fatal("parseRequestJobConfig accepted an invalid Slurm site profile")
	}
}
