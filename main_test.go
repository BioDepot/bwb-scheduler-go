package main

import (
	"log/slog"
	"testing"
)

func TestTemporalClientOptionsDefaultsAndOverrides(t *testing.T) {
	t.Setenv("BWB_TEMPORAL_ADDRESS", "")
	t.Setenv("BWB_TEMPORAL_NAMESPACE", "")
	logger := slog.Default()

	defaults := temporalClientOptions(logger)
	if defaults.HostPort != "localhost:7233" || defaults.Namespace != "default" {
		t.Fatalf("unexpected defaults: host=%q namespace=%q", defaults.HostPort, defaults.Namespace)
	}

	t.Setenv("BWB_TEMPORAL_ADDRESS", "temporal.example:7233")
	t.Setenv("BWB_TEMPORAL_NAMESPACE", "slurm-test")
	overrides := temporalClientOptions(logger)
	if overrides.HostPort != "temporal.example:7233" || overrides.Namespace != "slurm-test" {
		t.Fatalf("unexpected overrides: host=%q namespace=%q", overrides.HostPort, overrides.Namespace)
	}
}
