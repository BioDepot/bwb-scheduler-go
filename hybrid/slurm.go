package hybrid

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"go-scheduler/durable"
	"go.temporal.io/sdk/activity"
)

type SlurmReceipt struct {
	JobID       string `json:"job_id"`
	StageID     string `json:"stage_id"`
	SubmittedAt string `json:"submitted_at"`
}

var slurmID = regexp.MustCompile(`^[0-9]+$`)

func slurmCommand(ctx context.Context, input, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdin = strings.NewReader(input)
	out, e := cmd.Output()
	return strings.TrimSpace(string(out)), e
}

// RunSlurm executes on the configured transfer/login gateway. Scientific nodes
// see its explicitly shared root; only this gateway accesses S3.
func (a *Activities) RunSlurm(ctx context.Context, stage Stage) (LocalCommandResult, error) {
	if _, e := a.runner(ctx, stage, "status"); e != nil {
		return LocalCommandResult{}, e
	}
	var receipt SlurmReceipt
	err := a.Store.With(ctx, "slurm-stage/"+stage.JobID, func(entry *durable.Entry) error {
		e := entry.Read(&receipt)
		if e != nil && !os.IsNotExist(e) {
			return e
		}
		if receipt.JobID != "" {
			return nil
		}
		if receipt.StageID != "" {
			// Recover exact scheduler identity. Never resubmit an uncertain sbatch call.
			output, e := slurmCommand(ctx, "", "sacct", "-n", "-P", "-X", "--user", fmt.Sprint(os.Getuid()), "--starttime", receipt.SubmittedAt, "--name", stage.JobID, "--format", "JobIDRaw,JobName%128")
			if e != nil {
				return e
			}
			for _, line := range strings.Split(output, "\n") {
				parts := strings.Split(line, "|")
				if len(parts) >= 2 && parts[1] == stage.JobID && slurmID.MatchString(parts[0]) {
					if receipt.JobID != "" && receipt.JobID != parts[0] {
						return fmt.Errorf("ambiguous Slurm identity")
					}
					receipt.JobID = parts[0]
				}
			}
			if receipt.JobID == "" {
				return fmt.Errorf("Slurm submission is uncertain; retry reconciliation without resubmitting")
			}
			return entry.Write(receipt)
		}
		receipt = SlurmReceipt{StageID: stage.JobID, SubmittedAt: time.Now().Add(-time.Minute).UTC().Format("2006-01-02T15:04:05")}
		if e = entry.Write(receipt); e != nil {
			return e
		}
		payload := map[string]any{"job_id": stage.JobID, "command_id": "science", "work_root": stage.WorkRoot, "argv": stage.Command.Argv, "timeout_seconds": stage.Command.TimeoutSeconds}
		raw, _ := json.Marshal(payload)
		encoded := base64.StdEncoding.EncodeToString(raw)
		invocation := "python3 " + shellQuote(stage.Site.RunnerPath)
		script := "#!/bin/sh\nset -eu\nexport WORKBENCH_RUNNER_STATE=" + shellQuote(filepath.Join(a.Config.JournalRoot, "runner")) + "\n" + invocation + " start " + shellQuote(encoded) + "\n" + invocation + " wait " + shellQuote(encoded) + "\n"
		args := []string{"--parsable", "--job-name", stage.JobID, "--chdir", stage.WorkRoot, "--time", fmt.Sprint((stage.Command.TimeoutSeconds + 59) / 60), "--output", filepath.Join(stage.WorkRoot, "slurm-%j.log")}
		if stage.Site.SlurmPartition != "" {
			args = append(args, "--partition", stage.Site.SlurmPartition)
		}
		if stage.Site.SlurmAccount != "" {
			args = append(args, "--account", stage.Site.SlurmAccount)
		}
		output, e := slurmCommand(ctx, script, "sbatch", args...)
		if e != nil {
			return e
		}
		id := strings.Split(output, ";")[0]
		if !slurmID.MatchString(id) {
			return fmt.Errorf("invalid sbatch receipt")
		}
		receipt.JobID = id
		return entry.Write(receipt)
	})
	if err != nil {
		return LocalCommandResult{}, err
	}
	for {
		if ctx.Err() != nil {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
			defer cancel()
			if _, e := slurmCommand(cleanup, "", "scancel", receipt.JobID); e != nil {
				return LocalCommandResult{}, e
			}
			for cleanup.Err() == nil {
				state, e := a.runner(cleanup, stage, "status")
				terminal, queryErr := slurmTerminal(cleanup, receipt.JobID)
				if e == nil && queryErr == nil && terminal && state.Stopped {
					return LocalCommandResult{State: "cancelled", Stopped: true}, nil
				}
				select {
				case <-cleanup.Done():
				case <-time.After(time.Second):
				}
			}
			return LocalCommandResult{State: "cancellation_unconfirmed", Stopped: false}, fmt.Errorf("Slurm cancellation exit evidence unavailable")
		}
		result, e := a.runner(ctx, stage, "status")
		if e != nil {
			return result, e
		}
		if result.Stopped && result.State != "not_started" {
			return result, nil
		}
		if result.State == "unknown" {
			return result, fmt.Errorf("Slurm process outcome unknown")
		}
		out, e := slurmCommand(ctx, "", "sacct", "-n", "-P", "-X", "-j", receipt.JobID, "--format", "JobIDRaw,State")
		if e != nil {
			return result, e
		}
		for _, line := range strings.Split(out, "\n") {
			parts := strings.Split(line, "|")
			if len(parts) > 1 && parts[0] == receipt.JobID {
				switch strings.Split(parts[1], " ")[0] {
				case "FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL", "OUT_OF_MEMORY":
					return LocalCommandResult{State: strings.ToLower(parts[1]), Stopped: result.Stopped}, nil
				case "COMPLETED":
					if !result.Stopped {
						return result, fmt.Errorf("Slurm completed without runner exit evidence")
					}
				}
			}
		}
		if activity.IsActivity(ctx) {
			activity.RecordHeartbeat(ctx, receipt)
		}
		select {
		case <-ctx.Done():
		case <-time.After(3 * time.Second):
		}
	}
}

func slurmTerminal(ctx context.Context, id string) (bool, error) {
	out, e := slurmCommand(ctx, "", "sacct", "-n", "-P", "-X", "-j", id, "--format", "JobIDRaw,State")
	if e != nil {
		return false, e
	}
	for _, line := range strings.Split(out, "\n") {
		parts := strings.Split(line, "|")
		if len(parts) < 2 || parts[0] != id {
			continue
		}
		switch strings.Split(parts[1], " ")[0] {
		case "COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL", "OUT_OF_MEMORY":
			return true, nil
		}
	}
	return false, nil
}
