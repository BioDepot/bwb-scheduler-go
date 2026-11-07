package hybrid

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	st "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go-scheduler/durable"
	"go-scheduler/transfer"
	"go.temporal.io/sdk/activity"
)

type Activities struct {
	Config  Config
	Store   durable.Store
	Objects *transfer.S3Store
}
type LocalCommandResult struct {
	State    string `json:"state"`
	Stopped  bool   `json:"process_stopped"`
	ExitCode *int   `json:"exit_code,omitempty"`
}

func (a *Activities) check(stage Stage) error {
	site, ok := a.Config.Sites[stage.Node.Key]
	if !ok || !reflect.DeepEqual(site, stage.Site) {
		return fmt.Errorf("stage site differs from immutable worker configuration")
	}
	if stage.WorkRoot != filepath.Join(site.Root, stage.JobID) || !SafeRelative(stage.JobID) {
		return fmt.Errorf("stage workspace does not match identity")
	}
	resolved, err := filepath.EvalSymlinks(site.Root)
	if err != nil || resolved != site.Root {
		return fmt.Errorf("site root must exist and contain no symlinks")
	}
	return nil
}
func (a *Activities) PrepareWorkspace(_ context.Context, stage Stage) error {
	if e := a.check(stage); e != nil {
		return e
	}
	return os.MkdirAll(stage.WorkRoot, 0700)
}
func (a *Activities) runner(ctx context.Context, stage Stage, action string) (LocalCommandResult, error) {
	var result LocalCommandResult
	raw, e := os.ReadFile(stage.Site.RunnerPath)
	if e != nil {
		return result, e
	}
	if fmt.Sprintf("sha256:%x", sha256.Sum256(raw)) != stage.Site.RunnerHash {
		return result, fmt.Errorf("local runner hash mismatch")
	}
	payload := map[string]any{"job_id": stage.JobID, "command_id": "science", "work_root": stage.WorkRoot, "argv": stage.Command.Argv, "timeout_seconds": stage.Command.TimeoutSeconds}
	b, _ := json.Marshal(payload)
	cmd := exec.CommandContext(ctx, "python3", stage.Site.RunnerPath, action, base64.StdEncoding.EncodeToString(b))
	cmd.Env = append(os.Environ(), "WORKBENCH_RUNNER_STATE="+filepath.Join(a.Config.JournalRoot, "runner"))
	out, e := cmd.Output()
	if e != nil {
		return result, e
	}
	e = json.Unmarshal(out, &result)
	return result, e
}
func (a *Activities) RunLocal(ctx context.Context, stage Stage) (LocalCommandResult, error) {
	if e := a.check(stage); e != nil {
		return LocalCommandResult{}, e
	}
	if stage.Site.Kind == "slurm" {
		return a.RunSlurm(ctx, stage)
	}
	result, err := a.runner(ctx, stage, "start")
	if err != nil {
		return result, err
	}
	for !result.Stopped {
		select {
		case <-ctx.Done():
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
			defer cancel()
			_, err = a.runner(cleanup, stage, "cancel")
			if err != nil {
				return result, fmt.Errorf("cancellation unconfirmed: %w", err)
			}
			for cleanup.Err() == nil {
				result, err = a.runner(cleanup, stage, "status")
				if err != nil {
					return result, err
				}
				if result.Stopped {
					return result, ctx.Err()
				}
				time.Sleep(time.Second)
			}
			return result, fmt.Errorf("process cancellation remains unconfirmed")
		case <-time.After(time.Second):
			if activity.IsActivity(ctx) {
				activity.RecordHeartbeat(ctx, result)
			}
			result, err = a.runner(ctx, stage, "status")
			if err != nil {
				return result, err
			}
			if result.State == "unknown" {
				return result, fmt.Errorf("local scientific process outcome unknown")
			}
		}
	}
	return result, nil
}
func (a *Activities) Transfer(ctx context.Context, stage Stage, r transfer.Request) (transfer.Receipt, error) {
	if e := a.check(stage); e != nil {
		return transfer.Receipt{}, e
	}
	var policy struct {
		Input       []string `json:"input_prefixes"`
		Publication []string `json:"publication_prefixes"`
		Routes      []Route  `json:"routes"`
	}
	if e := json.Unmarshal(a.Config.Policy, &policy); e != nil {
		return transfer.Receipt{}, e
	}
	inputs := append([]string{}, policy.Input...)
	outputs := append([]string{}, policy.Publication...)
	for _, route := range policy.Routes {
		inputs = append(inputs, route.Prefix)
		outputs = append(outputs, route.Prefix)
	}
	endpoint := transfer.Endpoint{Key: stage.Node.Key, Root: stage.Site.Root}
	s3Endpoint := transfer.Endpoint{Key: "s3", ReadPrefixes: inputs, WritePrefixes: outputs}
	objects := *a.Objects
	if stage.Site.S3Encryption != "" {
		objects.Encryption = st.ServerSideEncryption(stage.Site.S3Encryption)
		objects.KMSKey = stage.Site.S3KMSKey
	}
	worker := transfer.Worker{Endpoints: map[string]transfer.Endpoint{stage.Node.Key: endpoint, "s3": s3Endpoint}, Objects: &objects, Journal: a.Store}
	return worker.Transfer(ctx, r)
}
func (a *Activities) Publish(ctx context.Context, stage Stage, out Output) (transfer.Artifact, error) {
	if e := a.check(stage); e != nil {
		return transfer.Artifact{}, e
	}
	if !SafeRelative(out.Path) {
		return transfer.Artifact{}, fmt.Errorf("invalid output path")
	}
	source, e := transfer.FileArtifactWithin(stage.WorkRoot, filepath.Join(stage.WorkRoot, out.Path))
	if e != nil {
		return transfer.Artifact{}, e
	}
	key := fmt.Sprintf("publish-%x", sha256.Sum256([]byte(stage.JobID+"/"+out.Key)))
	receipt, e := a.Transfer(ctx, stage, transfer.Request{ID: key, AttemptID: stage.JobID, SourceEndpoint: stage.Node.Key, DestinationEndpoint: "s3", Source: source, Destination: out.Destination})
	return receipt.Destination, e
}
func (a *Activities) PersistResult(ctx context.Context, workflowID string, result Result) error {
	return a.Store.Write(ctx, "hybrid-result/"+workflowID, result)
}
func (a *Activities) ReadResult(ctx context.Context, workflowID string) (Result, error) {
	var r Result
	e := a.Store.Read(ctx, "hybrid-result/"+workflowID, &r)
	return r, e
}
func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
