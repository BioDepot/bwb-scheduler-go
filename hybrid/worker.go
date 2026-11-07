package hybrid

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	st "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go-scheduler/cloudexec"
	"go-scheduler/durable"
	"go-scheduler/executioncontract"
	"go-scheduler/transfer"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const WorkflowName = "WorkbenchHybridWorkflowV1"

func LoadConfig(path string) (Config, error) {
	var c Config
	raw, e := os.ReadFile(path)
	if e != nil {
		return c, e
	}
	if e = json.Unmarshal(raw, &c); e != nil {
		return c, e
	}
	return c, ValidateConfig(c)
}
func ValidateConfig(c Config) error {
	if c.Queue == "" || c.Queue == "bwb_worker" || c.Queue == "scheduler-queue" {
		return fmt.Errorf("hybrid orchestration requires a dedicated task queue")
	}
	if !filepath.IsAbs(c.JournalRoot) || filepath.Clean(c.JournalRoot) != c.JournalRoot {
		return fmt.Errorf("durable evidence root must be canonical")
	}
	policy, e := executioncontract.Validate(c.Policy)
	if e != nil {
		return e
	}
	if policy["schema"] != executioncontract.PolicySchema {
		return fmt.Errorf("worker policy required")
	}
	awsPins := map[string]any{}
	for _, raw := range policy["profiles"].([]any) {
		p := raw.(map[string]any)
		ref := p["reference"].(map[string]any)
		key := fmt.Sprintf("%s@%s", ref["profile_id"], ref["revision"])
		if p["kind"] == "aws_ec2" {
			awsPins[key] = p["aws"]
		}
	}
	for key, p := range c.Profiles {
		if e = p.Validate(); e != nil {
			return e
		}
		configured, e := objectHash(p)
		if e != nil {
			return e
		}
		expected, e := objectHash(awsPins[key])
		if e != nil || configured != expected {
			return fmt.Errorf("AWS configuration does not match immutable worker policy: %s", key)
		}
	}
	for key, site := range c.Sites {
		if site.Queue == "" || site.MaxJobs < 1 || !filepath.IsAbs(site.Root) {
			return fmt.Errorf("invalid site %s", key)
		}
		if site.S3Encryption != "" && site.S3Encryption != "AES256" && site.S3Encryption != "aws:kms" {
			return fmt.Errorf("invalid site S3 encryption")
		}
		if site.S3Encryption == "aws:kms" && site.S3KMSKey == "" {
			return fmt.Errorf("site KMS key required")
		}
		if site.Kind == "aws_ec2" {
			if _, ok := c.Profiles[site.ProfileKey]; !ok {
				return fmt.Errorf("missing AWS profile")
			}
		} else if site.Kind != "temporal_worker" && site.Kind != "slurm" {
			return fmt.Errorf("unsupported site kind")
		}
	}
	return nil
}

// StartWorkers serves only selected queues on this host. A site's data paths
// never become visible merely because its config exists on an orchestrator.
func StartWorkers(ctx context.Context, c client.Client, cfg Config, queues []string) ([]worker.Worker, error) {
	if e := ValidateConfig(cfg); e != nil {
		return nil, e
	}
	if len(queues) == 0 {
		return nil, fmt.Errorf("select queues explicitly for this host")
	}
	store := durable.Store{Root: cfg.JournalRoot}
	controller := &cloudexec.Controller{Profiles: cfg.Profiles, Store: store, Clients: cloudexec.SDKClients}
	sdk, e := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.Region), config.WithHTTPClient(&http.Client{Timeout: 24 * time.Hour}), config.WithRetryer(func() aws.Retryer { return retry.AddWithMaxAttempts(retry.NewStandard(), 3) }))
	if e != nil {
		return nil, e
	}
	objects := &transfer.S3Store{Client: s3.NewFromConfig(sdk), Journal: store, Encryption: st.ServerSideEncryptionAes256, Progress: func(ctx context.Context, n int64) {
		if activity.IsActivity(ctx) {
			activity.RecordHeartbeat(ctx, n)
		}
	}}
	sort.Strings(queues)
	seen := map[string]bool{}
	started := []worker.Worker{}
	fail := func(e error) ([]worker.Worker, error) {
		for _, w := range started {
			w.Stop()
		}
		return nil, e
	}
	for _, queue := range queues {
		if seen[queue] {
			return fail(fmt.Errorf("duplicate queue"))
		}
		seen[queue] = true
		localCfg := cfg
		localCfg.Sites = map[string]Site{}
		awsQueue := false
		limit := 1
		allowed := map[string]bool{}
		for _, key := range cfg.TransferQueues[queue] {
			allowed[key] = true
		}
		for key, site := range cfg.Sites {
			if site.Queue == queue || allowed[key] {
				if site.Kind == "aws_ec2" {
					awsQueue = true
				} else {
					localCfg.Sites[key] = site
				}
				if site.MaxJobs > limit {
					limit = site.MaxJobs
				}
			}
		}
		if queue != cfg.Queue && !awsQueue && len(localCfg.Sites) == 0 {
			return fail(fmt.Errorf("queue %s has no declared worker capabilities", queue))
		}
		w := worker.New(c, queue, worker.Options{MaxConcurrentActivityExecutionSize: limit, MaxHeartbeatThrottleInterval: 5 * time.Second, WorkerStopTimeout: 2 * time.Minute})
		a := &Activities{Config: localCfg, Store: store, Objects: objects}
		if queue == cfg.Queue {
			w.RegisterWorkflowWithOptions(Run, workflow.RegisterOptions{Name: WorkflowName})
			w.RegisterWorkflow(NodeWorkflow)
			w.RegisterWorkflow(Inspect)
			w.RegisterWorkflow(Collect)
			w.RegisterActivityWithOptions(a.CollectionLease, activity.RegisterOptions{Name: "hybrid.CollectionLease"})
			w.RegisterActivityWithOptions(a.PersistResult, activity.RegisterOptions{Name: "hybrid.PersistResult"})
			w.RegisterActivityWithOptions(a.ReadResult, activity.RegisterOptions{Name: "hybrid.ReadResult"})
		}
		if len(localCfg.Sites) > 0 {
			w.RegisterActivityWithOptions(a.Logs, activity.RegisterOptions{Name: "hybrid.Logs"})
			w.RegisterActivityWithOptions(a.PrepareWorkspace, activity.RegisterOptions{Name: "hybrid.PrepareWorkspace"})
			w.RegisterActivityWithOptions(a.RunLocal, activity.RegisterOptions{Name: "hybrid.RunLocal"})
			w.RegisterActivityWithOptions(a.Transfer, activity.RegisterOptions{Name: "hybrid.Transfer"})
			w.RegisterActivityWithOptions(a.Publish, activity.RegisterOptions{Name: "hybrid.Publish"})
		}
		if awsQueue {
			w.RegisterWorkflow(cloudexec.StageWorkflow)
			for _, entry := range []struct {
				name string
				fn   any
			}{{"Probe", controller.Probe}, {"Acquire", controller.Acquire}, {"PrepareStorage", controller.PrepareStorage}, {"DispatchRunner", controller.DispatchRunner}, {"PollRunner", controller.PollRunner}, {"RecordOutcome", controller.RecordOutcome}, {"Cleanup", controller.Cleanup}, {"BeginCollection", controller.BeginCollection}, {"Reconcile", controller.Reconcile}, {"Status", controller.Status}, {"SaveStageResult", controller.SaveStageResult}, {"ReadStageResult", controller.ReadStageResult}} {
				w.RegisterActivityWithOptions(entry.fn, activity.RegisterOptions{Name: "cloud." + entry.name})
			}
		}
		if e = w.Start(); e != nil {
			return fail(e)
		}
		started = append(started, w)
	}
	return started, nil
}
