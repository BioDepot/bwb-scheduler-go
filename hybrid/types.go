// Package hybrid runs explicitly bounded stages on Temporal workers, EC2 and
// configured Slurm gateways. Legacy workflow parsing/execution remains intact.
package hybrid

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"

	"go-scheduler/cloudexec"
	"go-scheduler/executioncontract"
	"go-scheduler/transfer"
)

type Command struct {
	Argv           []string `json:"argv"`
	TimeoutSeconds int      `json:"timeout_seconds"`
}
type Request struct {
	ConfigurationHash string             `json:"configuration_hash,omitempty"`
	PolicyHash        string             `json:"policy_hash"`
	Schema            string             `json:"schema"`
	Plan              json.RawMessage    `json:"execution_plan"`
	Commands          map[string]Command `json:"commands"`
}
type Site struct {
	S3Encryption   string `json:"s3_encryption,omitempty"`
	S3KMSKey       string `json:"s3_kms_key,omitempty"`
	Kind           string `json:"kind"`
	Queue          string `json:"queue"`
	Root           string `json:"root"`
	ProfileKey     string `json:"profile_key"`
	MaxJobs        int    `json:"max_jobs"`
	RunnerPath     string `json:"runner_path"`
	RunnerHash     string `json:"runner_hash"`
	Region         string `json:"region"`
	SlurmPartition string `json:"slurm_partition,omitempty"`
	SlurmAccount   string `json:"slurm_account,omitempty"`
}
type Config struct {
	Region         string                       `json:"region"`
	Queue          string                       `json:"queue"`
	Policy         json.RawMessage              `json:"policy"`
	Sites          map[string]Site              `json:"sites"`
	Profiles       map[string]cloudexec.Profile `json:"profiles"`
	JournalRoot    string                       `json:"journal_root"`
	TransferQueues map[string][]string          `json:"transfer_queues"`
}
type Node struct {
	ID      string          `json:"node_id"`
	Key     string          `json:"executor_key"`
	Kind    string          `json:"kind"`
	Profile json.RawMessage `json:"profile"`
	Depends []string        `json:"depends_on"`
}
type Source struct {
	Kind     string `json:"kind"`
	URI      string `json:"uri,omitempty"`
	Executor string `json:"executor_key,omitempty"`
	Node     string `json:"node_id,omitempty"`
	Path     string `json:"path,omitempty"`
	Hash     string `json:"sha256,omitempty"`
	Size     int64  `json:"size_bytes,omitempty"`
	Version  string `json:"version_id,omitempty"`
}
type Artifact struct {
	ID     string `json:"artifact_id"`
	Source Source `json:"source"`
}
type Route struct {
	ID          string `json:"route_id"`
	Source      string `json:"source_executor_key"`
	Destination string `json:"destination_executor_key"`
	Queue       string `json:"worker_queue"`
	Prefix      string `json:"staging_prefix"`
}
type Input struct {
	ArtifactID string `json:"artifact_id"`
	NodeID     string `json:"to_node"`
	Path       string `json:"target_path"`
	RouteID    string `json:"route_id"`
}
type Publication struct {
	NodeID string `json:"node_id"`
	Path   string `json:"path"`
	Prefix string `json:"destination_prefix"`
}
type Plan struct {
	Checkpoints []Checkpoint `json:"checkpoints,omitempty"`
	Identity    struct {
		Owner      string `json:"owner_id"`
		RunID      string `json:"workbench_run_id"`
		RequestID  string `json:"request_id"`
		WorkflowID string `json:"workflow_id"`
		Attempt    int    `json:"attempt"`
	} `json:"identity"`
	GraphHash string `json:"graph_hash"`
	Placement struct {
		Nodes []Node `json:"nodes"`
	} `json:"placement"`
	Storage struct {
		Artifacts    []Artifact    `json:"artifacts"`
		Routes       []Route       `json:"routes"`
		Inputs       []Input       `json:"transfers"`
		Publications []Publication `json:"publications"`
	} `json:"storage"`
}
type Output struct {
	Key         string `json:"key"`
	Path        string `json:"path"`
	Destination string `json:"destination"`
}
type StagedSource struct {
	Stage   *Stage           `json:"stage"`
	Queue   string           `json:"queue"`
	Request transfer.Request `json:"request"`
}
type Stage struct {
	SourceUploads []StagedSource          `json:"source_uploads,omitempty"`
	EBSUploads    []transfer.Request      `json:"ebs_uploads,omitempty"`
	StorageRoots  []string                `json:"storage_roots,omitempty"`
	CloudStorage  cloudexec.S3Policy      `json:"cloud_storage"`
	Node          Node                    `json:"node"`
	Site          Site                    `json:"site"`
	JobID         string                  `json:"job_id"`
	WorkRoot      string                  `json:"work_root"`
	Command       Command                 `json:"command"`
	Inputs        []transfer.Request      `json:"inputs"`
	Outputs       []Output                `json:"outputs"`
	Cloud         *cloudexec.StageRequest `json:"cloud,omitempty"`
}
type Prepared struct {
	Resume      *Result          `json:"resume,omitempty"`
	Request     Request          `json:"request"`
	Plan        Plan             `json:"plan"`
	Stages      map[string]Stage `json:"stages"`
	Order       []string         `json:"order"`
	JournalRoot string           `json:"journal_root"`
}
type NodeResult struct {
	State       string                       `json:"state"`
	Compute     string                       `json:"compute"`
	Publication string                       `json:"publication"`
	Cleanup     string                       `json:"cleanup"`
	Artifacts   map[string]transfer.Artifact `json:"artifacts"`
	Error       string                       `json:"error,omitempty"`
}
type Result struct {
	Checkpoints map[string]CheckpointResult `json:"checkpoints,omitempty"`
	State       string                      `json:"state"`
	Nodes       map[string]NodeResult       `json:"nodes"`
	Owner       string                      `json:"owner_id"`
	WorkflowID  string                      `json:"workflow_id"`
}

func objectHash(v any) (string, error) {
	raw, e := json.Marshal(v)
	if e != nil {
		return "", e
	}
	value, e := executioncontract.Decode(raw)
	if e != nil {
		return "", e
	}
	return executioncontract.CanonicalHash(value)
}
func SafeRelative(p string) bool {
	return p != "" && !path.IsAbs(p) && path.Clean(p) == p && p != ".." && !strings.HasPrefix(p, "../") && !strings.Contains(p, "\\")
}
func Prepare(req Request, c Config) (Prepared, error) {
	if e := ValidateConfig(c); e != nil {
		return Prepared{}, e
	}
	if expected, _ := objectHash(c.Policy); req.PolicyHash != expected {
		return Prepared{}, fmt.Errorf("reviewed operator policy hash differs from scheduler configuration")
	}
	if req.ConfigurationHash != "" && req.ConfigurationHash != ConfigurationHash(c) {
		return Prepared{}, fmt.Errorf("reviewed executor site configuration changed")
	}
	if req.Schema != "biodepot.hybrid_workflow/v1" {
		return Prepared{}, fmt.Errorf("unsupported hybrid request schema")
	}
	if _, e := executioncontract.Authorize(req.Plan, c.Policy); e != nil {
		return Prepared{}, e
	}
	var p Plan
	if e := json.Unmarshal(req.Plan, &p); e != nil {
		return Prepared{}, e
	}
	hash, e := objectHash(req.Commands)
	if e != nil || hash != p.GraphHash {
		return Prepared{}, fmt.Errorf("reviewed graph hash does not match explicit commands")
	}
	if len(req.Commands) != len(p.Placement.Nodes) {
		return Prepared{}, fmt.Errorf("commands must exactly match explicit stage boundaries")
	}
	prepared := Prepared{Request: req, Plan: p, Stages: map[string]Stage{}, JournalRoot: c.JournalRoot}
	planHash, _ := objectHash(json.RawMessage(req.Plan))
	for _, node := range p.Placement.Nodes {
		site, ok := c.Sites[node.Key]
		if !ok || site.Kind != node.Kind || site.Queue == "" || site.MaxJobs < 1 {
			return prepared, fmt.Errorf("executor %s has no configured worker capability", node.Key)
		}
		var pin struct {
			ID       string `json:"profile_id"`
			Revision int    `json:"revision"`
		}
		if e = json.Unmarshal(node.Profile, &pin); e != nil {
			return prepared, e
		}
		if site.ProfileKey != fmt.Sprintf("%s@%d", pin.ID, pin.Revision) {
			return prepared, fmt.Errorf("executor site profile pin mismatch")
		}
		if !path.IsAbs(site.Root) || path.Clean(site.Root) != site.Root || site.Root == "/" {
			return prepared, fmt.Errorf("executor site root must be canonical")
		}
		cmd, ok := req.Commands[node.ID]
		if !ok || len(cmd.Argv) == 0 || cmd.TimeoutSeconds < 1 || cmd.TimeoutSeconds > 604800 {
			return prepared, fmt.Errorf("stage command or timeout missing")
		}
		for _, arg := range cmd.Argv {
			if strings.ContainsRune(arg, 0) {
				return prepared, fmt.Errorf("invalid command argument")
			}
		}
		ids, e := executioncontract.ExecutionIDs(req.Plan, node.ID, "science")
		if e != nil {
			return prepared, e
		}
		job := ids["node_workflow_id"].(string)
		root := path.Join(site.Root, job)
		stage := Stage{Node: node, Site: site, JobID: job, WorkRoot: root, Command: cmd}
		if node.Kind == "aws_ec2" {
			if !path.IsAbs(cmd.Argv[0]) {
				return prepared, fmt.Errorf("AWS scientific entrypoint must be an absolute pinned executable")
			}
			profile, ok := c.Profiles[site.ProfileKey]
			if !ok {
				return prepared, fmt.Errorf("AWS profile configuration missing")
			}
			if e = profile.Validate(); e != nil {
				return prepared, e
			}
			writable := false
			for _, v := range profile.Volumes {
				if !v.ReadOnly && transfer.Within(root, v.Mount) {
					writable = true
				}
			}
			if !writable || cmd.TimeoutSeconds > profile.Limits.Job {
				return prepared, fmt.Errorf("AWS workspace or timeout outside profile limits")
			}
			stage.Site.Region = profile.Region
			stage.CloudStorage = profile.S3
			for _, v := range profile.Volumes {
				stage.StorageRoots = append(stage.StorageRoots, v.Mount)
			}
			stage.Cloud = &cloudexec.StageRequest{Identity: cloudexec.Request{ProfileConfigHash: cloudexec.ProfileHash(profile), JobID: job, OwnerID: p.Identity.Owner, ProfileKey: site.ProfileKey, NodeID: node.ID, Attempt: p.Identity.Attempt, PlanHash: planHash}, Queue: site.Queue, BootstrapSeconds: profile.Limits.Bootstrap, JobSeconds: cmd.TimeoutSeconds, CollectionSeconds: profile.Limits.Collection, Command: cloudexec.RunnerRequest{CommandID: "science", WorkRoot: root, Argv: cmd.Argv, TimeoutSeconds: cmd.TimeoutSeconds, MinimumFreeBytes: 64 * 1024 * 1024}}
		}
		prepared.Stages[node.ID] = stage
		prepared.Order = append(prepared.Order, node.ID)
	}
	sort.Strings(prepared.Order)
	routes := map[string]Route{}
	for _, route := range p.Storage.Routes {
		routes[route.ID] = route
		caps, ok := c.TransferQueues[route.Queue]
		if !ok {
			return prepared, fmt.Errorf("transfer worker queue %s not configured", route.Queue)
		}
		capable := func(key string) bool {
			for _, v := range caps {
				if key == v {
					return true
				}
			}
			return false
		}
		if !capable(route.Source) || !capable(route.Destination) {
			return prepared, fmt.Errorf("transfer route %s lacks endpoint capability", route.ID)
		}
	}
	for _, a := range p.Storage.Artifacts {
		if a.Source.Kind == "ebs_path" {
			for _, in := range p.Storage.Inputs {
				if in.ArtifactID == a.ID && prepared.Stages[in.NodeID].Node.Key != a.Source.Executor {
					return prepared, fmt.Errorf("cross-executor EBS input requires an explicit source stage publication")
				}
			}
		}
		if a.Source.Kind == "stage_output" {
			stage := prepared.Stages[a.Source.Node]
			seen := map[string]bool{}
			for _, in := range p.Storage.Inputs {
				if in.ArtifactID == a.ID {
					r := routes[in.RouteID]
					if seen[r.ID] {
						continue
					}
					seen[r.ID] = true
					stage.Outputs = append(stage.Outputs, Output{Key: a.ID + "." + r.ID, Path: a.Source.Path, Destination: r.Prefix + stage.JobID + "/" + a.ID})
				}
			}
			prepared.Stages[a.Source.Node] = stage
		}
	}
	for i, pub := range p.Storage.Publications {
		stage := prepared.Stages[pub.NodeID]
		stage.Outputs = append(stage.Outputs, Output{Key: fmt.Sprintf("publication-%d", i), Path: pub.Path, Destination: pub.Prefix + stage.JobID + "/" + path.Base(pub.Path)})
		prepared.Stages[pub.NodeID] = stage
	}
	return prepared, nil
}
func DecodeRequest(raw []byte) (Request, error) {
	var r Request
	if _, err := executioncontract.Decode(raw); err != nil {
		return r, err
	}
	if len(raw) > 4*1024*1024 {
		return r, fmt.Errorf("request too large")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	e := d.Decode(&r)
	return r, e
}

// Persistence paths are host-local deployment details. Execution roots, queues,
// runner hashes, data encryption and profile/route capabilities are review pins.
func ConfigurationHash(c Config) string {
	c.JournalRoot = ""
	hash, _ := objectHash(c)
	return hash
}
