package staged

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

type TransferItem struct {
	SourcePath      string `json:"source_path"`
	DestinationPath string `json:"destination_path"`
	Recursive       bool   `json:"recursive"`
}

type Transfer struct {
	SourceEndpointID      string         `json:"source_endpoint_id"`
	DestinationEndpointID string         `json:"destination_endpoint_id"`
	Items                 []TransferItem `json:"items"`
	Label                 string         `json:"label"`
	SubmissionID          string         `json:"submission_id"`
	SyncLevel             string         `json:"sync_level"`
	VerifyChecksum        bool           `json:"verify_checksum"`
	PreserveTimestamp     bool           `json:"preserve_timestamp"`
	TimeoutSeconds        int            `json:"timeout_seconds"`
	PollIntervalSeconds   int            `json:"poll_interval_seconds"`
}

func (t *Transfer) UnmarshalJSON(data []byte) error {
	type plain Transfer
	defaulted := plain{VerifyChecksum: true, PreserveTimestamp: true}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&defaulted); err != nil {
		return err
	}
	*t = Transfer(defaulted)
	return nil
}

type SlurmResources struct {
	Cpus  int `json:"cpus"`
	Gpus  int `json:"gpus"`
	MemMB int `json:"mem_mb"`
}

type SlurmJob struct {
	Script    string         `json:"script"`
	Resources SlurmResources `json:"resources"`
	Config    SlurmOptions   `json:"config"`
	Name      string         `json:"name"`
}

type SlurmOptions struct {
	Partition   string   `json:"partition"`
	Time        string   `json:"time"`
	Nodes       int      `json:"nodes"`
	Ntasks      int      `json:"ntasks"`
	CpusPerTask int      `json:"cpus_per_task"`
	Mem         string   `json:"mem"`
	Gpus        *int     `json:"gpus,omitempty"`
	Modules     []string `json:"modules,omitempty"`
}

type SlurmStage struct {
	TaskQueue           string   `json:"task_queue"`
	Job                 SlurmJob `json:"job"`
	PollIntervalSeconds int      `json:"poll_interval_seconds"`
	TimeoutSeconds      int      `json:"timeout_seconds"`
}

type GPUJob struct {
	Image                 string            `json:"image"`
	Cmd                   []string          `json:"cmd"`
	InputFiles            map[string]string `json:"input_files"`
	UseGPU                bool              `json:"use_gpu"`
	GPUDevice             string            `json:"gpu_device"`
	Env                   map[string]string `json:"env"`
	TimeoutSeconds        int               `json:"timeout_seconds"`
	LocalOutputDir        string            `json:"local_output_dir"`
	Cleanup               *bool             `json:"cleanup,omitempty"`
	MinGPUFreeMB          int               `json:"min_gpu_free_mb"`
	GPUWaitTimeoutSeconds int               `json:"gpu_wait_timeout_seconds"`
}

type GPUStage struct {
	TaskQueue string `json:"task_queue"`
	Job       GPUJob `json:"job"`
}

func (g *GPUStage) UnmarshalJSON(data []byte) error {
	var decoded struct {
		TaskQueue string  `json:"task_queue"`
		Job       *GPUJob `json:"job"`
		GPUJob
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}
	g.TaskQueue = decoded.TaskQueue
	if decoded.Job != nil {
		g.Job = *decoded.Job
	} else {
		g.Job = decoded.GPUJob
	}
	return nil
}

type Request struct {
	WorkflowID      string      `json:"workflow_id"`
	TaskQueue       string      `json:"task_queue"`
	GlobusTaskQueue string      `json:"globus_task_queue"`
	StageIn         *Transfer   `json:"stage_in"`
	Slurm           *SlurmStage `json:"slurm"`
	StageBack       *Transfer   `json:"stage_back"`
	GPU             *GPUStage   `json:"gpu"`
	Publish         *Transfer   `json:"publish"`
}

type Site struct {
	IPAddr         string   `json:"ip_addr"`
	User           string   `json:"user"`
	Port           int      `json:"port"`
	StorageDir     string   `json:"storage_dir"`
	GPUDevice      string   `json:"gpu_device"`
	IdentityFile   string   `json:"identity_file"`
	KnownHostsFile string   `json:"known_hosts_file"`
	AllowedRoots   []string `json:"allowed_roots"`
}

type Config struct {
	Pipeline struct {
		TaskQueue       string `json:"task_queue"`
		GlobusTaskQueue string `json:"globus_task_queue"`
	} `json:"pipeline"`
	Executors struct {
		Globus struct {
			CLIPath            string   `json:"cli_path"`
			AllowedEndpointIDs []string `json:"allowed_endpoint_ids"`
		} `json:"globus"`
		Slurm     Site `json:"slurm"`
		SSHDocker Site `json:"ssh_docker"`
	} `json:"executors"`
}

var identifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:@-]{0,199}$`)
var simpleValue = regexp.MustCompile(`^[A-Za-z0-9_.:+-]+$`)
var imageValue = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_./:@-]+$`)

func SafePath(p string) bool {
	if p == "" || !filepath.IsAbs(p) || strings.ContainsAny(p, "\x00\r\n\t") {
		return false
	}
	for _, part := range strings.Split(p, "/") {
		if part == ".." || part == "." {
			return false
		}
	}
	return true
}

func (r Request) Validate() error {
	if !identifier.MatchString(r.WorkflowID) || !identifier.MatchString(r.TaskQueue) || !identifier.MatchString(r.GlobusTaskQueue) {
		return fmt.Errorf("workflow_id, task_queue, and globus_task_queue must be valid identifiers")
	}
	if r.StageIn == nil && r.Slurm == nil && r.StageBack == nil && r.GPU == nil && r.Publish == nil {
		return fmt.Errorf("at least one stage is required")
	}
	for name, t := range map[string]*Transfer{"stage_in": r.StageIn, "stage_back": r.StageBack, "publish": r.Publish} {
		if t == nil {
			continue
		}
		if !identifier.MatchString(t.SourceEndpointID) || !identifier.MatchString(t.DestinationEndpointID) || !identifier.MatchString(t.SubmissionID) || len(t.Items) == 0 || strings.TrimSpace(t.Label) == "" {
			return fmt.Errorf("%s needs endpoint IDs, submission_id, label, and items", name)
		}
		if t.SyncLevel != "" && t.SyncLevel != "checksum" && t.SyncLevel != "mtime" && t.SyncLevel != "size" && t.SyncLevel != "exists" {
			return fmt.Errorf("%s has unsupported sync_level", name)
		}
		for _, item := range t.Items {
			if !SafePath(item.SourcePath) || !SafePath(item.DestinationPath) {
				return fmt.Errorf("%s has unsafe transfer path", name)
			}
		}
	}
	if r.Slurm != nil {
		s := r.Slurm
		if !identifier.MatchString(s.TaskQueue) || strings.TrimSpace(s.Job.Script) == "" || (s.Job.Name != "" && !identifier.MatchString(s.Job.Name)) {
			return fmt.Errorf("slurm needs task_queue, script, and safe name")
		}
		if strings.ContainsRune(s.Job.Script, 0) || len(s.Job.Script) > 1<<20 {
			return fmt.Errorf("slurm script is invalid")
		}
		c := s.Job.Config
		if !simpleValue.MatchString(c.Partition) || (c.Time != "" && !simpleValue.MatchString(c.Time)) || (c.Mem != "" && !simpleValue.MatchString(c.Mem)) || c.Nodes < 0 || c.Ntasks < 0 || c.CpusPerTask < 0 || s.Job.Resources.Cpus < 0 || s.Job.Resources.MemMB < 0 || s.Job.Resources.Gpus < 0 || (c.Gpus != nil && *c.Gpus < 0) {
			return fmt.Errorf("slurm config is incomplete or unsafe")
		}
		for _, module := range c.Modules {
			if !imageValue.MatchString(module) {
				return fmt.Errorf("slurm module name is unsafe")
			}
		}
	}
	if r.GPU != nil {
		g := r.GPU
		if !identifier.MatchString(g.TaskQueue) || !imageValue.MatchString(g.Job.Image) || len(g.Job.Cmd) == 0 || !SafePath(g.Job.LocalOutputDir) {
			return fmt.Errorf("gpu needs task_queue, image, cmd, and local_output_dir")
		}
		if strings.Contains(strings.ToLower(g.Job.Image), "cellbender") || strings.Contains(strings.ToLower(g.Job.Cmd[0]), "cellbender") {
			cudaFlag := false
			for _, arg := range g.Job.Cmd {
				if arg == "--cuda" {
					cudaFlag = true
				}
			}
			if !g.Job.UseGPU || !cudaFlag {
				return fmt.Errorf("CellBender requires use_gpu=true and --cuda")
			}
		}
		for src, dst := range g.Job.InputFiles {
			if !SafePath(src) || dst == "" || filepath.IsAbs(dst) || strings.HasPrefix(filepath.Clean(dst), "../") || filepath.Clean(dst) == ".." {
				return fmt.Errorf("gpu has unsafe input mapping")
			}
		}
	}
	return nil
}

func (r *Request) Normalize() {
	if r.TaskQueue == "" {
		r.TaskQueue = "staged-slurm-gpu"
	}
	if r.GlobusTaskQueue == "" {
		r.GlobusTaskQueue = r.TaskQueue
	}
	if r.Slurm != nil && r.Slurm.Job.Name == "" {
		r.Slurm.Job.Name = "slurm-script"
	}
}

func (s Site) Queue() string {
	port := s.Port
	if port == 0 {
		port = 22
	}
	return fmt.Sprintf("%s@%s:%d", s.User, s.IPAddr, port)
}

func (s Site) Validate() error {
	if !identifier.MatchString(s.User) || !identifier.MatchString(s.IPAddr) || !SafePath(s.StorageDir) || s.Port < 0 || s.Port > 65535 {
		return fmt.Errorf("invalid SSH site configuration")
	}
	for _, root := range s.AllowedRoots {
		if !SafePath(root) {
			return fmt.Errorf("invalid allowed root")
		}
	}
	return nil
}
