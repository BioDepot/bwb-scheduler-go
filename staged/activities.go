package staged

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Activities struct{ Config Config }

func run(ctx context.Context, stdin string, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var out, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%s: %w: %s", name, err, tail(stderr.String(), 1000))
	}
	return out.String(), nil
}

func tail(s string, n int) string {
	if len(s) > n {
		return s[len(s)-n:]
	}
	return s
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }

func (a *Activities) globusCLI() string {
	if a.Config.Executors.Globus.CLIPath != "" {
		return a.Config.Executors.Globus.CLIPath
	}
	return "globus"
}

func (a *Activities) endpointAllowed(id string) bool {
	for _, allowed := range a.Config.Executors.Globus.AllowedEndpointIDs {
		if id == allowed {
			return true
		}
	}
	return false
}

func (a *Activities) SubmitTransfer(ctx context.Context, t Transfer) (string, error) {
	if !a.endpointAllowed(t.SourceEndpointID) || !a.endpointAllowed(t.DestinationEndpointID) {
		return "", fmt.Errorf("Globus endpoint is not allowlisted")
	}
	if t.SubmissionID == "" || len(t.Items) == 0 {
		return "", fmt.Errorf("Globus submission_id and items are required")
	}
	var batch strings.Builder
	for _, item := range t.Items {
		if !SafePath(item.SourcePath) || !SafePath(item.DestinationPath) {
			return "", fmt.Errorf("unsafe Globus transfer path")
		}
		if item.Recursive {
			batch.WriteString("--recursive ")
		}
		batch.WriteString(shellQuote(item.SourcePath) + " " + shellQuote(item.DestinationPath) + "\n")
	}
	syncLevel := t.SyncLevel
	if syncLevel == "" {
		syncLevel = "checksum"
	}
	args := []string{"transfer", t.SourceEndpointID + ":", t.DestinationEndpointID + ":", "--batch", "-", "--format", "json", "--notify", "off", "--label", t.Label, "--submission-id", t.SubmissionID, "--sync-level", syncLevel}
	if t.VerifyChecksum {
		args = append(args, "--verify-checksum")
	} else {
		args = append(args, "--no-verify-checksum")
	}
	if t.PreserveTimestamp {
		args = append(args, "--preserve-mtime")
	}
	ctx, cancel := context.WithTimeout(ctx, 9*time.Minute)
	defer cancel()
	out, err := run(ctx, batch.String(), a.globusCLI(), args...)
	if err != nil {
		return "", err
	}
	var response struct {
		TaskID string `json:"task_id"`
	}
	if err := json.Unmarshal([]byte(out), &response); err != nil {
		return "", fmt.Errorf("Globus returned invalid JSON: %w", err)
	}
	if response.TaskID == "" {
		return "", fmt.Errorf("Globus returned no task_id")
	}
	return response.TaskID, nil
}

func (a *Activities) PollTransfer(ctx context.Context, taskID string) (TransferStatus, error) {
	if !identifier.MatchString(taskID) {
		return TransferStatus{}, fmt.Errorf("invalid Globus task ID")
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	out, err := run(ctx, "", a.globusCLI(), "task", "show", taskID, "--format", "json")
	if err != nil {
		return TransferStatus{}, err
	}
	var raw struct {
		TaskID           string          `json:"task_id"`
		Status           string          `json:"status"`
		Files            int             `json:"files"`
		FilesTransferred int             `json:"files_transferred"`
		BytesTransferred int64           `json:"bytes_transferred"`
		Faults           int             `json:"faults"`
		FatalError       json.RawMessage `json:"fatal_error"`
	}
	if err := json.Unmarshal([]byte(out), &raw); err != nil {
		return TransferStatus{}, err
	}
	return TransferStatus{TaskID: taskID, Status: strings.ToUpper(raw.Status), Files: raw.Files, FilesTransferred: raw.FilesTransferred, BytesTransferred: raw.BytesTransferred, Faults: raw.Faults, FatalError: string(raw.FatalError)}, nil
}

func (a *Activities) ssh(ctx context.Context, site Site, command, stdin string) (string, error) {
	if err := site.Validate(); err != nil {
		return "", err
	}
	port := site.Port
	if port == 0 {
		port = 22
	}
	args := []string{"-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "ConnectTimeout=15", "-p", strconv.Itoa(port)}
	if site.IdentityFile != "" {
		args = append(args, "-i", site.IdentityFile)
	}
	if site.KnownHostsFile != "" {
		args = append(args, "-o", "UserKnownHostsFile="+site.KnownHostsFile)
	}
	args = append(args, site.User+"@"+site.IPAddr, command)
	return run(ctx, stdin, "ssh", args...)
}

func (a *Activities) rsync(ctx context.Context, site Site, source, dest string) error {
	if err := site.Validate(); err != nil {
		return err
	}
	port := site.Port
	if port == 0 {
		port = 22
	}
	transport := fmt.Sprintf("ssh -o BatchMode=yes -o StrictHostKeyChecking=yes -p %d", port)
	if site.IdentityFile != "" {
		transport += " -i " + shellQuote(site.IdentityFile)
	}
	if site.KnownHostsFile != "" {
		transport += " -o " + shellQuote("UserKnownHostsFile="+site.KnownHostsFile)
	}
	_, err := run(ctx, "", "rsync", "-az", "-s", "-e", transport, source, dest)
	return err
}

func (a *Activities) SubmitSlurm(ctx context.Context, job SlurmJob) (string, error) {
	site := a.Config.Executors.Slurm
	if err := site.Validate(); err != nil {
		return "", err
	}
	if job.Name == "" {
		job.Name = "slurm-script"
	}
	if !identifier.MatchString(job.Name) || !simpleValue.MatchString(job.Config.Partition) || (job.Config.Time != "" && !simpleValue.MatchString(job.Config.Time)) || (job.Config.Mem != "" && !simpleValue.MatchString(job.Config.Mem)) || job.Config.Nodes < 0 || job.Config.Ntasks < 0 || job.Config.CpusPerTask < 0 {
		return "", fmt.Errorf("invalid Slurm options")
	}
	if job.Script == "" || strings.ContainsRune(job.Script, 0) {
		return "", fmt.Errorf("empty or invalid Slurm script")
	}
	logDir := filepath.Join(site.StorageDir, "slurm")
	if _, err := a.ssh(ctx, site, "mkdir -p "+shellQuote(logDir), ""); err != nil {
		return "", err
	}
	args := []string{"sbatch", "--parsable", "--job-name=" + job.Name, "--partition=" + job.Config.Partition, "--output=" + filepath.Join(logDir, "%j.out"), "--error=" + filepath.Join(logDir, "%j.err")}
	if job.Config.Time != "" {
		args = append(args, "--time="+job.Config.Time)
	}
	if job.Config.Nodes > 0 {
		args = append(args, "--nodes="+strconv.Itoa(job.Config.Nodes))
	}
	if job.Config.Ntasks > 0 {
		args = append(args, "--ntasks="+strconv.Itoa(job.Config.Ntasks))
	}
	cpus := job.Config.CpusPerTask
	if cpus == 0 {
		cpus = job.Resources.Cpus
	}
	if cpus == 0 {
		cpus = 1
	}
	args = append(args, "--cpus-per-task="+strconv.Itoa(cpus))
	mem := job.Config.Mem
	if mem == "" {
		memMB := job.Resources.MemMB
		if memMB == 0 {
			memMB = 1024
		}
		mem = strconv.Itoa(memMB) + "MB"
	}
	args = append(args, "--mem="+mem)
	gpus := job.Resources.Gpus
	if job.Config.Gpus != nil {
		gpus = *job.Config.Gpus
	}
	if gpus > 0 {
		args = append(args, "--gres=gpu:"+strconv.Itoa(gpus))
	}
	var script strings.Builder
	script.WriteString("#!/bin/bash\n")
	for _, module := range job.Config.Modules {
		if !imageValue.MatchString(module) {
			return "", fmt.Errorf("invalid Slurm module")
		}
		script.WriteString("module load " + module + "\n")
	}
	script.WriteString(job.Script)
	script.WriteString("\n")
	cmd := strings.Join(args, " ")
	out, err := a.ssh(ctx, site, cmd, script.String())
	if err != nil {
		return "", err
	}
	id := strings.Split(strings.TrimSpace(out), ";")[0]
	if _, err := strconv.Atoi(id); err != nil {
		return "", fmt.Errorf("invalid sbatch job ID %q", out)
	}
	return id, nil
}

var slurmTerminal = map[string]bool{"COMPLETED": true, "FAILED": true, "CANCELLED": true, "TIMEOUT": true, "OUT_OF_MEMORY": true, "NODE_FAIL": true, "BOOT_FAIL": true, "PREEMPTED": true, "DEADLINE": true}
var slurmStateSuffix = regexp.MustCompile(`\+.*$`)

func (a *Activities) PollSlurm(ctx context.Context, jobID string) (SlurmStatus, error) {
	if _, err := strconv.Atoi(jobID); err != nil {
		return SlurmStatus{}, fmt.Errorf("invalid Slurm job ID")
	}
	site := a.Config.Executors.Slurm
	out, err := a.ssh(ctx, site, "sacct -j "+jobID+" -o JobIDRaw,State,ExitCode -n -P", "")
	if err != nil {
		return SlurmStatus{}, err
	}
	status := SlurmStatus{JobID: jobID, Status: "RUNNING"}
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Split(line, "|")
		if len(fields) < 3 || fields[0] != jobID {
			continue
		}
		status.Status = slurmStateSuffix.ReplaceAllString(strings.TrimSpace(fields[1]), "")
		exit, _ := strconv.Atoi(strings.Split(fields[2], ":")[0])
		status.ExitCode = exit
		break
	}
	if slurmTerminal[status.Status] {
		logDir := filepath.Join(site.StorageDir, "slurm")
		logs, err := a.ssh(ctx, site, "tail -c 2000 "+shellQuote(filepath.Join(logDir, jobID+".out"))+" "+shellQuote(filepath.Join(logDir, jobID+".err")), "")
		if err != nil {
			return SlurmStatus{}, fmt.Errorf("reading Slurm logs: %w", err)
		}
		status.Logs = logs
	}
	return status, nil
}

func (a *Activities) CancelSlurm(ctx context.Context, jobID string) error {
	if _, err := strconv.Atoi(jobID); err != nil {
		return err
	}
	_, err := a.ssh(ctx, a.Config.Executors.Slurm, "scancel "+jobID, "")
	return err
}

func (a *Activities) gpuDir(jobID string) (string, error) {
	if !identifier.MatchString(jobID) {
		return "", fmt.Errorf("invalid GPU job ID")
	}
	site := a.Config.Executors.SSHDocker
	if err := site.Validate(); err != nil {
		return "", err
	}
	return filepath.Join(site.StorageDir, jobID), nil
}

func withinRoots(path string, roots []string) bool {
	if !SafePath(path) {
		return false
	}
	resolved, err := resolveExistingAncestors(path)
	if err != nil {
		return false
	}
	for _, root := range roots {
		if !SafePath(root) {
			continue
		}
		resolvedRoot, err := resolveExistingAncestors(root)
		if err != nil {
			continue
		}
		rel, err := filepath.Rel(resolvedRoot, resolved)
		if err == nil && rel != ".." && !strings.HasPrefix(rel, "../") {
			return true
		}
	}
	return false
}

func resolveExistingAncestors(path string) (string, error) {
	current := filepath.Clean(path)
	var suffix []string
	for {
		resolved, err := filepath.EvalSymlinks(current)
		if err == nil {
			parts := append([]string{resolved}, suffix...)
			return filepath.Join(parts...), nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", err
		}
		suffix = append([]string{filepath.Base(current)}, suffix...)
		current = parent
	}
}

func (a *Activities) PreflightGPU(ctx context.Context, job GPUJob) error {
	site := a.Config.Executors.SSHDocker
	if err := site.Validate(); err != nil {
		return err
	}
	out, err := a.ssh(ctx, site, "docker info --format '{{.ID}}'", "")
	if err != nil || strings.TrimSpace(out) == "" {
		return fmt.Errorf("remote Docker unavailable: %v", err)
	}
	if !job.UseGPU {
		return nil
	}
	device := job.GPUDevice
	if device == "" {
		device = site.GPUDevice
	}
	out, err = a.ssh(ctx, site, "nvidia-smi --query-gpu=index,memory.free --format=csv,noheader,nounits", "")
	if err != nil {
		return fmt.Errorf("remote CUDA GPU unavailable: %w", err)
	}
	found := false
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Split(line, ",")
		if len(fields) != 2 {
			continue
		}
		index := strings.TrimSpace(fields[0])
		free, e := strconv.Atoi(strings.TrimSpace(fields[1]))
		if e != nil {
			continue
		}
		if device != "" && !strings.Contains(","+device+",", ","+index+",") {
			continue
		}
		if free >= job.MinGPUFreeMB {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("no requested GPU has %d MB free", job.MinGPUFreeMB)
	}
	return nil
}

func (a *Activities) SetupGPU(ctx context.Context, jobID string) error {
	dir, err := a.gpuDir(jobID)
	if err != nil {
		return err
	}
	_, err = a.ssh(ctx, a.Config.Executors.SSHDocker, "mkdir -p "+shellQuote(filepath.Join(dir, "work", "output")), "")
	return err
}

func (a *Activities) UploadGPU(ctx context.Context, jobID string, job GPUJob) error {
	dir, err := a.gpuDir(jobID)
	if err != nil {
		return err
	}
	site := a.Config.Executors.SSHDocker
	if len(site.AllowedRoots) == 0 {
		return fmt.Errorf("GPU worker requires allowed_roots for local file access")
	}
	for source, relative := range job.InputFiles {
		if !withinRoots(source, site.AllowedRoots) || relative == "" || filepath.IsAbs(relative) || filepath.Clean(relative) == ".." || strings.HasPrefix(filepath.Clean(relative), "../") {
			return fmt.Errorf("GPU input mapping is outside allowed roots")
		}
		dest := filepath.Join(dir, "work", relative)
		if _, err := a.ssh(ctx, site, "mkdir -p "+shellQuote(filepath.Dir(dest)), ""); err != nil {
			return err
		}
		info, err := os.Stat(source)
		if err != nil {
			return err
		}
		remote := site.User + "@" + site.IPAddr + ":" + dest
		if info.IsDir() {
			source = strings.TrimRight(source, "/") + "/"
			remote += "/"
		}
		if err := a.rsync(ctx, site, source, remote); err != nil {
			return err
		}
	}
	return nil
}

func (a *Activities) RunGPU(ctx context.Context, jobID string, job GPUJob) (string, error) {
	dir, err := a.gpuDir(jobID)
	if err != nil {
		return "", err
	}
	if !imageValue.MatchString(job.Image) || len(job.Cmd) == 0 {
		return "", fmt.Errorf("invalid Docker image or command")
	}
	site := a.Config.Executors.SSHDocker
	workDir := filepath.Join(dir, "work")
	args := []string{"docker", "run", "--rm", "--name", jobID, "--user", "$(id -u):$(id -g)", "-v", shellQuote(workDir + ":" + workDir), "-w", shellQuote(workDir)}
	if job.UseGPU {
		device := job.GPUDevice
		if device == "" {
			device = site.GPUDevice
		}
		if device == "" {
			args = append(args, "--gpus", "all")
		} else {
			if !regexp.MustCompile(`^[0-9,]+$`).MatchString(device) {
				return "", fmt.Errorf("invalid GPU device")
			}
			args = append(args, "--gpus", shellQuote("device="+device))
		}
	}
	for k, v := range job.Env {
		if !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`).MatchString(k) {
			return "", fmt.Errorf("invalid Docker environment key")
		}
		args = append(args, "-e", shellQuote(k+"="+v))
	}
	args = append(args, shellQuote(job.Image))
	for _, arg := range job.Cmd {
		args = append(args, shellQuote(arg))
	}
	out, err := a.ssh(ctx, site, strings.Join(args, " "), "")
	return tail(out, 2000), err
}

func (a *Activities) DownloadGPU(ctx context.Context, jobID, localDir string) (int, error) {
	dir, err := a.gpuDir(jobID)
	if err != nil {
		return 0, err
	}
	site := a.Config.Executors.SSHDocker
	if len(site.AllowedRoots) == 0 || !withinRoots(localDir, site.AllowedRoots) {
		return 0, fmt.Errorf("GPU output is outside allowed roots")
	}
	if err := os.MkdirAll(localDir, 0750); err != nil {
		return 0, err
	}
	remote := site.User + "@" + site.IPAddr + ":" + filepath.Join(dir, "work", "output") + "/"
	if err := a.rsync(ctx, site, remote, strings.TrimRight(localDir, "/")+"/"); err != nil {
		return 0, err
	}
	entries, err := os.ReadDir(localDir)
	if err != nil {
		return 0, err
	}
	return len(entries), nil
}

func (a *Activities) CleanupGPU(ctx context.Context, jobID string) error {
	dir, err := a.gpuDir(jobID)
	if err != nil {
		return err
	}
	_, err = a.ssh(ctx, a.Config.Executors.SSHDocker, "docker rm -f "+shellQuote(jobID)+" >/dev/null 2>&1 || true; rm -rf -- "+shellQuote(dir), "")
	return err
}
