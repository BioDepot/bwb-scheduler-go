package workflow

import (
	"bytes"
	"fmt"
	"go-scheduler/fs"
	"go-scheduler/parsing"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"golang.org/x/crypto/ssh"
)

const MAX_SLURM_RETRIES = 2

const (
	sectionStdout     = "STDOUT"
	sectionStderr     = "STDERR"
	sectionOutputFile = "OUTPUT_FILE"
	// missingFileMarker is emitted by the gather script when a file does not
	// exist after the 5-second grace period, replicating the old
	// AwaitFileExistence behaviour.
	missingFileMarker = "__SLURM_FILE_MISSING__"
)

var JOB_CODES = map[string]struct {
	done   bool
	failed bool
	fatal  bool
}{
	"RUNNING":       {done: false, failed: false, fatal: false},
	"COMPLETED":     {done: true, failed: false, fatal: false},
	"BOOT_FAIL":     {done: true, failed: true, fatal: false},
	"CANCELLED":     {done: true, failed: true, fatal: true},
	"DEADLINE":      {done: true, failed: true, fatal: false},
	"FAILED":        {done: true, failed: true, fatal: true},
	"NODE_FAIL":     {done: true, failed: true, fatal: false},
	"OUT_OF_MEMORY": {done: true, failed: true, fatal: false},
	"PREEMPTED":     {done: true, failed: true, fatal: false},
}

type SlurmJob struct {
	CmdId             int
	TmpOutputHostPath string
	ExpOutFilePnames  []string
	JobId             string
	SbatchPath        string
	OutPath           string
	ErrPath           string
}

type GetOutputsFuture struct {
	future   workflow.Future
	results  map[string]SacctResult
	callback func(workflow.Future)
}

type SlurmState struct {
	ParentWfId            string
	ParentWfRunId         string
	SlurmConfig           parsing.SshConfig
	SlurmFS               fs.SshFS
	RunningJobs           map[string]SlurmJob
	PossiblyCompletedJobs map[string]struct{}
	Requests              map[int]SlurmRequest
	NumRetries            map[int]int
	GetOutputFutures      map[int]GetOutputsFuture
	SchedDir              string
	StorageId             string
	MaxSlurmBatchId       int
	FinalErr              error
}

type SlurmActivity struct {
	Config     parsing.SshConfig
	Mtx        sync.RWMutex
	Client     *ssh.Client
	ConnConfig *ssh.ClientConfig
}

type CmdOut struct {
	ExitCode int
	StdOut   string
	StdErr   string
}

type SlurmRequest struct {
	Cmd    parsing.CmdRunParams
	Config parsing.SlurmJobConfig
}

type SlurmResponse struct {
	Result CmdOutput
	Error  *string
}

// rawJobOutputs holds the unparsed text sections for a single job.
type rawJobOutputs struct {
	stdOut      string
	stdErr      string
	outputFiles map[string]string
}

func GetTemporalSshQueueName(config parsing.SshConfig) string {
	return fmt.Sprintf("%s@%s", config.User, config.IpAddr)
}

func (connMan *SlurmActivity) ensureConnected() (*ssh.Client, error) {
	connMan.Mtx.RLock()
	c := connMan.Client
	connMan.Mtx.RUnlock()

	if c != nil {
		return c, nil
	}

	connMan.Mtx.Lock()
	defer connMan.Mtx.Unlock()
	// Handle case where another goroutine already reconnected.
	if connMan.Client != nil {
		return connMan.Client, nil
	}

	client, err := ssh.Dial("tcp", connMan.Config.IpAddr, connMan.ConnConfig)
	if err != nil {
		return nil, err
	}
	connMan.Client = client
	return client, nil
}

func (connMan *SlurmActivity) Close() {
	connMan.Mtx.Lock()
	defer connMan.Mtx.Unlock()
	if connMan.Client != nil {
		connMan.Client.Close()
		connMan.Client = nil
	}
}

func (connMan *SlurmActivity) ExecCmd(cmd string) (CmdOut, error) {
	fmt.Println(cmd)
	connMan.Mtx.RLock()
	defer connMan.Mtx.RUnlock()
	if connMan.Client == nil {
		return CmdOut{}, fmt.Errorf(
			"worker does not have established SSH client for %s@%s",
			connMan.Config.User, connMan.Config.IpAddr,
		)
	}

	var stdout, stderr bytes.Buffer
	session, err := connMan.Client.NewSession()
	if err != nil {
		// If we can't create session, try resetting client.
		connMan.Close()
		client, err := connMan.ensureConnected()
		if err != nil {
			return CmdOut{}, fmt.Errorf("error reconnecting to ssh: %s", err)
		}

		session, err = client.NewSession()
		if err != nil {
			return CmdOut{}, fmt.Errorf("error getting ssh session: %s", err)
		}
	}

	var fullCmd string
	if connMan.Config.CmdPrefix == nil {
		fullCmd = cmd
	} else {
		fullCmd = fmt.Sprintf("%s %s", *connMan.Config.CmdPrefix, cmd)
	}

	session.Stdout = &stdout
	session.Stderr = &stderr
	err = session.Run(fullCmd)
	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*ssh.ExitError); ok {
			exitCode = exitErr.ExitStatus()
		}
	}
	return CmdOut{
		ExitCode: exitCode,
		StdOut:   stdout.String(),
		StdErr:   stderr.String(),
	}, err
}

func WriteSbatchFile(
	outStream io.Writer, cmd parsing.CmdTemplate, volumes map[string]string,
	slurmConfig parsing.SshConfig, jobConfig parsing.SlurmJobConfig,
	slurmDir, imageDir, jobSlurmId string,
) (string, string, error) {
	outBasePath := fmt.Sprintf("%s.out", jobSlurmId)
	errBasePath := fmt.Sprintf("%s.err", jobSlurmId)
	outPath := filepath.Join(slurmDir, outBasePath)
	errPath := filepath.Join(slurmDir, errBasePath)
	fmt.Fprintln(outStream, "#!/bin/bash")
	fmt.Fprintf(outStream, "#SBATCH --output=%s\n", outPath)
	fmt.Fprintf(outStream, "#SBATCH --error=%s\n", errPath)

	if jobConfig.Partition != nil {
		fmt.Fprintf(outStream, "#SBATCH --partition=%s\n", *jobConfig.Partition)
	}

	if jobConfig.Time != nil {
		fmt.Fprintf(outStream, "#SBATCH --time=%s\n", *jobConfig.Time)
	}

	if jobConfig.Ntasks != nil {
		fmt.Fprintf(outStream, "#SBATCH --ntasks=%d\n", *jobConfig.Ntasks)
	}

	if jobConfig.Nodes != nil {
		fmt.Fprintf(outStream, "#SBATCH --nodes=%d\n", *jobConfig.Nodes)
	}

	if jobConfig.Gpus != nil {
		fmt.Fprintf(outStream, "#SBATCH --gpus=%s\n", *jobConfig.Gpus)
	}

	if jobConfig.Mem != nil {
		fmt.Fprintf(outStream, "#SBATCH --mem=%s\n", *jobConfig.Mem)
	} else {
		fmt.Fprintf(outStream, "#SBATCH --mem=%dMB\n", cmd.ResourceReqs.MemMb)
	}

	if jobConfig.CpusPerTask != nil {
		fmt.Fprintf(outStream, "#SBATCH --cpus-per-task=%d\n", *jobConfig.CpusPerTask)
	} else {
		fmt.Fprintf(outStream, "#SBATCH --cpus-per-task=%d\n", cmd.ResourceReqs.Cpus)
	}

	if jobConfig.Modules != nil {
		for _, module := range *jobConfig.Modules {
			fmt.Fprintf(outStream, "module load %s\n", module)
		}
	}

	if len(jobConfig.Environment) > 0 {
		keys := make([]string, 0, len(jobConfig.Environment))
		for key := range jobConfig.Environment {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			value := strings.ReplaceAll(jobConfig.Environment[key], "'", "'\\''")
			fmt.Fprintf(outStream, "export %s='%s'\n", key, value)
		}
	}

	localSifPath := filepath.Join(imageDir, cmd.ImageName)
	useGpu := jobConfig.Gpus != nil || cmd.ResourceReqs.Gpus > 0
	cmdStr, envs := parsing.FormSingularityCmd(
		cmd, volumes, localSifPath, useGpu,
	)
	fmt.Fprintf(outStream, "%s %s", strings.Join(envs, " "), cmdStr)
	fmt.Printf("Writing sbatch w/ cmd %s %s\n", strings.Join(envs, " "), cmdStr)

	return outPath, errPath, nil
}

type SacctResult struct {
	JobId    string
	State    string
	ExitCode string
}

func RunSacct(
	outstandingJobIds []string, runCmd CmdRunner,
) (map[string]SacctResult, error) {
	oustandingJobsStr := ""
	for i, jobId := range outstandingJobIds {
		oustandingJobsStr += jobId
		if i < len(outstandingJobIds)-1 {
			oustandingJobsStr += ","
		}
	}

	sacctCmd := fmt.Sprintf(
		"sacct -j %s -o JobID,State,ExitCode -n -P", oustandingJobsStr,
	)
	sacctOut, err := runCmd(sacctCmd)
	if err != nil {
		return nil, fmt.Errorf(
			"sacct failed with exit code %d, error %s, and stderr %s",
			sacctOut.ExitCode, err, sacctOut.StdErr,
		)
	}

	out := make(map[string]SacctResult, 0)
	jobRecords := strings.Split(sacctOut.StdOut, "\n")
	for _, rawRecord := range jobRecords {
		// Handle trailing / leading newlines.
		if rawRecord == "" {
			continue
		}

		rawFields := strings.Split(rawRecord, "|")
		if len(rawFields) != 3 {
			return nil, fmt.Errorf(
				"sacct returned record \"%s\" with more/fewer than 3 fields; "+
					"expected fields JobID, State, and ExitCode", rawRecord,
			)
		}

		if strings.HasSuffix(rawFields[0], ".batch") || strings.HasSuffix(rawFields[0], ".extern") {
			continue
		}

		cleanedJobId := strings.Split(rawFields[0], ".")[0]
		out[cleanedJobId] = SacctResult{
			JobId:    cleanedJobId,
			State:    rawFields[1],
			ExitCode: rawFields[2],
		}
	}

	return out, nil
}

// shellQuote wraps s in single quotes and escapes any embedded single quotes.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

// randomToken generates a 16-character hex string used as the section
// delimiter token embedded in the gather script.
func randomToken() string {
	return fmt.Sprintf("%016x", rand.Uint64())
}

// buildGatherScript constructs a shell script that, in a single SSH session:
//   - reads stdout, stderr, and all tmp-output files for every job in jobs,
//   - then removes each job's .out, .err, .sbatch, and tmp-output directory.
//
// Output sections are delimited by lines of the form:
//
//	<token>:<jobIndex>:<section>:<extra>
//
// where <extra> carries the filename for OUTPUT_FILE sections and is empty
// otherwise. The token is randomly generated per call, making accidental
// collision with actual job output negligible.
//
// The script replicates the old AwaitFileExistence grace period: it waits 5
// seconds and retries before concluding a file is absent, then emits
// missingFileMarker so the parser can surface a clean error.
func buildGatherScript(jobs []SlurmJob, token string) string {
	var b strings.Builder

	b.WriteString("#!/bin/sh\n")
	fmt.Fprintf(&b,
		"emit_sep() { printf '\\n%s:%%s:%%s:%%s\\n' \"$1\" \"$2\" \"$3\"; }\n",
		token,
	)
	b.WriteString("await_and_cat() {\n")
	b.WriteString("  local p=$1\n")
	b.WriteString("  if [ ! -f \"$p\" ]; then sleep 5; fi\n")
	b.WriteString("  if [ -f \"$p\" ]; then cat \"$p\"; else printf '%s' '" + missingFileMarker + "'; fi\n")
	b.WriteString("}\n")

	for i, job := range jobs {
		idx := fmt.Sprintf("%d", i)

		fmt.Fprintf(&b, "emit_sep %s %s ''\n", idx, sectionStdout)
		fmt.Fprintf(&b, "await_and_cat %s\n", shellQuote(job.OutPath))

		fmt.Fprintf(&b, "emit_sep %s %s ''\n", idx, sectionStderr)
		fmt.Fprintf(&b, "await_and_cat %s\n", shellQuote(job.ErrPath))

		// ls -1 produces no output (and exits non-zero) when the directory is
		// absent or empty; the for-loop simply has zero iterations in that case.
		fmt.Fprintf(&b,
			"for __f in $(ls -1 %s 2>/dev/null); do\n",
			shellQuote(job.TmpOutputHostPath),
		)
		fmt.Fprintf(&b, "  emit_sep %s %s \"$__f\"\n", idx, sectionOutputFile)
		fmt.Fprintf(&b,
			"  await_and_cat %s\n",
			// filepath.Join would strip the literal "$__f", so concatenate manually.
			shellQuote(job.TmpOutputHostPath+"/")+"\"$__f\"",
		)
		b.WriteString("done\n")

		// Clean up all remote artefacts for this job now that their contents
		// have been read. SbatchPath, OutPath, ErrPath are individual files;
		// TmpOutputHostPath is a directory. All are removed unconditionally —
		// if any are already absent the rm simply does nothing.
		fmt.Fprintf(&b,
			"rm -rf %s %s %s %s\n",
			shellQuote(job.SbatchPath),
			shellQuote(job.OutPath),
			shellQuote(job.ErrPath),
			shellQuote(job.TmpOutputHostPath),
		)
	}

	return b.String()
}

// parseGatherOutput splits the combined stdout of the gather script back into
// per-job sections. jobs must be in the same order that was passed to
// buildGatherScript. Returns a map from CmdId to rawJobOutputs.
func parseGatherOutput(raw string, jobs []SlurmJob, token string) (map[int]rawJobOutputs, error) {
	results := make(map[int]rawJobOutputs, len(jobs))
	for _, job := range jobs {
		results[job.CmdId] = rawJobOutputs{
			outputFiles: make(map[string]string),
		}
	}

	// Split on the delimiter prefix. emit_sep always writes a leading newline
	// before the token, so every delimiter line arrives as "\n<token>:...".
	// Splitting on that prefix leaves section bodies as plain strings.
	prefix := "\n" + token + ":"
	sections := strings.Split(raw, prefix)

	// The zeroth element is content before the first delimiter (empty in
	// practice). Discard it, but also handle the unlikely case where the
	// script output starts with the delimiter immediately (no leading newline).
	if len(sections) > 0 && strings.HasPrefix(sections[0], token+":") {
		sections[0] = sections[0][len(token)+1:]
	} else {
		sections = sections[1:]
	}

	for _, section := range sections {
		newline := strings.IndexByte(section, '\n')
		if newline < 0 {
			continue
		}
		header := section[:newline]
		content := section[newline+1:]

		// Header format: "<jobIndex>:<sectionType>:<extra>"
		// SplitN(..., 3) preserves colons that may appear in filenames.
		parts := strings.SplitN(header, ":", 3)
		if len(parts) != 3 {
			return nil, fmt.Errorf(
				"gather script produced malformed section header %q", header,
			)
		}
		idxStr, sectionType, extra := parts[0], parts[1], parts[2]

		var jobIdx int
		if _, err := fmt.Sscanf(idxStr, "%d", &jobIdx); err != nil {
			return nil, fmt.Errorf(
				"gather script produced non-integer job index %q in header %q",
				idxStr, header,
			)
		}
		if jobIdx < 0 || jobIdx >= len(jobs) {
			return nil, fmt.Errorf(
				"gather script produced out-of-range job index %d (have %d jobs)",
				jobIdx, len(jobs),
			)
		}

		job := jobs[jobIdx]
		entry := results[job.CmdId]

		switch sectionType {
		case sectionStdout:
			if content == missingFileMarker {
				return nil, fmt.Errorf(
					"stdout file missing for job ID %s (cmd ID %d): %s",
					job.JobId, job.CmdId, job.OutPath,
				)
			}
			entry.stdOut = content
		case sectionStderr:
			if content == missingFileMarker {
				return nil, fmt.Errorf(
					"stderr file missing for job ID %s (cmd ID %d): %s",
					job.JobId, job.CmdId, job.ErrPath,
				)
			}
			entry.stdErr = content
		case sectionOutputFile:
			if extra == "" {
				return nil, fmt.Errorf(
					"gather script produced OUTPUT_FILE section with empty filename for job ID %s",
					job.JobId,
				)
			}
			if content != missingFileMarker {
				entry.outputFiles[extra] = content
			}
		default:
			return nil, fmt.Errorf(
				"gather script produced unknown section type %q in header %q",
				sectionType, header,
			)
		}

		results[job.CmdId] = entry
	}

	return results, nil
}

// fetchAllJobOutputs runs a single SSH command that gathers stdout, stderr,
// and tmp-output-directory contents for all jobs simultaneously, then removes
// all remote artefacts for those jobs. Pays SSH session overhead exactly once.
// Returns a map from CmdId to rawJobOutputs.
func fetchAllJobOutputs(jobs []SlurmJob, runCmd CmdRunner) (map[int]rawJobOutputs, error) {
	if len(jobs) == 0 {
		return make(map[int]rawJobOutputs), nil
	}

	token := randomToken()
	script := buildGatherScript(jobs, token)

	// The script is sent as a bash here-document so the full multi-job read
	// and cleanup executes in a single SSH session.
	gatherCmd := fmt.Sprintf("bash -s << 'SLURM_GATHER_EOF'\n%s\nSLURM_GATHER_EOF", script)
	out, err := runCmd(gatherCmd)
	if err != nil && out.ExitCode != 0 {
		return nil, fmt.Errorf(
			"gather script failed with exit code %d, stderr: %s, err: %s",
			out.ExitCode, out.StdErr, err,
		)
	}

	return parseGatherOutput(out.StdOut, jobs, token)
}

// GetSlurmJobOutputs fetches stdout, stderr, and tmp-output files for a
// single job using fetchAllJobOutputs, then assembles CmdOutput.
func GetSlurmJobOutputs(job SlurmJob, runCmd CmdRunner) (CmdOutput, error) {
	rawOutputs, err := fetchAllJobOutputs([]SlurmJob{job}, runCmd)
	if err != nil {
		return CmdOutput{}, fmt.Errorf(
			"failed getting outputs for job ID %s (cmd ID %d): %s",
			job.JobId, job.CmdId, err,
		)
	}

	entry, ok := rawOutputs[job.CmdId]
	if !ok {
		return CmdOutput{}, fmt.Errorf(
			"failed getting outputs for job ID %s (cmd ID %d): missing from gather results",
			job.JobId, job.CmdId,
		)
	}

	cmdOutput := CmdOutput{
		Id:          job.CmdId,
		RawOutputs:  make(map[string]string),
		OutputFiles: make([]string, 0),
	}
	cmdOutput.StdOut = entry.stdOut
	cmdOutput.StdErr = entry.stdErr

	cleanedOutputs, newOutFiles := processRawCmdOutputs(entry.outputFiles, job.ExpOutFilePnames)
	cmdOutput.RawOutputs = cleanedOutputs
	cmdOutput.OutputFiles = append(cmdOutput.OutputFiles, newOutFiles...)

	return cmdOutput, nil
}

func (connMan *SlurmActivity) GetRemoteSlurmJobOutputsActivity(
	jobs []SlurmJob,
) ([]CmdOutput, error) {
	// Allow container volumes time to propagate backward onto host FS.
	time.Sleep(5 * time.Second)

	rawOutputs, err := fetchAllJobOutputs(jobs,
		func(cmd string) (CmdOut, error) {
			return connMan.ExecCmd(cmd)
		},
	)
	if err != nil {
		return nil, err
	}

	out := make([]CmdOutput, 0, len(jobs))
	for _, job := range jobs {
		entry, ok := rawOutputs[job.CmdId]
		if !ok {
			return nil, fmt.Errorf(
				"failed getting outputs for job ID %s (cmd ID %d): missing from gather results",
				job.JobId, job.CmdId,
			)
		}

		cmdOutput := CmdOutput{
			Id:          job.CmdId,
			RawOutputs:  make(map[string]string),
			OutputFiles: make([]string, 0),
		}
		cmdOutput.StdOut = entry.stdOut
		cmdOutput.StdErr = entry.stdErr

		cleanedOutputs, newOutFiles := processRawCmdOutputs(entry.outputFiles, job.ExpOutFilePnames)
		cmdOutput.RawOutputs = cleanedOutputs
		cmdOutput.OutputFiles = append(cmdOutput.OutputFiles, newOutFiles...)

		out = append(out, cmdOutput)
	}

	return out, nil
}

func (connMan *SlurmActivity) PollRemoteSlurmActivity(
	outstandingJobIds []string,
) (map[string]SacctResult, error) {
	// Keepalive.
	if len(outstandingJobIds) == 0 {
		_, err := connMan.ExecCmd("echo Keepalive")
		return nil, err
	}

	return RunSacct(outstandingJobIds,
		func(cmd string) (CmdOut, error) {
			return connMan.ExecCmd(cmd)
		},
	)
}

// mkdirIfNotExists is retained as a single-directory fallback utility.
func (connMan *SlurmActivity) mkdirIfNotExists(dir string) error {
	lsCmd := fmt.Sprintf("ls -1 %s", dir)
	lsOut, err := connMan.ExecCmd(lsCmd)
	if err == nil && lsOut.ExitCode == 0 {
		// CASE 1: Dir exists.
		return nil
	} else if lsOut.ExitCode != 2 {
		// CASE 2: Unrelated ls error.
		return fmt.Errorf(
			"\"%s\" failed with exit code %d, error %s, and stderr %s",
			lsCmd, lsOut.ExitCode, err, lsOut.StdErr,
		)
	}

	// CASE 3: File does not exist.
	mkdirCmd := fmt.Sprintf("mkdir -p %s", dir)
	mkdirOut, err := connMan.ExecCmd(mkdirCmd)
	if err != nil {
		return fmt.Errorf(
			"\"%s\" failed with exit code %d, error %s, and stderr %s",
			mkdirCmd, mkdirOut.ExitCode, err, mkdirOut.StdErr,
		)
	}
	return nil
}

// mkdirAll creates all dirs in a single SSH call using mkdir -p. Because
// mkdir -p is idempotent, no prior existence check is needed. All dirs are
// created (or confirmed to exist) for the cost of one SSH session regardless
// of how many there are.
func (connMan *SlurmActivity) mkdirAll(dirs []string) error {
	if len(dirs) == 0 {
		return nil
	}

	quotedDirs := make([]string, len(dirs))
	for i, d := range dirs {
		quotedDirs[i] = shellQuote(d)
	}
	mkdirCmd := fmt.Sprintf("mkdir -p %s", strings.Join(quotedDirs, " "))
	mkdirOut, err := connMan.ExecCmd(mkdirCmd)
	if err != nil {
		return fmt.Errorf(
			"\"%s\" failed with exit code %d, error %s, and stderr %s",
			mkdirCmd, mkdirOut.ExitCode, err, mkdirOut.StdErr,
		)
	}
	return nil
}

func (connMan *SlurmActivity) StartRemoteSlurmJobActivity(
	cmd parsing.CmdRunParams, jobConfig parsing.SlurmJobConfig,
	fs fs.SshFS, slurmDir, imageDir string,
) (SlurmJob, error) {
	jobSlurmId := randomString(16)
	tmpOutputHostPath := filepath.Join(slurmDir, jobSlurmId)

	// Create all required remote directories in a single SSH call.
	dirsToCreate := []string{tmpOutputHostPath}
	for _, dir := range cmd.HostDirsToCreate {
		dirsToCreate = append(dirsToCreate, dir)
	}

	if err := connMan.mkdirAll(dirsToCreate); err != nil {
		return SlurmJob{}, fmt.Errorf("failed to create remote directories: %s", err)
	}

	sbatchFname := fmt.Sprintf("%s.sbatch", jobSlurmId)
	sbatchLocalPath := filepath.Join("/tmp", sbatchFname)
	sbatchRemotePath := filepath.Join(slurmDir, sbatchFname)
	tmpFile, err := os.Create(sbatchLocalPath)
	if err != nil {
		return SlurmJob{}, fmt.Errorf(
			"failed to make local tmp file %s: %s",
			sbatchLocalPath, err,
		)
	}
	defer tmpFile.Close()

	volumes := getSlurmVolumes(cmd, tmpOutputHostPath)
	fmt.Println()
	fmt.Println("CMD", cmd)
	fmt.Println("VOLS", volumes)
	fmt.Println()
	outPath, errPath, err := WriteSbatchFile(
		tmpFile, cmd.Cmd, volumes, connMan.Config, jobConfig, slurmDir, imageDir, jobSlurmId,
	)
	if err != nil {
		return SlurmJob{}, fmt.Errorf("error writing sbatch file: %s", err)
	}

	tmpFile.Sync()
	if err := fs.Upload(sbatchLocalPath, sbatchRemotePath); err != nil {
		return SlurmJob{}, fmt.Errorf(
			"unable to upload sbatch file from %s to %s: %s",
			sbatchLocalPath, sbatchRemotePath, err,
		)
	}

	sbatchCmd := fmt.Sprintf("sbatch --parsable %s", sbatchRemotePath)
	sbatchOut, err := connMan.ExecCmd(sbatchCmd)
	if err != nil {
		return SlurmJob{}, fmt.Errorf(
			"\"%s\" failed with exit code %d, error %s, and stderr %s",
			sbatchCmd, sbatchOut.ExitCode, err, sbatchOut.StdErr,
		)
	}

	// sbatch --parsable output is either "JOBID" or "JOBID;CLUSTER".
	jobIdRaw := strings.Split(sbatchOut.StdOut, ";")[0]
	jobId := strings.TrimSuffix(jobIdRaw, "\n")
	return SlurmJob{
		CmdId:             cmd.Cmd.Id,
		JobId:             jobId,
		TmpOutputHostPath: tmpOutputHostPath,
		ExpOutFilePnames:  cmd.Cmd.OutFilePnames,
		SbatchPath:        sbatchRemotePath,
		OutPath:           outPath,
		ErrPath:           errPath,
	}, nil
}

func ProcessSacctResult(
	ctx workflow.Context, selector workflow.Selector,
	state *SlurmState, results map[string]SacctResult,
) {
	finishedJobs := make([]SlurmJob, 0)
	resultsByCmdId := make(map[int]SacctResult)
	for jobId, result := range results {
		logger := workflow.GetLogger(ctx)
		job, jobExists := state.RunningJobs[jobId]
		if !jobExists {
			logger.Warn(
				"job ID (returned by sacct) is not a running job", "jobId", jobId,
			)
			return
		}

		if JOB_CODES[result.State].done {
			if JOB_CODES[result.State].failed {
				delete(state.RunningJobs, jobId)
				if JOB_CODES[result.State].fatal {
					finishedJobs = append(finishedJobs, job)
				} else {
					req := state.Requests[job.CmdId]
					maxJobRetries := 0
					if req.Config.MaxRetries != nil {
						maxJobRetries = *req.Config.MaxRetries
					}
					if state.NumRetries[job.CmdId] < maxJobRetries {
						state.NumRetries[job.CmdId] += 1
						StartSlurmJob(req, ctx, state)
					} else {
						finishedJobs = append(finishedJobs, job)
					}
				}
			} else {
				// There is a weird bug in SLURM (possibly unique to the slurm docker
				// setup I use for testing) where a job will register as complete
				// immediately after submission, switch to PENDING/RUNNING, and then
				// show as complete later. We prevent that by registering a job as
				// possibly complete once it has shown up as COMPLETED once in sacct
				// and treating it as truly complete the next time it shows as
				// COMPLETED.
				_, possiblyCompleted := state.PossiblyCompletedJobs[jobId]
				if possiblyCompleted {
					finishedJobs = append(finishedJobs, job)
					delete(state.RunningJobs, jobId)
				} else {
					state.PossiblyCompletedJobs[jobId] = struct{}{}
				}
			}
		}
		resultsByCmdId[job.CmdId] = result
	}

	if len(finishedJobs) == 0 {
		return
	}

	var a SlurmActivity
	ao := workflow.ActivityOptions{
		TaskQueue:           GetTemporalSshQueueName(state.SlurmConfig),
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts:    2,
			BackoffCoefficient: 4,
		},
	}
	childCtx := workflow.WithActivityOptions(ctx, ao)
	f := workflow.ExecuteActivity(childCtx, a.GetRemoteSlurmJobOutputsActivity, finishedJobs)

	slurmBatchId := state.MaxSlurmBatchId
	callback := func(f workflow.Future) {
		NotifyCmdCompletion(ctx, state, slurmBatchId, resultsByCmdId, f)
	}
	state.GetOutputFutures[slurmBatchId] = GetOutputsFuture{
		future:   f,
		results:  results,
		callback: callback,
	}
	state.MaxSlurmBatchId++
	selector.AddFuture(f, callback)
}

func NotifyCmdCompletion(
	ctx workflow.Context, state *SlurmState, outputFutInd int,
	resultsByCmdId map[int]SacctResult, f workflow.Future,
) {
	var outputs []CmdOutput
	err := f.Get(ctx, &outputs)
	delete(state.GetOutputFutures, outputFutInd)
	if err != nil {
		state.FinalErr = err
		return
	}

	for _, output := range outputs {
		result, ok := resultsByCmdId[output.Id]
		if !ok {
			state.FinalErr = fmt.Errorf(
				"error notifying of cmd completion; output %v not in "+
					"resultsByCmd map", output,
			)
			return
		}
		if JOB_CODES[result.State].failed {
			jobErr := fmt.Sprintf("job failed with err %s", output.StdErr)
			workflow.SignalExternalWorkflow(
				ctx, state.ParentWfId, "", "slurm-response",
				SlurmResponse{Result: output, Error: &jobErr},
			)
		} else {
			workflow.SignalExternalWorkflow(
				ctx, state.ParentWfId, "", "slurm-response",
				SlurmResponse{Result: output, Error: nil},
			)
		}
	}
}

func StartSlurmJob(
	req SlurmRequest, ctx workflow.Context,
	state *SlurmState,
) {
	var job SlurmJob
	var a SlurmActivity
	ao := workflow.ActivityOptions{
		TaskQueue:           GetTemporalSshQueueName(state.SlurmConfig),
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts:    1,
			BackoffCoefficient: 4,
		},
	}
	childCtx := workflow.WithActivityOptions(ctx, ao)
	slurmDir := filepath.Join(state.SchedDir, "slurm")
	imageDir := filepath.Join(state.SchedDir, "images")
	err := workflow.ExecuteActivity(
		childCtx, a.StartRemoteSlurmJobActivity, req.Cmd,
		req.Config, state.SlurmFS, slurmDir, imageDir,
	).Get(ctx, &job)

	if err != nil {
		state.FinalErr = err
	}

	state.RunningJobs[job.JobId] = job
}

func PollSlurm(ctx workflow.Context, selector workflow.Selector, state *SlurmState) {
	runningJobIds := make([]string, 0)
	for jobId := range state.RunningJobs {
		runningJobIds = append(runningJobIds, jobId)
	}

	var a SlurmActivity
	var jobRes map[string]SacctResult
	ao := workflow.ActivityOptions{
		TaskQueue:           GetTemporalSshQueueName(state.SlurmConfig),
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts:    1,
			BackoffCoefficient: 4,
		},
	}
	childCtx := workflow.WithActivityOptions(ctx, ao)
	err := workflow.ExecuteActivity(
		childCtx, a.PollRemoteSlurmActivity, runningJobIds,
	).Get(ctx, &jobRes)

	if err != nil {
		state.FinalErr = err
	}

	ProcessSacctResult(ctx, selector, state, jobRes)
}

func SlurmPollerWorkflow(ctx workflow.Context, state SlurmState) error {
	// These should be empty at start of each workflow incarnation.
	state.FinalErr = nil
	state.GetOutputFutures = make(map[int]GetOutputsFuture)

	// These will be empty in initial incarnation but full after continue-as-new.
	if state.RunningJobs == nil {
		state.RunningJobs = make(map[string]SlurmJob)
	}
	if state.PossiblyCompletedJobs == nil {
		state.PossiblyCompletedJobs = make(map[string]struct{})
	}
	if state.NumRetries == nil {
		state.NumRetries = make(map[int]int)
	}
	if state.Requests == nil {
		state.Requests = make(map[int]SlurmRequest)
	}

	selector := workflow.NewSelector(ctx)

	slurmReqChan := workflow.GetSignalChannel(ctx, "slurm-request")
	selector.AddReceive(slurmReqChan, func(c workflow.ReceiveChannel, _ bool) {
		var pendingReq SlurmRequest
		c.Receive(ctx, &pendingReq)
		state.Requests[pendingReq.Cmd.Cmd.Id] = pendingReq
		StartSlurmJob(pendingReq, ctx, &state)
	})

	durationSecsAsTime := time.Second * 5
	var timerCallback func(workflow.Future)
	timerCallback = func(f workflow.Future) {
		PollSlurm(ctx, selector, &state)
		timer := workflow.NewTimer(ctx, durationSecsAsTime)
		selector.AddFuture(timer, timerCallback)
	}

	timer := workflow.NewTimer(ctx, durationSecsAsTime)
	selector.AddFuture(timer, timerCallback)

	for workflow.GetInfo(ctx).GetCurrentHistoryLength() < 9000 {
		selector.Select(ctx)
		if ctx.Err() != nil {
			return nil
		}

		if state.FinalErr != nil {
			return state.FinalErr
		}
	}

	// Drain pending requests before continuing as new, since they won't be
	// delivered to the successor workflow.
	var pendingReq SlurmRequest
	for {
		ok := slurmReqChan.ReceiveAsync(&pendingReq)
		if !ok {
			break
		}
		StartSlurmJob(pendingReq, ctx, &state)
	}

	// Drain in-flight GetOutput futures so we don't lose their callbacks
	// along with the selector.
	for slurmBatchInd, f := range state.GetOutputFutures {
		f.callback(f.future)
		delete(state.GetOutputFutures, slurmBatchInd)
	}
	state.GetOutputFutures = nil

	return workflow.NewContinueAsNewError(ctx, SlurmPollerWorkflow, state)
}
