// bwb_workflow.go
package workflow

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"go-scheduler/fs"
	"go-scheduler/parsing"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const TerminalEvidenceMemoKey = "bwb_terminal_evidence"

type WorkflowTerminalEvidence struct {
	WorkflowStatus    string                     `json:"workflow_status"`
	NodeStatuses      map[int]string             `json:"node_statuses"`
	NodeAttempts      map[int]int                `json:"node_attempts"`
	SlurmCancellation *SlurmCancellationEvidence `json:"slurm_cancellation,omitempty"`
	Identity          parsing.ExecutionIdentity  `json:"identity"`
	SlurmTarget       *SlurmReconciliationTarget `json:"slurm_reconciliation_target,omitempty"`
	ErrorCategory     string                     `json:"error_category,omitempty"`
	ErrorDetail       string                     `json:"error_detail,omitempty"`
	DeclaredArtifacts []string                   `json:"declared_artifacts,omitempty"`
	SlurmJobs         []SlurmJobEvidence         `json:"slurm_jobs,omitempty"`
}

func terminalNodeStatuses(statuses map[int]string, canceled bool) map[int]string {
	result := make(map[int]string, len(statuses))
	for nodeID, status := range statuses {
		if canceled && status != "FINISHED" {
			status = "CANCELED"
		}
		result[nodeID] = status
	}
	return result
}

type Executor interface {
	Setup(bool) error
	SetCmdHandler(CmdHandler)
	SetFileXferHandler(FileXferHandler)
	Shutdown() error
	GetErrors() []error
	RunCmds([]parsing.CmdRunParams)
	BuildImages([]string) error
	GetFS() fs.AbstractFileSystem
	Glob(string, string, bool, bool) ([]string, error)
	GetID() parsing.ExecType
}

const (
	SCHEDULER_QUEUE = "bwb_worker"
)

type FileXferHandler func(
	workflow.Context,
	string,
	[]parsing.ObligatoryXfer,
) ([]workflow.Future, error)

func DefaultFileXferHanlder(
	ctx workflow.Context,
	storageId string,
	xfers []parsing.ObligatoryXfer,
	execFSs map[parsing.ExecType]fs.AbstractFileSystem,
) ([]workflow.Future, error) {
	// src exec -> dst exec -> transfer
	xfersByExecs := make(map[parsing.ExecType]map[parsing.ExecType][]parsing.ObligatoryXfer)
	for _, xfer := range xfers {
		srcExec := xfer.SrcExecutor
		dstExec := xfer.DstExecutor
		if _, ok := xfersByExecs[srcExec]; !ok {
			xfersByExecs[srcExec] = make(map[parsing.ExecType][]parsing.ObligatoryXfer)
		}

		if _, ok := xfersByExecs[srcExec][dstExec]; !ok {
			xfersByExecs[srcExec][dstExec] = make([]parsing.ObligatoryXfer, 0)
		}

		xfersByExecs[srcExec][dstExec] = append(
			xfersByExecs[srcExec][dstExec], xfer,
		)
	}

	futures := make([]workflow.Future, 0)
	for srcExec := range xfersByExecs {
		srcFS, ok := execFSs[srcExec]
		if !ok {
			return nil, fmt.Errorf(
				"executor %d unfound in exec -> FS map %v",
				srcExec, execFSs,
			)
		}

		for dstExec, xfers := range xfersByExecs[srcExec] {
			dstFS, ok := execFSs[dstExec]
			if !ok {
				return nil, fmt.Errorf(
					"executor %d unfound in exec -> FS map %v",
					srcExec, execFSs,
				)
			}

			future := fs.RunTransferActivity(ctx, storageId, srcFS, dstFS, xfers)
			futures = append(futures, future)
		}
	}
	return futures, nil
}

type CmdOutput struct {
	Id          int
	StdOut      string
	StdErr      string
	RawOutputs  map[string]string
	OutputFiles []string
}

type CmdHandler func(CmdOutput, error, Executor, parsing.CmdRunParams)
type CmdRunner func(string) (CmdOut, error)

func getSifName(dockerImage string) string {
	// Replace all `/` in docker image name, since this is going
	// to be a filename
	imgBasename := strings.Replace(dockerImage, "/", ".", -1)
	return fmt.Sprintf("%s.sif", imgBasename)
}

func randomString(length int) string {
	b := make([]byte, length+2)
	rand.Read(b)
	return fmt.Sprintf("%x", b)[2 : length+2]
}

func saveDockerToTar(dockerImage string, tarName string) error {
	var stdout, stderr bytes.Buffer
	dockerInspectCmd := exec.Command("docker", "inspect", dockerImage)
	err := dockerInspectCmd.Run()
	imageNotFound := err != nil

	if imageNotFound {
		dockerPullCmd := exec.Command("docker", "pull", dockerImage)
		dockerPullCmd.Stdout = &stdout
		dockerPullCmd.Stderr = &stderr
		if err := dockerPullCmd.Run(); err != nil {
			return fmt.Errorf(
				"failed to pull docker image %s: %v\nSTDOUT: %s\nSTDERR: %s",
				dockerImage, err, stdout.String(), stderr.String(),
			)
		}
		stdout.Reset()
		stderr.Reset()
	}

	saveOciCmd := exec.Command("docker", "save", dockerImage, "-o", tarName)
	saveOciCmd.Stdout = &stdout
	saveOciCmd.Stderr = &stderr
	if err := saveOciCmd.Run(); err != nil {
		return fmt.Errorf(
			"failed to save image %s to OCI TAR: %v\nSTDOUT: %s\nSTDERR: %s",
			dockerImage, err, stdout.String(), stderr.String(),
		)
	}

	return nil
}

func BuildSingularitySIF(dockerImage string) (string, error) {
	if _, err := exec.LookPath("singularity"); err != nil {
		return "", fmt.Errorf("singularity not found in PATH: %v", err)
	}

	dataDir := os.Getenv("BWB_SCHED_DIR")
	imageDir := filepath.Join(dataDir, "images")
	if err := os.MkdirAll(imageDir, 0755); err != nil {
		return "", fmt.Errorf(
			"failed to create output directory %s: %v",
			imageDir, err,
		)
	}

	sifBasename := getSifName(dockerImage)
	outputPath := filepath.Join(imageDir, sifBasename)
	// Do not rebuild existing image.
	if _, err := os.Stat(outputPath); err == nil {
		return outputPath, nil
	}

	tarName := filepath.Join(imageDir, fmt.Sprintf("%s.tar", randomString(16)))
	if err := saveDockerToTar(dockerImage, tarName); err != nil {
		return "", err
	}
	defer os.RemoveAll(tarName)

	var stdout, stderr bytes.Buffer
	buildCmd := exec.Command(
		"singularity", "build", outputPath, "docker-archive://"+tarName,
	)
	buildCmd.Stdout = &stdout
	buildCmd.Stderr = &stderr
	if err := buildCmd.Run(); err != nil {
		return "", fmt.Errorf(
			"failed to build singularity image %s: %v\nSTDOUT: %s\nSTDERR: %s",
			dockerImage, err, stdout.String(), stderr.String(),
		)
	}

	return outputPath, nil
}

func processRawCmdOutputs(
	rawOutputs map[string]string,
	expOutFilePnames []string,
) (map[string]string, []string) {
	outKvs := make(map[string]string)
	for pname, contents := range rawOutputs {
		outKvs[pname] = strings.TrimSuffix(contents, "\n")
	}

	outFiles := make([]string, 0)
	for _, expOutFilePname := range expOutFilePnames {
		if outFilesRaw, outFileExists := outKvs[expOutFilePname]; outFileExists {
			outFileVals := strings.Split(outFilesRaw, "\n")
			if len(outFileVals) > 0 {
				if outFileVals[len(outFileVals)-1] == "" {
					outFileVals = outFileVals[:len(outFileVals)-1]
				}
				outFiles = append(outFiles, outFileVals...)
			}
		}
	}
	return outKvs, outFiles
}

func dockerGetCmdOutputs(
	dockerPrefix string, expOutFilePnames []string,
) (map[string]string, []string, error) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	findCmdStr := fmt.Sprintf(
		"%s find /tmp/output -maxdepth 1 -type f", dockerPrefix,
	)
	findCmd := exec.Command("sh", "-c", findCmdStr)
	findCmd.Stdout = &stdout
	findCmd.Stderr = &stderr
	if err := findCmd.Run(); err != nil {
		return nil, nil, fmt.Errorf(
			"cmd %s failed w/ stderr %s and err %s",
			findCmdStr, stderr.String(), err,
		)
	}

	files := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	rawOutputs := make(map[string]string)
	for _, outFile := range files {
		if outFile == "" {
			continue
		}

		stdout.Reset()
		stderr.Reset()
		catCmdStr := fmt.Sprintf("%s cat %s", dockerPrefix, outFile)
		catCmd := exec.Command("sh", "-c", catCmdStr)
		catCmd.Stdout = &stdout
		catCmd.Stderr = &stderr
		if err := catCmd.Run(); err != nil {
			return nil, nil, fmt.Errorf(
				"cmd %s failed w/ stderr %s and err %s",
				catCmdStr, stderr.String(), err,
			)
		}

		pname := filepath.Base(outFile)
		rawOutputs[pname] = stdout.String()
	}

	outKvs, outFiles := processRawCmdOutputs(rawOutputs, expOutFilePnames)
	return outKvs, outFiles, nil
}

func singularityGetCmdOutputs(
	tmpOutputHostPath string,
	expOutFilePnames []string,
) (map[string]string, []string, error) {
	if _, err := os.Stat(tmpOutputHostPath); os.IsNotExist(err) {
		return nil, nil, fmt.Errorf(
			"/tmp/output host path %s does not exist", tmpOutputHostPath,
		)
	}

	outPaths, err := os.ReadDir(tmpOutputHostPath)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"error reading output dir %s: %s", tmpOutputHostPath, err,
		)
	}

	rawOutputs := make(map[string]string)
	for _, outPath := range outPaths {
		pname := outPath.Name()
		if outPath.IsDir() {
			continue
		}

		fullPath := filepath.Join(tmpOutputHostPath, pname)
		data, err := os.ReadFile(fullPath)
		if err != nil {
			fmt.Printf("failed to read %s: %v\n", fullPath, err)
			continue
		}

		rawOutputs[pname] = string(data)
	}

	outKvs, outFiles := processRawCmdOutputs(rawOutputs, expOutFilePnames)
	return outKvs, outFiles, nil
}

func setupTmpDir() (string, error) {
	schedDir := os.Getenv("BWB_SCHED_DIR")
	randStr := randomString(32)
	tmpDir := filepath.Join(schedDir, randStr)

	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create dir %s: %s", tmpDir, err)
	}

	return tmpDir, nil
}

func setupImageDir() (string, error) {
	schedDir := os.Getenv("BWB_SCHED_DIR")
	imageDir := filepath.Join(schedDir, "images")

	if err := os.MkdirAll(imageDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create dir %s: %s", imageDir, err)
	}

	return imageDir, nil
}

func runCmdDocker(
	ctx context.Context,
	volumes map[string]string,
	cmdTemplate parsing.CmdTemplate,
) (CmdOutput, error) {
	useGpu := cmdTemplate.ResourceReqs.Gpus > 0
	cntName := randomString(16)
	cmdStr, envs := parsing.FormDockerCmd(
		cmdTemplate, volumes, useGpu, cntName,
	)

	var stdout, stderr bytes.Buffer
	cmd := exec.Command("sh", "-c", cmdStr)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Env = envs
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	cmdWithEnvStr := fmt.Sprintf("%s %s", strings.Join(envs, " "), cmdStr)
	fmt.Println(cmdWithEnvStr)

	dockerCmdPrefix := parsing.FormDockerCmdPrefix(
		cmdTemplate, volumes, useGpu, cntName,
	)
	rmTmpDirCmdStr := fmt.Sprintf("%s rm -rf /tmp/output", dockerCmdPrefix)
	rmTmpDirCmd := exec.Command("sh", "-c", rmTmpDirCmdStr)
	defer rmTmpDirCmd.Run()

	errChan := make(chan error)
	go func() {
		errChan <- cmd.Run()
	}()

	select {
	case <-ctx.Done():
		{
			rmCntCmdStr := fmt.Sprintf("docker rm -f %s", cntName)
			rmCntCmd := exec.Command("sh", "-c", rmCntCmdStr)
			rmCntCmd.Run()
			return CmdOutput{}, context.Canceled
		}
	case procErr := <-errChan:
		{
			if procErr != nil {
				return CmdOutput{}, fmt.Errorf(
					"error running command %s: %v\nSTDOUT: %s\nSTDERR: %s",
					cmdWithEnvStr, procErr, stdout.String(), stderr.String(),
				)
			}
		}
	}

	out := CmdOutput{
		Id:     cmdTemplate.Id,
		StdOut: stdout.String(),
		StdErr: stderr.String(),
	}

	var err error
	out.RawOutputs, out.OutputFiles, err = dockerGetCmdOutputs(
		dockerCmdPrefix, cmdTemplate.OutFilePnames,
	)

	if err != nil {
		return CmdOutput{}, fmt.Errorf(
			"error getting outputs of command %s: %s", cmd, err,
		)
	}
	return out, nil
}

func runCmdSingularity(
	ctx context.Context,
	volumes map[string]string,
	cmdTemplate parsing.CmdTemplate,
	rootDir string,
) (CmdOutput, error) {
	imageDir, err := setupImageDir()
	if err != nil {
		return CmdOutput{}, fmt.Errorf("unable to setup image dir: %s", err)
	}

	tmpDir, ok := volumes["/tmp/output"]
	if !ok {
		return CmdOutput{}, fmt.Errorf("failed to set /tmp/output volume")
	}
	defer os.RemoveAll(tmpDir)

	sifBasename := getSifName(cmdTemplate.ImageName)
	localSifPath := filepath.Join(imageDir, sifBasename)

	if _, err := os.Stat(localSifPath); os.IsNotExist(err) {
		return CmdOutput{}, fmt.Errorf(
			"SIF image %s not found at expected path %s",
			cmdTemplate.ImageName, localSifPath,
		)
	}

	useGpu := cmdTemplate.ResourceReqs.Gpus > 0
	cmdStr, envs := parsing.FormSingularityCmd(
		cmdTemplate, volumes, localSifPath, useGpu,
	)

	// Configure cmd to get killed automatically if ctx is cancelled.
	cmd := exec.CommandContext(ctx, "sh", "-c", cmdStr)
	var stdout, stderr bytes.Buffer
	cmd.Env = envs
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	cmdWithEnvStr := fmt.Sprintf("%s %s", strings.Join(envs, " "), cmdStr)
	fmt.Println(cmdWithEnvStr)

	errChan := make(chan error)
	go func() {
		errChan <- cmd.Run()
	}()
	select {
	case <-ctx.Done():
		{
			return CmdOutput{}, context.Canceled
		}
	case procErr := <-errChan:
		{
			if procErr != nil {
				return CmdOutput{}, fmt.Errorf(
					"error running command %s: %v\nSTDOUT: %s\nSTDERR: %s",
					cmdWithEnvStr, procErr, stdout.String(), stderr.String(),
				)
			}
		}
	}

	out := CmdOutput{
		Id:     cmdTemplate.Id,
		StdOut: stdout.String(),
		StdErr: stderr.String(),
	}
	out.RawOutputs, out.OutputFiles, err = singularityGetCmdOutputs(
		tmpDir, cmdTemplate.OutFilePnames,
	)

	if err != nil {
		return CmdOutput{}, fmt.Errorf(
			"error getting outputs of command %s: %s", cmd, err,
		)
	}
	return out, nil
}

func RunCmd(
	ctx context.Context,
	cmdRunParams parsing.CmdRunParams,
	useDocker bool,
	rootDir string,
) (CmdOutput, error) {
	finalVolumes := make(map[string]string)
	for _, mnt := range cmdRunParams.Volumes {
		finalVolumes[mnt.CntPath] = mnt.HostPath
	}

	for _, dir := range cmdRunParams.HostDirsToCreate {
		_, err := os.Stat(dir)
		if errors.Is(err, os.ErrNotExist) {
			if err := os.MkdirAll(dir, 0755); err != nil {
				return CmdOutput{}, fmt.Errorf(
					"failed to create dir %s on host FS: %s",
					dir, err,
				)
			}
		}
	}

	tmpDir, err := setupTmpDir()
	if err != nil {
		return CmdOutput{}, fmt.Errorf(
			"unable to setup /tmp/output dir: %s", err,
		)
	}

	finalVolumes["/tmp/output"] = tmpDir
	if useDocker {
		return runCmdDocker(ctx, finalVolumes, cmdRunParams.Cmd)
	} else {
		return runCmdSingularity(ctx, finalVolumes, cmdRunParams.Cmd, rootDir)
	}
}

func RunCmdActivity(
	ctx context.Context,
	cmd parsing.CmdRunParams,
	useDocker bool,
	rootDir string,
) (CmdOutput, error) {
	outChan := make(chan struct {
		out CmdOutput
		err error
	})
	type outType struct {
		out CmdOutput
		err error
	}

	go func() {
		out, err := RunCmd(ctx, cmd, useDocker, rootDir)
		outChan <- outType{out: out, err: err}
	}()

	finished := false
	var outVal outType
	for !finished {
		select {
		case <-time.After(1 * time.Second):
			{
				activity.RecordHeartbeat(ctx, struct{}{})
			}
		case outVal = <-outChan:
			{
				finished = true
			}
		}
	}
	return outVal.out, outVal.err
}

func HandleCompletedCmd(
	logger log.Logger, result CmdOutput, err error, softFail bool,
	cmdMan *parsing.CmdManager, executors map[int]Executor,
	completedCmd parsing.CmdRunParams, fsByExec map[int]fs.AbstractFileSystem,
	finalErr *error,
) {
	logger.Debug("Finished cmd", "cmdId", completedCmd.Cmd.Id, "nodeId", completedCmd.Cmd.NodeId)
	cmdSucceeded := err == nil
	if !softFail && !cmdSucceeded {
		*finalErr = err
		return
	}
	succCmds, err := cmdMan.GetSuccCmds(
		completedCmd.Cmd, result.RawOutputs,
		func(nodeId int, root, pattern string, findFile, findDir bool) ([]string, error) {
			executor, ok := executors[nodeId]
			if !ok {
				return nil, fmt.Errorf("no executor for node %d", nodeId)
			}
			return executor.Glob(root, pattern, findFile, findDir)
		}, cmdSucceeded,
	)

	if err != nil {
		*finalErr = fmt.Errorf("cmd manager failed: %s", err)
		return
	}

	logger.Debug("Got succ CMDs", "succCmds", succCmds)
	fmt.Println("Got succ CMDs", "succCmds", succCmds)
	RunCmds(executors, succCmds)
}

func setupExecutors(
	ctx workflow.Context,
	selector workflow.Selector,
	storageId string,
	bwbWorkflow parsing.Workflow,
	cmdMan *parsing.CmdManager,
	workers map[string]WorkerInfo,
	masterFS fs.LocalFS,
	jobConfig parsing.JobConfig,
) (map[int]Executor, []Executor, error) {
	executors := make(map[int]Executor)
	executorList := make([]Executor, 0)
	if len(jobConfig.LocalConfigsByNode) > 0 {
		if len(jobConfig.SlurmConfigsByNode) > 0 || len(jobConfig.TemporalConfigsByNode) > 0 {
			return nil, nil, fmt.Errorf(
				"cannot have temporal / SLURM executor alongside local one",
			)
		}
	}

	if len(jobConfig.TemporalConfigsByNode) > 0 {
		temporalExecutor := NewTemporalExecutor(
			ctx, &selector, cmdMan, masterFS, workers, storageId,
			jobConfig.TemporalConfigsByNode,
		)
		executorList = append(executorList, &temporalExecutor)
		for nodeId := range jobConfig.TemporalConfigsByNode {
			executors[nodeId] = &temporalExecutor
		}
	}

	if len(jobConfig.SlurmConfigsByNode) > 0 {
		slurmExecutor := NewSlurmRemoteExecutor(
			ctx, &selector, masterFS, storageId, jobConfig.SlurmConfigsByNode,
			jobConfig.SlurmExecutor, jobConfig.ExecutionIdentity,
		)
		executorList = append(executorList, &slurmExecutor)
		for nodeId := range jobConfig.SlurmConfigsByNode {
			executors[nodeId] = &slurmExecutor
		}
	}

	for _, nodeId := range bwbWorkflow.GetNodeIds() {
		if _, execExists := executors[nodeId]; !execExists {
			return nil, nil, fmt.Errorf(
				"no executor set for node %d in config", nodeId,
			)
		}
	}

	return executors, executorList, nil
}

func RunCmds(
	executors map[int]Executor,
	cmdsByNode map[int][]parsing.CmdRunParams,
) error {
	for nodeId, cmdList := range cmdsByNode {
		executor, ok := executors[nodeId]
		if !ok {
			return fmt.Errorf("no executor for node ID %d", nodeId)
		}
		executor.RunCmds(cmdList)
	}
	return nil
}

func RunBwbWorkflowHelper(
	logger log.Logger,
	cmdMan *parsing.CmdManager,
	executorsByNode map[int]Executor,
	executors []Executor,
	softFail bool,
	isDone func() bool,
	selectFunc func(),
	v1 bool,
) (returnErr error) {
	if cmdMan == nil {
		return errors.New("received nil CMD manager")
	}

	var finalErr error = nil
	imageNames := cmdMan.GetImageNames()
	fsByExec := make(map[int]fs.AbstractFileSystem)
	initializedExecutors := make([]Executor, 0, len(executors))
	defer func() {
		for index := len(initializedExecutors) - 1; index >= 0; index-- {
			if err := initializedExecutors[index].Shutdown(); err != nil {
				returnErr = errors.Join(returnErr, err)
			}
		}
	}()
	for _, executor := range executors {
		if err := executor.Setup(v1); err != nil {
			return err
		}
		initializedExecutors = append(initializedExecutors, executor)

		fsByExec[int(executor.GetID())] = executor.GetFS()

		if err := executor.BuildImages(imageNames); err != nil {
			return err
		}
	}

	// Type alias is biting me in the ass, fix later
	fsByExecCorrectType := make(map[parsing.ExecType]fs.AbstractFileSystem)
	for execType, fs := range fsByExec {
		fsByExecCorrectType[parsing.ExecType(execType)] = fs
	}

	// Wait until fsByExec is set before passing to completed cmd handler.
	for _, executor := range executors {
		executor.SetCmdHandler(func(res CmdOutput, err error, exec Executor, cmd parsing.CmdRunParams) {
			HandleCompletedCmd(
				logger, res, err, softFail, cmdMan, executorsByNode, cmd, fsByExec, &finalErr,
			)
		})

		executor.SetFileXferHandler(func(
			ctx workflow.Context, storageID string, xfers []parsing.ObligatoryXfer,
		) ([]workflow.Future, error) {
			return DefaultFileXferHanlder(ctx, storageID, xfers, fsByExecCorrectType)
		},
		)
	}

	initialCmds, err := cmdMan.GetInitialCmds(
		func(nodeId int, root, pattern string, findFile, findDir bool) ([]string, error) {
			executor, ok := executorsByNode[nodeId]
			if !ok {
				return nil, fmt.Errorf("no executor for node %d", nodeId)
			}
			return executor.Glob(root, pattern, findFile, findDir)
		},
	)

	if err != nil {
		return fmt.Errorf("error getting initial cmds: %s", err)
	}

	RunCmds(executorsByNode, initialCmds)
	for !cmdMan.IsComplete() && finalErr == nil && !isDone() {
		for _, executor := range executors {
			execErrs := executor.GetErrors()
			if len(execErrs) > 0 {
				errStr := ""
				for _, err := range execErrs {
					errStr += fmt.Sprintf("\t%s\n", err.Error())
				}
				finalErr = errors.New(errStr)
				break
			}

			selectFunc()
		}
	}

	if finalErr != nil {
		return finalErr
	}

	return nil
}

// NOTE: You cannot collapse RunBwbWorkflowV1 and RunBwbWOrkflowV0 into
// a single function which takes an interface; these are temporal workflows,
// which requires that parameters have a set JSON structure, meaning
// interfaces can't be used as arguments.
func RunBwbWorkflowV1(
	ctx workflow.Context,
	storageId string,
	jobConfig parsing.JobConfig,
	bwbWorkflow parsing.ResolvedWorkflow,
	index parsing.WorkflowIndex,
	workers map[string]WorkerInfo,
	masterFS fs.LocalFS,
	softFail bool,
) error {
	masterRootDir := masterFS.GetRootDir()
	rootDirs := map[parsing.ExecType]string{
		parsing.EXEC_TEMPORAL: masterRootDir,
		parsing.EXEC_SLURM:    jobConfig.SlurmExecutor.SchedDir,
	}
	cmdMan := parsing.NewCmdManager(&bwbWorkflow, index, jobConfig, rootDirs)
	if err := workflow.SetQueryHandler(ctx, "getNodeStatuses", func() (map[int]string, error) {
		return cmdMan.GetNodeStatus(), nil
	}); err != nil {
		return fmt.Errorf("failed to register getNodeStatuses query handler: %s", err)
	}
	selector := workflow.NewSelector(ctx)
	executors, executorList, err := setupExecutors(
		ctx, selector, "", &bwbWorkflow, &cmdMan, workers, masterFS, jobConfig,
	)
	if err != nil {
		return fmt.Errorf("error parsing job config: %s", err)
	}
	logger := workflow.GetLogger(ctx)
	runErr := RunBwbWorkflowHelper(
		logger, &cmdMan, executors, executorList, softFail, func() bool {
			return ctx.Err() != nil
		},
		func() { selector.Select(ctx) },
		true,
	)
	evidence := WorkflowTerminalEvidence{
		NodeStatuses: terminalNodeStatuses(cmdMan.GetNodeStatus(), ctx.Err() != nil),
		NodeAttempts: make(map[int]int),
		Identity:     jobConfig.ExecutionIdentity,
	}
	for _, executor := range executorList {
		if provider, ok := executor.(interface {
			TerminalEvidence() *SlurmCancellationEvidence
		}); ok {
			evidence.SlurmCancellation = provider.TerminalEvidence()
			if evidence.SlurmCancellation != nil {
				evidence.SlurmJobs = append(
					evidence.SlurmJobs, evidence.SlurmCancellation.Jobs...,
				)
			}
		}
		if provider, ok := executor.(interface {
			ReconciliationTarget() *SlurmReconciliationTarget
		}); ok {
			evidence.SlurmTarget = provider.ReconciliationTarget()
		}
	}
	cleanupFailed := evidence.SlurmCancellation != nil &&
		!evidence.SlurmCancellation.Verified &&
		evidence.SlurmCancellation.CleanupStatus == "failed"
	if cleanupFailed {
		evidence.WorkflowStatus = "CANCEL_CLEANUP_FAILED"
	} else if ctx.Err() != nil {
		evidence.WorkflowStatus = "CANCELED"
	} else if runErr != nil {
		evidence.WorkflowStatus = "FAILED"
	} else {
		evidence.WorkflowStatus = "FINISHED"
	}
	if cleanupFailed {
		evidence.ErrorCategory = "slurm_cleanup"
		evidence.ErrorDetail = redactOperationalError(evidence.SlurmCancellation.Error)
	} else if runErr != nil && ctx.Err() == nil {
		evidence.ErrorCategory = "workflow_execution"
		evidence.ErrorDetail = redactOperationalError(runErr.Error())
	}
	artifactSet := make(map[string]struct{})
	for nodeID := range evidence.NodeStatuses {
		evidence.NodeAttempts[nodeID] = 1
	}
	for _, job := range evidence.SlurmJobs {
		if job.Attempt > evidence.NodeAttempts[job.NodeID] {
			evidence.NodeAttempts[job.NodeID] = job.Attempt
		}
		for _, artifact := range job.DeclaredArtifacts {
			artifactSet[artifact] = struct{}{}
		}
	}
	for artifact := range artifactSet {
		evidence.DeclaredArtifacts = append(evidence.DeclaredArtifacts, artifact)
	}
	sort.Strings(evidence.DeclaredArtifacts)
	if memoErr := workflow.UpsertMemo(ctx, map[string]interface{}{
		TerminalEvidenceMemoKey: evidence,
	}); memoErr != nil && runErr == nil {
		return fmt.Errorf("failed persisting terminal workflow evidence: %w", memoErr)
	}
	if jobConfig.EvidenceDir != "" {
		persistCtx, _ := workflow.NewDisconnectedContext(ctx)
		persistCtx = workflow.WithActivityOptions(persistCtx, workflow.ActivityOptions{
			TaskQueue:           SCHEDULER_QUEUE,
			StartToCloseTimeout: 15 * time.Second,
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts: 3,
			},
		})
		record := DurableWorkflowRecord{
			Identity:         jobConfig.ExecutionIdentity,
			TemporalRunID:    workflow.GetInfo(ctx).WorkflowExecution.RunID,
			SlurmTarget:      evidence.SlurmTarget,
			TerminalEvidence: &evidence,
		}
		if persistErr := workflow.ExecuteActivity(
			persistCtx, PersistDurableWorkflowRecordActivity, jobConfig.EvidenceDir, record,
		).Get(persistCtx, nil); persistErr != nil && runErr == nil {
			return fmt.Errorf("failed persisting durable terminal evidence: %w", persistErr)
		}
	}
	if runErr != nil {
		return runErr
	}
	if ctx.Err() != nil {
		return temporal.NewCanceledError(evidence)
	}
	return nil
}

func RunBwbWorkflowV0(
	ctx workflow.Context,
	storageId string,
	jobConfig parsing.JobConfig,
	bwbWorkflow parsing.WorkflowV0,
	index parsing.WorkflowIndex,
	workers map[string]WorkerInfo,
	masterFS fs.LocalFS,
	softFail bool,
) error {
	rootDirs := map[parsing.ExecType]string{
		parsing.EXEC_TEMPORAL: masterFS.GetRootDir(),
		parsing.EXEC_SLURM:    jobConfig.SlurmExecutor.SchedDir,
	}
	cmdMan := parsing.NewCmdManager(&bwbWorkflow, index, jobConfig, rootDirs)
	selector := workflow.NewSelector(ctx)
	executors, executorList, err := setupExecutors(
		ctx, selector, storageId, &bwbWorkflow, &cmdMan, workers, masterFS, jobConfig,
	)
	if err != nil {
		return fmt.Errorf("error parsing job config: %s", err)
	}
	logger := workflow.GetLogger(ctx)
	return RunBwbWorkflowHelper(
		logger, &cmdMan, executors, executorList, softFail, func() bool {
			return ctx.Err() != nil
		},
		func() { selector.Select(ctx) },
		false,
	)
}

func RunBwbWorkflowNoTemporal(
	ctx context.Context,
	storageId string,
	jobConfig parsing.JobConfig,
	bwbWorkflow parsing.Workflow,
	index parsing.WorkflowIndex,
	localWorker WorkerInfo,
	masterFS fs.LocalFS,
	softFail bool,
	logger slog.Logger,
) error {
	rootDirs := map[parsing.ExecType]string{
		parsing.EXEC_LOCAL: masterFS.GetRootDir(),
	}
	cmdMan := parsing.NewCmdManager(bwbWorkflow, index, jobConfig, rootDirs)
	executors := make(map[int]Executor)
	localExecutor := NewLocalExecutor(
		ctx, &cmdMan, masterFS, localWorker, storageId,
		jobConfig.LocalConfigsByNode, logger,
	)
	executorList := []Executor{&localExecutor}
	for _, nodeId := range bwbWorkflow.GetNodeIds() {
		executors[nodeId] = &localExecutor
	}
	v1 := bwbWorkflow.GetVersion() == "biodepot.resolved_workflow/v1"
	return RunBwbWorkflowHelper(
		&logger, &cmdMan, executors, executorList, softFail, func() bool {
			return ctx.Err() != nil
		},
		func() { localExecutor.Select() },
		v1,
	)
}
