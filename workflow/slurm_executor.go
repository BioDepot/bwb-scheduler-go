package workflow

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"go-scheduler/fs"
	"go-scheduler/parsing"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type SlurmRemoteExecutor struct {
	ctx               workflow.Context
	masterFS          fs.AbstractFileSystem
	storageId         string
	SlurmFS           fs.SshFS
	cmdsById          map[int]parsing.CmdRunParams
	errors            []error
	sshConfig         parsing.SshConfig
	handleFinishedCmd CmdHandler
	handleFileXfers   FileXferHandler
	configsByNode     map[int]parsing.SlurmJobConfig
	schedDir          string
	selector          *workflow.Selector
	slurmPollerWE     workflow.Execution
	slurmPollerFuture workflow.ChildWorkflowFuture
	cancelChild       func()
	cancellation      *SlurmCancellationEvidence
}

func NewSlurmRemoteExecutor(
	ctx workflow.Context, selector *workflow.Selector,
	masterFS fs.LocalFS, storageId string,
	configsByNode map[int]parsing.SlurmJobConfig,
	sshConfig parsing.SshConfig,
) SlurmRemoteExecutor {
	var state SlurmRemoteExecutor
	state.ctx = ctx
	state.selector = selector
	state.masterFS = masterFS
	state.storageId = storageId
	state.cmdsById = make(map[int]parsing.CmdRunParams)
	state.errors = make([]error, 0)
	state.sshConfig = sshConfig
	state.schedDir = sshConfig.SchedDir
	state.configsByNode = configsByNode
	return state
}

func (exec *SlurmRemoteExecutor) SetFileXferHandler(xferHandler FileXferHandler) {
	exec.handleFileXfers = xferHandler
}

func (exec *SlurmRemoteExecutor) GetFS() fs.AbstractFileSystem {
	return exec.SlurmFS
}

func (exec *SlurmRemoteExecutor) setupFS(v1 bool) (fs.SshFS, error) {
	// Setup container filesystem on SLURM fs.
	var a SlurmActivity
	dataDir := filepath.Join(exec.schedDir, exec.storageId)
	ao := workflow.ActivityOptions{
		TaskQueue:           GetTemporalSshQueueName(exec.sshConfig),
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	mkdirCtx := workflow.WithActivityOptions(exec.ctx, ao)

	err := workflow.ExecuteActivity(
		mkdirCtx, a.ExecCmd, fmt.Sprintf("mkdir -p -- %s", shellQuote(filepath.Join(dataDir, "data"))),
	).Get(exec.ctx, nil)
	if err != nil {
		return fs.SshFS{}, err
	}

	return fs.SshFS{
		User:           exec.sshConfig.User,
		Endpt:          exec.sshConfig.TransferAddr,
		Port:           exec.sshConfig.TransferPort,
		IdentityFile:   exec.sshConfig.IdentityFile,
		KnownHostsFile: exec.sshConfig.KnownHostsFile,
		RemoteRootDir:  dataDir,
		LocalRootDir:   exec.masterFS.GetRootDir(),
		RootDir:        dataDir,
	}, nil
}

// getSlurmVolumes builds the container-to-host volume map for the job.
// Directory creation is intentionally not performed here; call collectSlurmDirs
// and mkdirAll before this to ensure all paths exist.
func getSlurmVolumes(
	cmdRunParams parsing.CmdRunParams, tmpOutputHostPath string,
) map[string]string {
	volumes := make(map[string]string)
	for _, mnt := range cmdRunParams.Volumes {
		volumes[mnt.CntPath] = mnt.HostPath
	}
	volumes["/tmp/output"] = tmpOutputHostPath
	return volumes
}

func (exec *SlurmRemoteExecutor) handleSlurmCompleteSignal(jobRes SlurmResponse) {
	cmd := exec.cmdsById[jobRes.Result.Id]
	var jobErr error
	if jobRes.Error == nil {
		jobErr = nil
	} else {
		jobErr = fmt.Errorf("%s", *jobRes.Error)
	}
	exec.handleFinishedCmd(jobRes.Result, jobErr, exec, cmd)
	return
}

func (exec *SlurmRemoteExecutor) Setup(v1 bool) error {
	// Execute slurm poller child workflow.
	slurmFS, err := exec.setupFS(v1)
	if err != nil {
		return err
	}
	exec.SlurmFS = slurmFS

	childCtx, cancelChild := workflow.WithCancel(exec.ctx)
	queueName := GetTemporalSshQueueName(exec.sshConfig)
	parentInfo := workflow.GetInfo(exec.ctx).WorkflowExecution
	schedChildWfOptions := workflow.ChildWorkflowOptions{
		WorkflowID: slurmPollerWorkflowID(
			parentInfo.ID, parentInfo.RunID, exec.storageId, exec.sshConfig,
		),
		TaskQueue:           queueName,
		WaitForCancellation: true,
		ParentClosePolicy:   enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
	}
	childCtx = workflow.WithChildOptions(childCtx, schedChildWfOptions)

	workflowId := workflow.GetInfo(exec.ctx).WorkflowExecution.ID
	workflowRunId := workflow.GetInfo(exec.ctx).WorkflowExecution.RunID
	exec.slurmPollerFuture = workflow.ExecuteChildWorkflow(
		childCtx, SlurmPollerWorkflow, SlurmState{
			ParentWfId:    workflowId,
			ParentWfRunId: workflowRunId,
			SlurmConfig:   exec.sshConfig,
			StorageId:     exec.storageId,
			SchedDir:      exec.schedDir,
			SlurmFS:       slurmFS,
		},
	)

	err = exec.slurmPollerFuture.GetChildWorkflowExecution().Get(exec.ctx, &exec.slurmPollerWE)
	if err != nil {
		outErr := fmt.Errorf("failed getting child WF execution: %s", err)
		return outErr
	}
	exec.cancelChild = cancelChild

	slurmJobResChan := workflow.GetSignalChannel(exec.ctx, "slurm-response")
	(*exec.selector).AddReceive(slurmJobResChan, func(c workflow.ReceiveChannel, _ bool) {
		var jobRes SlurmResponse
		c.Receive(exec.ctx, &jobRes)
		exec.handleSlurmCompleteSignal(jobRes)
	})

	var checkForChildFailure func(workflow.Future)
	checkForChildFailure = func(f workflow.Future) {
		if exec.slurmPollerFuture.IsReady() {
			err := exec.slurmPollerFuture.Get(exec.ctx, nil)
			exec.errors = append(exec.errors, fmt.Errorf(
				"slurm poller WF failed w/ err %s", err,
			))
		}
		timer := workflow.NewTimer(exec.ctx, 1*time.Minute)
		(*exec.selector).AddFuture(timer, checkForChildFailure)
	}

	timer := workflow.NewTimer(exec.ctx, 1*time.Minute)
	(*exec.selector).AddFuture(timer, checkForChildFailure)
	return nil
}

func (exec *SlurmRemoteExecutor) RunCmds(
	cmds []parsing.CmdRunParams,
) {
	ao := workflow.ActivityOptions{
		TaskQueue:           GetTemporalSshQueueName(exec.sshConfig),
		StartToCloseTimeout: time.Hour * 1,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	downloadCtx := workflow.WithActivityOptions(exec.ctx, ao)

	for _, cmd := range cmds {
		slurmConfig, configExists := exec.configsByNode[cmd.Cmd.NodeId]
		if !configExists {
			exec.errors = append(exec.errors, fmt.Errorf(
				"no slurm config for node %d", cmd.Cmd.NodeId,
			))
		}
		exec.cmdsById[cmd.Cmd.Id] = cmd
		req := SlurmRequest{
			Cmd:    cmd,
			Config: slurmConfig,
			CorrelationKey: slurmJobCorrelationKey(
				workflow.GetInfo(exec.ctx).WorkflowExecution.ID,
				workflow.GetInfo(exec.ctx).WorkflowExecution.RunID,
				cmd.Cmd.NodeId,
				cmd.Cmd.Id,
			),
		}
		if len(cmd.Xfers) == 0 {
			workflow.SignalExternalWorkflow(
				exec.ctx, exec.slurmPollerWE.ID, "",
				"slurm-request", req,
			)
			continue
		}

		logger := workflow.GetLogger(exec.ctx)
		logger.Info("Performing xfers", "infiles", cmd.Xfers)

		downloadFutures, err := exec.handleFileXfers(downloadCtx, exec.storageId, cmd.Xfers)
		if err != nil {
			exec.errors = append(exec.errors, err)
			return
		}

		remaining := len(downloadFutures)
		failed := false
		for _, fut := range downloadFutures {
			(*exec.selector).AddFuture(fut, func(f workflow.Future) {
				if failed {
					return
				}

				if err := f.Get(exec.ctx, nil); err != nil {
					exec.errors = append(exec.errors, fmt.Errorf(
						"xfer failed with error %s", err,
					))
					return
				}

				remaining--
				if remaining == 0 {
					workflow.SignalExternalWorkflow(
						exec.ctx, exec.slurmPollerWE.ID, "",
						"slurm-request", req,
					)
				}
			})
		}
	}
}

func (exec *SlurmRemoteExecutor) SetCmdHandler(handler CmdHandler) {
	exec.handleFinishedCmd = handler
}

func (exec *SlurmRemoteExecutor) Shutdown() error {
	if exec.cancelChild == nil || exec.slurmPollerFuture == nil {
		return nil
	}
	exec.cancelChild()
	disconnectedCtx, _ := workflow.NewDisconnectedContext(exec.ctx)
	err := exec.slurmPollerFuture.Get(disconnectedCtx, nil)
	if err == nil {
		return nil
	}
	var canceledErr *temporal.CanceledError
	if errors.As(err, &canceledErr) {
		if canceledErr.HasDetails() {
			var evidence SlurmCancellationEvidence
			if detailErr := canceledErr.Details(&evidence); detailErr != nil {
				return fmt.Errorf("failed decoding Slurm cancellation evidence: %w", detailErr)
			}
			exec.cancellation = &evidence
		}
		return nil
	}
	exec.cancellation = &SlurmCancellationEvidence{
		CleanupStatus: "failed",
		Verified:      false,
		Error:         err.Error(),
	}
	return fmt.Errorf("Slurm poller shutdown failed: %w", err)
}

func (exec *SlurmRemoteExecutor) TerminalEvidence() *SlurmCancellationEvidence {
	return exec.cancellation
}

func slurmPollerWorkflowID(
	parentWorkflowID, parentRunID, storageID string, config parsing.SshConfig,
) string {
	identity := strings.Join([]string{
		parentWorkflowID,
		parentRunID,
		storageID,
		config.User,
		config.IpAddr,
		config.TransferAddr,
		fmt.Sprintf("%d", config.TransferPort),
		config.SchedDir,
	}, "\x00")
	sum := sha256.Sum256([]byte(identity))
	prefix := safeWorkflowIDComponent(parentWorkflowID)
	if len(prefix) > 24 {
		prefix = prefix[:24]
	}
	return fmt.Sprintf("slurm-poller-%s-%s", prefix, hex.EncodeToString(sum[:10]))
}

func safeWorkflowIDComponent(value string) string {
	var b strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	value = strings.Trim(b.String(), "-_")
	if value == "" {
		return "workflow"
	}
	return value
}

func slurmJobCorrelationKey(workflowID, runID string, nodeID, commandID int) string {
	identity := fmt.Sprintf("%s\x00%s\x00%d\x00%d", workflowID, runID, nodeID, commandID)
	sum := sha256.Sum256([]byte(identity))
	return "morphic-" + hex.EncodeToString(sum[:12])
}

func (exec *SlurmRemoteExecutor) GetErrors() []error {
	return exec.errors
}

func (exec *SlurmRemoteExecutor) BuildImages(imageNames []string) error {
	buildAo := workflow.ActivityOptions{
		TaskQueue:           "bwb_worker",
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	buildCtx := workflow.WithActivityOptions(exec.ctx, buildAo)
	xferAo := workflow.ActivityOptions{
		TaskQueue:           GetTemporalSshQueueName(exec.sshConfig),
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	xferCtx := workflow.WithActivityOptions(exec.ctx, xferAo)
	for _, imageName := range imageNames {
		var outPath string
		err := workflow.ExecuteActivity(
			buildCtx, BuildSingularitySIF, imageName,
		).Get(exec.ctx, &outPath)
		if err != nil {
			return err
		}

		dstPath := filepath.Join(exec.schedDir, "images", imageName)
		err = workflow.ExecuteActivity(
			xferCtx, fs.SshUploadActivity, exec.SlurmFS, outPath, dstPath,
		).Get(exec.ctx, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (exec *SlurmRemoteExecutor) Glob(
	root string, pattern string, findFile bool, findDir bool,
) ([]string, error) {
	var a SlurmActivity
	dataDir := filepath.Join(exec.schedDir, exec.storageId)
	ao := workflow.ActivityOptions{
		TaskQueue:           GetTemporalSshQueueName(exec.sshConfig),
		StartToCloseTimeout: 1 * time.Minute,
	}
	findCtx := workflow.WithActivityOptions(exec.ctx, ao)

	hostDataMnt := exec.SlurmFS.GetRootDir()
	volumes := map[string]string{"/data": dataDir}
	hostRootPath, ok := fs.GetV0HostPath(root, hostDataMnt)
	if !ok {
		return nil, fmt.Errorf("invalid root path %s", hostRootPath)
	}

	var typeStr string
	if findFile && findDir {
		typeStr = ""
	} else if findFile {
		typeStr = "-type f"
	} else if findDir {
		typeStr = "-type d"
	}

	var findOut CmdOut
	findCmd := fmt.Sprintf("find %s -name \"%s\" %s", hostRootPath, pattern, typeStr)
	err := workflow.ExecuteActivity(
		findCtx, a.ExecCmd, findCmd,
	).Get(exec.ctx, &findOut)
	if err != nil {
		return nil, err
	}

	out := make([]string, 0)
	lines := strings.Split(findOut.StdOut, "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		cntPath, ok := fs.GetV0CntPath(line, hostDataMnt)
		if !ok {
			return nil, fmt.Errorf(
				"couldn't convert host path %s to cnt path with volumes %#v",
				line, volumes,
			)
		}

		out = append(out, cntPath)
	}

	return out, nil
}

func (exec *SlurmRemoteExecutor) GetID() parsing.ExecType {
	return parsing.EXEC_SLURM
}
