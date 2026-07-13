package parsing

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
)

type ObligatoryXfer struct {
	SrcExecutor ExecType
	DstExecutor ExecType
	SrcHostPath string
	DstHostPath string
}

type CmdRunParams struct {
	Cmd CmdTemplate
	// cnt -> host
	Volumes          map[string]ExecVolumeMnt
	Xfers            []ObligatoryXfer
	HostDirsToCreate []string
}

type CmdManager struct {
	state           *WorkflowExecutionState
	jobConfig       JobConfig
	currentMaxCmdId int
	cmdIdToParams   map[int]NodeParams
	// cmdID -> param name -> mount
	cmdIdToVolumes map[int]map[string]ExecVolumeMnt
	completedCmds  map[int]struct{}
	remainingIters map[int]map[string]map[int]struct{}
	nodeStatus     map[int]string
	// executor ID -> root dir of corresponding filesystem
	executorFsRootDirs map[ExecType]string
}

func NewCmdManager(
	workflow Workflow, index WorkflowIndex, config JobConfig,
	executorFsRootDirs map[ExecType]string,
) CmdManager {
	var cmdMan CmdManager
	cmdMan.cmdIdToParams = make(map[int]NodeParams)
	cmdMan.cmdIdToVolumes = make(map[int]map[string]ExecVolumeMnt)
	cmdMan.remainingIters = make(map[int]map[string]map[int]struct{})
	cmdMan.completedCmds = make(map[int]struct{})
	cmdMan.nodeStatus = make(map[int]string)
	for _, nodeId := range workflow.GetNodeIds() {
		cmdMan.remainingIters[nodeId] = make(map[string]map[int]struct{})
		cmdMan.nodeStatus[nodeId] = "AWAITING_PREDECESSORS"
	}
	wes := NewWorkflowExecutionState(workflow, index)
	cmdMan.state = &wes
	cmdMan.jobConfig = config
	cmdMan.executorFsRootDirs = executorFsRootDirs
	return cmdMan
}

func resolveExplicitHostPath(executorRoot, declaredPath string) string {
	cleanedPath := filepath.Clean(declaredPath)
	if cleanedPath == "/data" || strings.HasPrefix(cleanedPath, "/data/") {
		return filepath.Join(executorRoot, cleanedPath)
	}
	return declaredPath
}

func (cmdMan *CmdManager) GetImageNames() []string {
	imageNames := make([]string, 0)
	nodes := cmdMan.state.workflow.GetNodes()
	for _, node := range nodes {
		imageName := fmt.Sprintf("%s:%s", node.GetImageName(), node.GetImageTag())
		imageNames = append(imageNames, imageName)
	}
	return imageNames
}

func (cmdMan *CmdManager) GetNodeStatus() map[int]string {
	return cmdMan.nodeStatus
}

func (cmdMan *CmdManager) GetSuccCmds(
	completedCmd CmdTemplate,
	rawOutputs map[string]string,
	glob GlobFunc,
	success bool,
) (map[int][]CmdRunParams, error) {
	inputParams, inputParamsExist := cmdMan.cmdIdToParams[completedCmd.Id]
	if !inputParamsExist {
		return nil, fmt.Errorf("cmd has invalid ID %d; no params found", completedCmd.Id)
	}

	inputVols, inputVolsExist := cmdMan.cmdIdToVolumes[completedCmd.Id]
	if !inputVolsExist {
		return nil, fmt.Errorf("cmd has invalid ID %d; no volumes found", completedCmd.Id)
	}

	// Do not return successors for already completed cmd,
	// these have already been consumed by some prior call.
	if _, ok := cmdMan.completedCmds[completedCmd.Id]; ok {
		return nil, nil
	}

	cmdMan.completedCmds[completedCmd.Id] = struct{}{}
	nodeId := inputParams.NodeId
	node, _ := cmdMan.state.workflow.GetNode(nodeId)
	nodeRunId := fmt.Sprintf("%#v", inputParams.AncList)
	delete(cmdMan.remainingIters[nodeId][nodeRunId], completedCmd.Id)

	outputTp := node.ParseOutputs(rawOutputs)
	if !node.IsAsync() && len(cmdMan.remainingIters[nodeId][nodeRunId]) > 0 {
		return nil, nil
	}

	cmdMan.nodeStatus[completedCmd.NodeId] = "FINISHED"
	succParams, succVols, err := cmdMan.state.getSuccParams(
		inputParams, []TypedParams{outputTp}, inputVols, success,
	)

	if err != nil {
		return nil, err
	}

	cmds, err := cmdMan.getCmdsFromParams(succParams, succVols, glob)
	if err != nil {
		return nil, err
	}

	for nodeId := range cmds {
		for i := range cmds[nodeId] {
			RemoveElideableFileXfers(
				&cmds[nodeId][i].Cmd, cmdMan.state.workflow, cmdMan.state.index,
				cmdMan.jobConfig,
			)
		}
	}

	return cmds, err
}

func (cmdMan *CmdManager) IsComplete() bool {
	return cmdMan.state.IsComplete()
}

func (cmdMan *CmdManager) HasFailed() bool {
	return cmdMan.state.HasFailed()
}

func (cmdMan *CmdManager) GetInitialCmds(glob GlobFunc) (map[int][]CmdRunParams, error) {
	initialParams, err := cmdMan.state.getInitialNodeParams()
	if err != nil {
		return nil, err
	}

	// Propagated volumes are the set of volumes propagated to each node
	// from their predecessors. Initial nodes have no predecessors.
	emptyPropVols := make(map[int][]map[string]ExecVolumeMnt)
	for nodeId, params := range initialParams {
		emptyPropVols[nodeId] = make([]map[string]ExecVolumeMnt, len(params))
		for i := range params {
			emptyPropVols[nodeId][i] = nil
		}
	}

	return cmdMan.getCmdsFromParams(initialParams, emptyPropVols, glob)
}

func resolvePqs(node WorkflowNodeV0, tp *TypedParams, glob GlobFunc) error {
	for pname, argType := range node.ArgTypes {
		if argType.ArgType == "patternQuery" {
			shouldEval := (argType.Flag != nil || argType.Env != nil ||
				(argType.IsArgument != nil && *argType.IsArgument))
			if shouldEval {
				err := tp.ResolvePq(pname, node, glob)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Returns (1) cnt path -> host path, (2) xfers, (3) dirs to create.
func (cmdMan *CmdManager) getCmdVolumesAndXfers(
	executorId ExecType, abstractVols NodeVolumes,
) (map[string]ExecVolumeMnt, []ObligatoryXfer, []string, error) {
	dstExecRootDir, exists := cmdMan.executorFsRootDirs[executorId]
	if !exists {
		return nil, nil, nil, fmt.Errorf("invalid executor id %d", executorId)
	}

	vols := make(map[string]ExecVolumeMnt)
	xfers := make([]ObligatoryXfer, 0)
	dirsToCreate := make([]string, 0)

	if abstractVols.MountRootToData {
		vols["__general__"] = ExecVolumeMnt{
			CntPath:  "/data",
			HostPath: dstExecRootDir,
			Executor: executorId,
		}

	}

	// Explicit mounts are assumed to have host paths referencing the
	// FS of their executor.
	for pname, mnt := range abstractVols.ExplicitMnts {
		hostPath := resolveExplicitHostPath(dstExecRootDir, mnt.HostPath)
		vols[pname] = ExecVolumeMnt{
			Executor:       executorId,
			CntPath:        mnt.CntPath,
			HostPath:       hostPath,
			TargetFileCnt:  mnt.CntPath,
			TargetFileHost: hostPath,
		}
	}

	// Propagated paths are those generated by previous nodes, which may or
	// may not have been generated on the same executor / filesystem as this one.
	for pname, propagatedHostPath := range abstractVols.PropagatedPaths {
		if executorId == propagatedHostPath.MntSrc.Executor {
			// If generated on the same filesystem, then just use the same mount.
			vols[pname] = ExecVolumeMnt{
				Executor:       executorId,
				CntPath:        propagatedHostPath.CntPath,
				HostPath:       propagatedHostPath.MntSrc.TargetFileHost,
				TargetFileCnt:  propagatedHostPath.MntSrc.TargetFileCnt,
				TargetFileHost: propagatedHostPath.MntSrc.TargetFileHost,
			}
		} else {
			// Input file was generated on a different executor, needs to be transferred.
			// The remotely generated file will be downloaded to "[ROOT_DIR]/[CNT_PATH]"
			// then mounted to "[CNT_PATH]" inside the container.
			// Note that we are using TargetFileCnt instead of CntPath. CntPath is what
			// was actually mounted in the container that passed us this volume. But if
			// the passed parameter is an output file, CntDir would have been the parent
			// dir of the output file, since we couldn't mount the (then non-existent)
			// output file before it was created. TargetFileCnt is the container path
			// of the output file itself.
			var hostPath string
			if abstractVols.MountRootToData && strings.HasPrefix(propagatedHostPath.CntPath, "/data") {
				hostPath = filepath.Join(dstExecRootDir, strings.TrimLeft(propagatedHostPath.CntPath, "/data"))
			} else {
				hostPath = filepath.Join(dstExecRootDir, propagatedHostPath.CntPath)
			}
			xfers = append(xfers, ObligatoryXfer{
				SrcExecutor: propagatedHostPath.MntSrc.Executor,
				DstExecutor: executorId,
				SrcHostPath: propagatedHostPath.MntSrc.TargetFileHost,
				DstHostPath: hostPath,
			})
			vols[pname] = ExecVolumeMnt{
				Executor:       executorId,
				CntPath:        propagatedHostPath.CntPath,
				HostPath:       hostPath,
				TargetFileCnt:  propagatedHostPath.CntPath,
				TargetFileHost: hostPath,
			}
		}
	}

	// For output files (which don't yet exist on host FS), we create find its parent dir
	// in the container path, create it, and mount host path "[ROOT_DIR]/[CNT_PARENT_DIR]"
	// to "[CNT_PARENT_DIR]"
	for pname, cntOutFile := range abstractVols.OutputFiles {
		parentCntDir := filepath.Dir(cntOutFile)
		// Prevent mounting something to container path "/", which would overwrite rest of container FS.
		if parentCntDir == "/" {
			return nil, nil, nil, fmt.Errorf(
				"invalid output file path %s: cannot mount an output file with root as parent dir",
				cntOutFile,
			)
		}
		parentHostDir := filepath.Join(dstExecRootDir, parentCntDir)
		targetFileHost := filepath.Join(dstExecRootDir, cntOutFile)
		vols[pname] = ExecVolumeMnt{
			Executor:       executorId,
			CntPath:        parentCntDir,
			HostPath:       parentHostDir,
			TargetFileCnt:  cntOutFile,
			TargetFileHost: targetFileHost,
		}
		dirsToCreate = append(dirsToCreate, parentHostDir)
	}

	for pname, cntOutDir := range abstractVols.OutputDirs {
		hostOutDir := filepath.Join(dstExecRootDir, cntOutDir)
		vols[pname] = ExecVolumeMnt{
			Executor:       executorId,
			CntPath:        cntOutDir,
			HostPath:       hostOutDir,
			TargetFileCnt:  cntOutDir,
			TargetFileHost: hostOutDir,
		}
		dirsToCreate = append(dirsToCreate, hostOutDir)
	}

	return vols, xfers, dirsToCreate, nil
}

func (cmdMan *CmdManager) getCmdsFromParams(
	nodeParams map[int][]NodeParams, propagatedVols map[int][]map[string]ExecVolumeMnt, glob GlobFunc,
) (map[int][]CmdRunParams, error) {
	ret := make(map[int][]CmdRunParams, 0)
	for nodeId, paramSets := range nodeParams {
		executorId, ok := cmdMan.jobConfig.ExecTypeByNode[nodeId]
		if !ok {
			return nil, fmt.Errorf("executor for node %d not in config", nodeId)
		}
		cmdMan.nodeStatus[nodeId] = "RUNNING"
		node, _ := cmdMan.state.workflow.GetNode(nodeId)
		for i, paramSet := range paramSets {
			vols := propagatedVols[nodeId][i]
			if err := node.ResolveGlob(&paramSet.Params, glob); err != nil {
				return nil, fmt.Errorf(
					"error parsing node %d pattern queries: %s",
					nodeId, err,
				)
			}

			nodeCmds, volumes, err := node.ParseCmd(
				paramSet.Params, vols, cmdMan.state.index, cmdMan.jobConfig,
				cmdMan.executorFsRootDirs,
			)
			if err != nil {
				return nil, err
			}

			if len(nodeCmds) != len(volumes) {
				return nil, fmt.Errorf("received %d cmds, %d volumes sets", len(nodeCmds), len(volumes))
			}

			nodeRunId := fmt.Sprintf("%#v", paramSet.AncList)
			if cmdMan.remainingIters[nodeId][nodeRunId] == nil {
				cmdMan.remainingIters[nodeId][nodeRunId] = make(map[int]struct{})
			}

			for i := range nodeCmds {
				cmdId := cmdMan.currentMaxCmdId
				nodeCmds[i].Id = cmdId
				nodeCmds[i].Priority = cmdMan.state.index.MaxDistanceFromSink[nodeId]
				cmdMan.cmdIdToParams[cmdId] = paramSet
				cmdMan.remainingIters[nodeId][nodeRunId][cmdId] = struct{}{}
				cmdMan.currentMaxCmdId += 1

				vols, xfers, dirsToCreate, err := cmdMan.getCmdVolumesAndXfers(executorId, volumes[i])
				if err != nil {
					return nil, fmt.Errorf("error creating vols: %s", err)
				}

				ret[nodeId] = append(ret[nodeId], CmdRunParams{
					Cmd:              nodeCmds[i],
					Volumes:          vols,
					Xfers:            xfers,
					HostDirsToCreate: dirsToCreate,
				})
				cmdMan.cmdIdToVolumes[cmdId] = vols
			}

			if len(nodeCmds) == 0 {
				marshaledCmd, _ := json.MarshalIndent(paramSet.Params, "", "\t")
				return nil, fmt.Errorf(
					"params for node %d generated 0 commands: %s",
					nodeId, string(marshaledCmd),
				)
			}
		}
	}
	return ret, nil
}
