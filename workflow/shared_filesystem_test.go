package workflow

import (
	"context"
	"fmt"
	"go-scheduler/fs"
	"go-scheduler/parsing"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestSharedFilesystemProbe(t *testing.T) {
	for _, mode := range []string{"shared", "missing", "different_contents"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			probe, err := WriteSharedFilesystemProbe(root)
			require.NoError(t, err)
			require.Equal(t, root, filepath.Dir(probe.Path))
			if mode == "missing" {
				require.NoError(t, os.Remove(probe.Path))
			} else if mode == "different_contents" {
				require.NoError(t, os.WriteFile(probe.Path, []byte("stale mount"), 0600))
			}
			err = CheckSharedFilesystemProbe(probe)
			if mode == "shared" {
				require.NoError(t, err)
				_, err = os.Stat(probe.Path)
				require.True(t, os.IsNotExist(err), "successful probe must be removed")
			} else {
				require.ErrorContains(t, err, probe.Path)
				host, errHost := os.Hostname()
				require.NoError(t, errHost)
				require.ErrorContains(t, err, host)
			}
		})
	}
	t.Run("relative_root", func(t *testing.T) {
		_, err := WriteSharedFilesystemProbe("relative")
		require.ErrorContains(t, err, "absolute path")
	})
}

func TestSharedWorkerImage(t *testing.T) {
	for _, mode := range []string{"shared", "symlink", "missing", "different", "directory", "unset"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			t.Setenv("BWB_SCHED_DIR", root)
			image := "example/image:1"
			expected := filepath.Join(root, "images", getSifName(image))
			require.NoError(t, os.MkdirAll(filepath.Dir(expected), 0755))
			built := filepath.Join(t.TempDir(), getSifName(image))
			require.NoError(t, os.WriteFile(built, []byte("SIF"), 0644))
			switch mode {
			case "shared":
				built = expected
				require.NoError(t, os.WriteFile(expected, []byte("SIF"), 0644))
			case "symlink":
				require.NoError(t, os.Symlink(built, expected))
			case "different":
				require.NoError(t, os.WriteFile(expected, []byte("old SIF"), 0644))
			case "directory":
				require.NoError(t, os.Mkdir(expected, 0755))
			case "unset":
				t.Setenv("BWB_SCHED_DIR", "")
			}
			err := CheckWorkerImage(image, built)
			if mode == "shared" || mode == "symlink" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				host, _ := os.Hostname()
				require.ErrorContains(t, err, host)
				if mode != "unset" {
					require.ErrorContains(t, err, expected)
					require.ErrorContains(t, err, built)
				}
			}
		})
	}
}

func sharedTestExecutors(ctx workflow.Context, shared bool) (map[int]Executor, error) {
	selector := workflow.NewSelector(ctx)
	cfg := parsing.JobConfig{
		TemporalExecutor:      parsing.TemporalExecutorConfig{SharedFilesystemWithScheduler: shared},
		TemporalConfigsByNode: map[int]parsing.LocalJobConfig{1: {}},
		SlurmConfigsByNode:    map[int]parsing.SlurmJobConfig{2: {}},
		SlurmExecutor:         parsing.SshConfig{User: "user", IpAddr: "cluster:22"},
	}
	execs, _, err := setupExecutors(ctx, selector, "run", &parsing.ResolvedWorkflow{
		Nodes: map[int]parsing.ResolvedNode{1: {Id: 1}, 2: {Id: 2}},
	}, nil, map[string]WorkerInfo{"worker": {QueueId: "worker"}}, fs.LocalFS{}, cfg)
	return execs, err
}

func TestSharedTransferQueues(t *testing.T) {
	for _, shared := range []bool{false, true} {
		for _, direction := range []string{"slurm_to_worker", "worker_to_slurm"} {
			t.Run(fmt.Sprintf("shared_%v/%s", shared, direction), func(t *testing.T) {
				var suite testsuite.WorkflowTestSuite
				env := suite.NewTestWorkflowEnvironment()
				transferQueue, commandQueue := "", ""
				finishedTransfer, finishedCmd := false, false
				env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
					if info.ActivityType.Name == "RunCmdActivity" {
						commandQueue = info.TaskQueue
						require.True(t, finishedTransfer)
					} else {
						transferQueue = info.TaskQueue
					}
				})
				env.OnActivity(fs.TransferSshToLocalFS, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(func(string, fs.SshFS, fs.LocalFS, []parsing.ObligatoryXfer) error {
						finishedTransfer = true
						return nil
					}).Maybe()
				env.OnActivity(fs.TransferLocalToSshFS, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(func(string, fs.LocalFS, fs.SshFS, []parsing.ObligatoryXfer) error {
						finishedTransfer = true
						return nil
					}).Maybe()
				env.OnActivity(RunCmdActivity, mock.Anything, mock.Anything, false, mock.Anything).Return(CmdOutput{}, nil).Maybe()
				env.OnSignalExternalWorkflow(mock.Anything, "resources", "", "release-allocation", mock.Anything).Return(nil).Maybe()
				env.OnSignalExternalWorkflow(mock.Anything, "poller", "", "slurm-request", mock.Anything).Return(nil).Maybe()
				env.ExecuteWorkflow(func(ctx workflow.Context) error {
					execs, err := sharedTestExecutors(ctx, shared)
					if err != nil {
						return err
					}
					local := execs[1].(*TemporalExecutor)
					slurm := execs[2].(*SlurmRemoteExecutor)
					local.workerFSs["worker"] = fs.LocalFS{RootDir: "/shared/run"}
					local.schedulerWE.ID = "resources"
					slurm.slurmPollerWE.ID = "poller"
					filesystems := map[parsing.ExecType]fs.AbstractFileSystem{
						parsing.EXEC_TEMPORAL: local.workerFSs["worker"], parsing.EXEC_SLURM: fs.SshFS{},
					}
					handler := func(c workflow.Context, id string, x []parsing.ObligatoryXfer) ([]workflow.Future, error) {
						return DefaultFileXferHanlder(c, id, x, filesystems)
					}
					local.SetFileXferHandler(handler)
					slurm.SetFileXferHandler(handler)
					local.SetCmdHandler(func(CmdOutput, error, Executor, parsing.CmdRunParams) { finishedCmd = true })
					xfer := parsing.ObligatoryXfer{SrcExecutor: parsing.EXEC_SLURM, DstExecutor: parsing.EXEC_TEMPORAL}
					if direction == "slurm_to_worker" {
						local.RunXfersAndCmdWithGrant(parsing.CmdRunParams{Cmd: parsing.CmdTemplate{NodeId: 1}, Xfers: []parsing.ObligatoryXfer{xfer}}, ResourceGrant{WorkerId: "worker"})
						for !finishedCmd {
							(*local.selector).Select(ctx)
						}
					} else {
						xfer.SrcExecutor, xfer.DstExecutor = parsing.EXEC_TEMPORAL, parsing.EXEC_SLURM
						slurm.RunCmds([]parsing.CmdRunParams{{Cmd: parsing.CmdTemplate{NodeId: 2}, Xfers: []parsing.ObligatoryXfer{xfer}}})
						for !finishedTransfer {
							(*slurm.selector).Select(ctx)
						}
					}
					return nil
				})
				require.NoError(t, env.GetWorkflowError())
				wantQueue := GetTemporalSshQueueName(parsing.SshConfig{User: "user", IpAddr: "cluster:22"})
				if !shared && direction == "slurm_to_worker" {
					wantQueue = "worker"
				}
				require.Equal(t, wantQueue, transferQueue)
				if direction == "slurm_to_worker" {
					require.Equal(t, "worker", commandQueue)
				}
				env.AssertExpectations(t)
			})
		}
	}
}

func TestSharedFilesystemSetup(t *testing.T) {
	for _, mode := range []string{"disabled", "shared", "missing"} {
		t.Run(mode, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			root := t.TempDir()
			env.OnWorkflow(ResourceSchedulerWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()
			env.OnActivity(fs.SetupRootDir, "run").Return(root, nil).Once()
			env.RegisterActivity(WriteSharedFilesystemProbe)
			env.RegisterActivity(CheckSharedFilesystemProbe)
			checked := false
			env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
				switch info.ActivityType.Name {
				case "SetupRootDir", "WriteSharedFilesystemProbe":
					require.Equal(t, "worker", info.TaskQueue)
				case "CheckSharedFilesystemProbe":
					checked = true
					require.Equal(t, GetTemporalSshQueueName(parsing.SshConfig{User: "user", IpAddr: "cluster:22"}), info.TaskQueue)
					if mode == "missing" {
						markers, err := filepath.Glob(filepath.Join(root, ".scheduler-shared-fs-*"))
						require.NoError(t, err)
						require.Len(t, markers, 1)
						require.NoError(t, os.Remove(markers[0]))
					}
				}
			})
			env.ExecuteWorkflow(func(ctx workflow.Context) error {
				execs, err := sharedTestExecutors(ctx, mode != "disabled")
				if err != nil {
					return err
				}
				return execs[1].Setup(true)
			})
			if mode == "missing" {
				require.ErrorContains(t, env.GetWorkflowError(), root)
				host, _ := os.Hostname()
				require.ErrorContains(t, env.GetWorkflowError(), host)
				require.ErrorContains(t, env.GetWorkflowError(), "scheduler host")
			} else {
				require.NoError(t, env.GetWorkflowError())
			}
			require.Equal(t, mode != "disabled", checked)
			env.AssertExpectations(t)
		})
	}
}

func TestSharedImageSetup(t *testing.T) {
	for _, mode := range []string{"disabled", "shared", "missing", "docker"} {
		t.Run(mode, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			root := t.TempDir()
			t.Setenv("BWB_SCHED_DIR", root)
			image := "example/image:1"
			path := filepath.Join(root, "images", getSifName(image))
			if mode == "shared" {
				require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
				require.NoError(t, os.WriteFile(path, []byte("SIF"), 0644))
			}
			env.OnActivity(BuildSingularitySIF, image).Return(path, nil).Once()
			env.RegisterActivity(CheckWorkerImage)
			checks := 0
			env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
				if info.ActivityType.Name == "CheckWorkerImage" {
					checks++
					require.Equal(t, "worker", info.TaskQueue)
				} else {
					require.Equal(t, SCHEDULER_QUEUE, info.TaskQueue)
				}
			})
			env.ExecuteWorkflow(func(ctx workflow.Context) error {
				execs, err := sharedTestExecutors(ctx, mode != "disabled")
				if err != nil {
					return err
				}
				local := execs[1].(*TemporalExecutor)
				local.configsByNode[1] = parsing.LocalJobConfig{UseDocker: mode == "docker"}
				return local.BuildImages([]string{image})
			})
			if mode == "missing" {
				require.ErrorContains(t, env.GetWorkflowError(), path)
			} else {
				require.NoError(t, env.GetWorkflowError())
			}
			want := 1
			if mode == "disabled" || mode == "docker" {
				want = 0
			}
			require.Equal(t, want, checks)
			env.AssertExpectations(t)
		})
	}
}
