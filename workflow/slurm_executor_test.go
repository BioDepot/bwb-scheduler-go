// slurm_executor_test.go
package workflow

import (
	"fmt"
	"go-scheduler/fs"
	"go-scheduler/parsing"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// When SlurmRemoteExecutor.RunCmds is called on a command with input files,
// require that these files are downloaded *before* a signal is sent to the
// slurm poller workflow to start the SLURM job. When there are no input
// files, no download should occur before the signal is sent.
func TestSlurmExecutorFileDownloads(t *testing.T) {
	testCases := []struct {
		name                   string
		xfers                  []parsing.ObligatoryXfer
		expectFinishedTransfer bool
	}{
		{
			name: "withDownloads",
			xfers: []parsing.ObligatoryXfer{
				parsing.ObligatoryXfer{
					SrcExecutor: parsing.EXEC_TEMPORAL,
					DstExecutor: parsing.EXEC_SLURM,
					SrcHostPath: "/src/host/path",
					DstHostPath: "/dst/host/path",
				},
			},
			expectFinishedTransfer: true,
		},
		{
			name:                   "noDownloads",
			xfers:                  []parsing.ObligatoryXfer{},
			expectFinishedTransfer: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()

			// Register activities invoked during setup as NO-OP.
			var a SlurmActivity
			env.RegisterActivity(fs.TransferLocalToSshFS)
			env.RegisterActivity(a.ExecCmd)
			env.OnActivity(a.ExecCmd, mock.Anything).Return(CmdOut{}, nil)

			storageId := "storageId"
			localFS := fs.LocalFS{
				RootDir: "/localRoot",
			}
			execToFs := map[parsing.ExecType]fs.AbstractFileSystem{
				parsing.EXEC_TEMPORAL: localFS,
				parsing.EXEC_SLURM: fs.SshFS{
					RootDir: "remote",
				},
			}

			finishedTransfer := false
			transferCall := env.OnActivity(
				fs.TransferLocalToSshFS, storageId, execToFs[parsing.EXEC_TEMPORAL],
				execToFs[parsing.EXEC_SLURM], tc.xfers,
			).Return(func(string, fs.LocalFS, fs.SshFS, []parsing.ObligatoryXfer) error {
				finishedTransfer = true
				return nil
			})
			if tc.expectFinishedTransfer {
				transferCall.Once()
			} else {
				transferCall.Maybe()
			}

			cmdToRun := parsing.CmdRunParams{
				Cmd: parsing.CmdTemplate{
					Id:     1,
					NodeId: 2,
				},
				Xfers: tc.xfers,
			}
			memLval := "40G"
			configForCmd := parsing.SlurmJobConfig{
				Mem: &memLval,
			}

			// NOTE: The temporal docs don't say this, but you can't use
			// OnSignalExternalWorkflow between parent and child workflows
			// in the same testing env, even though the reverse is fine.
			// You have to actually register a mock child workflow.
			receivedSignal := false
			env.RegisterWorkflowWithOptions(func(ctx workflow.Context, _ SlurmState) error {
				selector := workflow.NewSelector(ctx)
				selector.AddReceive(
					workflow.GetSignalChannel(ctx, "slurm-request"),
					func(c workflow.ReceiveChannel, _ bool) {
						var req SlurmRequest
						if ok := c.Receive(ctx, &req); !ok {
							t.Fatalf("could not convert signal contents to slurm req")
						}
						receivedSignal = true
						require.Equal(t, req.Cmd, cmdToRun)
						require.Equal(t, req.Config, configForCmd)
						if tc.expectFinishedTransfer {
							require.True(t, finishedTransfer)
						}
					},
				)
				workflow.Sleep(ctx, 100*time.Second)
				selector.Select(ctx)
				return nil
			}, workflow.RegisterOptions{
				Name: "SlurmPollerWorkflow",
			})

			testCmdRunWf := func(ctx workflow.Context) error {
				selector := workflow.NewSelector(ctx)
				slurmExec := NewSlurmRemoteExecutor(
					ctx, &selector, localFS, storageId,
					map[int]parsing.SlurmJobConfig{cmdToRun.Cmd.NodeId: configForCmd},
					parsing.SshConfig{SchedDir: execToFs[parsing.EXEC_SLURM].GetRootDir()},
				)

				// Part of the "contract" of executors is that Setup()
				// gets called before trying to run commands.
				if err := slurmExec.Setup(false); err != nil {
					return fmt.Errorf("setup failed: %s", err)
				}
				slurmExec.SetFileXferHandler(
					func(
						ctx workflow.Context, s string, ox []parsing.ObligatoryXfer,
					) ([]workflow.Future, error) {
						return DefaultFileXferHanlder(ctx, s, ox, execToFs)
					},
				)
				slurmExec.RunCmds([]parsing.CmdRunParams{cmdToRun})

				// Call select a few times to trigger signal-handling logic.
				// I don't know of a better way to test this, but this corresponds
				// to the actual lifecycle of an executor.
				for i := 0; i < 10; i++ {
					selector.Select(ctx)
				}
				return nil
			}
			env.RegisterWorkflow(testCmdRunWf)
			env.ExecuteWorkflow(testCmdRunWf)
			err := env.GetWorkflowError()
			if err != nil {
				t.Fatalf("workflow failed w/ err %s", err)
			}

			env.AssertExpectations(t)
			require.True(t, receivedSignal)
			require.Equal(t, tc.expectFinishedTransfer, finishedTransfer)
		})
	}
}

// When the underlying slurm workflow fails, SlurmExecutor.GetErrors()
// should eventually return a corresponding error.
func TestSlurmExecutorChildWorkflowFailure(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	var a SlurmActivity
	env.RegisterActivity(fs.TransferLocalToSshFS)
	env.RegisterActivity(a.ExecCmd)
	env.OnActivity(a.ExecCmd, mock.Anything).Return(CmdOut{}, nil)

	// NOTE: The temporal docs don't say this, but you can't use
	// OnSignalExternalWorkflow between parent and child workflows
	// in the same testing env, even though the reverse is fine.
	// You have to actually register a mock child workflow.
	env.RegisterWorkflowWithOptions(func(ctx workflow.Context, _ SlurmState) error {
		return fmt.Errorf("generic mocked workflow error")
	}, workflow.RegisterOptions{
		Name: "SlurmPollerWorkflow",
	})

	testCmdRunWf := func(ctx workflow.Context) error {
		selector := workflow.NewSelector(ctx)
		slurmExec := NewSlurmRemoteExecutor(
			ctx, &selector, fs.LocalFS{}, "", nil, parsing.SshConfig{},
		)

		// Part of the "contract" of executors is that Setup()
		// gets called before trying to run commands.
		if err := slurmExec.Setup(false); err != nil {
			fmt.Printf("setup failed: %s", err)
			return fmt.Errorf("setup failed: %s", err)
		}

		// Call select a few times to trigger signal-handling logic.
		// I don't know of a better way to test this, but this corresponds
		// to the actual lifecycle of an executor.
		var errs []error = nil
		for i := 0; i < 10; i++ {
			selector.Select(ctx)
			if errs = slurmExec.GetErrors(); len(errs) > 0 {
				break
			}
		}

		if len(errs) == 0 {
			return nil
		}
		return fmt.Errorf("got errors")
	}
	env.RegisterWorkflow(testCmdRunWf)
	env.ExecuteWorkflow(testCmdRunWf)
	err := env.GetWorkflowError()
	if err == nil {
		t.Fatalf("GetErrors failed to return errors after child WF failure")
	}
}
func TestSlurmPollerWorkflowIDIsStableAndExecutionSpecific(t *testing.T) {
	config := parsing.SshConfig{
		User:         "tutorial",
		IpAddr:       "gpu.example.org:22",
		TransferAddr: "gpu.example.org",
		SchedDir:     "/srv/slurm_mnt",
	}

	first := slurmPollerWorkflowID("workflow-a", "run-a", "storage", config)
	if got := slurmPollerWorkflowID("workflow-a", "run-a", "storage", config); got != first {
		t.Fatalf("poller workflow ID is not deterministic: %q != %q", got, first)
	}
	if got := slurmPollerWorkflowID("workflow-b", "run-b", "storage", config); got == first {
		t.Fatalf("different parent executions collided on poller ID %q", got)
	}
	changedSite := config
	changedSite.SchedDir = "/srv/other-slurm"
	if got := slurmPollerWorkflowID("workflow-a", "run-a", "storage", changedSite); got == first {
		t.Fatalf("different Slurm site identities collided on poller ID %q", got)
	}
}

func TestSlurmJobCorrelationKeyIsStableAndCommandSpecific(t *testing.T) {
	first := slurmJobCorrelationKey("workflow-a", "run-a", 10, 1)
	if got := slurmJobCorrelationKey("workflow-a", "run-a", 10, 1); got != first {
		t.Fatalf("correlation key is not deterministic: %q != %q", got, first)
	}
	if got := slurmJobCorrelationKey("workflow-a", "run-a", 10, 2); got == first {
		t.Fatalf("different commands collided on correlation key %q", got)
	}
	if !slurmCorrelationPattern.MatchString(first) {
		t.Fatalf("correlation key is not safe for Slurm: %q", first)
	}
}
