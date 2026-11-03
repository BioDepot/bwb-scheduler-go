package workflow

import (
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go-scheduler/fs"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func startGroup(t *testing.T, script string) (*exec.Cmd, chan error) {
	cmd := exec.Command("sh", "-c", script)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	done := make(chan error, 1)
	require.NoError(t, cmd.Start())
	go func() { done <- cmd.Wait() }()
	return cmd, done
}

// groupAlive reports whether any process of the group still exists, allowing
// a moment for killed processes to be reaped.
func groupAlive(pgid int) bool {
	for i := 0; i < 20; i++ {
		if syscall.Kill(-pgid, 0) != nil {
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
	return true
}

// A command that exits on SIGTERM is gone at once, with its children.
func TestStopProcessGroupTerminatesTree(t *testing.T) {
	cmd, done := startGroup(t, "sleep 60 & sleep 60 & wait")
	time.Sleep(200 * time.Millisecond)
	start := time.Now()
	stopProcessGroup(cmd, done, 5*time.Second)
	require.Less(t, time.Since(start), 2*time.Second)
	require.False(t, groupAlive(cmd.Process.Pid))
}

// A command that ignores SIGTERM, with a child that does too, is killed after
// the grace period.
func TestStopProcessGroupKillsAfterGrace(t *testing.T) {
	cmd, done := startGroup(t, `trap "" TERM; (trap "" TERM; sleep 60) & while true; do sleep 1; done`)
	time.Sleep(300 * time.Millisecond)
	start := time.Now()
	stopProcessGroup(cmd, done, 2*time.Second)
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 2*time.Second)
	require.Less(t, elapsed, 4*time.Second)
	require.False(t, groupAlive(cmd.Process.Pid))
}

func TestCommandWorkersThrottleHeartbeats(t *testing.T) {
	o := commandWorkerOptions(false)
	require.Equal(t, 2*time.Second, o.MaxHeartbeatThrottleInterval)
	require.Equal(t, 2*time.Second, o.DefaultHeartbeatThrottleInterval)
	require.Equal(t, time.Duration(0), commandWorkerOptions(true).MaxHeartbeatThrottleInterval)
}

// New executions take version 1; a history without the marker takes the
// default (old) behaviour.
func TestSetupSelectsPromptStopByVersion(t *testing.T) {
	for _, pinOld := range []bool{false, true} {
		env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
		env.OnWorkflow(ResourceSchedulerWorkflow, mock.Anything, mock.Anything).Return(nil).Maybe()
		env.OnActivity(fs.SetupRootDir, "storage").Return("", nil).Maybe()
		var prompt bool
		env.ExecuteWorkflow(func(ctx workflow.Context) error {
			if pinOld {
				workflow.GetVersion(ctx, "stop-kills-commands", workflow.DefaultVersion, workflow.DefaultVersion)
			}
			// the ledger path needs the ledger; use the legacy scheduler
			workflow.GetVersion(ctx, "shared-worker-ledger", workflow.DefaultVersion, workflow.DefaultVersion)
			selector := workflow.NewSelector(ctx)
			exec := NewTemporalExecutor(ctx, &selector, nil, fs.LocalFS{},
				map[string]WorkerInfo{"q": {QueueId: "q"}}, "storage", nil)
			_ = exec.Setup(false)
			prompt = exec.promptStop
			return nil
		})
		require.Equal(t, !pinOld, prompt)
	}
}
