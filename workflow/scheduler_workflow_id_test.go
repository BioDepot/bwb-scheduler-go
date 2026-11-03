package workflow

import (
	"go-scheduler/fs"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// runSetupAndReportSchedulerID runs a parent workflow with the given ID through
// TemporalExecutor.Setup against a stand-in resource scheduler and returns the
// workflow ID the stand-in ran under and the ID the executor will signal.
func runSetupAndReportSchedulerID(t *testing.T, parentID string) (childID, signalTarget string) {
	t.Helper()
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: parentID})
	env.RegisterWorkflowWithOptions(func(ctx workflow.Context, _ SchedWorkflowState) error {
		childID = workflow.GetInfo(ctx).WorkflowExecution.ID
		return workflow.Sleep(ctx, 1)
	}, workflow.RegisterOptions{Name: "ResourceSchedulerWorkflow"})
	env.OnActivity(fs.SetupRootDir, mock.Anything).Return("", nil).Maybe()

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		selector := workflow.NewSelector(ctx)
		exec := NewTemporalExecutor(ctx, &selector, nil, fs.LocalFS{},
			map[string]WorkerInfo{"q": {QueueId: "q"}}, "storage", nil)
		if err := exec.Setup(false); err != nil {
			return err
		}
		signalTarget = exec.schedulerWE.ID
		return nil
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	return childID, signalTarget
}

// Two workflows in one namespace must not start their resource schedulers under
// the same ID (the cause of "child workflow execution already started"), and
// each executor must signal the scheduler it started.
func TestSchedulerWorkflowIDDiffersBetweenConcurrentWorkflows(t *testing.T) {
	childA, targetA := runSetupAndReportSchedulerID(t, "workflow-A")
	childB, targetB := runSetupAndReportSchedulerID(t, "workflow-B")
	require.NotEqual(t, legacySchedulerWorkflowID, childA)
	require.NotEqual(t, legacySchedulerWorkflowID, childB)
	require.NotEqual(t, childA, childB)
	require.Equal(t, childA, targetA)
	require.Equal(t, childB, targetB)
}

// A later workflow gets its own ID too, whether or not an earlier workflow's
// scheduler has finished closing.
func TestSchedulerWorkflowIDDiffersBetweenSuccessiveWorkflows(t *testing.T) {
	first, _ := runSetupAndReportSchedulerID(t, "workflow-first")
	second, _ := runSetupAndReportSchedulerID(t, "workflow-second")
	require.NotEqual(t, first, second)
}

func TestResourceSchedulerWorkflowIDIsStableAndBounded(t *testing.T) {
	a := resourceSchedulerWorkflowID("wf", "run1")
	require.Equal(t, a, resourceSchedulerWorkflowID("wf", "run1"))
	require.NotEqual(t, a, resourceSchedulerWorkflowID("wf", "run2"))
	long := resourceSchedulerWorkflowID(strings.Repeat("x", 500), "run1")
	require.Less(t, len(long), 100)
}
