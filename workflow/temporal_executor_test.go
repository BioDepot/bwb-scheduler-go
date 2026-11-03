package workflow

import (
	"go-scheduler/fs"
	"go-scheduler/parsing"
	"testing"
	"time"
    "fmt"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// Temporal executor should request resource grant from
// scheduling workflow and should schedule the activity to
// run only after receiving a corresponding resource grant
// from the child workflow. Furthermore, it should schedule
// the RunCmd activity to the queue specified in the resource
// grant.
func TestTemporalExecutorGrantRequest(t *testing.T) {
    testSuite := &testsuite.WorkflowTestSuite{}
    env := testSuite.NewTestWorkflowEnvironment()
    env.RegisterActivity(RunCmd)

    assignedQueue := "ASSIGNED_QUEUE"
    readyToRunJob := false
    cmdToRun := parsing.CmdRunParams{Cmd: parsing.CmdTemplate{Id: 1805, ResourceReqs: parsing.ResourceVector{MemMb: 500}}}
    env.RegisterWorkflowWithOptions(func(ctx workflow.Context, _ SchedWorkflowState) error {
        selector := workflow.NewSelector(ctx)
        selector.AddReceive(
            workflow.GetSignalChannel(ctx, "new-request"), 
            func(c workflow.ReceiveChannel, _ bool) {
                var req ResourceRequest
                if ok := c.Receive(ctx, &req); !ok {
                    t.Fatalf("could not convert signal contents to resource req")
                }
                // Ensure request was passed faithfully by executor.
                require.Equal(t, req.Id, cmdToRun.Cmd.Id)
                require.Equal(t, req.Requirements, cmdToRun.Cmd.ResourceReqs)
                workflow.SignalExternalWorkflow(
                    ctx, req.CallerWorkflowId, "", "allocation-response", ResourceGrant{
                        RequestId: req.Id,
                        WorkerId: assignedQueue,
                    },
                )
                readyToRunJob = true
            },
        )
        
        // IMPORTANT: Force mocked child workflow to sleep before selecting
        // (and therefore before responding to signal), giving the executor
        // the opportunity to prematurely schedule the job, which would result
        // in the test failing.
        workflow.Sleep(ctx, 900 * time.Second)
        selector.Select(ctx)
        return nil
    }, workflow.RegisterOptions{
        Name: "ResourceSchedulerWorkflow",
    })

    // Handle an activity invoked during setup.
    env.OnActivity(fs.SetupRootDir, mock.Anything).Return("", nil).Maybe()
    env.OnActivity(RunCmdActivity, mock.Anything, cmdToRun, mock.Anything, mock.Anything).
        Return(func(_ map[string]string, _ parsing.CmdTemplate) (CmdOutput, error) {
            //require.Equal(t, queueName, assignedQueue)
            require.True(t, readyToRunJob)
            return CmdOutput{}, nil
        }).Once()

    env.ExecuteWorkflow(func(ctx workflow.Context) error {
pinLegacyScheduler(ctx)
        selector := workflow.NewSelector(ctx)
        temporalExec := NewTemporalExecutor(
            ctx, &selector, nil, fs.LocalFS{}, map[string]WorkerInfo{
                assignedQueue: { QueueId: assignedQueue },
            }, "", map[int]parsing.LocalJobConfig{
                cmdToRun.Cmd.Id: { UseDocker: false },
            })
        if err := temporalExec.Setup(false); err != nil {
            t.Fatalf("setup failed: %s", err)
        }
        temporalExec.RunCmds([]parsing.CmdRunParams{parsing.CmdRunParams{Cmd: cmdToRun.Cmd}})
        for i := 0; i < 10; i++ {
            selector.Select(ctx)
        }
        return nil
    })
    env.AssertExpectations(t)
}

// Temporal executor should request resource grant from scheduling
// workflow and should schedule the RunCmd activity only after both
// the resource grant is received AND any obligatory input files have
// been transferred from their source (here, Slurm) filesystem into
// the local filesystem.
func TestTemporalExecutorFileDownloads(t *testing.T) {
    testSuite := &testsuite.WorkflowTestSuite{}
    env := testSuite.NewTestWorkflowEnvironment()
    env.RegisterActivity(RunCmd)
    env.RegisterActivity(fs.TransferSshToLocalFS)

    assignedQueue := "ASSIGNED_QUEUE"
    storageId := "storageId"
    readyToRunJob := false

    localFS := fs.LocalFS{
        RootDir: "/localRoot",
    }
    sshFS := fs.SshFS{
        RootDir: "remote",
    }
    execToFs := map[parsing.ExecType]fs.AbstractFileSystem{
        parsing.EXEC_TEMPORAL: localFS,
        parsing.EXEC_SLURM:    sshFS,
    }

    xfers := []parsing.ObligatoryXfer{
        parsing.ObligatoryXfer{
            SrcExecutor: parsing.EXEC_SLURM,
            DstExecutor: parsing.EXEC_TEMPORAL,
            SrcHostPath: "/src/host/path",
            DstHostPath: "/dst/host/path",
        },
    }
    cmdToRun := parsing.CmdRunParams{
        Cmd: parsing.CmdTemplate{
            Id:           1805,
            ResourceReqs: parsing.ResourceVector{MemMb: 500},
        },
        Xfers: xfers,
    }

    finishedTransfer := false
    env.OnActivity(
        fs.TransferSshToLocalFS, storageId, sshFS, localFS, xfers,
    ).Return(func(string, fs.SshFS, fs.LocalFS, []parsing.ObligatoryXfer) error {
        finishedTransfer = true
        return nil
    }).Once()

    env.RegisterWorkflowWithOptions(func(ctx workflow.Context, _ SchedWorkflowState) error {
        selector := workflow.NewSelector(ctx)
        selector.AddReceive(
            workflow.GetSignalChannel(ctx, "new-request"),
            func(c workflow.ReceiveChannel, _ bool) {
                var req ResourceRequest
                if ok := c.Receive(ctx, &req); !ok {
                    t.Fatalf("could not convert signal contents to resource req")
                }
                // Ensure request was passed faithfully by executor.
                require.Equal(t, req.Id, cmdToRun.Cmd.Id)
                require.Equal(t, req.Requirements, cmdToRun.Cmd.ResourceReqs)
                workflow.SignalExternalWorkflow(
                    ctx, req.CallerWorkflowId, "", "allocation-response", ResourceGrant{
                        RequestId: req.Id,
                        WorkerId:  assignedQueue,
                    },
                )
                readyToRunJob = true
            },
        )

        // Force mocked child workflow to sleep before selecting (and
        // therefore before responding to signal), giving the executor
        // the opportunity to prematurely schedule the job, which would
        // result in the test failing.
        workflow.Sleep(ctx, 900*time.Second)
        selector.Select(ctx)
        return nil
    }, workflow.RegisterOptions{
        Name: "ResourceSchedulerWorkflow",
    })

    // Handle an activity invoked during setup.
    env.OnActivity(fs.SetupRootDir, mock.Anything).Return("", nil).Maybe()
    env.OnActivity(RunCmdActivity, mock.Anything, cmdToRun, mock.Anything, mock.Anything).
        Return(func(_ map[string]string, _ parsing.CmdTemplate) (CmdOutput, error) {
            require.True(t, readyToRunJob)
            require.True(t, finishedTransfer)
            return CmdOutput{}, nil
        }).Once()

    env.ExecuteWorkflow(func(ctx workflow.Context) error {
pinLegacyScheduler(ctx)
        selector := workflow.NewSelector(ctx)
        temporalExec := NewTemporalExecutor(
            ctx, &selector, nil, localFS, map[string]WorkerInfo{
                assignedQueue: {QueueId: assignedQueue},
            }, storageId, map[int]parsing.LocalJobConfig{
                cmdToRun.Cmd.Id: {UseDocker: false},
            })
        if err := temporalExec.Setup(false); err != nil {
            t.Fatalf("setup failed: %s", err)
        }
        temporalExec.SetFileXferHandler(
            func(
                ctx workflow.Context, s string, ox []parsing.ObligatoryXfer,
            ) ([]workflow.Future, error) {
                return DefaultFileXferHanlder(ctx, s, ox, execToFs)
            },
        )
        temporalExec.RunCmds([]parsing.CmdRunParams{cmdToRun})
        for i := 0; i < 10; i++ {
            selector.Select(ctx)
        }
        return nil
    })
    env.AssertExpectations(t)
    require.True(t, finishedTransfer)
}

// NOTE: I am unable to test grant releases because 
// of an unresolved bug in the temporal test env 
// (https://github.com/temporalio/sdk-go/issues/1961).
// Define signal names for clarity
const SignalA = "signalA"
const SignalB = "signalB"
const SignalC = "signalC"
func TestParentChildSignalExchange(t *testing.T) {
    var ts testsuite.WorkflowTestSuite
    env := ts.NewTestWorkflowEnvironment()
    var childReceivedC bool

    mockChildWorkflow := func(ctx workflow.Context, parentWfId string) error {
        s := workflow.NewSelector(ctx)
        s.AddReceive(workflow.GetSignalChannel(ctx, SignalA), func(c workflow.ReceiveChannel, _ bool) {
            var val string
            c.Receive(ctx, &val)
            fmt.Println("Child received signal A")
            _ = workflow.SignalExternalWorkflow(ctx, parentWfId, "", SignalB, "payload-B").Get(ctx, nil)
            fmt.Println("Child sent signal B")
        })

        s.AddReceive(workflow.GetSignalChannel(ctx, SignalC), func(c workflow.ReceiveChannel, _ bool) {
            var val string
            c.Receive(ctx, &val)
            fmt.Println("Child received signal C")
            childReceivedC = true
        })

        // Selects for signal A and signal C
        s.Select(ctx)
        s.Select(ctx)
        return nil
    }
    parentWorkflow := func(ctx workflow.Context) error {
        cwo := workflow.ChildWorkflowOptions{
            WorkflowID: "child-workflow-id",
        }
        var childWE workflow.Execution
        workflowId := workflow.GetInfo(ctx).WorkflowExecution.ID
        ctx = workflow.WithChildOptions(ctx, cwo)
        childWorkflowFuture := workflow.ExecuteChildWorkflow(ctx, "MockChildWorkflow", workflowId)
        err := childWorkflowFuture.GetChildWorkflowExecution().Get(ctx, &childWE)
        if err != nil {
            return err
        }

        workflow.SignalExternalWorkflow(ctx, childWE.ID, "", SignalA, "payload-A")
        fmt.Println("Parent sent signal A")
        var bVal string
        s := workflow.NewSelector(ctx)
        s.AddReceive(workflow.GetSignalChannel(ctx, SignalB), func(c workflow.ReceiveChannel, _ bool) {
            c.Receive(ctx, &bVal)
            fmt.Println("Parent received signal B")
            _ = workflow.SignalExternalWorkflow(ctx, childWE.ID, "", SignalC, "payload-C").Get(ctx, nil)
            fmt.Println("Parent sent signal C")
        })
        s.Select(ctx)

        var childResult interface{}
        err = childWorkflowFuture.Get(ctx, &childResult)
        if err != nil {
            return err
        }
        return nil
    }
    env.RegisterWorkflow(parentWorkflow)
    env.RegisterWorkflowWithOptions(mockChildWorkflow, workflow.RegisterOptions{Name: "MockChildWorkflow"})

    env.ExecuteWorkflow(parentWorkflow)
    require.True(t, env.IsWorkflowCompleted())
    require.True(t, childReceivedC, "child workflow should have received signal C")
}
