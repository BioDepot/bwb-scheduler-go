package workflow

import (
	"context"
	"go-scheduler/fs"
	"go-scheduler/parsing"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func newTestLedger(cpus, gpus int) *LedgerState {
	st := &LedgerState{Queue: "q"}
	st.init()
	w := st.Sched.Workers["q"]
	w.IsAlive = true
	st.Sched.Workers["q"] = w
	_ = cpus
	_ = gpus
	return st
}

func hold(id string) LedgerHolder { return LedgerHolder{WorkflowId: id, RunId: "run-" + id} }

func register(t *testing.T, st *LedgerState, id string, cpus, gpus int) LedgerAck {
	return st.register(LedgerRegistration{
		Queue: "q", Capacity: parsing.ResourceVector{MemMb: 1000, Cpus: cpus, Gpus: gpus}, Holder: hold(id),
	})
}

func req(id string, n, cpus, gpus, rank int) ResourceRequest {
	return ResourceRequest{
		Id: n, Rank: rank, CallerWorkflowId: id, CallerRunId: "run-" + id,
		Requirements: parsing.ResourceVector{MemMb: 10, Cpus: cpus, Gpus: gpus},
	}
}

// The same command ID in two workflows must not collide, and two workflows
// asking for the whole worker are served one after the other.
func TestLedgerSerializesWorkflowsOnOneQueue(t *testing.T) {
	st := newTestLedger(1, 0)
	require.Empty(t, register(t, st, "A", 1, 0).Error)
	require.Empty(t, register(t, st, "B", 1, 0).Error)
	st.request(req("A", 1, 1, 0, 0))
	st.request(req("B", 1, 1, 0, 0))
	grants := st.process()
	require.Len(t, grants, 1)
	require.Equal(t, "A", grants[0].RecipientWorkflowId, "first come, first served")
	require.Empty(t, st.process(), "no capacity left for B")
	st.free(activeKey(hold("A"), 1))
	grants = st.process()
	require.Len(t, grants, 1)
	require.Equal(t, "B", grants[0].RecipientWorkflowId)
}

func TestLedgerGpuCountIsShared(t *testing.T) {
	st := newTestLedger(4, 1)
	register(t, st, "A", 4, 1)
	register(t, st, "B", 4, 1)
	st.request(req("A", 1, 1, 1, 0))
	st.request(req("B", 1, 1, 1, 0))
	require.Len(t, st.process(), 1)
	require.Equal(t, 1, st.Sched.Workers["q"].UsedResources.Gpus)
}

func TestLedgerSmallerRequestsFitAroundBlockedOne(t *testing.T) {
	st := newTestLedger(2, 0)
	register(t, st, "A", 2, 0)
	register(t, st, "B", 2, 0)
	st.request(req("A", 1, 1, 0, 0))
	st.process()
	st.request(req("B", 1, 2, 0, 0)) // does not fit now
	st.request(req("A", 2, 1, 0, 0)) // fits
	grants := st.process()
	require.Len(t, grants, 1)
	require.Equal(t, 2, grants[0].RequestId)
}

func TestLedgerRankOrdersAcrossWorkflowsAndTiesAreFifo(t *testing.T) {
	st := newTestLedger(1, 0)
	register(t, st, "A", 1, 0)
	register(t, st, "B", 1, 0)
	register(t, st, "C", 1, 0)
	st.request(req("A", 1, 1, 0, 0))
	st.request(req("B", 1, 1, 0, 0))
	st.request(req("C", 1, 1, 0, 5))
	require.Equal(t, "C", st.process()[0].RecipientWorkflowId)
	st.free(activeKey(hold("C"), 1))
	require.Equal(t, "A", st.process()[0].RecipientWorkflowId)
}

func TestLedgerRefusesDifferentCapacityWhileShared(t *testing.T) {
	st := newTestLedger(1, 0)
	require.Empty(t, register(t, st, "A", 4, 0).Error)
	ack := register(t, st, "B", 8, 0)
	require.Contains(t, ack.Error, "must declare the same capacity")
	require.NotContains(t, st.Holders, hold("B").key())
	// Same capacity is accepted, and registering twice is harmless.
	require.Empty(t, register(t, st, "B", 4, 0).Error)
	require.Empty(t, register(t, st, "B", 4, 0).Error)
	require.Len(t, st.Holders, 2)
	// Once everyone has left a new capacity may be set.
	st.reclaim(hold("A"))
	st.reclaim(hold("B"))
	require.Empty(t, register(t, st, "C", 8, 0).Error)
	require.Equal(t, 8, st.Sched.Workers["q"].TotalResources.Cpus)
}

func TestLedgerReclaimFreesGrantsAndQueuedRequests(t *testing.T) {
	st := newTestLedger(1, 0)
	register(t, st, "A", 1, 0)
	register(t, st, "B", 1, 0)
	st.request(req("A", 1, 1, 0, 0))
	st.process()
	st.request(req("A", 2, 1, 0, 0))
	st.request(req("B", 1, 1, 0, 0))
	st.reclaim(hold("A"))
	require.Equal(t, 0, st.Sched.Workers["q"].UsedResources.Cpus)
	require.Len(t, st.Pending, 1)
	require.Equal(t, "B", st.process()[0].RecipientWorkflowId)
	// A request from a holder that is not registered is ignored.
	st.request(req("A", 3, 1, 0, 0))
	require.Len(t, st.Pending, 0)
}

func TestWorkerLedgerWorkflowID(t *testing.T) {
	a := workerLedgerWorkflowID("q1")
	require.Equal(t, a, workerLedgerWorkflowID("q1"))
	require.NotEqual(t, a, workerLedgerWorkflowID("q2"))
	require.NotEqual(t, workerLedgerWorkflowID("a b"), workerLedgerWorkflowID("a-b"))
	require.Less(t, len(workerLedgerWorkflowID(string(make([]byte, 500)))), 100)
}

// Runs the ledger workflow: A and B register and ask for the whole worker; B
// is only granted after A's holder is found closed by the liveness check.
func TestWorkerLedgerWorkflowReclaimsClosedHolder(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	env.RegisterActivity(&LedgerActivities{})
	env.OnActivity(WorkerHeartbeatActivity, mock.Anything, mock.Anything).Return(nil).Maybe()
	var grants []ResourceGrant
	var acks []LedgerAck
	env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, ledgerRegisteredSignal, mock.Anything).
		Return(func(ns, wf, run, sig string, arg interface{}) error {
			acks = append(acks, arg.(LedgerAck))
			return nil
		}).Maybe()
	env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, "allocation-response", mock.Anything).
		Return(func(ns, wf, run, sig string, arg interface{}) error {
			grants = append(grants, arg.(ResourceGrant))
			return nil
		}).Maybe()
	aliveChecks := 0
	env.OnActivity("CheckHoldersAlive", mock.Anything, mock.Anything).
		Return(func(_ context.Context, hs []LedgerHolder) ([]LedgerHolder, error) {
			aliveChecks++
			for _, h := range hs {
				if h.WorkflowId == "A" {
					return []LedgerHolder{h}, nil // A has closed
				}
			}
			return nil, nil
		}).Maybe()

	capacity := parsing.ResourceVector{MemMb: 1000, Cpus: 1}
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ledgerRegisterSignal, LedgerRegistration{Queue: "q", Capacity: capacity, Holder: hold("A")})
		env.SignalWorkflow(ledgerRegisterSignal, LedgerRegistration{Queue: "q", Capacity: capacity, Holder: hold("B")})
		env.SignalWorkflow(ledgerRegisterSignal, LedgerRegistration{Queue: "q", Capacity: parsing.ResourceVector{Cpus: 9}, Holder: hold("C")})
		env.SignalWorkflow(ledgerRequestSignal, req("A", 1, 1, 0, 0))
		env.SignalWorkflow(ledgerRequestSignal, req("B", 1, 1, 0, 0))
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		require.Len(t, grants, 1, "only A is granted while A is alive")
		require.Equal(t, "A", grants[0].RecipientWorkflowId)
	}, 20*time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ledgerDeregisterSignal, LedgerDeregistration{Holder: hold("B")})
	}, 120*time.Second)

	env.ExecuteWorkflow(WorkerLedgerWorkflow, LedgerState{Queue: "q", IdleSeconds: 30})
	require.True(t, env.IsWorkflowCompleted(), "ledger exits once idle")
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, grants, 2)
	require.Equal(t, "B", grants[1].RecipientWorkflowId, "B proceeds after A is reclaimed")
	require.Greater(t, aliveChecks, 0)
	require.Len(t, acks, 3)
	require.NotEmpty(t, acks[2].Error)
}

// A registrant gets the ledger's refusal as an error from Setup.
func TestTemporalExecutorSetupSurfacesLedgerRefusal(t *testing.T) {
	for _, refuse := range []bool{true, false} {
		env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
		env.RegisterActivity(&LedgerActivities{})
		env.OnActivity("EnsureWorkerLedger", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity(fs.SetupRootDir, mock.Anything).Return("", nil).Maybe()
		env.RegisterDelayedCallback(func() {
			ack := LedgerAck{Queue: "q"}
			if refuse {
				ack.Error = "capacity mismatch"
			}
			env.SignalWorkflow(ledgerRegisteredSignal, ack)
		}, time.Second)
		env.ExecuteWorkflow(func(ctx workflow.Context) error {
			selector := workflow.NewSelector(ctx)
			exec := NewTemporalExecutor(ctx, &selector, nil, fs.LocalFS{},
				map[string]WorkerInfo{"q": {QueueId: "q"}}, "storage", nil)
			return exec.Setup(false)
		})
		require.True(t, env.IsWorkflowCompleted())
		if refuse {
			require.ErrorContains(t, env.GetWorkflowError(), "capacity mismatch")
		} else {
			require.NoError(t, env.GetWorkflowError())
		}
	}
}
