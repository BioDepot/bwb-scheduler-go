package workflow

import (
	"context"
	"go-scheduler/parsing"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

func TestHeartbeatRetryWaitV1Schedule(t *testing.T) {
	want := []int{30, 60, 120, 240, 300, 300, 300}
	for i, w := range want {
		require.Equal(t, w, heartbeatRetryWaitV1(i+1), "after failure %d", i+1)
	}
}

func newBackoffState(v1 bool) *SchedWorkflowState {
	return &SchedWorkflowState{
		Workers:                    map[string]WorkerInfo{"q": {QueueId: "q", IsAlive: true}},
		NumRetries:                 map[string]int{},
		SecondsSinceHeartbeatSched: map[string]int{},
		SecondsUntilHeartbeatRetry: map[string]int{},
		BackoffV1:                  v1,
	}
}

// Consecutive failures wait 30, 60, 120, 240, 300, 300 s; a success resets.
func TestMarkWorkerDeadWaitsAndResets(t *testing.T) {
	st := newBackoffState(true)
	var waits []int
	for i := 0; i < 6; i++ {
		markWorkerDead(st, "q")
		require.False(t, st.Workers["q"].IsAlive)
		waits = append(waits, st.SecondsUntilHeartbeatRetry["q"])
	}
	require.Equal(t, []int{30, 60, 120, 240, 300, 300}, waits)

	// the retried heartbeat is picked up and outlives the timeout
	st.SecondsSinceHeartbeatSched["q"] = 0
	resurrectWorkers(st, 5*7)
	require.True(t, st.Workers["q"].IsAlive)
	require.NotContains(t, st.NumRetries, "q")

	markWorkerDead(st, "q")
	require.Equal(t, 30, st.SecondsUntilHeartbeatRetry["q"], "back to the first wait after a success")
}

// A failed retry (no poller) is not a success, so the wait keeps growing.
func TestFailedRetryDoesNotReset(t *testing.T) {
	st := newBackoffState(true)
	markWorkerDead(st, "q")
	markWorkerDead(st, "q")
	st.SecondsSinceHeartbeatSched["q"] = 0
	resurrectWorkers(st, 5) // not yet past the timeout
	require.Equal(t, 2, st.NumRetries["q"])
}

// The earlier schedule is kept for workflows that began before the change.
func TestLegacyBackoffUnchanged(t *testing.T) {
	st := newBackoffState(false)
	var waits []int
	for i := 0; i < 6; i++ {
		markWorkerDead(st, "q")
		waits = append(waits, st.SecondsUntilHeartbeatRetry["q"])
	}
	require.Equal(t, []int{30, 120, 480, 1920, 7680, 9000}, waits)
	st.SecondsSinceHeartbeatSched["q"] = 0
	resurrectWorkers(st, 40)
	require.Equal(t, 6, st.NumRetries["q"], "legacy never resets")
}

// A command worker that signals the ledger when it starts is usable at once;
// without the signal the ledger waits for its retry timer and a further 30 s.
func TestLedgerWorkerUpSignalSkipsRetryWait(t *testing.T) {
	for _, signalUp := range []bool{true, false} {
		env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
		env.RegisterActivity(&LedgerActivities{})
		calls := 0
		env.OnActivity(WorkerHeartbeatActivity, mock.Anything, mock.Anything).
			Return(func(_ context.Context, _ string) error {
				calls++
				if calls == 1 {
					return temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START, nil)
				}
				return nil
			}).Maybe()
		env.OnActivity("CheckHoldersAlive", mock.Anything, mock.Anything).Return([]LedgerHolder{}, nil).Maybe()
		env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, ledgerRegisteredSignal, mock.Anything).Return(nil).Maybe()
		var grantedAt time.Duration = -1
		start := env.Now()
		env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, "allocation-response", mock.Anything).
			Return(func(ns, wf, run, sig string, arg interface{}) error {
				if grantedAt < 0 {
					grantedAt = env.Now().Sub(start)
				}
				return nil
			}).Maybe()
		capacity := parsing.ResourceVector{MemMb: 1000, Cpus: 1}
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(ledgerRegisterSignal, LedgerRegistration{Queue: "q", Capacity: capacity, Holder: hold("A")})
			env.SignalWorkflow(ledgerRequestSignal, req("A", 1, 1, 0, 0))
		}, time.Second)
		if signalUp {
			env.RegisterDelayedCallback(func() { env.SignalWorkflow(ledgerWorkerUpSignal, "q") }, 3*time.Second)
		}
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(ledgerDeregisterSignal, LedgerDeregistration{Holder: hold("A")})
		}, 200*time.Second)
		env.ExecuteWorkflow(WorkerLedgerWorkflow, LedgerState{Queue: "q", IdleSeconds: 30})
		require.True(t, env.IsWorkflowCompleted())
		require.NoError(t, env.GetWorkflowError())
		require.GreaterOrEqual(t, int64(grantedAt), int64(0), "granted")
		if signalUp {
			require.Less(t, grantedAt, 15*time.Second)
			require.Equal(t, 2, calls, "one failed heartbeat and one started by the signal")
		} else {
			require.Greater(t, grantedAt, 30*time.Second)
		}
	}
}
