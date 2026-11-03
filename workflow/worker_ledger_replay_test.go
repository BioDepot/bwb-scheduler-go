package workflow

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// Histories recorded by the earlier scheduler builds must still replay under
// the current code. Each is the parent workflow (RunBwbWorkflowV1, one command
// on the Temporal executor) of a real run on a local Temporal server:
//
//	history_fb76cb5_parent.json: child scheduler started as "sched-workflow"
//	history_04b91ca_parent.json: child scheduler with an ID derived from the
//	                             parent's workflow and run ID
//
// Both predate the shared ledger, so GetVersion must give them the old path.
// The workflow ID is the original one because 04b91ca derives the child's ID
// from it.
func TestReplayHistoriesFromEarlierBuilds(t *testing.T) {
	for file, originalID := range map[string]string{
		"testdata/history_fb76cb5_parent.json": "old-s1",
		"testdata/history_04b91ca_parent.json": "new-s1",
	} {
		t.Run(file, func(t *testing.T) {
			f, err := os.Open(file)
			require.NoError(t, err)
			defer f.Close()
			history, err := client.HistoryFromJSON(f, client.HistoryJSONOptions{})
			require.NoError(t, err)
			replayer := worker.NewWorkflowReplayer()
			replayer.RegisterWorkflow(RunBwbWorkflowV1)
			replayer.RegisterWorkflow(ResourceSchedulerWorkflow)
			require.NoError(t, replayer.ReplayWorkflowHistoryWithOptions(nil, history,
				worker.ReplayWorkflowHistoryOptions{OriginalExecution: workflow.Execution{ID: originalID}}))
		})
	}
}

// Histories made by this build (parent in ledger mode, and the ledger itself)
// are kept so that later changes to either keep replaying.
func TestReplayLedgerHistories(t *testing.T) {
	for file, c := range map[string]struct {
		id string
		wf interface{}
	}{
		"testdata/history_ledger_parent.json":   {"a-1", RunBwbWorkflowV1},
		"testdata/history_ledger_workflow.json": {"worker-ledger-q-shared-c94110165377a59b6500", WorkerLedgerWorkflow},
	} {
		t.Run(file, func(t *testing.T) {
			f, err := os.Open(file)
			require.NoError(t, err)
			defer f.Close()
			history, err := client.HistoryFromJSON(f, client.HistoryJSONOptions{})
			require.NoError(t, err)
			replayer := worker.NewWorkflowReplayer()
			replayer.RegisterWorkflow(c.wf)
			require.NoError(t, replayer.ReplayWorkflowHistoryWithOptions(nil, history,
				worker.ReplayWorkflowHistoryOptions{OriginalExecution: workflow.Execution{ID: c.id}}))
		})
	}
}

// Histories of workflows stopped while their command ran: one made by 5bfd0b8
// (which does not wait for commands), one by the build
// that waits for commands to confirm cancellation (version marker
// "stop-kills-commands" = 1). Sanitized: no host, user or scratch paths.
func TestReplayStoppedWorkflowHistory(t *testing.T) {
	for file, id := range map[string]string{
		"testdata/history_stop_cancel_parent.json":     "wf-Ac1b",
		"testdata/history_5bfd0b8_stopped_parent.json": "wf-Ac5",
	} {
		t.Run(file, func(t *testing.T) { replayStopped(t, file, id) })
	}
}

func replayStopped(t *testing.T, file, id string) {
	f, err := os.Open(file)
	require.NoError(t, err)
	defer f.Close()
	history, err := client.HistoryFromJSON(f, client.HistoryJSONOptions{})
	require.NoError(t, err)
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflow(RunBwbWorkflowV1)
	require.NoError(t, replayer.ReplayWorkflowHistoryWithOptions(nil, history,
		worker.ReplayWorkflowHistoryOptions{OriginalExecution: workflow.Execution{ID: id}}))
}

// A ledger history made by d26cc29, before the heartbeat back-off changed: a
// worker killed and restarted several times, with heartbeat failures and
// retries at 30, 120 and 480 s. Sanitized. The current code must replay it
// with the earlier schedule (GetVersion "heartbeat-backoff-reset").
func TestReplayLedgerHistoryFromBeforeBackoffChange(t *testing.T) {
	f, err := os.Open("testdata/history_d26cc29_ledger_backoff.json")
	require.NoError(t, err)
	defer f.Close()
	history, err := client.HistoryFromJSON(f, client.HistoryJSONOptions{})
	require.NoError(t, err)
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflow(WorkerLedgerWorkflow)
	require.NoError(t, replayer.ReplayWorkflowHistoryWithOptions(nil, history,
		worker.ReplayWorkflowHistoryOptions{OriginalExecution: workflow.Execution{ID: "worker-ledger-q-b-86238ce42aaf0807dfcd"}}))
}
