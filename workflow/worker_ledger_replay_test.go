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
