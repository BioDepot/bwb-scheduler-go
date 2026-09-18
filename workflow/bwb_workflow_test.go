package workflow

import "testing"

func TestTerminalNodeStatusesMarksUnfinishedNodesCanceled(t *testing.T) {
	live := map[int]string{
		10: "FINISHED",
		20: "RUNNING",
		30: "AWAITING_PREDECESSORS",
	}
	terminal := terminalNodeStatuses(live, true)

	if terminal[10] != "FINISHED" || terminal[20] != "CANCELED" || terminal[30] != "CANCELED" {
		t.Fatalf("unexpected terminal statuses: %#v", terminal)
	}
	if live[20] != "RUNNING" {
		t.Fatalf("input status map was mutated: %#v", live)
	}
}

func TestTerminalNodeStatusesPreservesNormalCompletionSnapshot(t *testing.T) {
	live := map[int]string{10: "FINISHED", 20: "RUNNING"}
	terminal := terminalNodeStatuses(live, false)
	if terminal[10] != "FINISHED" || terminal[20] != "RUNNING" {
		t.Fatalf("unexpected terminal statuses: %#v", terminal)
	}
}
