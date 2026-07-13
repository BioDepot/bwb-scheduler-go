package parsing

import (
	"sort"
	"testing"
)

func TestGetImageNamesDeduplicatesSharedImages(t *testing.T) {
	wf := ResolvedWorkflow{
		Nodes: map[int]ResolvedNode{
			10: {Id: 10, ImageName: "biodepot/cellbender", ImageTag: "0.3.2"},
			20: {Id: 20, ImageName: "biodepot/scrna-matrices", ImageTag: "latest"},
			30: {Id: 30, ImageName: "biodepot/cellbender", ImageTag: "0.3.2"},
		},
	}
	index, err := ParseAndValidateWorkflow(&wf)
	if err != nil {
		t.Fatalf("failed to validate workflow: %v", err)
	}
	cmdMan := NewCmdManager(&wf, index, JobConfig{}, nil)

	got := cmdMan.GetImageNames()
	sort.Strings(got)
	want := []string{
		"biodepot/cellbender:0.3.2",
		"biodepot/scrna-matrices:latest",
	}
	if len(got) != len(want) {
		t.Fatalf("image count = %d, want %d: %v", len(got), len(want), got)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("image %d = %q, want %q", index, got[index], want[index])
		}
	}
}
