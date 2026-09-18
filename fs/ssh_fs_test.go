package fs

import "testing"

func TestRsyncRemoteSpecDoesNotAddLiteralShellQuotes(t *testing.T) {
	sshFS := SshFS{User: "alice", Endpt: "cluster.example"}
	got := sshFS.rsyncRemoteSpec("/shared/images/example/tool:latest")
	want := "alice@cluster.example:/shared/images/example/tool:latest"
	if got != want {
		t.Fatalf("remote spec = %q, want %q", got, want)
	}
}
