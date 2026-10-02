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

func TestRsyncOperandsForAFileAreUnchanged(t *testing.T) {
	src, dst := rsyncOperands("/a/prepare/counts.h5ad", "/b/prepare/counts.h5ad", false)
	if src != "/a/prepare/counts.h5ad" || dst != "/b/prepare/counts.h5ad" {
		t.Fatalf("operands = %q, %q", src, dst)
	}
}

func TestRsyncOperandsForADirectoryCopyItsContents(t *testing.T) {
	// Without the trailing slashes rsync would create /b/prepare/prepare.
	for _, src := range []string{"/a/prepare", "/a/prepare/"} {
		gotSrc, gotDst := rsyncOperands(src, "/b/prepare", true)
		if gotSrc != "/a/prepare/" || gotDst != "/b/prepare/" {
			t.Fatalf("operands for %q = %q, %q", src, gotSrc, gotDst)
		}
	}
}
