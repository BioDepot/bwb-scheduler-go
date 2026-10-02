package fs

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

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

// rsyncEntryIsDir is what Download uses to tell a remote directory from a
// remote file. It runs here against local paths: rsync lists a local entry the
// same way as a remote one.
func TestRsyncEntryIsDir(t *testing.T) {
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("rsync is not installed")
	}
	root := t.TempDir()
	dir := filepath.Join(root, "prepare")
	file := filepath.Join(dir, "counts.h5ad")
	if err := os.MkdirAll(filepath.Join(dir, "sub"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte("counts"), 0644); err != nil {
		t.Fatal(err)
	}
	// A name that starts with "d" must not be taken for a directory.
	dFile := filepath.Join(root, "denoised.h5")
	if err := os.WriteFile(dFile, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	linkToDir := filepath.Join(root, "link_to_dir")
	if err := os.Symlink(dir, linkToDir); err != nil {
		t.Fatal(err)
	}
	linkToFile := filepath.Join(root, "link_to_file")
	if err := os.Symlink(file, linkToFile); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		path string
		want bool
	}{
		{"directory", dir, true},
		{"file", file, false},
		{"file whose name starts with d", dFile, false},
		{"link to a directory", linkToDir, true},
		{"link to a file", linkToFile, false},
		{"missing path", filepath.Join(root, "missing"), false},
	} {
		got, err := rsyncEntryIsDir(nil, tc.path)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got != tc.want {
			t.Fatalf("%s: is a directory = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// A transfer endpoint that accepts rsync but refuses other commands answers
// `test -d` with exit status 1, exactly as `test -d` answers for a file. The
// directory test must therefore not go through a remote shell command: it
// must fail loudly when the transport fails instead of answering "file".
func TestRsyncEntryIsDirReportsATransportFailure(t *testing.T) {
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("rsync is not installed")
	}
	// A remote shell that refuses every command, as a restricted node does
	// for a command that is not on its list.
	refuse := filepath.Join(t.TempDir(), "refuse")
	if err := os.WriteFile(refuse, []byte("#!/bin/sh\necho 'Login denied' >&2\nexit 1\n"), 0755); err != nil {
		t.Fatal(err)
	}
	isDir, err := rsyncEntryIsDir([]string{"-e", refuse}, "user@host:/shared/prepare")
	if err == nil {
		t.Fatalf("a refused connection was read as is a directory = %v", isDir)
	}
}

func TestRemoteIsDirUsesRsyncOverTheSameSSHTransport(t *testing.T) {
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("rsync is not installed")
	}
	// Stand-in for ssh: records its arguments and refuses, so that no
	// connection is attempted.
	tmp := t.TempDir()
	record := filepath.Join(tmp, "argv")
	fakeSSH := filepath.Join(tmp, "ssh")
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > " + shellQuote(record) + "\nexit 255\n"
	if err := os.WriteFile(fakeSSH, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", tmp+string(os.PathListSeparator)+os.Getenv("PATH"))

	sshFS := SshFS{User: "alice", Endpt: "data.example", Port: 22, KnownHostsFile: "/home/alice/.ssh/known_hosts"}
	if _, err := sshFS.remoteIsDir("/shared/run/prepare"); err == nil {
		t.Fatal("expected the refused connection to be reported")
	}
	argv, err := os.ReadFile(record)
	if err != nil {
		t.Fatalf("rsync did not start ssh: %v", err)
	}
	got := string(argv)
	for _, want := range []string{"StrictHostKeyChecking=yes", "UserKnownHostsFile=/home/alice/.ssh/known_hosts", "data.example", "rsync"} {
		if !strings.Contains(got, want) {
			t.Fatalf("ssh arguments %q do not contain %q", got, want)
		}
	}
	if strings.Contains(got, "test -d") {
		t.Fatalf("the directory test still runs a remote shell command: %q", got)
	}
}
