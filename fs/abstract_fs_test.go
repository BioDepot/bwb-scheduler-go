package fs

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go-scheduler/parsing"
)

// recordingFS stands in for a remote file system.
type recordingFS struct {
	downloads [][2]string
	uploads   [][2]string
	err       error
}

func (r *recordingFS) GetRootDir() string                            { return "" }
func (r *recordingFS) Glob(_, _ string, _, _ bool) ([]string, error) { return nil, nil }
func (r *recordingFS) Download(src, dst string) error {
	r.downloads = append(r.downloads, [2]string{src, dst})
	return r.err
}
func (r *recordingFS) Upload(src, dst string) error {
	r.uploads = append(r.uploads, [2]string{src, dst})
	return r.err
}

func TestTransferBetweenLocalFileSystemsCopiesFilesAndDirectories(t *testing.T) {
	tmp := t.TempDir()
	srcRoot := filepath.Join(tmp, "worker-a")
	dstRoot := filepath.Join(tmp, "worker-b")
	writeTestFile(t, filepath.Join(srcRoot, "prepare", "unfiltered_counts.h5ad"), "counts", 0644)
	writeTestFile(t, filepath.Join(srcRoot, "star", "Solo.out", "matrix.mtx"), "matrix", 0644)

	xfers := []parsing.ObligatoryXfer{
		{
			SrcHostPath: filepath.Join(srcRoot, "prepare", "unfiltered_counts.h5ad"),
			DstHostPath: filepath.Join(dstRoot, "prepare", "unfiltered_counts.h5ad"),
		},
		{
			SrcHostPath: filepath.Join(srcRoot, "star"),
			DstHostPath: filepath.Join(dstRoot, "star"),
		},
		// The directory that holds a file transferred above.
		{
			SrcHostPath: filepath.Join(srcRoot, "prepare"),
			DstHostPath: filepath.Join(dstRoot, "prepare"),
		},
	}

	// A transfer with a local side makes no intermediate copy, so it must
	// leave the scheduler directory alone.
	schedDir := t.TempDir()
	t.Setenv("BWB_SCHED_DIR", schedDir)
	if err := TransferLocalToLocalFS("storage", LocalFS{RootDir: srcRoot}, LocalFS{RootDir: dstRoot}, xfers); err != nil {
		t.Fatalf("transfer: %v", err)
	}

	if got := readTestFile(t, filepath.Join(dstRoot, "prepare", "unfiltered_counts.h5ad")); got != "counts" {
		t.Fatalf("file = %q", got)
	}
	if got := readTestFile(t, filepath.Join(dstRoot, "star", "Solo.out", "matrix.mtx")); got != "matrix" {
		t.Fatalf("directory entry = %q", got)
	}
	if _, err := os.Stat(filepath.Join(dstRoot, "prepare", "prepare")); !os.IsNotExist(err) {
		t.Fatalf("directory was nested one level too deep")
	}
	if entries, err := os.ReadDir(schedDir); err != nil || len(entries) != 0 {
		t.Fatalf("intermediate copy made under the scheduler directory: %v %v", entries, err)
	}
}

func TestTransferToLocalDownloadsStraightToTheDestination(t *testing.T) {
	remote := &recordingFS{}
	xfers := []parsing.ObligatoryXfer{{SrcHostPath: "/remote/root/prepare", DstHostPath: "/local/root/prepare"}}

	if err := Transfer("storage", xfers, remote, LocalFS{RootDir: "/local/root"}); err != nil {
		t.Fatalf("transfer: %v", err)
	}
	want := [][2]string{{"/remote/root/prepare", "/local/root/prepare"}}
	if len(remote.downloads) != 1 || remote.downloads[0] != want[0] {
		t.Fatalf("downloads = %v, want %v", remote.downloads, want)
	}
	if len(remote.uploads) != 0 {
		t.Fatalf("unexpected uploads: %v", remote.uploads)
	}
}

func TestTransferFromLocalUploadsStraightFromTheSource(t *testing.T) {
	remote := &recordingFS{}
	xfers := []parsing.ObligatoryXfer{{SrcHostPath: "/local/root/prepare", DstHostPath: "/remote/root/prepare"}}

	if err := Transfer("storage", xfers, LocalFS{RootDir: "/local/root"}, remote); err != nil {
		t.Fatalf("transfer: %v", err)
	}
	want := [][2]string{{"/local/root/prepare", "/remote/root/prepare"}}
	if len(remote.uploads) != 1 || remote.uploads[0] != want[0] {
		t.Fatalf("uploads = %v, want %v", remote.uploads, want)
	}
	if len(remote.downloads) != 0 {
		t.Fatalf("unexpected downloads: %v", remote.downloads)
	}
}

func TestTransferReportsTheFailingPath(t *testing.T) {
	remote := &recordingFS{err: errors.New("connection refused")}
	xfers := []parsing.ObligatoryXfer{{SrcHostPath: "/remote/a", DstHostPath: "/local/a"}}

	err := Transfer("storage", xfers, remote, LocalFS{})
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"/remote/a", "/local/a", "connection refused"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not mention %q", err, want)
		}
	}
}

func TestTransferBetweenRemotesRefreshesTheIntermediateCopy(t *testing.T) {
	schedDir := t.TempDir()
	t.Setenv("BWB_SCHED_DIR", schedDir)
	src, dst := &recordingFS{}, &recordingFS{}
	xfers := []parsing.ObligatoryXfer{{SrcHostPath: "/remote-a/out", DstHostPath: "/remote-b/out"}}
	intermediate := filepath.Join(schedDir, "storage", "transfer", "/remote-b/out")
	// A copy left by an earlier transfer must not stop the download.
	writeTestFile(t, intermediate, "stale", 0644)

	if err := Transfer("storage", xfers, src, dst); err != nil {
		t.Fatalf("transfer: %v", err)
	}
	if len(src.downloads) != 1 || src.downloads[0] != [2]string{"/remote-a/out", intermediate} {
		t.Fatalf("downloads = %v", src.downloads)
	}
	if len(dst.uploads) != 1 || dst.uploads[0] != [2]string{intermediate, "/remote-b/out"} {
		t.Fatalf("uploads = %v", dst.uploads)
	}
}
