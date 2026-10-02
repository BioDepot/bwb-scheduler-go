package fs

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTestFile(t *testing.T, path, content string, perm os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), perm); err != nil {
		t.Fatal(err)
	}
}

func readTestFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestLocalFSCopyFileKeepsContentAndMode(t *testing.T) {
	tmp := t.TempDir()
	src := filepath.Join(tmp, "src", "run.sh")
	dst := filepath.Join(tmp, "dst", "nested", "run.sh")
	writeTestFile(t, src, "#!/bin/sh\n", 0750)

	if err := (LocalFS{}).Copy(src, dst); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if got := readTestFile(t, dst); got != "#!/bin/sh\n" {
		t.Fatalf("content = %q", got)
	}
	info, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0750 {
		t.Fatalf("mode = %v, want 0750", info.Mode().Perm())
	}
}

func TestLocalFSCopyFileReplacesReadOnlyDestination(t *testing.T) {
	tmp := t.TempDir()
	src := filepath.Join(tmp, "src.txt")
	dst := filepath.Join(tmp, "dst.txt")
	writeTestFile(t, src, "new", 0444)
	writeTestFile(t, dst, "old", 0444)

	if err := (LocalFS{}).Copy(src, dst); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if got := readTestFile(t, dst); got != "new" {
		t.Fatalf("content = %q, want %q", got, "new")
	}
	leftovers, err := filepath.Glob(filepath.Join(tmp, ".dst.txt.xfer-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(leftovers) != 0 {
		t.Fatalf("temporary files left behind: %v", leftovers)
	}
}

func TestLocalFSCopyDirectoryTree(t *testing.T) {
	tmp := t.TempDir()
	src := filepath.Join(tmp, "src", "prepare")
	dst := filepath.Join(tmp, "dst", "prepare")
	writeTestFile(t, filepath.Join(src, "counts.h5ad"), "counts", 0644)
	writeTestFile(t, filepath.Join(src, "Solo.out", "GeneFull", "matrix.mtx"), "matrix", 0644)
	if err := os.MkdirAll(filepath.Join(src, "empty"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("counts.h5ad", filepath.Join(src, "latest")); err != nil {
		t.Fatal(err)
	}

	if err := (LocalFS{}).Copy(src, dst); err != nil {
		t.Fatalf("Copy: %v", err)
	}

	// The contents land in dst itself, not in dst/prepare.
	if got := readTestFile(t, filepath.Join(dst, "counts.h5ad")); got != "counts" {
		t.Fatalf("counts.h5ad = %q", got)
	}
	if got := readTestFile(t, filepath.Join(dst, "Solo.out", "GeneFull", "matrix.mtx")); got != "matrix" {
		t.Fatalf("matrix.mtx = %q", got)
	}
	if info, err := os.Stat(filepath.Join(dst, "empty")); err != nil || !info.IsDir() {
		t.Fatalf("empty directory not copied: %v", err)
	}
	link, err := os.Readlink(filepath.Join(dst, "latest"))
	if err != nil {
		t.Fatalf("link not copied as a link: %v", err)
	}
	if link != "counts.h5ad" {
		t.Fatalf("link target = %q", link)
	}
	if _, err := os.Stat(filepath.Join(dst, "prepare")); !os.IsNotExist(err) {
		t.Fatalf("directory was nested one level too deep")
	}
}

func TestLocalFSCopyDirectoryCompletesPartialDestination(t *testing.T) {
	tmp := t.TempDir()
	src := filepath.Join(tmp, "src", "prepare")
	dst := filepath.Join(tmp, "dst", "prepare")
	writeTestFile(t, filepath.Join(src, "counts.h5ad"), "counts v2", 0644)
	writeTestFile(t, filepath.Join(src, "qc", "metrics.json"), "{}", 0644)
	// An earlier transfer of one file left part of the directory behind.
	writeTestFile(t, filepath.Join(dst, "counts.h5ad"), "counts v1", 0644)
	writeTestFile(t, filepath.Join(dst, "written_by_consumer.txt"), "keep", 0644)

	if err := (LocalFS{}).Copy(src, dst); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if got := readTestFile(t, filepath.Join(dst, "counts.h5ad")); got != "counts v2" {
		t.Fatalf("stale file kept: %q", got)
	}
	if got := readTestFile(t, filepath.Join(dst, "qc", "metrics.json")); got != "{}" {
		t.Fatalf("missing file not added: %q", got)
	}
	if got := readTestFile(t, filepath.Join(dst, "written_by_consumer.txt")); got != "keep" {
		t.Fatalf("file only in the destination was not kept")
	}
}

func TestLocalFSCopyFollowsLinkToDirectoryAtTheRoot(t *testing.T) {
	tmp := t.TempDir()
	real := filepath.Join(tmp, "real")
	writeTestFile(t, filepath.Join(real, "a.txt"), "a", 0644)
	link := filepath.Join(tmp, "link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(tmp, "dst")

	if err := (LocalFS{}).Copy(link, dst); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if got := readTestFile(t, filepath.Join(dst, "a.txt")); got != "a" {
		t.Fatalf("a.txt = %q", got)
	}
}

func TestLocalFSCopyMissingSource(t *testing.T) {
	tmp := t.TempDir()
	if err := (LocalFS{}).Copy(filepath.Join(tmp, "missing"), filepath.Join(tmp, "dst")); err == nil {
		t.Fatal("expected an error for a missing source")
	}
}
