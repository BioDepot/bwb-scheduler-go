package main

import (
	"archive/tar"
	"os"
	"path/filepath"
	"testing"
)

func TestBundleExtractionRejectsTraversalAndLinks(t *testing.T) {
	for _, name := range []string{"../outside", "/absolute", "safe"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			f, e := os.Create(filepath.Join(root, "input.tar"))
			if e != nil {
				t.Fatal(e)
			}
			w := tar.NewWriter(f)
			h := &tar.Header{Name: name, Mode: 0700, Size: 1}
			if name == "safe" {
				h.Typeflag = tar.TypeSymlink
				h.Linkname = "/outside"
				h.Size = 0
			}
			if e = w.WriteHeader(h); e != nil {
				t.Fatal(e)
			}
			if h.Size > 0 {
				w.Write([]byte("x"))
			}
			w.Close()
			f.Close()
			if e = extractBundle(root, "input.tar", "bundle"); e == nil {
				t.Fatal("unsafe archive accepted")
			}
		})
	}
}
func TestBundleExtractionRetry(t *testing.T) {
	root := t.TempDir()
	f, _ := os.Create(filepath.Join(root, "input.tar"))
	w := tar.NewWriter(f)
	w.WriteHeader(&tar.Header{Name: "bin/run.sh", Mode: 0700, Size: 4})
	w.Write([]byte("true"))
	w.Close()
	f.Close()
	if e := extractBundle(root, "input.tar", "bundle"); e != nil {
		t.Fatal(e)
	}
	if e := extractBundle(root, "input.tar", "bundle"); e != nil {
		t.Fatal(e)
	}
	data, e := os.ReadFile(filepath.Join(root, "bundle/bin/run.sh"))
	if e != nil || string(data) != "true" {
		t.Fatal(string(data), e)
	}
}
