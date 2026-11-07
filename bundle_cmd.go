package main

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

// bundle-run is an explicit stage command, not a shell-script graph parser.
// The archive's bytes are pinned and verified by the input transfer contract.
func bundleRunCommand() *cobra.Command {
	var archive, destination string
	cmd := &cobra.Command{Use: "bundle-run --archive PATH --destination DIR -- COMMAND [ARGS...]", Short: "Run an explicitly reviewed bundle archive in an isolated stage directory", Args: cobra.MinimumNArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		root, e := os.Getwd()
		if e != nil {
			return e
		}
		if e = extractBundle(root, archive, destination); e != nil {
			return e
		}
		child := exec.CommandContext(context.Background(), args[0], args[1:]...)
		child.Dir = filepath.Join(root, destination)
		child.Stdin = os.Stdin
		child.Stdout = os.Stdout
		child.Stderr = os.Stderr
		return child.Run()
	}}
	cmd.Flags().StringVar(&archive, "archive", "", "Previously checksummed stage input archive")
	cmd.Flags().StringVar(&destination, "destination", "bundle", "New extraction directory within the stage workspace")
	_ = cmd.MarkFlagRequired("archive")
	return cmd
}
func relativeBundlePath(p string) bool {
	return p != "" && !filepath.IsAbs(p) && filepath.Clean(p) == p && p != ".." && !strings.HasPrefix(p, "../") && !strings.Contains(p, "\\")
}
func extractBundle(workspace, archive, destination string) error {
	if !relativeBundlePath(archive) || !relativeBundlePath(destination) {
		return fmt.Errorf("bundle paths must be relative to the stage workspace")
	}
	root, e := os.OpenRoot(workspace)
	if e != nil {
		return e
	}
	defer root.Close()
	input, e := root.Open(archive)
	if e != nil {
		return e
	}
	defer input.Close()
	hash := sha256.New()
	if _, e = io.Copy(hash, input); e != nil {
		return e
	}
	digest := fmt.Sprintf("sha256:%x", hash.Sum(nil))
	if _, e = input.Seek(0, 0); e != nil {
		return e
	}
	// The runner already owns command idempotency. Conflicting extraction targets
	// are never merged with other computations or overwritten on resume.
	if _, e = root.Stat(destination); e == nil {
		f, e := root.Open(filepath.Join(destination, ".workbench-bundle.json"))
		if e != nil {
			return fmt.Errorf("bundle destination already exists without receipt")
		}
		defer f.Close()
		var record map[string]string
		if e = json.NewDecoder(f).Decode(&record); e != nil || record["sha256"] != digest {
			return fmt.Errorf("bundle extraction conflicts")
		}
		return nil
	}
	temp, e := os.MkdirTemp(workspace, ".bundle-*")
	if e != nil {
		return e
	}
	defer os.RemoveAll(temp)
	target, e := os.OpenRoot(temp)
	if e != nil {
		return e
	}
	defer target.Close()
	mkdir := func(p string) error {
		part := ""
		for _, v := range strings.Split(p, "/") {
			part = filepath.Join(part, v)
			if e := target.Mkdir(part, 0700); e != nil && !os.IsExist(e) {
				return e
			}
		}
		return nil
	}
	reader := tar.NewReader(input)
	count := 0
	for {
		h, e := reader.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			return e
		}
		count++
		if count > 100000 {
			return fmt.Errorf("bundle file count exceeds bound")
		}
		name := strings.TrimSuffix(h.Name, "/")
		if !relativeBundlePath(name) || name == ".workbench-bundle.json" {
			return fmt.Errorf("unsafe archive path")
		}
		switch h.Typeflag {
		case tar.TypeDir:
			if e = mkdir(name); e != nil {
				return e
			}
		case tar.TypeReg, tar.TypeRegA:
			if e = mkdir(filepath.Dir(name)); e != nil {
				return e
			}
			f, e := target.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.FileMode(h.Mode)&0700)
			if e != nil {
				return e
			}
			_, e = io.CopyN(f, reader, h.Size)
			if e == nil {
				e = f.Sync()
			}
			closeErr := f.Close()
			if e != nil {
				return e
			}
			if closeErr != nil {
				return closeErr
			}
		default:
			return fmt.Errorf("bundle archives may contain only regular files and directories")
		}
	}
	f, e := target.Create(".workbench-bundle.json")
	if e != nil {
		return e
	}
	e = json.NewEncoder(f).Encode(map[string]string{"sha256": digest})
	if e == nil {
		e = f.Sync()
	}
	f.Close()
	if e != nil {
		return e
	}
	// Destination parent must already exist. Rename only within the canonical,
	// runner-owned stage workspace; archive entries cannot create symlinks.
	parent, e := filepath.EvalSymlinks(filepath.Dir(filepath.Join(workspace, destination)))
	if e != nil || !strings.HasPrefix(parent+"/", workspace+"/") {
		return fmt.Errorf("destination parent escapes workspace")
	}
	return os.Rename(temp, filepath.Join(workspace, destination))
}
