// local_fs.go
package fs

import (
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"path/filepath"
	"strings"
)

type LocalFS struct {
	RootDir string
}

func (fs LocalFS) GetRootDir() string {
	// TODO: This is a hacky way to maintain compatability between v0 and v1,
	// but fix.
	return fs.RootDir
}

func (fs LocalFS) Glob(
	root, pattern string, includeFiles, includeDirs bool,
) ([]string, error) {
	hostRootPath, ok := GetV0HostPath(root, fs.RootDir)
	if !ok {
		return nil, fmt.Errorf("invalid root path %s", hostRootPath)
	}

	if info, err := os.Stat(hostRootPath); err != nil {
		return nil, fmt.Errorf(
			"error resolving pattern query: root %s (host %s) does not exist",
			root, hostRootPath,
		)
	} else {
		if !info.Mode().IsDir() {
			return nil, fmt.Errorf(
				"root %s (host %s) is not a directory", root, hostRootPath,
			)
		}

	}

	fullPattern := filepath.Join(hostRootPath, pattern)
	matches, err := filepath.Glob(fullPattern)
	if err != nil {
		return nil, fmt.Errorf("error glob-ing %s: %s", fullPattern, err)
	}

	// Python BWB uses glob.glob with recursive=True, which considers
	// root/file to be a match for root/**/file, unlike golang glob.
	if strings.HasPrefix(pattern, "**/") {
		depth1Pattern := filepath.Join(hostRootPath, pattern[3:])
		depth1Matches, err := filepath.Glob(depth1Pattern)
		if err != nil {
			return nil, fmt.Errorf("error glob-ing %s: %s", depth1Pattern, err)
		}
		matches = append(matches, depth1Matches...)
	}

	ret := make([]string, 0)
	for _, match := range matches {
		stat, err := os.Stat(match)
		if err != nil {
			return nil, fmt.Errorf("error stat-ing %s: %s", stat, err)
		}

		isFile := stat.Mode().IsRegular()
		if (stat.IsDir() && includeDirs) || (isFile && includeFiles) {
			matchCntPath, ok := GetV0CntPath(match, fs.RootDir)
			if !ok {
				return nil, fmt.Errorf(
					"couldn't convert host path %s to cnt path with root dir %s",
					match, fs.RootDir,
				)
			}
			ret = append(ret, matchCntPath)
		}
	}

	return ret, nil
}

// Copy copies a file or a directory tree from src to dst. A directory is merged
// into dst: entries of dst that src does not have are left in place.
func (fs LocalFS) Copy(src, dst string) error {
	if src == dst {
		return nil
	}

	info, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("opening source %q: %w", src, err)
	}

	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return fmt.Errorf("creating parent directory: %w", err)
	}

	if info.IsDir() {
		return copyDir(src, dst)
	}
	return copyFile(src, dst, info.Mode().Perm())
}

// copyDir copies the tree under src into dst. Like rsync -a, it recreates
// symbolic links instead of following them.
func copyDir(src, dst string) error {
	// WalkDir does not descend into a root that is itself a symbolic link.
	root, err := filepath.EvalSymlinks(src)
	if err != nil {
		return fmt.Errorf("resolving source %q: %w", src, err)
	}

	return filepath.WalkDir(root, func(path string, entry iofs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("reading %q: %w", path, err)
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("reading %q: %w", path, err)
		}

		switch {
		case entry.IsDir():
			// Keep the directory writable for its owner so that its
			// entries can be created.
			if err := os.MkdirAll(target, info.Mode().Perm()|0700); err != nil {
				return fmt.Errorf("creating directory %q: %w", target, err)
			}
			return nil
		case info.Mode()&os.ModeSymlink != 0:
			link, err := os.Readlink(path)
			if err != nil {
				return fmt.Errorf("reading link %q: %w", path, err)
			}
			if err := os.Remove(target); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("replacing %q: %w", target, err)
			}
			if err := os.Symlink(link, target); err != nil {
				return fmt.Errorf("creating link %q: %w", target, err)
			}
			return nil
		case info.Mode().IsRegular():
			return copyFile(path, target, info.Mode().Perm())
		default:
			return fmt.Errorf("cannot copy %q: unsupported file type %s", path, info.Mode().Type())
		}
	})
}

// copyFile writes src to a temporary file next to dst and renames it into
// place, so that a reader never sees a partly written dst.
func copyFile(src, dst string, perm os.FileMode) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("opening source %q: %w", src, err)
	}
	defer srcFile.Close()

	tmpFile, err := os.CreateTemp(filepath.Dir(dst), "."+filepath.Base(dst)+".xfer-*")
	if err != nil {
		return fmt.Errorf("creating destination %q: %w", dst, err)
	}
	tmpPath := tmpFile.Name()
	renamed := false
	defer func() {
		if !renamed {
			tmpFile.Close()
			os.Remove(tmpPath)
		}
	}()

	if _, err := io.Copy(tmpFile, srcFile); err != nil {
		return fmt.Errorf("copying %q -> %q: %w", src, dst, err)
	}
	if err := tmpFile.Sync(); err != nil {
		return fmt.Errorf("syncing %q: %w", dst, err)
	}
	if err := tmpFile.Chmod(perm); err != nil {
		return fmt.Errorf("setting mode of %q: %w", dst, err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("closing %q: %w", dst, err)
	}
	if err := os.Rename(tmpPath, dst); err != nil {
		return fmt.Errorf("creating destination %q: %w", dst, err)
	}
	renamed = true
	return nil
}

func (fs LocalFS) Upload(src string, dst string) error {
	return fs.Copy(src, dst)
}

func (fs LocalFS) Download(src string, dst string) error {
	return fs.Copy(src, dst)
}
