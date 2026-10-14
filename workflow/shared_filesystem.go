package workflow

import (
	"fmt"
	"os"
	"path/filepath"
)

// SharedFilesystemProbe is written on the command worker and checked on the
// scheduler host. A fresh name and token prevent an old staging directory from
// being mistaken for a shared mount.
type SharedFilesystemProbe struct {
	Path       string
	Token      string
	WorkerHost string
}

func WriteSharedFilesystemProbe(rootDir string) (SharedFilesystemProbe, error) {
	if !filepath.IsAbs(rootDir) {
		return SharedFilesystemProbe{}, fmt.Errorf("shared worker root must be an absolute path, got %q", rootDir)
	}
	host, err := os.Hostname()
	if err != nil {
		return SharedFilesystemProbe{}, err
	}
	file, err := os.CreateTemp(rootDir, ".scheduler-shared-fs-*")
	if err != nil {
		return SharedFilesystemProbe{}, fmt.Errorf("cannot write shared filesystem marker in %q on worker host %q: %w", rootDir, host, err)
	}
	probe := SharedFilesystemProbe{Path: file.Name(), Token: randomString(32), WorkerHost: host}
	_, writeErr := file.WriteString(probe.Token)
	closeErr := file.Close()
	if writeErr != nil {
		os.Remove(probe.Path)
		return SharedFilesystemProbe{}, writeErr
	}
	if closeErr != nil {
		os.Remove(probe.Path)
		return SharedFilesystemProbe{}, closeErr
	}
	return probe, nil
}

func CheckSharedFilesystemProbe(probe SharedFilesystemProbe) error {
	host, err := os.Hostname()
	if err != nil {
		return err
	}
	data, err := os.ReadFile(probe.Path)
	if err != nil {
		return fmt.Errorf("shared worker root %q from worker host %q is not visible at the same path on scheduler host %q: cannot read marker %q: %w", filepath.Dir(probe.Path), probe.WorkerHost, host, probe.Path, err)
	}
	if string(data) != probe.Token {
		return fmt.Errorf("shared filesystem marker %q from worker host %q has different contents on scheduler host %q", probe.Path, probe.WorkerHost, host)
	}
	return os.Remove(probe.Path)
}

// CheckWorkerImage runs on the command worker after the scheduler builds its
// SIF. RunCmdActivity resolves images through the worker's BWB_SCHED_DIR.
func CheckWorkerImage(imageName, schedulerPath string) error {
	host, err := os.Hostname()
	if err != nil {
		return err
	}
	schedDir := os.Getenv("BWB_SCHED_DIR")
	if !filepath.IsAbs(schedDir) {
		return fmt.Errorf("BWB_SCHED_DIR on worker host %q must be absolute for shared images, got %q", host, schedDir)
	}
	expected := filepath.Join(schedDir, "images", getSifName(imageName))
	file, err := os.Open(expected)
	if err != nil {
		return fmt.Errorf("SIF image %q expected at %q on worker host %q (scheduler built %q): %w; share BWB_SCHED_DIR/images at the same path", imageName, expected, host, schedulerPath, err)
	}
	defer file.Close()
	workerInfo, err := file.Stat()
	if err != nil {
		return err
	}
	schedulerInfo, err := os.Stat(schedulerPath)
	if err != nil || !workerInfo.Mode().IsRegular() || !os.SameFile(workerInfo, schedulerInfo) {
		return fmt.Errorf("SIF image %q expected at %q on worker host %q does not resolve to the scheduler image %q (stat error: %v); share BWB_SCHED_DIR/images at the same path", imageName, expected, host, schedulerPath, err)
	}
	return nil
}
