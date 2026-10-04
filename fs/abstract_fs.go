// abstract_fs.go
package fs

import (
	"fmt"
    "path"
	"go-scheduler/parsing"
	"os"
	"path/filepath"
	"strings"

	"go.temporal.io/sdk/workflow"
)

type AbstractFileSystem interface {
	GetRootDir() string
	// Glob impl with string include dirs, include files as args.
	Glob(string, string, bool, bool) ([]string, error)
	Upload(string, string) error
	Download(string, string) error
}

type DummyFS struct{}

func (DummyFS) Glob(_, _ string, _, _ bool) ([]string, error) { return nil, nil }
func (DummyFS) GetRootDir() string                            { return "" }
func (DummyFS) Upload(_, _ string) error                      { return nil }
func (DummyFS) Download(_, _ string) error                    { return nil }

func GetV0HostPath(cntPath, dataHostDir string) (string, bool) {
    cntPath = path.Clean(cntPath)

    if cntPath != "/data" && !strings.HasPrefix(cntPath, "/data/") {
        return "", false
    }

    rel := strings.TrimPrefix(cntPath, "/data")
    rel = strings.TrimPrefix(rel, "/")

    return filepath.Join(dataHostDir, rel), true
}

func GetV0CntPath(hostPath, dataHostDir string) (string, bool) {
    hostPath = filepath.Clean(hostPath)
    dataHostDir = filepath.Clean(dataHostDir)

    if hostPath != dataHostDir &&
        !strings.HasPrefix(hostPath, dataHostDir+string(filepath.Separator)) {
        return "", false
    }

    rel, err := filepath.Rel(dataHostDir, hostPath)
    if err != nil {
        return "", false
    }

    return path.Join("/data", filepath.ToSlash(rel)), true
}

// Transfer copies each path, a file or a directory, from srcFS to dstFS. It
// must run on the host of whichever side is a LocalFS, and copies directly
// between the two. Only a transfer between two remote file systems passes
// through this host.
func Transfer(
	storageId string, xfers []parsing.ObligatoryXfer,
	srcFS, dstFS AbstractFileSystem,
) error {
	_, srcIsLocal := srcFS.(LocalFS)
	_, dstIsLocal := dstFS.(LocalFS)
	if !srcIsLocal && !dstIsLocal {
		return TransferViaIntermediary(storageId, xfers, srcFS, dstFS)
	}

	for _, xfer := range xfers {
		var err error
		if dstIsLocal {
			err = srcFS.Download(xfer.SrcHostPath, xfer.DstHostPath)
		} else {
			err = dstFS.Upload(xfer.SrcHostPath, xfer.DstHostPath)
		}
		if err != nil {
			return fmt.Errorf(
				"error transferring %s to %s: %s",
				xfer.SrcHostPath, xfer.DstHostPath, err,
			)
		}
	}
	return nil
}

// Revise to take host paths
func TransferViaIntermediary(
	storageId string, xfers []parsing.ObligatoryXfer,
	srcFS, dstFS AbstractFileSystem,
) error {
	localRoot, err := GetRootDir(storageId)
    if err != nil {
        return err
    }
	for _, xfer := range xfers {
		localPath := filepath.Join(localRoot, "transfer", xfer.DstHostPath)
		// Always refresh the intermediate copy: one left by an earlier
		// transfer may be older or hold only part of a directory.
		if downloadErr := srcFS.Download(xfer.SrcHostPath, localPath); downloadErr != nil {
			return fmt.Errorf(
				"error downloading %s to local path %s: %s",
				xfer.SrcHostPath, localPath, downloadErr,
			)
		}

		if uploadErr := dstFS.Upload(localPath, xfer.DstHostPath); uploadErr != nil {
			return fmt.Errorf(
				"error uploading local path %s to %s: %s",
				localPath, xfer.DstHostPath, uploadErr,
			)
		}
	}

	return nil
}

func GetRootDir(storageId string) (string, error) {
	schedDir, ok := os.LookupEnv("BWB_SCHED_DIR")
    if !ok {
        return "", fmt.Errorf("BWB_SCHED_DIR unset")
    }
	dataDir := filepath.Join(schedDir, storageId)
    return dataDir, nil

}

func SetupRootDir(storageId string) (string, error) {
    dataDir, err := GetRootDir(storageId)
    if err != nil {
        return "", err
    }
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create dir %s: %s", dataDir, err)
	}

    err = os.MkdirAll(dataDir, 0o755)
    if err != nil {
        return "", fmt.Errorf("error making local dir %s: %s", dataDir, err)
    }
    return dataDir, nil
}

func GlobActivity[T AbstractFileSystem](
	fs T, root, pattern string, findFile, findDir bool,
) ([]string, error) {
	return fs.Glob(root, pattern, findFile, findDir)
}

func TransferLocalToSshFS(storageId string, srcFS LocalFS, dstFS SshFS, xfers []parsing.ObligatoryXfer) error {
	return Transfer(storageId, xfers, srcFS, dstFS)
}

func TransferLocalToLocalFS(storageId string, srcFS LocalFS, dstFS LocalFS, xfers []parsing.ObligatoryXfer) error {
	return Transfer(storageId, xfers, srcFS, dstFS)
}

func TransferSshToLocalFS(storageId string, srcFS SshFS, dstFS LocalFS, xfers []parsing.ObligatoryXfer) error {
	return Transfer(storageId, xfers, srcFS, dstFS)
}

func TransferSshToSshFS(storageId string, srcFS, dstFS SshFS, xfers []parsing.ObligatoryXfer) error {
	return Transfer(storageId, xfers, srcFS, dstFS)
}

// It's extremely regrettable that we need to do this, but temporal activities need to know concrete types.
func RunTransferActivity(ctx workflow.Context, storageId string, srcFS, dstFS AbstractFileSystem, xfers []parsing.ObligatoryXfer) workflow.Future {
	switch srcFS.(type) {
	case LocalFS:
		{
			switch dstFS.(type) {
			case LocalFS:
				{
					return workflow.ExecuteActivity(
						ctx, TransferLocalToLocalFS, storageId, srcFS.(LocalFS), dstFS.(LocalFS), xfers,
					)
				}
			case SshFS:
				{
					return workflow.ExecuteActivity(
						ctx, TransferLocalToSshFS, storageId, srcFS.(LocalFS), dstFS.(SshFS), xfers,
					)
				}
			}
		}
	case SshFS:
		{
			switch dstFS.(type) {
			case LocalFS:
				{
					return workflow.ExecuteActivity(
						ctx, TransferSshToLocalFS, storageId, srcFS.(SshFS), dstFS.(LocalFS), xfers,
					)

				}
			case SshFS:
				{
					return workflow.ExecuteActivity(
						ctx, TransferSshToSshFS, storageId, srcFS.(SshFS), dstFS.(SshFS), xfers,
					)
				}
			}
		}
	}
	return nil
}
