// ssh_fs.go
package fs

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type CmdOut struct {
	ExitCode int
	StdOut   string
	StdErr   string
}
type CmdRunner func(string) (CmdOut, error)
type SshFS struct {
	User           string
	Endpt          string
	Port           int
	IdentityFile   string
	KnownHostsFile string
	RemoteRootDir  string
	LocalRootDir   string
	RootDir        string
}

func NewSshFS(
	user, endpt, localRoot, remoteRoot string,
) SshFS {
	return SshFS{
		User:          user,
		Endpt:         endpt,
		LocalRootDir:  localRoot,
		RemoteRootDir: remoteRoot,
	}
}

func LocalRunCmd(cmdStr string) (CmdOut, error) {
	var stdout, stderr bytes.Buffer
	cmd := exec.Command("sh", "-c", cmdStr)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	var err error
	exitCode := 0
	if err = cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		}
	}

	return CmdOut{
		ExitCode: exitCode,
		StdOut:   stdout.String(),
		StdErr:   stderr.String(),
	}, err
}

func (fs SshFS) TranslatePath(path string) (string, bool) {
	return GetV0HostPath(path, fs.RemoteRootDir)
}

func (fs SshFS) GetRootDir() string {
	return fs.RootDir
}

func (fs SshFS) Glob(
	root, pattern string, includeFiles, includeDirs bool,
) ([]string, error) {
	return nil, nil
	//hostRootPath, ok := GetHostPath(root, fs.RemoteVolumes)
	//if !ok {
	//    return nil, fmt.Errorf("invalid root path %s", hostRootPath)
	//}

	//fullPattern := filepath.Join(hostRootPath, pattern)

	//var typeStr string
	//if includeFiles && includeDirs {
	//    typeStr = ""
	//} else if includeFiles {
	//    typeStr = "-type f"
	//} else if includeDirs {
	//    typeStr = "-type d"
	//}

	//var findOut CmdOut
	//findCmd := fmt.Sprintf("find -name %s %s", fullPattern, typeStr)
	//findOut, err := fs.RemoteRunCmd(findCmd)
	//if err != nil {
	//    return nil, err
	//}

	//out := make([]string, 0)
	//lines := strings.Split(findOut.StdOut, "\n")
	//for _, line := range lines {
	//    if line == "" {
	//        continue
	//    }
	//    cntPath, ok := GetCntPath(line, fs.RemoteVolumes)
	//    if !ok {
	//        return nil, fmt.Errorf(
	//            "couldn't convert host path %s to cnt path with volumes %#v",
	//            line, fs.RemoteVolumes,
	//        )
	//    }

	//    out = append(out, cntPath)
	//}

	//return out, nil
}

func (fs SshFS) Upload(src, dst string) error {
	if err := fs.validateRemotePath(dst); err != nil {
		return err
	}
	parent := filepath.Dir(dst)
	mkdirArgs := append(fs.sshArgs(), fs.remoteTarget(), "mkdir -p -- "+shellQuote(parent))
	mkdirOut, err := runLocalCommand("ssh", mkdirArgs...)
	if err != nil {
		return fmt.Errorf("remote mkdir failed for %s: %s: %s", parent, err, strings.TrimSpace(mkdirOut.StdErr))
	}

	// NOTE: We cannot use rsync --mkpath, since some versions
	// do not support this.
	rsyncArgs := []string{"-av", "-s", "-e", strings.Join(append([]string{"ssh"}, quoteCommandArgs(fs.sshArgs())...), " ")}
	rsyncArgs = append(rsyncArgs, src, fs.rsyncRemoteSpec(dst))
	rsyncOut, err := runLocalCommand("rsync", rsyncArgs...)
	if err != nil {
		return fmt.Errorf("rsync upload failed for %s: %s: %s", dst, err, strings.TrimSpace(rsyncOut.StdErr))
	}
	return nil
}

func (fs SshFS) Download(src, dst string) error {
	if err := fs.validateRemotePath(src); err != nil {
		return err
	}
	parent := filepath.Dir(dst)
	err := os.MkdirAll(parent, 0755)
	if err != nil {
		return fmt.Errorf("failed to make parent dir %s of dst %s", parent, dst)
	}
	rsyncArgs := []string{"--mkpath", "-av", "-s", "-e", strings.Join(append([]string{"ssh"}, quoteCommandArgs(fs.sshArgs())...), " ")}
	rsyncArgs = append(rsyncArgs, fs.rsyncRemoteSpec(src), dst)
	rsyncOut, err := runLocalCommand("rsync", rsyncArgs...)
	if err != nil {
		return fmt.Errorf("rsync download failed for %s: %s: %s", src, err, strings.TrimSpace(rsyncOut.StdErr))
	}
	return nil
}

func (fs SshFS) validateRemotePath(path string) error {
	cleaned := filepath.Clean(path)
	root := filepath.Clean(fs.RemoteRootDir)
	if !filepath.IsAbs(cleaned) || cleaned != path {
		return fmt.Errorf("remote path must be absolute and canonical: %q", path)
	}
	if root != "." && root != "/" && cleaned != root && !strings.HasPrefix(cleaned, root+string(filepath.Separator)) {
		return fmt.Errorf("remote path %q escapes configured root %q", path, root)
	}
	return nil
}

func (fs SshFS) remoteTarget() string {
	return fs.User + "@" + fs.Endpt
}

func (fs SshFS) rsyncRemoteSpec(path string) string {
	// rsync receives argv directly. Shell quotes here become literal remote path
	// characters; -s/--secluded-args protects this unquoted path in transit.
	return fs.remoteTarget() + ":" + path
}

func (fs SshFS) sshArgs() []string {
	args := []string{"-o", "StrictHostKeyChecking=yes"}
	if fs.Port > 0 {
		args = append(args, "-p", fmt.Sprintf("%d", fs.Port))
	}
	if fs.IdentityFile != "" {
		args = append(args, "-i", fs.IdentityFile)
	}
	if fs.KnownHostsFile != "" {
		args = append(args, "-o", "UserKnownHostsFile="+fs.KnownHostsFile)
	}
	return args
}

func runLocalCommand(name string, args ...string) (CmdOut, error) {
	var stdout, stderr bytes.Buffer
	cmd := exec.Command(name, args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	exitCode := 0
	if exitErr, ok := err.(*exec.ExitError); ok {
		exitCode = exitErr.ExitCode()
	}
	return CmdOut{ExitCode: exitCode, StdOut: stdout.String(), StdErr: stderr.String()}, err
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func quoteCommandArgs(args []string) []string {
	quoted := make([]string, len(args))
	for i, arg := range args {
		quoted[i] = shellQuote(arg)
	}
	return quoted
}

func SshUploadActivity(fs SshFS, src, dst string) error {
	return fs.Upload(src, dst)
}

func SshDownloadActivity(fs SshFS, src, dst string) error {
	return fs.Download(src, dst)
}
