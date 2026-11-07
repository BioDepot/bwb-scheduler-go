package cloudexec

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func runner(t *testing.T, root, action string, request map[string]any) (map[string]any, error) {
	t.Helper()
	b, _ := json.Marshal(request)
	cmd := exec.Command("python3", "runner/runner.py", action, base64.StdEncoding.EncodeToString(b))
	cmd.Env = append(os.Environ(), "WORKBENCH_RUNNER_STATE="+root)
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	var v map[string]any
	err = json.Unmarshal(out, &v)
	return v, err
}
func TestRunnerDuplicateDeliveryAndCancellation(t *testing.T) {
	root, work := t.TempDir(), t.TempDir()
	request := map[string]any{"job_id": "job", "command_id": "command", "work_root": work, "timeout_seconds": 30, "argv": []string{"python3", "-c", "from pathlib import Path; p=Path('count'); p.write_text(p.read_text()+'x' if p.exists() else 'x')"}}
	if _, e := runner(t, root, "start", request); e != nil {
		t.Fatal(e)
	}
	if _, e := runner(t, root, "start", request); e != nil {
		t.Fatal(e)
	}
	var status map[string]any
	for i := 0; i < 100; i++ {
		var e error
		status, e = runner(t, root, "status", request)
		if e != nil {
			t.Fatal(e)
		}
		if status["process_stopped"] == true {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if status["state"] != "succeeded" {
		t.Fatal(status)
	}
	b, e := os.ReadFile(filepath.Join(work, "count"))
	if e != nil || string(b) != "x" {
		t.Fatal("duplicate computation", string(b), e)
	}
	request["argv"] = []string{"true"}
	if _, e = runner(t, root, "start", request); e == nil {
		t.Fatal("conflicting command accepted")
	}
	request["command_id"] = "cancelled-command"
	request["argv"] = []string{"python3", "-c", "import time;time.sleep(60)"}
	if _, e = runner(t, root, "start", request); e != nil {
		t.Fatal(e)
	}
	if _, e = runner(t, root, "cancel", request); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 100; i++ {
		status, e = runner(t, root, "status", request)
		if e != nil {
			t.Fatal(e)
		}
		if status["process_stopped"] == true {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if status["state"] != "cancelled" || status["process_stopped"] != true {
		t.Fatal(status)
	}
}

func TestRunnerCancelBeforeLateDelivery(t *testing.T) {
	root, work := t.TempDir(), t.TempDir()
	request := map[string]any{"job_id": "job", "command_id": "late", "work_root": work, "timeout_seconds": 10, "argv": []string{"/bin/sh", "-c", "touch must-not-exist"}}
	if _, e := runner(t, root, "cancel", request); e != nil {
		t.Fatal(e)
	}
	result, e := runner(t, root, "start", request)
	if e != nil || result["state"] != "cancelled" || result["process_stopped"] != true {
		t.Fatal(result, e)
	}
	if _, e = os.Stat(filepath.Join(work, "must-not-exist")); !os.IsNotExist(e) {
		t.Fatal("late command executed")
	}
}
func TestRuntimeReceiptAndFullDiskGate(t *testing.T) {
	script := `
import importlib.util,json,platform,tempfile
from pathlib import Path
from types import SimpleNamespace
spec=importlib.util.spec_from_file_location('runner','runner/runner.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
with tempfile.TemporaryDirectory() as temp:
 manifest=Path(temp)/'receipt.json'
 good='sha256:'+'a'*64
 manifest.write_text(json.dumps({'executables':[{'path':'/opt/workbench/bwbScheduler','sha256':good},{'path':'/bin/true','sha256':good}]}))
 m.Path=lambda p: manifest if p=='/opt/workbench/runtime.json' else Path(p)
 m.file_hash=lambda p: good
 m.os.statvfs=lambda p:SimpleNamespace(f_bavail=0,f_frsize=4096)
 request={'runtime':{'os':platform.system().lower(),'architecture':{'x86_64':'x86_64','aarch64':'arm64'}[platform.machine()],'runner_hash':good,'manifest_hash':good},'argv':['/bin/true'],'work_root':temp,'minimum_free_bytes':1}
 try:m.runtime(request)
 except ValueError as e:assert 'insufficient free disk' in str(e)
 else:raise AssertionError('full disk passed readiness')
 request['runtime']['runner_hash']='sha256:'+'b'*64
 try:m.runtime(request)
 except ValueError as e:assert 'runner hash mismatch' in str(e)
 else:raise AssertionError('unpinned runner passed readiness')
`
	if out, e := exec.Command("python3", "-c", script).CombinedOutput(); e != nil {
		t.Fatal(e, string(out))
	}
}

func TestRunnerPrepareKeepsFilesystemProgressOutOfJSON(t *testing.T) {
	script := `
import importlib.util,json,subprocess,sys,tempfile
from pathlib import Path
spec=importlib.util.spec_from_file_location('runner','runner/runner.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
real_run=subprocess.run
with tempfile.TemporaryDirectory() as temp:
 m.ROOT=Path(temp)/'state'
 mount=Path(temp)/'work'
 def output(argv):
  if argv[0]=='lsblk':return json.dumps({'blockdevices':[{'path':'/dev/fixture','serial':'vol0123456789abcdef0','fstype':None,'mountpoints':[]}]}).encode()
  if argv[0]=='wipefs':return b'{"signatures":[]}'
  if argv[0]=='findmnt':return b'{"filesystems":[{"options":"rw"}]}'
  raise AssertionError(argv)
 m.subprocess.check_output=output
 def noisy_tool(argv,**kwargs):
  assert argv[0] in ('mkfs.ext4','mount')
  return real_run([sys.executable,'-c','print("filesystem progress")'],**kwargs)
 m.subprocess.run=noisy_tool
 result=m.prepare({'job_id':'job','work_root':str(mount/'jobs'), 'volumes':[{'id':'vol-0123456789abcdef0','owned':True,'spec':{'mount_path':str(mount),'mode':'new','read_only':False}}]})
 print(json.dumps(result))
`
	out, err := exec.Command("python3", "-c", script).Output()
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err = json.Unmarshal(out, &result); err != nil {
		t.Fatalf("filesystem output corrupted runner JSON: %s: %v", out, err)
	}
	if result["state"] != "prepared" {
		t.Fatal(result)
	}
}
