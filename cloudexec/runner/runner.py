#!/usr/bin/env python3
"""Pinned Linux SSM runner. Immutable command IDs, durable results, no shell eval.

Install root-owned at /opt/workbench/runner.py and create /var/lib/workbench-runner
on durable storage. SSM calls are delivery attempts; command records are process
truth. Never restart an uncertain command under the same logical ID.
"""
import argparse
import base64
import fcntl
import hashlib
import json
import os
import platform
import re
import signal
import subprocess
import sys
import time
import tempfile
from pathlib import Path

IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
ROOT = Path(os.environ.get("WORKBENCH_RUNNER_STATE", "/var/lib/workbench-runner"))


def digest(value):
    return "sha256:" + hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def file_hash(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return "sha256:" + h.hexdigest()


def atomic(path, value):
    fd, name = tempfile.mkstemp(prefix=".result-", dir=path.parent)
    temp = Path(name)
    with os.fdopen(fd, "w") as f:
        os.chmod(temp, 0o600)
        json.dump(value, f, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, path)
    fd = os.open(path.parent, os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def process_identity(pid):
    try:
        # /proc comm may itself contain spaces or parentheses.
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return {"pid": pid, "start_ticks": fields[19], "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip()}
    except (OSError, IndexError):
        return None


def alive(identity):
    return bool(identity and process_identity(identity["pid"]) == identity)


def record_paths(request):
    for key in ("job_id", "command_id"):
        if not IDENTIFIER.fullmatch(request.get(key, "")):
            raise ValueError("invalid command identity")
    directory = ROOT / request["job_id"] / request["command_id"]
    directory.mkdir(parents=True, mode=0o700, exist_ok=True)
    return directory, directory / "result.json"


def command_spec(request):
    argv = request.get("argv")
    if not isinstance(argv, list) or not argv or any(not isinstance(a, str) or "\0" in a for a in argv):
        raise ValueError("argv must be a nonempty string array")
    root = Path(request["work_root"])
    if not root.is_absolute() or str(root.resolve()) != str(root) or not root.is_dir():
        raise ValueError("work root must be a real canonical directory")
    timeout = request.get("timeout_seconds")
    if type(timeout) is not int or not 1 <= timeout <= 604800:
        raise ValueError("timeout must be bounded")
    return {"argv": argv, "work_root": str(root), "timeout_seconds": timeout,
            "job_id": request["job_id"], "command_id": request["command_id"]}


def status(request):
    directory, path = record_paths(request)
    if not path.exists():
        return {"state": "not_started", "process_stopped": True}
    record = json.loads(path.read_text())
    if record["state"] in {"running", "starting"}:
        identities = [record.get("process"), record.get("supervisor")]
        if not any(alive(p) for p in identities):
            # A missing process is not evidence of success. A supervisor may have
            # died before recording the child's PID or terminal exit.
            starting_owner = False
            if record["state"] == "starting":
                with (directory / "lock").open("a+") as lock:
                    try:
                        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError:
                        starting_owner = True
            if not starting_owner:
                record = {**record, "state": "unknown", "process_stopped": False}
    if record.get("process_stopped"):
        output = directory / "stdout.log"
        if output.exists() and output.stat().st_size <= 8192:
            try:
                record["command_result"] = json.loads(output.read_text())
            except (ValueError, UnicodeError):
                pass
        record["log_tails"] = {}
        for stream in ("stdout", "stderr"):
            path = directory / (stream + ".log")
            if path.exists():
                size = path.stat().st_size
                offset = max(0, size - 1024)
                with path.open("rb") as f:
                    f.seek(offset)
                    data = f.read(1024)
                record["log_tails"][stream] = {"stream": stream, "cursor": offset,
                    "next_cursor": offset + len(data), "text": data.decode("utf-8", errors="replace"),
                    "truncated": offset > 0}
    return record


def start(request):
    spec = command_spec(request)
    directory, path = record_paths(request)
    lock = open(directory / "lock", "a+")
    try:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            record = status(request)
            if record.get("fingerprint") != digest(spec):
                raise ValueError("command identity conflicts")
            return record
        if path.exists():
            record = status(request)
            if record.get("fingerprint") != digest(spec):
                raise ValueError("command identity conflicts")
            return record
        if (directory / "cancel").exists():
            record = {"state": "cancelled", "fingerprint": digest(spec), "spec": spec, "process_stopped": True}
            atomic(path, record)
            return record
        atomic(path, {"state": "starting", "fingerprint": digest(spec), "spec": spec, "process_stopped": False})
        encoded = base64.b64encode(json.dumps(spec).encode()).decode()
        # The inherited flock prevents duplicate delivery from launching a second
        # supervisor. The scientific process also inherits it across crashes.
        subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "supervise", encoded, str(lock.fileno())],
                         start_new_session=True, pass_fds=(lock.fileno(),), stdin=subprocess.DEVNULL,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return status(request)
    finally:
        lock.close()


def supervise(spec, lock_fd):
    directory, path = record_paths(spec)
    record = json.loads(path.read_text())
    record["supervisor"] = process_identity(os.getpid())
    atomic(path, record)
    if (directory / "cancel").exists():
        atomic(path, {**record, "state": "cancelled", "process_stopped": True})
        return
    with open(directory / "stdout.log", "ab", buffering=0) as stdout, open(directory / "stderr.log", "ab", buffering=0) as stderr:
        try:
            child = subprocess.Popen(spec["argv"], cwd=spec["work_root"], start_new_session=True,
                                     stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr, pass_fds=(lock_fd,))
        except Exception as exc:
            atomic(path, {**record, "state": "failed", "exit_code": 127, "process_stopped": True, "error": type(exc).__name__})
            return
        record.update(state="running", process=process_identity(child.pid))
        atomic(path, record)
        deadline = time.monotonic() + spec["timeout_seconds"]
        reason = None
        while child.poll() is None:
            if (directory / "cancel").exists() or time.monotonic() >= deadline:
                reason = "cancelled" if (directory / "cancel").exists() else "timed_out"
                os.killpg(child.pid, signal.SIGTERM)
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                break
            time.sleep(0.1)
        code = child.wait()
        if group_members(child.pid):
            os.killpg(child.pid, signal.SIGKILL)
            for _ in range(100):
                if not group_members(child.pid):
                    break
                time.sleep(0.05)
        stopped = not group_members(child.pid)
        # A child may spawn detached descendants; only process-group members are
        # covered. The runtime contract forbids daemonizing scientific commands.
        atomic(path, {**record, "state": reason or ("cancelled" if (directory / "cancel").exists() else ("succeeded" if code == 0 else "failed")),
                      "exit_code": code, "process_stopped": stopped,
                      "stdout_sha256": file_hash(directory / "stdout.log"), "stderr_sha256": file_hash(directory / "stderr.log")})


def group_members(pgid):
    result = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
            if int(fields[2]) == pgid and fields[0] != "Z":
                result.append(int(entry.name))
        except (OSError, ValueError, IndexError):
            continue
    return result


def cancel(request):
    directory, _ = record_paths(request)
    atomic(directory / "cancel", {"requested": True})
    record = status(request)
    if record["state"] in {"not_started", "succeeded", "failed", "cancelled", "timed_out"}:
        return record
    process = record.get("process")
    if alive(process):
        os.killpg(process["pid"], signal.SIGTERM)
    return status(request)


def wait(request):
    deadline = time.monotonic() + request["timeout_seconds"] + 30
    while time.monotonic() < deadline:
        result = status(request)
        if result.get("process_stopped"):
            return result
        if result["state"] == "unknown":
            raise ValueError("process outcome is unknown")
        time.sleep(1)
    raise ValueError("runner wait timed out")


def runtime(request):
    pin = request["runtime"]
    if platform.system().lower() != pin["os"] or {"aarch64": "arm64", "x86_64": "x86_64"}.get(platform.machine()) != pin["architecture"]:
        raise ValueError("runtime architecture mismatch")
    if file_hash(__file__) != pin["runner_hash"]:
        raise ValueError("runner hash mismatch")
    manifest_path = Path("/opt/workbench/runtime.json")
    if file_hash(manifest_path) != pin["manifest_hash"]:
        raise ValueError("runtime manifest mismatch")
    manifest = json.loads(manifest_path.read_text())
    executables = manifest.get("executables")
    if not isinstance(executables, list) or not executables:
        raise ValueError("runtime receipt must pin executable files")
    paths = set()
    for binary in executables:
        path = binary["path"]
        if not Path(path).is_absolute() or path in paths or not re.fullmatch(r"sha256:[a-f0-9]{64}", binary["sha256"]):
            raise ValueError("invalid runtime executable receipt")
        paths.add(path)
        if file_hash(path) != binary["sha256"]:
            raise ValueError("runtime executable mismatch")
    if "/opt/workbench/bwbScheduler" not in paths or request["argv"][0] not in paths:
        raise ValueError("runtime must pin both the transfer binary and scientific entrypoint")
    stat = os.statvfs(request["work_root"])
    if stat.f_bavail * stat.f_frsize < request.get("minimum_free_bytes", 1):
        raise ValueError("insufficient free disk")
    return {"state": "ready", "runtime": pin, "free_bytes": stat.f_bavail * stat.f_frsize}


def prepare(request):
    if not IDENTIFIER.fullmatch(request.get("job_id", "")):
        raise ValueError("invalid job ID")
    mount_record = ROOT / request["job_id"] / "mounts.json"
    mount_record.parent.mkdir(parents=True, exist_ok=True)
    previous = json.loads(mount_record.read_text()) if mount_record.exists() else {}
    receipts = []
    for volume in request["volumes"]:
        volume_id = volume["id"]
        if not re.fullmatch(r"vol-[a-f0-9]{8,17}", volume_id):
            raise ValueError("invalid volume ID")
        # NVMe serial carries the EBS ID without its hyphen. lsblk is also used
        # for Xen devices. Never select a disk by enumeration order.
        devices = json.loads(subprocess.check_output(["lsblk", "--json", "--output", "PATH,SERIAL,FSTYPE,MOUNTPOINTS"]))["blockdevices"]
        matches = [d for d in devices if str(d.get("serial", "")).replace("-", "") == volume_id.replace("-", "")]
        if len(matches) != 1:
            raise ValueError("EBS device identity is not uniquely resolved")
        device = matches[0]
        mount = Path(volume["spec"]["mount_path"])
        if not mount.is_absolute() or str(mount.resolve()) != str(mount):
            raise ValueError("mount path is not canonical")
        mounted = [p for p in device.get("mountpoints", []) if p]
        if mounted and mounted != [str(mount)]:
            raise ValueError("volume is already mounted elsewhere")
        mode = volume["spec"]["mode"]
        if mode == "mounted" and mounted != [str(mount)]:
            raise ValueError("approved existing mount is missing")
        if not device.get("fstype"):
            if mode != "new" or not volume.get("owned") or mounted:
                raise ValueError("formatting existing or mounted storage is forbidden")
            signatures = json.loads(subprocess.check_output(["wipefs", "--json", "--no-act", device["path"]]))
            if signatures.get("signatures"):
                raise ValueError("new volume is not empty")
            # stdout is the SSM JSON protocol; filesystem tools emit progress.
            subprocess.run(["mkfs.ext4", "-F", device["path"]], check=True, stdout=sys.stderr)
        if not mounted:
            mount.mkdir(parents=True, exist_ok=True)
            if any(mount.iterdir()):
                raise ValueError("mount would hide existing files")
            previous[volume_id] = {"volume_id": volume_id, "mount_path": str(mount), "mounted_by_runner": True}
            atomic(mount_record, previous)
            subprocess.run(["mount", "-o", "ro" if volume["spec"]["read_only"] else "rw", device["path"], str(mount)], check=True, stdout=sys.stderr)
        observed = json.loads(subprocess.check_output(["findmnt", "--json", "--mountpoint", str(mount), "--output", "OPTIONS"]))["filesystems"][0]
        readonly = "ro" in observed["options"].split(",")
        if readonly != volume["spec"]["read_only"]:
            raise ValueError("existing mount mode does not match the approved profile")
        receipt = previous.get(volume_id, {"volume_id": volume_id, "mount_path": str(mount), "mounted_by_runner": False})
        if receipt["mount_path"] != str(mount):
            raise ValueError("recorded mount ownership conflicts")
        receipts.append(receipt)
    for receipt in receipts:
        previous[receipt["volume_id"]] = receipt
    atomic(mount_record, previous)
    work = Path(request["work_root"])
    if not any(not v["spec"]["read_only"] and work.is_relative_to(Path(v["spec"]["mount_path"])) for v in request["volumes"]):
        raise ValueError("workspace is outside writable mounts")
    work.mkdir(parents=True, exist_ok=True)
    return {"state": "prepared", "mounts": receipts}


def unmount(request):
    if not IDENTIFIER.fullmatch(request.get("job_id", "")):
        raise ValueError("invalid job ID")
    path = ROOT / request["job_id"] / "mounts.json"
    records = json.loads(path.read_text()) if path.exists() else {}
    for result in path.parent.glob("*/result.json"):
        command = result.parent.name
        if not status({"job_id": request["job_id"], "command_id": command}).get("process_stopped"):
            raise ValueError("cannot unmount while a command may still run")
    for record in records.values():
        if record["mounted_by_runner"]:
            mount = record["mount_path"]
            check = subprocess.run(["findmnt", "--mountpoint", mount], capture_output=True)
            if check.returncode == 0:
                devices = json.loads(subprocess.check_output(["lsblk", "--json", "--output", "PATH,SERIAL,MOUNTPOINTS"]))["blockdevices"]
                matches = [d for d in devices if mount in (d.get("mountpoints") or [])]
                if len(matches) != 1 or str(matches[0].get("serial", "")).replace("-", "") != record["volume_id"].replace("-", ""):
                    raise ValueError("mount now belongs to a different device")
                subprocess.run(["umount", mount], check=True, stdout=sys.stderr)
            elif check.returncode != 1:
                raise ValueError("cannot determine mount state")
    return {"state": "unmounted"}


def logs(request):
    directory, _ = record_paths(request)
    stream = request.get("stream", "stdout")
    cursor = request.get("cursor", 0)
    if stream not in {"stdout", "stderr"} or type(cursor) is not int or cursor < 0:
        raise ValueError("invalid log cursor or stream")
    path = directory / (stream + ".log")
    if not path.exists():
        return {"stream": stream, "cursor": cursor, "next_cursor": cursor, "text": ""}
    with path.open("rb") as f:
        f.seek(cursor)
        data = f.read(4096)
    return {"stream": stream, "cursor": cursor, "next_cursor": cursor + len(data), "text": data.decode("utf-8", errors="replace")}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["start", "status", "cancel", "prepare", "runtime", "unmount", "wait", "logs", "supervise"])
    parser.add_argument("request")
    parser.add_argument("lock_fd", nargs="?")
    args = parser.parse_args()
    request = json.loads(base64.b64decode(args.request, validate=True))
    if args.action == "supervise":
        supervise(request, int(args.lock_fd))
    else:
        if args.action in {"prepare", "unmount"}:
            job_id = request.get("job_id", "")
            if not IDENTIFIER.fullmatch(job_id):
                raise ValueError("invalid job ID")
            directory = ROOT / job_id
            directory.mkdir(parents=True, exist_ok=True)
            with (directory / "mount.lock").open("a+") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX)
                result = globals()[args.action](request)
        else:
            result = globals()[args.action](request)
        if isinstance(result, dict):
            result.pop("spec", None)
        print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
