#!/usr/bin/env python3
"""Small, dependency-free helpers for scheduler acceptance evidence."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import signal
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable


SENSITIVE_FRAGMENTS = ("password", "secret", "token", "private_key", "credential")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def unique_id(prefix: str) -> str:
    return f"{prefix}-{utc_stamp().lower()}-{secrets.token_hex(4)}"


def replace_string_token(value: Any, token: str, replacement: str) -> tuple[Any, int]:
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        replacements = 0
        for key, item in value.items():
            replaced, count = replace_string_token(item, token, replacement)
            result[key] = replaced
            replacements += count
        return result, replacements
    if isinstance(value, list):
        result = []
        replacements = 0
        for item in value:
            replaced, count = replace_string_token(item, token, replacement)
            result.append(replaced)
            replacements += count
        return result, replacements
    if isinstance(value, str):
        return value.replace(token, replacement), value.count(token)
    return value, 0


def redact(value: Any) -> Any:
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, item in value.items():
            lowered = key.lower()
            if any(fragment in lowered for fragment in SENSITIVE_FRAGMENTS):
                result[key] = "[REDACTED]"
            else:
                result[key] = redact(item)
        return result
    if isinstance(value, list):
        return [redact(item) for item in value]
    return value


def require_checks(checks: Iterable[tuple[str, Callable[[], bool]]]) -> dict[str, bool]:
    results: dict[str, bool] = {}
    failures: list[str] = []
    for name, check in checks:
        try:
            passed = bool(check())
        except Exception:
            passed = False
        results[name] = passed
        if not passed:
            failures.append(name)
    if failures:
        raise RuntimeError("preflight failed: " + ", ".join(failures))
    return results


def write_checksums(root: Path, destination: Path) -> None:
    lines: list[str] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path == destination:
            continue
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        lines.append(f"{digest}  {path.relative_to(root)}")
    destination.write_text("\n".join(lines) + "\n", encoding="utf-8")


def verify_checksums(root: Path, checksum_file: Path) -> bool:
    for line in checksum_file.read_text(encoding="utf-8").splitlines():
        digest, relative = line.split("  ", 1)
        path = root / relative
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            return False
    return True


class ManagedProcesses:
    def __init__(self) -> None:
        self.processes: dict[str, subprocess.Popen[bytes]] = {}

    def start(self, name: str, argv: list[str], log_path: Path, env: dict[str, str]) -> int:
        log_handle = log_path.open("ab", buffering=0)
        process = subprocess.Popen(
            argv,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            env=env,
            start_new_session=True,
        )
        process._acceptance_log_handle = log_handle  # type: ignore[attr-defined]
        self.processes[name] = process
        return process.pid

    def stop(self, name: str, timeout: float = 10.0) -> None:
        process = self.processes.pop(name, None)
        if process is None:
            return
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=timeout)
        process._acceptance_log_handle.close()  # type: ignore[attr-defined]

    def stop_all(self) -> None:
        for name in list(reversed(self.processes)):
            self.stop(name)

    def __enter__(self) -> "ManagedProcesses":
        return self

    def __exit__(self, *_: object) -> None:
        self.stop_all()


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
