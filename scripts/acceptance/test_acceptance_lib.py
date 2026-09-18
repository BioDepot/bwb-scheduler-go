#!/usr/bin/env python3

import subprocess
import tempfile
import unittest
from pathlib import Path

try:
    from .acceptance_lib import (
        ManagedProcesses,
        redact,
        require_checks,
        unique_id,
        verify_checksums,
        write_checksums,
    )
except ImportError:
    from acceptance_lib import (
        ManagedProcesses,
        redact,
        require_checks,
        unique_id,
        verify_checksums,
        write_checksums,
    )


class AcceptanceHelpersTest(unittest.TestCase):
    def test_unique_ids(self) -> None:
        first = unique_id("run")
        second = unique_id("run")
        self.assertNotEqual(first, second)
        self.assertTrue(first.startswith("run-"))

    def test_redaction(self) -> None:
        value = {"token": "x", "nested": {"password_value": "y", "identity_file": "/key"}}
        self.assertEqual(redact(value)["token"], "[REDACTED]")
        self.assertEqual(redact(value)["nested"]["password_value"], "[REDACTED]")
        self.assertEqual(redact(value)["nested"]["identity_file"], "/key")

    def test_failed_preflight(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "bad"):
            require_checks([("good", lambda: True), ("bad", lambda: False)])

    def test_managed_process_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            manager = ManagedProcesses()
            manager.start("sleep", ["sleep", "60"], root / "sleep.log", dict())
            process = manager.processes["sleep"]
            manager.stop_all()
            self.assertIsNotNone(process.poll())

    def test_checksums(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            (root / "a").write_text("a")
            checksums = root / "checksums.sha256"
            write_checksums(root, checksums)
            self.assertTrue(verify_checksums(root, checksums))
            (root / "a").write_text("changed")
            self.assertFalse(verify_checksums(root, checksums))


if __name__ == "__main__":
    unittest.main()
