#!/usr/bin/env python3
"""Run one reproducible Temporal-Slurm pre-merge acceptance scenario."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from acceptance_lib import (
    ManagedProcesses,
    redact,
    require_checks,
    unique_id,
    utc_stamp,
    verify_checksums,
    write_checksums,
    write_json,
)


TERMINAL = {"FINISHED", "FAILED", "CANCELED", "CANCEL_CLEANUP_FAILED", "TERMINATED", "TIMED_OUT"}


def command_ok(argv: list[str]) -> bool:
    return subprocess.run(argv, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0


def load_slurm_profile(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    document = json.loads(path.read_text(encoding="utf-8"))
    profile = document.get("executors", {}).get("slurm")
    if not isinstance(profile, dict):
        raise RuntimeError("site profile does not contain executors.slurm")
    return document, profile


def ssh_argv(profile: dict[str, Any], remote_command: str) -> list[str]:
    argv = ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes"]
    if profile.get("known_hosts_file"):
        argv += ["-o", f"UserKnownHostsFile={profile['known_hosts_file']}"]
    if profile.get("identity_file"):
        argv += ["-i", str(profile["identity_file"])]
    host = str(profile["ip_addr"])
    port = profile.get("command_port")
    if not port and host.count(":") == 1:
        candidate_host, candidate_port = host.rsplit(":", 1)
        if candidate_port.isdigit():
            host, port = candidate_host, int(candidate_port)
    if port:
        argv += ["-p", str(port)]
    argv += [f"{profile['user']}@{host}", remote_command]
    return argv


def api_call(base: str, route: str, payload: dict[str, Any], token: str) -> tuple[int, dict[str, Any]]:
    request = urllib.request.Request(
        base + route,
        data=json.dumps(payload).encode(),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read())


def wait_api(base: str, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            request = urllib.request.Request(base + "/workflow_status", data=b"{}", method="POST")
            urllib.request.urlopen(request, timeout=2)
        except urllib.error.HTTPError as error:
            if error.code == 401:
                return
        except OSError:
            pass
        time.sleep(0.25)
    raise RuntimeError("scheduler API did not become ready")


def wait_status(
    base: str, workflow_id: str, token: str, evidence: Path, timeout: float,
    history: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    deadline = time.time() + timeout
    if history is None:
        history = []
    while time.time() < deadline:
        code, status = api_call(base, "/workflow_status", {"workflow_id": workflow_id}, token)
        if code == 200:
            history.append(status)
            write_json(evidence, history)
            if status.get("workflow_status") in TERMINAL:
                return status
        time.sleep(1)
    raise RuntimeError(f"workflow {workflow_id} did not reach terminal state")


def wait_for_manifest_jobs(
    profile: dict[str, Any], manifest_path: str, workflow_id: str, timeout: float,
) -> list[str]:
    deadline = time.time() + timeout
    command = (
        "if [ -f " + shlex_quote(manifest_path) + " ]; then "
        + "awk -F '\\t' -v workflow=" + shlex_quote(workflow_id)
        + " '$5 == workflow {print $2}' " + shlex_quote(manifest_path) + "; fi"
    )
    while time.time() < deadline:
        result = subprocess.run(
            ssh_argv(profile, command), capture_output=True, text=True,
        )
        if result.returncode == 0:
            job_ids = [line for line in result.stdout.splitlines() if line]
            if job_ids:
                return job_ids
        time.sleep(0.5)
    raise RuntimeError(f"workflow {workflow_id} did not create a Slurm manifest record")


def temporal_argv(
    temporal_address: str, temporal_namespace: str, arguments: list[str],
) -> list[str]:
    if shutil.which("temporal"):
        return [
            "temporal", *arguments,
            "--address", temporal_address, "--namespace", temporal_namespace,
        ]
    container = os.environ.get(
        "BWB_TEMPORAL_ADMIN_CONTAINER", "temporal-scheduler-local-admin-tools"
    )
    return [
        "docker", "exec", container, "temporal", *arguments,
        "--namespace", temporal_namespace,
    ]


def temporal_json(
    temporal_address: str, temporal_namespace: str, arguments: list[str],
) -> dict[str, Any]:
    result = subprocess.run(
        temporal_argv(temporal_address, temporal_namespace, arguments),
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "Temporal command failed")
    return json.loads(result.stdout)


def observe_slurm_poller_continue_as_new(
    temporal_address: str, temporal_namespace: str, parent_workflow_id: str,
    evidence: Path, timeout: float,
) -> dict[str, str]:
    deadline = time.time() + timeout
    child_workflow_id = ""
    initial_run_id = ""
    current_run_id = ""
    while time.time() < deadline:
        try:
            parent_history = temporal_json(
                temporal_address, temporal_namespace,
                ["workflow", "show", "--workflow-id", parent_workflow_id, "--output", "json"],
            )
            for event in parent_history.get("events", []):
                attributes = event.get("childWorkflowExecutionStartedEventAttributes", {})
                execution = attributes.get("workflowExecution", {})
                if execution.get("workflowId", "").startswith("slurm-poller-"):
                    child_workflow_id = execution.get("workflowId", "")
                    initial_run_id = execution.get("runId", "")
                    break
            if child_workflow_id and initial_run_id:
                description = temporal_json(
                    temporal_address, temporal_namespace,
                    ["workflow", "describe", "--workflow-id", child_workflow_id, "--output", "json"],
                )
                current_run_id = (
                    description.get("workflowExecutionInfo", {})
                    .get("execution", {})
                    .get("runId", "")
                )
                if current_run_id and current_run_id != initial_run_id:
                    initial_history = temporal_json(
                        temporal_address, temporal_namespace,
                        [
                            "workflow", "show", "--workflow-id", child_workflow_id,
                            "--run-id", initial_run_id, "--output", "json",
                        ],
                    )
                    event_types = {
                        event.get("eventType") for event in initial_history.get("events", [])
                    }
                    if "EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW" not in event_types:
                        raise RuntimeError(
                            "Slurm poller run ID changed without a continued-as-new close event"
                        )
                    write_json(evidence / "temporal" / "slurm-poller-initial-history.json", initial_history)
                    observation = {
                        "workflow_id": child_workflow_id,
                        "initial_run_id": initial_run_id,
                        "successor_run_id": current_run_id,
                    }
                    write_json(evidence / "temporal" / "slurm-poller-continue-as-new.json", observation)
                    return observation
        except (RuntimeError, json.JSONDecodeError):
            pass
        time.sleep(1)
    raise RuntimeError("Slurm poller did not expose a continue-as-new run boundary")


def materialize_request(template: Path, scenario: str, queue: str) -> dict[str, Any]:
    request = json.loads(template.read_text(encoding="utf-8"))
    suffix = unique_id(scenario)
    request.update(
        request_id=f"request-{suffix}",
        workflow_id=f"workflow-{suffix}",
        workbench_run_id=f"workbench-{suffix}",
        executor_id=f"slurm-{suffix}",
        site_profile_id=f"site-{suffix}",
    )
    request.setdefault("worker_info", {})["QueueId"] = queue
    return request


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scenario",
        choices=(
            "smoke", "submission-loss", "cleanup-failure", "continue-as-new",
            "continue-as-new-cancel",
        ),
        required=True,
    )
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--site-profile", type=Path, required=True)
    parser.add_argument("--request-template", type=Path, required=True)
    parser.add_argument("--evidence-root", type=Path, required=True)
    parser.add_argument("--api-port", type=int, default=5448)
    parser.add_argument("--timeout", type=float, default=900)
    parser.add_argument("--allow-dirty", action="store_true")
    args = parser.parse_args()

    repo = Path(__file__).resolve().parents[2]
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()
    short = commit[:7]
    evidence = args.evidence_root / f"{utc_stamp()}_{short}_{args.scenario}"
    for child in ("requests", "responses", "temporal", "slurm", "logs"):
        (evidence / child).mkdir(parents=True, exist_ok=False)
    validation: dict[str, Any] = {"status": "FAIL", "scenario": args.scenario, "checks": {}}
    write_json(evidence / "validation.json", validation)

    profile_document, profile = load_slurm_profile(args.site_profile)
    sched_dir = str(profile["sched_dir"])
    temporal_address = os.environ.get("BWB_TEMPORAL_ADDRESS", "127.0.0.1:7233")
    temporal_namespace = os.environ.get("BWB_TEMPORAL_NAMESPACE", "slurm-hardening")
    temporal_host, temporal_port = temporal_address.rsplit(":", 1)
    dirty = bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=repo, text=True).strip())
    api_token = os.environ.get("BWB_API_BEARER_TOKEN", "")
    admin_token = os.environ.get("BWB_ADMIN_BEARER_TOKEN", "")

    def temporal_reachable() -> bool:
        with socket.create_connection((temporal_host, int(temporal_port)), timeout=3):
            return True

    def temporal_namespace_healthy() -> bool:
        if shutil.which("temporal"):
            return command_ok([
                "temporal", "operator", "namespace", "describe",
                "--namespace", temporal_namespace, "--address", temporal_address,
            ])
        container = os.environ.get("BWB_TEMPORAL_ADMIN_CONTAINER", "temporal-scheduler-local-admin-tools")
        if not shutil.which("docker"):
            return False
        return command_ok([
            "docker", "exec", container, "tctl", "--ns", temporal_namespace,
            "namespace", "describe",
        ])

    remote_preflight = " && ".join(
        [f"command -v {tool} >/dev/null" for tool in ("sbatch", "squeue", "sacct", "scancel", "flock", "sha256sum")]
        + [f"test -d {shlex_quote(sched_dir)}", f"df -Pk {shlex_quote(sched_dir)} >/dev/null"]
    )
    checks = require_checks(
        [
            ("clean_source", lambda: args.allow_dirty or not dirty),
            ("binary_executable", lambda: args.binary.is_file() and os.access(args.binary, os.X_OK)),
            ("required_local_tools", lambda: all(shutil.which(tool) for tool in ("git", "ssh", "rsync"))),
            ("api_token_configured", lambda: bool(api_token)),
            ("admin_token_configured", lambda: bool(admin_token)),
            ("known_hosts_configured", lambda: bool(profile.get("known_hosts_file"))),
            ("host_fingerprint_configured", lambda: bool(profile.get("expected_host_key_fingerprint"))),
            ("temporal_reachable", temporal_reachable),
            ("temporal_namespace_healthy", temporal_namespace_healthy),
            ("remote_scheduler_preflight", lambda: command_ok(ssh_argv(profile, remote_preflight))),
        ]
    )
    validation["checks"].update(checks)
    (evidence / "scheduler-commit.txt").write_text(commit + "\n", encoding="utf-8")
    digest = hashlib.sha256(args.binary.read_bytes()).hexdigest()
    (evidence / "binary.sha256").write_text(f"{digest}  {args.binary.name}\n", encoding="utf-8")
    write_json(evidence / "site-profile.redacted.json", redact(profile_document))

    queue = unique_id("acceptance-queue")
    request = materialize_request(args.request_template, args.scenario, queue)
    request["config"] = profile_document
    write_json(evidence / "requests" / "start.json", redact(request))
    base = f"http://127.0.0.1:{args.api_port}"
    environment = os.environ.copy()
    environment.update(
        BWB_SCHED_DIR=str(evidence / "scheduler-state"),
        BWB_EVIDENCE_DIR=str(evidence / "durable-evidence"),
    )
    pause_hook = evidence / "submission-loss"
    cleanup_hook = evidence / "cleanup-failure.marker"
    if args.scenario == "submission-loss":
        environment["BWB_TEST_PAUSE_AFTER_SLURM_SUBMIT_FILE"] = str(pause_hook)
    if args.scenario == "cleanup-failure":
        environment["BWB_TEST_FAIL_SLURM_CLEANUP_FILE"] = str(cleanup_hook)

    owned_job_ids: set[str] = set()
    scenario_completed = False
    try:
        with ManagedProcesses() as children:
            worker_argv = [
                str(args.binary), "workers", "--config", str(args.site_profile),
                "--workerName", queue, "--ram", "1GB", "--cpus", "2",
            ]
            children.start("worker", worker_argv, evidence / "logs" / "worker.log", environment)
            children.start(
                "api", [str(args.binary), "serve", "--addr", f"127.0.0.1:{args.api_port}"],
                evidence / "logs" / "api.log", environment,
            )
            wait_api(base)
            code, response = api_call(base, "/start_workflow", request, api_token)
            write_json(evidence / "responses" / "start.json", response)
            if code != 200:
                raise RuntimeError(f"start returned HTTP {code}: {response}")

            if args.scenario == "submission-loss":
                ready = Path(str(pause_hook) + ".ready")
                deadline = time.time() + 120
                while time.time() < deadline and not ready.exists():
                    time.sleep(0.25)
                if not ready.exists():
                    raise RuntimeError("submission-loss hook was not reached")
                hook_evidence = json.loads(ready.read_text(encoding="utf-8"))
                owned_job_ids.add(str(hook_evidence["job_id"]))
                children.stop("worker")
                children.start("worker", worker_argv, evidence / "logs" / "worker-restarted.log", environment)
                validation["checks"]["worker_killed_after_durable_submission"] = True

            if args.scenario == "cleanup-failure":
                manifest_jobs = wait_for_manifest_jobs(
                    profile, f"{sched_dir}/slurm/submissions.tsv",
                    request["workflow_id"], min(args.timeout, 180),
                )
                owned_job_ids.update(manifest_jobs)
                write_json(
                    evidence / "slurm" / "cleanup-failure-target.json",
                    {"workflow_id": request["workflow_id"], "job_ids": manifest_jobs},
                )
                validation["checks"]["job_submitted_before_cleanup_failure"] = True
                stop_code, stop = api_call(
                    base, "/stop_workflow", {"workflow_id": request["workflow_id"]}, api_token
                )
                write_json(evidence / "responses" / "stop.json", stop)
                if stop_code != 200:
                    raise RuntimeError(f"stop returned HTTP {stop_code}: {stop}")

            status_history: list[dict[str, Any]] = []
            continue_observation: dict[str, str] | None = None
            if args.scenario == "continue-as-new-cancel":
                continue_observation = observe_slurm_poller_continue_as_new(
                    temporal_address, temporal_namespace, request["workflow_id"],
                    evidence, min(args.timeout, 180),
                )
                continued_manifest_jobs = wait_for_manifest_jobs(
                    profile, f"{sched_dir}/slurm/submissions.tsv",
                    request["workflow_id"], min(args.timeout, 180),
                )
                owned_job_ids.update(continued_manifest_jobs)
                write_json(
                    evidence / "slurm" / "continue-as-new-cancel-target.json",
                    {
                        "workflow_id": request["workflow_id"],
                        "job_ids": continued_manifest_jobs,
                    },
                )
                stop_code, stop = api_call(
                    base, "/stop_workflow", {"workflow_id": request["workflow_id"]}, api_token
                )
                write_json(evidence / "responses" / "stop-after-continue-as-new.json", stop)
                if stop_code != 200:
                    raise RuntimeError(f"stop after continue-as-new returned HTTP {stop_code}: {stop}")

            terminal = wait_status(
                base, request["workflow_id"], api_token,
                evidence / "responses" / "status-history.json", args.timeout,
                status_history,
            )
            for job in terminal.get("slurm_jobs", []):
                if job.get("job_id"):
                    owned_job_ids.add(str(job["job_id"]))

            if args.scenario == "submission-loss":
                hook_evidence = json.loads(
                    Path(str(pause_hook) + ".ready").read_text(encoding="utf-8")
                )
                terminal_jobs = terminal.get("slurm_jobs", [])
                terminal_job_ids = {
                    str(job["job_id"]) for job in terminal_jobs if job.get("job_id")
                }
                if terminal_job_ids != {str(hook_evidence["job_id"])}:
                    raise RuntimeError(
                        "submission-loss hook and terminal evidence do not identify "
                        f"one job: hook={hook_evidence}, terminal={terminal_jobs}"
                    )
                if len(terminal_jobs) != 1 or terminal_jobs[0].get("submission_source") != "manifest":
                    raise RuntimeError(
                        f"submission was not recovered exactly once from the manifest: {terminal_jobs}"
                    )
                write_json(
                    evidence / "slurm" / "submission-loss-recovery.json",
                    {"hook": hook_evidence, "terminal_job": terminal_jobs[0]},
                )
                validation["checks"]["single_job_submission_recovery"] = True

            if args.scenario == "cleanup-failure":
                if terminal.get("workflow_status") != "CANCEL_CLEANUP_FAILED":
                    raise RuntimeError(f"cleanup failure was not durable: {terminal}")
                dry_code, dry = api_call(
                    base, "/admin/reconcile_slurm", {"workflow_id": request["workflow_id"]}, admin_token
                )
                write_json(evidence / "responses" / "reconcile-dry-run.json", dry)
                if dry_code != 200 or not dry.get("evidence", {}).get("dry_run"):
                    raise RuntimeError(f"administrator dry run failed: HTTP {dry_code}: {dry}")
                observed = set(dry.get("evidence", {}).get("last_observed_job_ids", []))
                observed.update(dry.get("evidence", {}).get("recovered_job_ids", []))
                if not set(manifest_jobs).issubset(observed):
                    raise RuntimeError(
                        f"administrator dry run did not inventory owned jobs: {dry}"
                    )
                apply_code, applied = api_call(
                    base, "/admin/reconcile_slurm",
                    {"workflow_id": request["workflow_id"], "apply": True}, admin_token,
                )
                write_json(evidence / "responses" / "reconcile-apply.json", applied)
                if apply_code != 200 or not applied.get("evidence", {}).get("verified"):
                    raise RuntimeError(f"administrator recovery failed: HTTP {apply_code}: {applied}")
                terminal_ids = set(
                    applied.get("evidence", {}).get("verified_terminal_ids", [])
                )
                if not set(manifest_jobs).issubset(terminal_ids):
                    raise RuntimeError(
                        f"administrator recovery did not verify owned jobs terminal: {applied}"
                    )
                recovered_code, recovered = api_call(
                    base, "/workflow_status", {"workflow_id": request["workflow_id"]}, api_token
                )
                write_json(evidence / "responses" / "status-after-reconciliation.json", recovered)
                if (
                    recovered_code != 200
                    or recovered.get("workflow_status") != "CANCELED"
                    or not recovered.get("slurm_reconciliations")
                ):
                    raise RuntimeError(
                        f"reconciled workflow status was not durable: HTTP {recovered_code}: {recovered}"
                    )
                terminal = recovered
                validation["checks"]["cleanup_failure_recovery"] = True

            if args.scenario in {"continue-as-new", "continue-as-new-cancel"}:
                if continue_observation is None:
                    continue_observation = observe_slurm_poller_continue_as_new(
                        temporal_address, temporal_namespace, request["workflow_id"],
                        evidence, min(args.timeout, 180),
                    )
                validation["checks"]["continue_as_new"] = True

            if args.scenario == "continue-as-new-cancel":
                if terminal.get("workflow_status") != "CANCELED":
                    raise RuntimeError(f"workflow was not canceled after continue-as-new: {terminal}")
                verified_ids = set(
                    terminal.get("slurm_cancellation", {}).get("verified_terminal_ids", [])
                )
                if not set(continued_manifest_jobs).issubset(verified_ids):
                    raise RuntimeError(
                        "cancellation after continue-as-new did not verify the owned job terminal: "
                        f"{terminal}"
                    )
                validation["checks"]["cancel_after_continue_as_new"] = True

            if terminal.get("workflow_status") not in {"FINISHED", "CANCELED", "CANCEL_CLEANUP_FAILED"}:
                raise RuntimeError(f"unexpected terminal status: {terminal}")
            validation["checks"]["terminal_evidence"] = True
            scenario_completed = True
    finally:
        history_command = temporal_argv(
            temporal_address, temporal_namespace,
            ["workflow", "show", "--workflow-id", str(request["workflow_id"]), "--output", "json"],
        )
        history_result = subprocess.run(history_command, capture_output=True, text=True)
        (evidence / "temporal" / "history.json").write_text(
            history_result.stdout, encoding="utf-8"
        )
        (evidence / "temporal" / "history.stderr.txt").write_text(
            history_result.stderr, encoding="utf-8"
        )
        validation["checks"]["temporal_history_captured"] = history_result.returncode == 0
        manifest_path = f"{sched_dir}/slurm/submissions.tsv"
        manifest_lookup = subprocess.run(
            ssh_argv(
                profile,
                "if [ -f " + shlex_quote(manifest_path) + " ]; then "
                + "awk -F '\\t' -v workflow=" + shlex_quote(str(request["workflow_id"]))
                + " '$5 == workflow {print $2}' " + shlex_quote(manifest_path) + "; fi",
            ),
            capture_output=True,
            text=True,
        )
        (evidence / "slurm" / "manifest-owned-job-ids.txt").write_text(
            manifest_lookup.stdout, encoding="utf-8"
        )
        for candidate in manifest_lookup.stdout.splitlines():
            if candidate.isdigit() or (
                "_" in candidate and all(part.isdigit() for part in candidate.split("_", 1))
            ):
                owned_job_ids.add(candidate)
        if owned_job_ids:
            ids = ",".join(sorted(owned_job_ids))
            subprocess.run(
                ssh_argv(profile, f"scancel -- {shlex_quote(ids)} 2>/dev/null || true"),
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
        final_queue = subprocess.run(
            ssh_argv(profile, f"squeue --noheader --user={shlex_quote(str(profile['user']))} --format='%A|%j|%T'"),
            capture_output=True, text=True,
        )
        (evidence / "slurm" / "squeue-final.txt").write_text(final_queue.stdout, encoding="utf-8")
        if owned_job_ids:
            accounting = subprocess.run(
                ssh_argv(profile, "sacct -X -j " + shlex_quote(",".join(sorted(owned_job_ids))) + " -n -P -o JobIDRaw,JobName,State,ExitCode,Submit,Start,End"),
                capture_output=True, text=True,
            )
            (evidence / "slurm" / "sacct-final.txt").write_text(accounting.stdout, encoding="utf-8")
        remaining = [line for line in final_queue.stdout.splitlines() if any(line.startswith(job + "|") for job in owned_job_ids)]
        validation["checks"]["no_owned_jobs_remaining"] = not remaining
        validation["checks"]["scenario_completed"] = scenario_completed
        validation["checks"]["checksum_verification"] = True
        validation["status"] = "PASS" if all(validation["checks"].values()) else "FAIL"
        write_json(evidence / "validation.json", validation)
        (evidence / "README.md").write_text(
            f"# Scheduler acceptance evidence\n\nScenario: `{args.scenario}`\n\nCommit: `{commit}`\n\nStatus: `{validation['status']}`\n",
            encoding="utf-8",
        )
        write_checksums(evidence, evidence / "checksums.sha256")
        if not verify_checksums(evidence, evidence / "checksums.sha256"):
            validation["status"] = "FAIL"
            validation["checks"]["checksum_verification"] = False
            write_json(evidence / "validation.json", validation)
            write_checksums(evidence, evidence / "checksums.sha256")

    print(evidence)
    return 0 if validation["status"] == "PASS" else 1


def shlex_quote(value: str) -> str:
    import shlex
    return shlex.quote(value)


if __name__ == "__main__":
    sys.exit(main())
