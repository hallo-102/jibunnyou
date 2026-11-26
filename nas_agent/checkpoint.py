"""合成データ専用の再開試験。ジョブ入力に実行コマンドやパスを含めない。"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile

from .files import atomic_json, beneath
from .report import record
from .schema import TERMINAL, Rejected, digest, utcnow

VALUES = tuple(range(1, 1025))
INPUT_HASH = digest(VALUES)
CONDITIONS = {"algorithm": "sum-of-squares-v1", "batch_size": 128, "total_count": len(VALUES)}
RESUMABLE_ERRORS = {"GITHUB_UNAVAILABLE", "TIMEOUT"}
EXPORT_FILES = ("result.json", "execution.log", "checkpoint.json")
SMOKE_INTERRUPT_COUNT = 384


class SmokeInterrupted(Exception):
    """受入用の依頼で承認された、チェックポイント直後の模擬中断。"""


def _job_dir(config, job_id):
    root = Path(config["state_dir"]) / "jobs"
    return beneath(root, root / job_id)


def _resume(config, state, job):
    previous_id = job["resume_from"]
    previous = state.get(previous_id)
    if previous is None or not (
        previous["status"] == "INTERRUPTED"
        or (previous["status"] == "FAILED" and previous["error_code"] in RESUMABLE_ERRORS)
    ):
        raise Rejected("RESUME_SOURCE_NOT_INTERRUPTED")
    prior_job = json.loads(previous["job"])
    contract = ("job_type", "commit_sha", "input_id", "raceday", "scope")
    if any(prior_job[key] != job[key] for key in contract):
        raise Rejected("RESUME_CONDITIONS_CHANGED")
    root = _job_dir(config, previous_id)
    path = beneath(root, root / "checkpoint.json")
    if not path.is_file() or path.stat().st_size > 8192:
        raise Rejected("RESUME_CHECKPOINT_MISSING")
    payload = path.read_bytes()
    if hashlib.sha256(payload).hexdigest() != job["checkpoint_sha256"]:
        raise Rejected("RESUME_CHECKPOINT_HASH_MISMATCH")
    try:
        saved = json.loads(payload)
        count, subtotal = saved["completed_count"], saved["sum_squares"]
        if (saved["input_sha256"] != INPUT_HASH or saved["conditions"] != CONDITIONS
                or saved["job_id"] != previous_id or saved["request_hash"] != previous["request_hash"]
                or saved["issue_number"] != previous["issue_number"] or saved["commit_sha"] != job["commit_sha"]
                or type(count) is not int or not 0 <= count <= len(VALUES)
                or count % CONDITIONS["batch_size"] or type(subtotal) is not int
                or subtotal != count * (count + 1) * (2 * count + 1) // 6):
            raise ValueError("checkpoint contents")
    except (KeyError, TypeError, ValueError) as exc:
        raise Rejected("RESUME_CHECKPOINT_INVALID") from exc
    return count, subtotal


def run_smoke(config, state, row, job, control):
    if job["commit_sha"] != config["agent_commit"]:
        raise Rejected("CHECKPOINT_AGENT_COMMIT_MISMATCH")
    control()
    completed, subtotal = _resume(config, state, job) if "resume_from" in job else (0, 0)
    workspace = _job_dir(config, job["job_id"])
    workspace.mkdir(parents=True, mode=0o700)
    checkpoint = {"job_id": job["job_id"], "issue_number": row["issue_number"], "request_hash": row["request_hash"],
                  "commit_sha": job["commit_sha"], "input_sha256": INPUT_HASH, "started_at": row["started_at"],
                  "conditions": CONDITIONS, "total_count": len(VALUES), "resume_from": job.get("resume_from")}

    def save(phase):
        # 書込みが完了した分だけを、同じファイルに結果と一緒に記録する。
        atomic_json(workspace / "checkpoint.json", dict(checkpoint, phase=phase, completed_count=completed,
                    sum_squares=subtotal, updated_at=utcnow().isoformat()), mode=0o600)

    with (workspace / "execution.log").open("x", encoding="utf-8") as log:
        save("counting")
        log.write(f"start completed={completed} total={len(VALUES)}\n")
        log.flush()
        for start in range(completed, len(VALUES), CONDITIONS["batch_size"]):
            control()
            end = start + CONDITIONS["batch_size"]
            subtotal += sum(value * value for value in VALUES[start:end])
            completed = end
            save("counting")
            log.write(f"checkpoint completed={completed}\n")
            log.flush()
            if job.get("interrupt_after_checkpoint") and completed == SMOKE_INTERRUPT_COUNT:
                raise SmokeInterrupted
        control()
        save("complete")
        result = workspace / "result.json"
        atomic_json(result, {"count": completed, "sum_squares": subtotal}, mode=0o600)
    return {"artifacts_sha256": {"result.json": hashlib.sha256(result.read_bytes()).hexdigest()}}


def export_smoke(config, state, job_id, destination):
    """隔離ステージへ固定名の成果物を出力し、管理者がSFTPへ手動で引き渡す。"""
    row = state.get(job_id)
    if not row or row["status"] not in TERMINAL or json.loads(row["job"])["job_type"] != "checkpoint_smoke":
        raise Rejected("EXPORT_REQUIRES_TERMINAL_SMOKE_JOB")
    source = _job_dir(config, job_id)
    if not source.is_dir() or not beneath(source, source / "checkpoint.json").is_file():
        raise Rejected("EXPORT_CHECKPOINT_MISSING")
    destination = Path(destination)
    if not destination.is_absolute() or destination.resolve() != destination or not destination.is_dir():
        raise Rejected("EXPORT_REQUIRES_EXISTING_DIRECTORY")
    if os.name == "posix":
        access = destination.stat()
        if access.st_uid != os.geteuid() or access.st_mode & 0o077:
            raise Rejected("EXPORT_REQUIRES_PRIVATE_DIRECTORY")
    target = beneath(destination, destination / job_id)
    if target.exists():
        raise Rejected("EXPORT_ALREADY_EXISTS")
    with tempfile.TemporaryDirectory(prefix=".nas-agent-export-", dir=destination) as temp:
        stage = Path(temp) / job_id
        stage.mkdir(mode=0o700)
        files = ["nas-agent-status.json"]
        atomic_json(stage / files[0], record(config, row), mode=0o600)
        for name in EXPORT_FILES:
            item = beneath(source, source / name)
            if item.is_file():
                shutil.copyfile(item, stage / name)
                files.append(name)
        with (stage / "SHA256SUMS").open("x", encoding="ascii", newline="\n") as manifest:
            for name in sorted(files):
                checksum = hashlib.sha256((stage / name).read_bytes()).hexdigest()
                manifest.write(f"{checksum}  {name}\n")
        stage.rename(target)
    return target
