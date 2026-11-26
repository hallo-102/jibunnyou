"""GitHubへ送るのは固定schemaの集計だけ。生ログ・パス・馬情報は送らない。"""
import json
import re

from .audit import EXIT_CODES, REQUIREMENTS, REVENUE_STATUSES
from .schema import STATES, utcnow

SUMMARY = {"architecture", "python_version", "acceptance", "protected_unchanged", "ready_verified", "input_match",
           "windows_comparison", "arm64_verified", "races", "horses", "excluded_races", "changed_horses", "top5_changed_races", "tickets_changed_races", "fixed_tickets_changed_races"}
WORDS = {"aarch64", "arm64", "AMD64", "x86_64", "unknown", "PASS", "PARTIAL", "NOT_AVAILABLE", "MATCH", "MISMATCH"}


def audit_summary(source):
    summary = {}
    status = source.get("audit_status")
    if isinstance(status, str) and (status in EXIT_CODES or status == "invalid_manifest"):
        summary["audit_status"] = status
    revenue_status = source.get("revenue_evaluation_status")
    if isinstance(revenue_status, str) and revenue_status in REVENUE_STATUSES:
        summary["revenue_evaluation_status"] = revenue_status
    for key in ("races", "horses", "past_runs", "exit_code"):
        value = source.get(key)
        minimum = -255 if key == "exit_code" else 0
        if type(value) is int and minimum <= value <= 1_000_000_000:
            summary[key] = value
    elapsed = source.get("elapsed_seconds")
    if type(elapsed) in (int, float) and 0 <= elapsed <= 86400:
        summary["elapsed_seconds"] = elapsed
    requirements = source.get("requirements")
    if isinstance(requirements, dict):
        counts = {}
        for req in REQUIREMENTS:
            item = requirements.get(req)
            if not isinstance(item, dict):
                continue
            issues, evaluated = item.get("issues"), item.get("evaluated")
            if type(issues) is int and type(evaluated) is int and 0 <= issues <= evaluated <= 1_000_000_000:
                counts[req] = {"issues": issues, "evaluated": evaluated,
                               "issue_pct": round(100 * issues / evaluated, 3) if evaluated else None}
        summary["requirements"] = counts
    return summary


def standard_summary(source):
    summary = {}
    for key, value in source.items():
        if key not in SUMMARY:
            continue
        if type(value) is bool or (type(value) is int and 0 <= value <= 1000000) or (isinstance(value, str) and (value in WORDS or (key == "python_version" and re.fullmatch(r"\d+\.\d+\.\d+", value)))):
            summary[key] = value
    if source.get("evaluation_mode") in ("formal", "reference"):
        summary["evaluation_mode"] = source["evaluation_mode"]
    return summary


def public_result(result, *, audit=False):
    source = result.get("summary", {})
    summary = audit_summary(source) if audit else standard_summary(source)
    artifacts = {key: value for key, value in result.get("artifacts_sha256", {}).items()
                 if re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", key) and isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value)}
    return {"summary": summary, "artifacts_sha256": artifacts, "drive": {"mode": "dry-run", "reference": None}}


def record(config, row):
    job = json.loads(row["job"])
    assert row["status"] in STATES
    return {"schema_version": 1, "instance_id": config["instance_id"], "issue_number": row["issue_number"],
            "job_id": job["job_id"], "request_hash": row["request_hash"], "commit_sha": job["commit_sha"],
            "status": row["status"], "started_at": row["started_at"], "finished_at": row["finished_at"],
            "heartbeat_at": row["heartbeat_at"], "error_code": row["error_code"],
            "state_history": row.get("history", []),
            **public_result(json.loads(row["result"]), audit=job["job_type"] == "full_feature_audit")}


def comment(payload):
    marker = f"<!-- nas-agent:{payload['instance_id']}:{payload['job_id']}:{payload['request_hash']} -->"
    body = "NAS固定ジョブの状態（offline、自動採用なし）\n\n```json\n" + json.dumps(payload, ensure_ascii=False, indent=2) + "\n```\n\nheartbeatが古い場合はNAS停止・通信障害・poller停止を含む未応答です。成功/失敗を推測しないでください。"
    return marker, body
