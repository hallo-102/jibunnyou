"""GitHubへ送るのは固定schemaの集計だけ。生ログ・パス・馬情報は送らない。"""
import json
import re

from .schema import STATES, utcnow

SUMMARY = {"architecture", "python_version", "acceptance", "protected_unchanged", "ready_verified", "input_match",
           "windows_comparison", "races", "horses", "excluded_races", "changed_horses", "top5_changed_races", "fixed_tickets_changed_races"}
WORDS = {"aarch64", "arm64", "AMD64", "x86_64", "unknown", "PASS", "PARTIAL", "NOT_AVAILABLE", "MATCH", "MISMATCH"}


def public_result(result):
    summary = {}
    for key, value in result.get("summary", {}).items():
        if key not in SUMMARY:
            continue
        if type(value) is bool or (type(value) is int and 0 <= value <= 1000000) or (isinstance(value, str) and (value in WORDS or (key == "python_version" and re.fullmatch(r"\d+\.\d+\.\d+", value)))):
            summary[key] = value
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
            **public_result(json.loads(row["result"]))}


def comment(payload):
    marker = f"<!-- nas-agent:{payload['instance_id']}:{payload['job_id']}:{payload['request_hash']} -->"
    body = "NAS固定ジョブの状態（offline、自動採用なし）\n\n```json\n" + json.dumps(payload, ensure_ascii=False, indent=2) + "\n```\n\nheartbeatが古い場合はNAS停止・通信障害・poller停止を含む未応答です。成功/失敗を推測しないでください。"
    return marker, body
