"""Issue本文は非信頼入力。シェル・URL・パスを受け取らない。"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import re

REPOSITORY = "hallo-102/jibunnyou"
PR17_SHA = "3ae9ce0dc5eaf41c2aa5d26c6e62fcef8dc26b92"
COMPARISON_SHA = "a40fd7f899385913274d0e64235aba2a46e1054b"
FIELDS = {"schema_version", "job_id", "job_type", "commit_sha", "raceday", "input_id", "expires_at", "scope"}
TYPES = {"healthcheck", "issue16_acceptance", "full_feature_audit", "ab_comparison", "checkpoint_smoke"}
RESUME_FIELDS = {"resume_from", "checkpoint_sha256"}
SMOKE_INTERRUPT_FIELDS = {"interrupt_after_checkpoint"}
STATES = {"QUEUED", "WAIT_APPROVAL", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED", "INTERRUPTED"}
TERMINAL = {"SUCCEEDED", "FAILED", "CANCELLED", "INTERRUPTED"}


class Rejected(ValueError):
    """公開してよい固定エラーコードだけを保持する。"""


def utcnow():
    return datetime.now(timezone.utc)


def timestamp(value):
    if not isinstance(value, str) or len(value) > 40:
        raise Rejected("INVALID_EXPIRY")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise Rejected("INVALID_EXPIRY") from exc
    if result.tzinfo is None:
        raise Rejected("INVALID_EXPIRY")
    return result.astimezone(timezone.utc)


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise Rejected("DUPLICATE_FIELD")
        result[key] = value
    return result


def parse_job(body):
    if not isinstance(body, str):
        raise Rejected("INVALID_BODY")
    try:
        if len(body.encode()) > 8192:
            raise Rejected("INVALID_BODY")
    except UnicodeError as exc:
        raise Rejected("INVALID_BODY") from exc
    try:
        job = json.loads(body, object_pairs_hook=_unique_pairs, parse_constant=lambda _: (_ for _ in ()).throw(Rejected("INVALID_JSON")))
    except (json.JSONDecodeError, UnicodeError, RecursionError) as exc:
        raise Rejected("INVALID_JSON") from exc
    if (not isinstance(job, dict) or set(job) not in (FIELDS, FIELDS | RESUME_FIELDS, FIELDS | SMOKE_INTERRUPT_FIELDS)
            or type(job["schema_version"]) is not int or job["schema_version"] != 1):
        raise Rejected("INVALID_SCHEMA")
    for field in ("job_id", "input_id"):
        if not isinstance(job[field], str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", job[field]):
            raise Rejected("INVALID_IDENTIFIER")
    if not isinstance(job["job_type"], str) or job["job_type"] not in TYPES or job["scope"] != "offline":
        raise Rejected("INVALID_JOB_TYPE_OR_SCOPE")
    if not isinstance(job["commit_sha"], str) or not re.fullmatch(r"[0-9a-f]{40}", job["commit_sha"]):
        raise Rejected("INVALID_COMMIT")
    if job["job_type"] == "checkpoint_smoke" and job["input_id"] != "synthetic-v1":
        raise Rejected("INVALID_SYNTHETIC_INPUT")
    if "interrupt_after_checkpoint" in job and (job["job_type"] != "checkpoint_smoke"
                                                 or job["interrupt_after_checkpoint"] is not True):
        raise Rejected("INVALID_SMOKE_INTERRUPTION")
    if "resume_from" in job:
        previous, checksum = job["resume_from"], job["checkpoint_sha256"]
        if (job["job_type"] != "checkpoint_smoke" or not isinstance(previous, str)
                or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", previous) or previous == job["job_id"]
                or not isinstance(checksum, str) or not re.fullmatch(r"[0-9a-f]{64}", checksum)):
            raise Rejected("INVALID_RESUME_REQUEST")
    if not isinstance(job["raceday"], str) or not re.fullmatch(r"\d{8}", job["raceday"]):
        raise Rejected("INVALID_DATE")
    try:
        datetime.strptime(job["raceday"], "%Y%m%d")
    except ValueError as exc:
        raise Rejected("INVALID_DATE") from exc
    timestamp(job["expires_at"])
    return job


def request(issue, repository=REPOSITORY):
    if "pull_request" in issue or not re.fullmatch(r"\[NAS-JOB\](?: [A-Za-z0-9_-]{1,80})?", issue.get("title", "")):
        raise Rejected("NOT_A_JOB")
    job = parse_job(issue.get("body"))
    author = issue.get("user", {})
    if type(issue.get("number")) is not int or type(author.get("id")) is not int or not re.fullmatch(r"[A-Za-z0-9-]{1,39}", author.get("login", "")):
        raise Rejected("INVALID_ISSUE_IDENTITY")
    envelope = {"repository": repository, "issue_number": issue["number"], "requester_id": author["id"], "job": job}
    return job, digest(envelope)


def authorized(policy, job, request_hash, author_id, *, edited=False):
    """ラベルは参照しない。ローカルの固定範囲と、個別承認/初回用grantだけを使う。"""
    if author_id not in policy["requester_ids"] or job["job_type"] not in policy["job_types"] or job["commit_sha"] not in policy["approved_commits"]:
        return False
    if job["job_type"] == "ab_comparison" and job["commit_sha"] != COMPARISON_SHA:
        return False
    if job["job_type"] in {"issue16_acceptance", "full_feature_audit", "ab_comparison"}:
        entry = policy["inputs"].get(job["input_id"])
        if not entry or entry["raceday"] != job["raceday"]:
            return False
    if request_hash in policy["approved_requests"]:
        return True
    if edited or job["job_type"] == "checkpoint_smoke":
        return False  # 同一Issueの編集を広いgrantで再承認しない。
    return any(g["job_type"] == job["job_type"] and g["commit_sha"] == job["commit_sha"]
               and g["input_id"] == job["input_id"] and g["from_date"] <= job["raceday"] <= g["through_date"]
               and timestamp(g["expires_at"]) > utcnow() for g in policy.get("grants", []))
