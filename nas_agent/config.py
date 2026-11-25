"""パス・運用範囲は管理者ファイルからのみ読み込む。"""
import os
from pathlib import Path
import re

from .files import isolated, private_json
from .schema import REPOSITORY, TYPES, Rejected

CODE_ROOT = Path(__file__).resolve().parents[1]


def load_config(path, role):
    if os.name == "posix" and os.geteuid() == 0:
        raise Rejected("NONROOT_AGENT_REQUIRED")
    if role == "poller" and os.name == "posix" and os.access("/var/run/docker.sock", os.W_OK):
        raise Rejected("POLLER_MUST_NOT_HAVE_DOCKER_ACCESS")
    config = private_json(path)
    if config.get("repository") != REPOSITORY or config.get("role") != role or not re.fullmatch(r"[0-9a-f]{16}", config.get("instance_id", "")):
        raise Rejected("INVALID_LOCAL_CONFIG")
    if not re.fullmatch(r"[0-9a-f]{40}", config.get("agent_commit", "")):
        raise Rejected("INVALID_AGENT_COMMIT")
    for key in ("state_dir", "spool_dir"):
        if not Path(config[key]).is_absolute():
            raise Rejected("CONFIG_REQUIRES_ABSOLUTE_PATHS")
    if config.get("token_file") and not Path(config["token_file"]).is_absolute():
        raise Rejected("CONFIG_REQUIRES_ABSOLUTE_PATHS")
    if not 15 <= config.get("poll_seconds", 60) <= 3600:
        raise Rejected("INVALID_POLL_INTERVAL")
    if role == "worker":
        for key in ("policy_file", "checkout_root", "validation_root", "validation_python"):
            if not Path(config[key]).is_absolute():
                raise Rejected("CONFIG_REQUIRES_ABSOLUTE_PATHS")
        policy = load_policy(config["policy_file"])
        protected = [CODE_ROOT, policy["production_root"], *[entry["root"] for entry in policy["inputs"].values()],
                     *[entry["reference_root"] for entry in policy["inputs"].values() if entry.get("reference_root")]]
        roots = [isolated(config[key], protected) for key in ("state_dir", "spool_dir", "checkout_root", "validation_root")]
        for index, left in enumerate(roots):
            isolated(left, roots[index + 1:])
    else:
        isolated(config["state_dir"], [CODE_ROOT, "/volume1/docker/keiba_yosou_2026_nas"])
    state = Path(config["state_dir"])
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    if state.is_symlink() or (os.name == "posix" and (state.stat().st_uid != os.geteuid() or state.stat().st_mode & 0o077)):
        raise Rejected("UNSAFE_STATE_PERMISSIONS")
    if role == "worker":
        for key in ("checkout_root", "validation_root"):
            root = Path(config[key])
            root.mkdir(parents=True, exist_ok=True, mode=0o700)
            if root.is_symlink() or (os.name == "posix" and (root.stat().st_uid != os.geteuid() or root.stat().st_mode & 0o077)):
                raise Rejected("UNSAFE_PRIVATE_ROOT_PERMISSIONS")
    return config


def load_policy(path):
    policy = private_json(path)
    if policy.get("schema_version") != 1 or policy.get("repository") != REPOSITORY:
        raise Rejected("INVALID_POLICY")
    if not set(policy["job_types"]) <= TYPES or not all(type(x) is int and x > 0 for x in policy["requester_ids"]):
        raise Rejected("INVALID_POLICY")
    if not all(re.fullmatch(r"[0-9a-f]{40}", x) for x in policy["approved_commits"]):
        raise Rejected("INVALID_POLICY")
    if not all(re.fullmatch(r"[0-9a-f]{64}", x) for x in policy["approved_requests"]):
        raise Rejected("INVALID_POLICY")
    if not Path(policy["production_root"]).is_absolute():
        raise Rejected("INVALID_POLICY")
    if type(policy.get("allow_build", False)) is not bool:
        raise Rejected("INVALID_BUILD_APPROVAL")
    for grant in policy.get("grants", []):
        from .schema import timestamp
        if set(grant) != {"job_type", "commit_sha", "input_id", "from_date", "through_date", "expires_at"} or grant["job_type"] not in TYPES or grant["commit_sha"] not in policy["approved_commits"] or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", grant["input_id"]):
            raise Rejected("INVALID_PREAPPROVAL_GRANT")
        if not all(re.fullmatch(r"\d{8}", grant[key]) for key in ("from_date", "through_date")) or grant["from_date"] > grant["through_date"]:
            raise Rejected("INVALID_PREAPPROVAL_GRANT")
        timestamp(grant["expires_at"])
    for name, entry in policy["inputs"].items():
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", name) or not Path(entry["root"]).is_absolute() or not re.fullmatch(r"[0-9a-f]{64}", entry["manifest_sha256"]):
            raise Rejected("INVALID_INPUT_POLICY")
        if not re.fullmatch(r"\d{8}", entry["raceday"]):
            raise Rejected("INVALID_INPUT_POLICY")
        if entry.get("reference_root") and (not Path(entry["reference_root"]).is_absolute() or not re.fullmatch(r"[0-9a-f]{64}", entry.get("reference_manifest_sha256", ""))):
            raise Rejected("INVALID_REFERENCE_POLICY")
    limits = policy["limits"]
    if not 30 <= limits["timeout_seconds"] <= 14400 or not 0.1 <= limits["cpus"] <= 4 or not 256 <= limits["memory_mb"] <= 4096 or not 32 <= limits["pids"] <= 1024:
        raise Rejected("INVALID_RESOURCE_LIMITS")
    window = policy.get("execution_window_jst")
    if window is not None:
        if set(window) != {"start", "end"} or not all(re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", value) for value in window.values()) or window["start"] >= window["end"]:
            raise Rejected("INVALID_EXECUTION_WINDOW")
    return policy
