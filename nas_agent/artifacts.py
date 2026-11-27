"""Local recovery bundles and opt-in handoff to an administrator provisioned directory."""
import json
import os
from pathlib import Path
import re
import shutil
import stat
from tempfile import TemporaryDirectory

from .comparison import ARTIFACTS, sha256
from .files import atomic_json, beneath, isolated
from .report import record
from .schema import Rejected

METADATA = {"nas-agent-status.json", "failure-diagnostics.json", "execution.log"}
AUDIT_FILES = {
    "run_manifest.json", "real_audit_details.csv", "feature_audit_values.csv",
    "real_audit_summary.csv", "feature_audit_summary.csv", "full_feature_audit_results.csv",
    "condition_stats.csv", "data_quality_audit.md", "race_schedule_audit.md",
    "time_speed_audit.md", "jockey_condition_audit.md", "integration_audit.md", "full_feature_audit.md",
}
FILES = {
    "healthcheck": set(),
    "checkpoint_smoke": {"result.json", "checkpoint.json"},
    "full_feature_audit": AUDIT_FILES,
    "ab_comparison": ARTIFACTS,
    "issue16_acceptance": {"candidate_manifest.json", "prediction.xlsx", "windows_linux.json"},
    "prepare_fixture": {"manifest.json", "run.json", "snapshots.json", "results.json"},
}
# Input archives and raw subprocess logs stay in the validation workspace.
TOKEN_PATTERN = re.compile(r"(?:github_pat_|gh[pousr]_)[A-Za-z0-9_]+|(?:Bearer|Basic)\s+\S+", re.I)


def validate_exchange(policy):
    exchange = policy.get("artifact_exchange")
    if exchange is None:
        return
    if (not isinstance(exchange, dict) or set(exchange) != {"root", "destination", "jobs"}
            or not isinstance(exchange["root"], str) or not isinstance(exchange["destination"], str)
            or not isinstance(exchange["jobs"], dict)):
        raise Rejected("INVALID_ARTIFACT_EXCHANGE")
    root = Path(exchange["root"])
    destination = Path(exchange["destination"])
    if (not root.is_absolute() or root.resolve() != root or destination.resolve() != destination
            or destination == root or not destination.is_relative_to(root)):
        raise Rejected("INVALID_ARTIFACT_EXCHANGE")
    protected = [policy["production_root"]]
    for entry in policy["inputs"].values():
        protected.extend(entry[key] for key in ("root", "reference_root") if entry.get(key))
    isolated(root, protected)
    for kind, names in exchange["jobs"].items():
        if (kind not in FILES or not isinstance(names, list)
                or any(not isinstance(name, str) or name not in FILES[kind] | METADATA for name in names)):
            raise Rejected("INVALID_ARTIFACT_ALLOWLIST")


def sources(config, job):
    kind, job_id = job["job_type"], job["job_id"]
    if kind in {"healthcheck", "checkpoint_smoke"}:
        root = Path(config["state_dir"]) / "jobs" / job_id
        return {name: root / name for name in FILES[kind]}, root / "execution.log"
    if "validation_root" not in config:
        return {}, None
    root = Path(config["validation_root"]) / job_id
    output = root / "output"
    if kind == "ab_comparison" and (output / "reference").is_dir():
        output = output / "reference"
    if kind == "issue16_acceptance":
        candidates = sorted((root / "runs").glob("*/manifest.json"))
        paths = {"windows_linux.json": root / "comparisons/windows_linux.json"}
        if len(candidates) == 1:
            paths.update({"candidate_manifest.json": candidates[0], "prediction.xlsx": candidates[0].with_name("prediction.xlsx")})
        return paths, Path(config["state_dir"]) / "jobs" / job_id / "execution.log"
    return {name: output / name for name in FILES[kind]}, root / "execution.log"


def regular(path):
    return path.resolve() == path and path.is_file() and path.stat().st_nlink == 1


def checksums(directory):
    return {path.name: sha256(path) for path in sorted(directory.iterdir())
            if path.name != "SHA256SUMS" and regular(path)}


def checksum_text(directory, *, hashes=None):
    values = checksums(directory) if hashes is None else hashes
    return "".join(f"{value}  {name}\n" for name, value in values.items())


def manifest(directory):
    (directory / "SHA256SUMS").write_text(checksum_text(directory), encoding="ascii")


def safe_log(path, row, token):
    lines = [f"{event['at']} {event['status']} {event['error_code'] or ''}" for event in row["history"]]
    if path is not None and regular(path):
        with path.open("rb") as log:
            # Keep the tail of long logs. Replace an incomplete first line.
            offset = max(0, path.stat().st_size - 1024 * 1024)
            log.seek(offset)
            if offset:
                log.readline(1024 * 1024)
            raw = log.read(1024 * 1024).decode("utf-8", errors="replace")
        if token:
            raw = raw.replace(token, "[REDACTED]")
        lines.append(TOKEN_PATTERN.sub("[REDACTED]", raw))
    return "\n".join(lines) + "\n"


def bundle(config, row, diagnostic, token=None, *, pending_handoff=False):
    job = json.loads(row["job"])
    root = Path(config["state_dir"]) / "recovery" / job["job_id"]
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    target = beneath(root, root / ("pending-handoff" if pending_handoff else row["status"]))
    if target.is_dir():
        return target
    paths, log = sources(config, job)
    with TemporaryDirectory(prefix=".bundle-", dir=root) as temp:
        staged = Path(temp) / "bundle"
        staged.mkdir(mode=0o700)
        if ((pending_handoff or row["status"] != "PENDING_FINALIZATION")
                and row["error_code"] != "SAVED_ARTIFACT_SHA_MISMATCH"
                and (root / "PENDING_FINALIZATION").is_dir()):
            saved = root / "PENDING_FINALIZATION"
            paths = {name: saved / name for name in paths}
        for name, path in paths.items():
            if regular(path):
                shutil.copyfile(path, staged / name)
        (staged / "execution.log").write_text(safe_log(log, row, token), encoding="utf-8")
        atomic_json(staged / "nas-agent-status.json", record(config, row, include_pending=True), mode=0o600)
        if diagnostic or row["error_code"]:
            fallback = {"phase": "execution", "code": row["error_code"],
                        "http_status": None, "rate_limited": False, "transport": None}
            atomic_json(staged / "failure-diagnostics.json", {
                "job_id": job["job_id"], "issue_number": row["issue_number"],
                "failure": diagnostic or fallback, "error_code": row["error_code"],
            }, mode=0o600)
        manifest(staged)
        staged.rename(target)
    return target


def handoff(config, policy, row, source):
    exchange = policy.get("artifact_exchange")
    if exchange is None:
        return
    job = json.loads(row["job"])
    names = exchange["jobs"].get(job["job_type"])
    if not names:
        return
    root = Path(exchange["root"])
    destination = beneath(root, Path(exchange["destination"]))
    isolated(root, [Path(__file__).resolve().parents[1],
                    *(config[key] for key in ("state_dir", "spool_dir", "checkout_root", "validation_root") if key in config)])
    if not destination.is_dir() or destination != Path(exchange["destination"]):
        raise Rejected("EXCHANGE_DESTINATION_NOT_PROVISIONED")
    info = destination.stat()
    if os.name == "posix" and (info.st_uid not in {0, os.geteuid()} or info.st_mode & 0o002):
        raise Rejected("UNSAFE_EXCHANGE_PERMISSIONS")
    target = beneath(destination, destination / f"{job['job_id']}--{row['status'].lower()}")
    with TemporaryDirectory(prefix=".nas-agent-", dir=destination) as temp:
        staged = Path(temp) / "bundle"
        staged.mkdir(mode=0o750)
        os.chmod(staged, 0o2750 if info.st_mode & stat.S_ISGID else 0o750)
        if os.name == "posix" and info.st_mode & stat.S_ISGID:
            os.chown(staged, -1, info.st_gid)
        for name in sorted(set(names)):
            path = beneath(source, source / name)
            if regular(path):
                shutil.copyfile(path, staged / name)
        manifest(staged)
        for path in staged.iterdir():
            os.chmod(path, 0o640)
        if target.exists():
            if (not target.is_dir() or checksums(target) != checksums(staged)
                    or set(p.name for p in target.iterdir()) != set(p.name for p in staged.iterdir())
                    or not regular(target / "SHA256SUMS")
                    or (target / "SHA256SUMS").read_bytes() != (staged / "SHA256SUMS").read_bytes()):
                raise Rejected("EXCHANGE_BUNDLE_CONFLICT")
            return
        staged.rename(target)
