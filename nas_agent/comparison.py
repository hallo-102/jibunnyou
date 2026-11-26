"""Offline comparison fixture and artifact contract (Issue #35)."""
from datetime import date, timezone, timedelta
import hashlib
from pathlib import Path
import re
import shutil

from .files import beneath, spool_json
from .schema import Rejected, timestamp

JST = timezone(timedelta(hours=9))
SNAPSHOT_FILES = ("workbook", "race_levels", "base_time", "odds_csv")
CONTROL_FILES = {"run.json", "snapshots.json", "results.json"}
MAX_JSON_BYTES = 4 * 1024 * 1024
MAX_INPUT_FILES = 256
MAX_INPUT_BYTES = 1024 * 1024 * 1024
AB_MODE = "single_day_ab"
MAX_ARTIFACT_FILES = 11
MAX_ARTIFACT_BYTES = 64 * 1024 * 1024
MAX_ARTIFACT_TOTAL = 128 * 1024 * 1024
ARTIFACTS = {"manifest.json"} | {
    f"{name}.{extension}"
    for name in ("comparison", "breakdown", "races", "diagnostics", "exclusions")
    for extension in ("json", "csv")
}
REQUIRED_ARTIFACTS = {"manifest.json", "comparison.json", "comparison.csv", "exclusions.json"}


def sha256(path):
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def archive_path(root, name):
    if (not isinstance(name, str) or not name.startswith("archive/") or "\\" in name
            or any(part in {"", ".", ".."} for part in name.split("/"))):
        raise Rejected("INVALID_COMPARISON_ARCHIVE_PATH")
    path = beneath(root, root / name)
    if not path.is_file():
        raise Rejected("COMPARISON_INPUT_MISSING")
    return path


def validate_rows(snapshots, results, day, *, legacy=True):
    if not isinstance(snapshots, list) or not snapshots or not isinstance(results, list):
        raise Rejected("INVALID_COMPARISON_ROWS")
    starts = {}
    for row in snapshots:
        race_id = row["race_id"]
        if not re.fullmatch(r"[0-9]{10,13}", race_id) or race_id in starts:
            raise Rejected("INVALID_COMPARISON_RACE_ID")
        start = timestamp(row["start_at"])
        grade = row.get("input_evidence_grade", "A" if legacy else "C")
        if (grade not in {"A", "B", "C", "D"} or start.astimezone(JST).date() != day
                or (grade in {"A", "B"} and timestamp(row.get("observed_at")) >= start)):
            raise Rejected("INVALID_COMPARISON_SNAPSHOT_TIME")
        starts[race_id] = start
    seen = set()
    for row in results:
        race_id = row["race_id"]
        grade = row.get("evidence_grade", "A" if row.get("finalized_at") is not None else "D")
        if (race_id not in starts or race_id in seen or grade not in {"A", "B", "C", "D"}
                or (grade == "A" and timestamp(row.get("finalized_at")) <= starts[race_id])):
            raise Rejected("INVALID_COMPARISON_RESULT")
        seen.add(race_id)


def validate_evidence(row, records):
    def has_source(proof):
        source = proof.get("source")
        return isinstance(source, str) and bool(source.strip())

    observed = timestamp(row["observed_at"])
    for key in SNAPSHOT_FILES:
        if key not in row:
            continue
        proof = records.get(key, {})
        captured = row["observed_at"] if key == "workbook" else row.get(f"{key}_observed_at")
        if (not has_source(proof)
                or proof.get("sha256") != row[f"{key}_sha256"]
                or proof.get("observed_at") != captured
                or timestamp(captured) > observed):
            raise Rejected("COMPARISON_CAPTURE_EVIDENCE_MISMATCH")
    for key in ("popularity", "dl"):
        if not row.get(key):
            continue
        proof = records.get(key, {})
        captured = row.get(f"{key}_observed_at", row["observed_at"] if key == "popularity" else None)
        if (not has_source(proof) or proof.get("observed_at") != captured
                or proof.get("values") != row[key] or timestamp(captured) > observed):
            raise Rejected("COMPARISON_CAPTURE_EVIDENCE_MISMATCH")
        if key == "dl" and any(proof.get(field) != row.get(field) for field in
                               ("dl_model_sha256", "dl_model_training_end_at")):
            raise Rejected("COMPARISON_CAPTURE_EVIDENCE_MISMATCH")


def validate_run(settings, day):
    # The fixed checkout supplies the GitHub baseline and its repository-pinned weights.
    allowed = {"schema_version", "mode", "methods", "snapshots", "results", "periods",
               "unit_yen", "box_size", "top_payout_races", "evaluation_mode"}
    if (not isinstance(settings, dict) or settings.keys() - allowed
            or type(settings.get("schema_version")) is not int or settings["schema_version"] not in (1, 2)
            or settings.get("evaluation_mode", "formal") not in ("formal", "reference")
            or settings.get("snapshots") != "snapshots.json" or settings.get("results") != "results.json"):
        raise Rejected("INVALID_COMPARISON_RUN")
    if (settings.get("mode") != AB_MODE or settings.get("methods") != ["A", "B"]
            or settings.get("periods") != {"validation": [day.isoformat(), day.isoformat()]}):
        raise Rejected("INVALID_COMPARISON_SINGLE_DAY_MODE")


def stage_fixture(entry, day, staged, control):
    root = Path(entry["root"]).resolve(strict=True)
    manifest = beneath(root, root / "manifest.json")
    if not manifest.is_file() or manifest.stat().st_size > MAX_JSON_BYTES or sha256(manifest) != entry["manifest_sha256"]:
        raise Rejected("INPUT_MANIFEST_MISMATCH")
    day = date.fromisoformat(f"{day[:4]}-{day[4:6]}-{day[6:]}")
    staged.mkdir()
    shutil.copyfile(manifest, staged / manifest.name)
    if sha256(staged / manifest.name) != entry["manifest_sha256"]:
        raise Rejected("INPUT_MANIFEST_MISMATCH")
    metadata = spool_json(staged, staged / manifest.name, MAX_JSON_BYTES)
    if not isinstance(metadata, dict) or type(metadata.get("schema_version")) is not int:
        raise Rejected("INVALID_COMPARISON_MANIFEST")
    evidence_name = metadata.get("capture_evidence")
    if "files_sha256" in metadata:
        # The approved PR #40 preparation format pins inputs in one flat map.
        hashes = metadata["files_sha256"]
        if (metadata["schema_version"] not in (1, 2) or not isinstance(hashes, dict)
                or not CONTROL_FILES <= hashes.keys()
                or any(key in metadata for key in ("capture_evidence", "original_files_sha256", "generated_files_sha256"))):
            raise Rejected("INVALID_COMPARISON_MANIFEST")
        generated = {name: hashes[name] for name in CONTROL_FILES}
    else:
        originals = metadata.get("original_files_sha256")
        generated = metadata.get("generated_files_sha256")
        if (metadata["schema_version"] != 2 or metadata.get("raceday") != day.isoformat()
                or not isinstance(originals, dict) or not isinstance(generated, dict)
                or generated.keys() != CONTROL_FILES or originals.keys() & CONTROL_FILES
                or not isinstance(evidence_name, str) or evidence_name not in originals):
            raise Rejected("INVALID_COMPARISON_MANIFEST")
        hashes = {**originals, **generated}
    if len(hashes) > MAX_INPUT_FILES:
        raise Rejected("INVALID_COMPARISON_MANIFEST")

    total = 0
    for name, expected in hashes.items():
        control()
        path = beneath(root, root / name) if name in CONTROL_FILES else archive_path(root, name)
        if not path.is_file() or not isinstance(expected, str) or not re.fullmatch(r"[0-9a-f]{64}", expected):
            raise Rejected("INVALID_COMPARISON_MANIFEST")
        remaining = MAX_INPUT_BYTES - total
        limit = min(remaining, MAX_JSON_BYTES) if name in CONTROL_FILES or name == evidence_name else remaining
        if path.stat().st_size > limit:
            raise Rejected("COMPARISON_INPUT_LIMIT")
        dest = staged / name
        dest.parent.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256()
        with path.open("rb") as source, dest.open("xb") as output:
            while chunk := source.read(1024 * 1024):
                control()
                limit -= len(chunk)
                if limit < 0:
                    raise Rejected("COMPARISON_INPUT_LIMIT")
                output.write(chunk)
                digest.update(chunk)
                total += len(chunk)
        if digest.hexdigest() != expected:
            raise Rejected("COMPARISON_INPUT_SHA_MISMATCH")

    settings = spool_json(staged, staged / "run.json", MAX_JSON_BYTES)
    snapshots = spool_json(staged, staged / "snapshots.json", MAX_JSON_BYTES)
    results = spool_json(staged, staged / "results.json", MAX_JSON_BYTES)
    validate_run(settings, day)
    if "files_sha256" in metadata and metadata["schema_version"] != settings["schema_version"]:
        raise Rejected("INVALID_COMPARISON_MANIFEST")
    legacy = settings["schema_version"] == 1
    validate_rows(snapshots, results, day, legacy=legacy)
    evidence = spool_json(staged, staged / evidence_name, MAX_JSON_BYTES) if evidence_name else None
    if evidence_name is not None and not isinstance(evidence, dict):
        raise Rejected("INVALID_COMPARISON_MANIFEST")
    for row in snapshots:
        default_grade = "A" if legacy else "C"
        if "workbook" not in row and (evidence is not None or row.get("input_evidence_grade", default_grade) in {"A", "B"}):
            raise Rejected("COMPARISON_WORKBOOK_REQUIRED")
        for key in SNAPSHOT_FILES:
            if key in row:
                name = row[key]
                archive_path(staged, name)
                if row.get(f"{key}_sha256") != hashes.get(name):
                    raise Rejected("COMPARISON_INPUT_SHA_MISMATCH")
        if evidence is not None:
            validate_evidence(row, evidence.get(row["race_id"], {}))
    return {**generated, "manifest.json": entry["manifest_sha256"]}


def comparison_result(output, commit, input_hashes, code, evaluation_mode):
    paths = {}
    total = 0
    for path in output.iterdir():
        if len(paths) >= MAX_ARTIFACT_FILES or path.name not in ARTIFACTS or path.is_symlink() or not path.is_file():
            raise Rejected("INVALID_COMPARISON_ARTIFACT")
        size = path.stat().st_size
        total += size
        if size > MAX_ARTIFACT_BYTES or total > MAX_ARTIFACT_TOTAL:
            raise Rejected("COMPARISON_ARTIFACT_LIMIT")
        paths[path.name] = path
    if code not in (0, 2) or not REQUIRED_ARTIFACTS <= paths.keys():
        raise Rejected("COMPARISON_COMMAND_FAILED")
    manifest = spool_json(output, paths["manifest.json"], MAX_JSON_BYTES)
    digests = {name: sha256(path) for name, path in sorted(paths.items())}
    expected_inputs = {"config_sha256": "run.json", "snapshots_sha256": "snapshots.json", "results_sha256": "results.json",
                       "fixture_manifest_sha256": "manifest.json"}
    if (manifest.get("mode") != AB_MODE or manifest["code_sha"] != commit
            or manifest.get("evaluation_mode") != evaluation_mode
            or any(manifest["inputs"].get(key) != input_hashes[name] for key, name in expected_inputs.items())
            or manifest["output_sha256"] != {name: value for name, value in digests.items() if name != "manifest.json"}):
        raise Rejected("COMPARISON_OUTPUT_SHA_MISMATCH")
    return {"summary": {"evaluation_mode": evaluation_mode}, "artifacts_sha256": digests,
            "error_code": "COMPARISON_PARITY_FAILED" if code else None}
