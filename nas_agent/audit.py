"""固定監査CLIと公開用の集計。入力と明細はNAS内に留める。"""
import csv
from datetime import datetime
import hashlib
import json
from pathlib import Path

from .files import beneath
from .schema import Rejected

REQUIREMENTS = tuple(f"D-{i:02d}" for i in range(1, 13))
EXIT_CODES = {"observed_with_limits": 0, "partial": 0, "no_real_data": 2, "startup_error": 2}


def verify_fixture(entry):
    root = Path(entry["root"]).resolve(strict=True)
    manifest = beneath(root, root / "manifest.json")
    with manifest.open("rb") as handle:
        actual = hashlib.file_digest(handle, "sha256").hexdigest()
    if actual != entry["manifest_sha256"]:
        raise Rejected("INPUT_MANIFEST_MISMATCH")
    return root


def audit_args(python, source, fixture, output, raceday):
    day = datetime.strptime(raceday, "%Y%m%d").date().isoformat()
    args = [python, str(source / "scripts/full_feature_audit.py"), "--data-dir", str(fixture),
            "--output-dir", str(output), "--start-date", day, "--end-date", day]
    settled = beneath(fixture, fixture / "settled_bets.csv")
    if settled.is_file():
        args.extend(["--settled-bets", str(settled)])
    return args


def observation_counts(output):
    races, horses, history = set(), set(), set()
    for name in ("real_audit_details.csv", "feature_audit_values.csv"):
        with beneath(output, output / name).open(encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                if row["race_id"]:
                    race = (row["date"], row["race_id"])
                    races.add(race)
                    if row["horse_number"] or row["horse"]:
                        horses.add((*race, row["horse_number"] or row["horse"]))
                if row.get("metric") == "date_parsing" and row["history_row"]:
                    history.add((row["file"], row["race_id"], row["horse_number"], row["horse"], row["history_row"]))
    return {"races": len(races), "horses": len(horses), "past_runs": len(history)}


def requirement_counts(output):
    counts = {req: {"evaluated": 0, "issues": 0} for req in REQUIREMENTS}
    with beneath(output, output / "real_audit_summary.csv").open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["requirement"] in counts:
                evaluated, issues = int(row["evaluated"]), int(row["issues"])
                if not 0 <= issues <= evaluated:
                    raise ValueError("INVALID_AUDIT_COUNTS")
                count = counts[row["requirement"]]
                count["evaluated"] += evaluated
                count["issues"] += issues
    return counts


def audit_result(output, code, elapsed):
    summary = {"audit_status": "invalid_manifest", "exit_code": code, "elapsed_seconds": elapsed}
    result = {"summary": summary, "error_code": "AUDIT_INVALID_MANIFEST"}
    try:
        manifest = json.loads(beneath(output, output / "run_manifest.json").read_text(encoding="utf-8"))
        status = manifest["status"]
        expected = EXIT_CODES[status]
    except (OSError, ValueError, KeyError, TypeError):
        return result
    summary["audit_status"] = status
    if code != expected:
        result["error_code"] = "AUDIT_EXIT_STATUS_MISMATCH"
    elif status == "startup_error":
        result["error_code"] = "AUDIT_STARTUP_ERROR"
    else:
        try:
            summary.update(observation_counts(output), requirements=requirement_counts(output))
        except (OSError, ValueError, KeyError, TypeError, csv.Error):
            result["error_code"] = "AUDIT_INVALID_SUMMARY"
        else:
            result["error_code"] = {
                "observed_with_limits": None, "partial": "AUDIT_PARTIAL", "no_real_data": "AUDIT_NO_REAL_DATA",
            }[status]
    return result
