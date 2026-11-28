"""開催日単位の再開に対応する、9方式の固定重み比較（Issue #53）。"""

import argparse
from collections import Counter
from contextlib import closing
import gzip
import json
import os
from pathlib import Path
import platform
import sqlite3
import sys
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd

from model_registry import sha256_file
from tools.ab_comparison_diagnostics import git
from tools.ab_comparison_evidence import timestamp
from tools.ab_comparison_inputs import BET_TYPES, pipeline, score
from tools.ab_comparison_metrics import JUDGE_BUY, common_purchase, purchase_decision, race_metrics, summarize
from tools.compare_ab_models import FEAT_COLS, PROJECT_ROOT, baselines_from, read_json, save_csv, save_json
from tools.period_comparison_features import METHODS, period_builders
from tools.period_comparison_inputs import AuditInputs, PERIODS, day_features, digest, inventory

UNIT_YEN = 100
BOX_SIZE = 5
CACHE_PAIRS = {
    "legacy_cache_effect": ("M00", "legacy_no_cache"),
    "corrected_cache_effect": ("corrected_with_cache", "M04"),
}
FRAME_LABELS = {"legacy": "M00", "corrected": "M04"}
LABELS = (*METHODS, "legacy_no_cache", "corrected_with_cache")
COVERAGE_KEYS = (
    "inventory_races", "feature_races", "finish_and_ticket_payout_matched",
    "all_reported_payouts_matched", "formal_revenue_races", "reference_revenue_races",
)


def atomic_json(path, data):
    with TemporaryDirectory(dir=path.parent, prefix=".write-") as temp:
        candidate = Path(temp) / path.name
        save_json(candidate, data)
        os.replace(candidate, path)


def write_bundle(path, identity, build):
    """1開催日分の全方式を一度に公開する。不完全な一時出力は参照しない。"""
    if path.exists():
        manifest = read_json(path / "manifest.json")
        if manifest["identity"] != identity:
            raise ValueError(f"保存済み実行のSHAが一致しません: {path}")
        for name, expected in manifest["files_sha256"].items():
            if sha256_file(path / name) != expected:
                raise ValueError(f"保存済み成果物のSHAが一致しません: {path / name}")
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=path.parent, prefix=".day-") as temp:
        candidate = Path(temp) / "bundle"
        candidate.mkdir()
        build(candidate)
        save_json(candidate / "manifest.json", {
            "identity": identity,
            "files_sha256": {p.name: sha256_file(p) for p in sorted(candidate.iterdir())},
        })
        candidate.rename(path)
    return True


def write_frame(path, frame):
    # 通常の JSON エンコーダーで float を往復させる。欠損と空文字を区別する。
    values = frame.astype(object).where(frame.notna(), None).to_numpy().tolist()
    table = {"columns": list(frame.columns), "dtypes": frame.dtypes.astype(str).tolist(), "rows": values}
    content = json.dumps(table, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
    path.write_bytes(gzip.compress(content, compresslevel=3, mtime=0))


def read_frame(path):
    with gzip.open(path, "rt", encoding="utf-8") as source:
        table = json.load(source)
    return pd.DataFrame(table["rows"], columns=table["columns"]).astype(
        dict(zip(table["columns"], table["dtypes"], strict=True))
    )


def result_cohort(record):
    left, right = record["input_grade"], record["result"]["evidence"]["grade"]
    if left == right == "A":
        return "formal"
    if left in "AB" and right in "AB":
        return "reference"
    return None


def coverage(records, horses, reasons):
    counts = Counter(dict.fromkeys(COVERAGE_KEYS, 0))
    input_grades, result_grades = Counter(), Counter()
    for rid, record in records.items():
        counts["inventory_races"] += 1
        input_grades[record["input_grade"]] += 1
        result = record["result"]
        result_grades[result["evidence"]["grade"]] += 1
        if rid not in horses:
            continue
        counts["feature_races"] += 1
        payouts = result["payouts"]
        if result["evidence"]["grade"] != "D" and result["top"] and "3連複" in payouts:
            counts["finish_and_ticket_payout_matched"] += 1
            if set(BET_TYPES) <= payouts.keys():
                counts["all_reported_payouts_matched"] += 1
            cohort = result_cohort(record)
            if cohort and horses[rid] >= BOX_SIZE:
                counts[f"{cohort}_revenue_races"] += 1
    return {
        **counts, "input_evidence_grade": max(input_grades, default="D"),
        "result_evidence_grade": max(result_grades, default="D"),
        "input_race_grades": dict(input_grades), "result_race_grades": dict(result_grades),
        "reasons": dict(reasons),
    }


def save_features(path, inputs, day, item, builders):
    records, frames, reasons = day_features(inputs, day, item, builders)
    horses = {rid: len(variants["legacy"]) for rid, variants in frames.items()}
    save_json(path / "coverage.json", coverage(records, horses, reasons))
    save_json(path / "evidence.json", records)
    for label in builders.keys() | {"corrected_with_cache"}:
        combined = pd.concat([variants[label] for variants in frames.values()], ignore_index=True) if frames else pd.DataFrame()
        write_frame(path / f"{FRAME_LABELS.get(label, label)}.json.gz", combined)


def changes(frame, reference, ranked, ref_ranked, decision, ref_decision):
    left, right = (part.set_index("馬番").sort_index() for part in (frame, reference))
    equal = np.isclose(left[FEAT_COLS].to_numpy(dtype=float), right[FEAT_COLS].to_numpy(dtype=float),
                       rtol=0, atol=1e-9, equal_nan=True)
    changed = {col: int(n) for col, n in zip(FEAT_COLS, (~equal).sum(axis=0), strict=True) if n}
    scores, base = (part.set_index("馬番").sort_index() for part in (ranked, ref_ranked))
    top, ref_top = (part["馬番"].head(5).tolist() for part in (ranked, ref_ranked))
    return {
        "changed_feature_cells": changed,
        "score_changes": int((~np.isclose(scores["score_raw"], base["score_raw"], atol=1e-9, rtol=0)).sum()),
        "rank_changes": int(scores["rank"].ne(base["rank"]).sum()),
        "top1_changed": top[0] != ref_top[0],
        "top5_order_changed": top != ref_top,
        "top5_membership_changed": set(top) != set(ref_top),
        "purchase_decision_changed": None if decision is None else decision != ref_decision,
    }


def decisions_for(rid, record, ranked, assessment):
    if record["input_grade"] not in "AB":
        return dict.fromkeys(ranked)
    race = {key: timestamp(record[key + "_at"]) for key in ("start", "observed", "pop_observed")}
    return {label: purchase_decision(frame, race, rid, assessment) for label, frame in ranked.items()}


def score_day(path, features_path, day, stage, weights, assessment):
    records = read_json(features_path / "evidence.json")
    frames = {}
    for label in LABELS:
        table = read_frame(features_path / f"{label}.json.gz")
        frames[label] = dict(tuple(table.groupby("rid_str", sort=False))) if not table.empty else {}

    races, diagnostics = [], []
    score_frames = {label: [] for label in LABELS}
    for rid, record in sorted(records.items(), key=lambda item: (item[1]["start_at"] or "", item[0])):
        if rid not in frames["M00"]:
            continue
        methods = {label: variants[rid] for label, variants in frames.items()}
        ranked = {label: score(frame, weights).sort_values("rank") for label, frame in methods.items()}
        for label, frame in ranked.items():
            # Scores and ranks are separate from reusable features.
            score_frames[label].append(frame[["rid_str", "馬番", "score_raw", "rank"]])
        decisions = decisions_for(rid, record, ranked, assessment)
        shared = common_purchase(decisions[label] for label in METHODS)
        result = record["result"] if result_cohort(record) else None
        race = {"race_id": rid, "date": day.isoformat(), "start_at": record["start_at"], "stage": stage,
                "input_evidence_grade": record["input_grade"], "result_evidence_grade": record["result"]["evidence"]["grade"]}
        for label in METHODS:
            metrics = race_metrics(ranked[label], result, UNIT_YEN, BOX_SIZE)
            if metrics["ticket_payout"] is None:
                metrics["ticket_investment"] = None
            races.append({
                **race, "method": label, "horses": len(methods[label]),
                "cohort": result_cohort(record), "common_purchase": shared, "purchase_decision": decisions[label],
                **metrics, **changes(methods[label], methods["M00"], ranked[label], ranked["M00"],
                                     decisions[label], decisions["M00"]),
            })
        for label, (variant, reference) in CACHE_PAIRS.items():
            diagnostics.append({
                **race, "comparison": label, "candidate": variant, "reference": reference,
                **changes(methods[variant], methods[reference], ranked[variant], ranked[reference],
                          decisions[variant], decisions[reference]),
            })

    for label, parts in score_frames.items():
        write_frame(path / f"{label}.json.gz", pd.concat(parts, ignore_index=True) if parts else pd.DataFrame())
    save_json(path / "races.json", races)
    save_json(path / "cache_diagnostics.json", diagnostics)


def change_totals(rows):
    cells = Counter()
    for row in rows:
        cells.update(row["changed_feature_cells"])
    return {
        "changed_feature_cells": dict(cells), "changed_feature_cell_total": sum(cells.values()),
        "race_sample": len(rows),
        "purchase_decision_sample": sum(row["purchase_decision_changed"] is not None for row in rows),
        "purchase_candidate_races": sum(row["purchase_decision"] == JUDGE_BUY for row in rows),
        "common_purchase_candidate_races": sum(row["common_purchase"] for row in rows),
        **{name: sum(row[name] or 0 for row in rows) for name in (
            "score_changes", "rank_changes", "top1_changed", "top5_order_changed",
            "top5_membership_changed", "purchase_decision_changed",
        )},
    }


def cohort_performance(rows):
    report = summarize(rows, UNIT_YEN, 0)
    settled = [row for row in rows if row["ticket_payout"] is not None]
    for label, subset in (
        ("filtered", [row for row in settled if row["common_purchase"]]),
        ("own_filtered", [row for row in settled if row["purchase_decision"] == JUDGE_BUY]),
    ):
        rates = summarize(subset, UNIT_YEN, 0)
        report.update({f"{label}_{key}": value for key, value in rates.items()
                       if key.startswith(("win_", "place_", "winner_in_top5_", "top5_"))})
    return report


def comparisons(races):
    summary = []
    for method in METHODS:
        method_rows = [row for row in races if row["method"] == method]
        for stage in ("all", *PERIODS):
            cohort = [row for row in method_rows if stage == "all" or stage == row["stage"]]
            for tier in ("formal", "reference"):
                rows = [row for row in cohort if row["cohort"] == tier]
                summary.append({"method": method, "stage": stage, "cohort": tier,
                                **cohort_performance(rows), **change_totals(rows)})
            summary.append({"method": method, "stage": stage, "cohort": "feature_diagnostics",
                            **change_totals(cohort)})
    return summary


def load_assessment():
    filter_path = str(PROJECT_ROOT / "race_purchase_filter")
    sys.path.append(filter_path)
    try:
        from race_purchase_filter import assessment
    finally:
        sys.path.remove(filter_path)
    return assessment


def run(inventory_path, output):
    days = inventory(inventory_path)
    if pipeline.SCORING_MODEL_VERSION != "legacy":
        raise ValueError("KEIBA_SCORING_MODEL_VERSION=legacy が必要です")
    if git(PROJECT_ROOT, "status", "--porcelain", "--untracked-files=no").strip():
        raise ValueError("実行コードのSHAを固定するため、追跡済みの差分をコミットしてください")
    git(PROJECT_ROOT, "ls-files", "--error-unmatch", str(Path(__file__)), *(
        str(Path(__file__).with_name(f"period_comparison_{part}.py")) for part in ("features", "inputs")
    ))
    code = git(PROJECT_ROOT, "rev-parse", "HEAD").decode().strip()
    weights, weight_metadata = baselines_from({}, PROJECT_ROOT)["github"]
    assessment = load_assessment()
    policy = assessment.policy_version(assessment.load_policy())
    runtime = {"python": platform.python_version(), "pandas": pd.__version__, "numpy": np.__version__}
    identity = {"code_sha": code, "inventory_sha256": sha256_file(inventory_path),
                "weights": weight_metadata, "purchase_policy": policy, "runtime": runtime}

    output.mkdir(parents=True, exist_ok=True)
    # SQLite の書込ロックは Windows/Linux 共通で、プロセス停止時に解放される。
    with closing(sqlite3.connect(output / ".run-lock.sqlite", timeout=0)) as lock:
        lock.execute("BEGIN IMMEDIATE")
        return run_days(days, AuditInputs(inventory_path.parent), output, identity, weights, assessment)


def run_days(days, inputs, output, identity, weights, assessment):
    descriptor = output / "run.json"
    if descriptor.exists():
        if read_json(descriptor) != identity:
            raise ValueError("実行条件が保存済みの条件と異なります。別の出力先を指定してください")
    else:
        lock_files = {".run-lock.sqlite", ".run-lock.sqlite-journal"}
        if any(p.name not in lock_files and not p.name.startswith(".write-") for p in output.iterdir()):
            raise ValueError("初回実行の出力先には空のディレクトリを指定してください")
        atomic_json(descriptor, identity)
    complete = output / "manifest.json"
    if complete.exists():
        manifest = read_json(complete)
        for name, expected in manifest["output_sha256"].items():
            if sha256_file(output / name) != expected:
                raise ValueError(f"保存済み集計のSHAが一致しません: {name}")
        return manifest

    builders = None
    feature_metadata = output / "feature_configuration.json"
    daily, seen, pending_scores = [], set(), []
    for day, (stage, item) in days.items():
        path = output / "features" / day.isoformat()
        fixed = {"code_sha": identity["code_sha"], "input_sha256": digest(item), "date": day.isoformat()}

        def build(path_to_write):
            nonlocal builders
            if builders is None:
                builders, metadata = period_builders(PROJECT_ROOT)
                atomic_json(feature_metadata, metadata)
            save_features(path_to_write, inputs, day, item, builders)

        write_bundle(path, fixed, build)
        rids = read_json(path / "evidence.json").keys()
        if seen & rids:
            raise ValueError(f"開催日間でレースIDが重複しています: {sorted(seen & rids)}")
        seen.update(rids)
        row = {"date": day.isoformat(), "stage": stage, **read_json(path / "coverage.json")}
        daily.append(row)
        print({key: row[key] for key in ("date", *COVERAGE_KEYS)}, flush=True)
        pending_scores.append((day, stage, path, fixed))

    coverage_report = {
        "days": daily, "expected_audit_days": 160, "inventory_days": len(daily),
        "totals": {key: sum(row[key] for row in daily) for key in COVERAGE_KEYS},
        "periods": {stage: {key: sum(row[key] for row in daily if row["stage"] == stage)
                            for key in COVERAGE_KEYS} for stage in PERIODS},
    }
    atomic_json(output / "coverage.json", coverage_report)
    print({"coverage_before_scoring": coverage_report["totals"]}, flush=True)

    races, diagnostics, artifacts = [], [], []
    for day, stage, path, fixed in pending_scores:
        scores = output / "scores" / day.isoformat()
        score_identity = {**fixed, "features_manifest_sha256": sha256_file(path / "manifest.json"),
                          "effective_weight_sha256": identity["weights"]["effective_weight_sha256"]}
        write_bundle(scores, score_identity,
                     lambda destination: score_day(destination, path, day, stage, weights, assessment))
        races.extend(read_json(scores / "races.json"))
        diagnostics.extend(read_json(scores / "cache_diagnostics.json"))
        artifacts.append({
            "date": day.isoformat(), "input_sha256": fixed["input_sha256"],
            "features_manifest_sha256": score_identity["features_manifest_sha256"],
            "scores_manifest_sha256": sha256_file(scores / "manifest.json"),
        })

    for name, rows in (("comparison", comparisons(races)), ("races", races),
                       ("cache_diagnostics", diagnostics), ("daily_coverage", daily)):
        atomic_json(output / f"{name}.json", rows)
        with TemporaryDirectory(dir=output, prefix=".write-") as temp:
            candidate = Path(temp) / f"{name}.csv"
            save_csv(candidate, rows)
            os.replace(candidate, output / candidate.name)
    manifest = {
        **identity, "methods": {label: list(items) for label, items in METHODS.items()},
        "feature_configuration": read_json(feature_metadata),
        "periods": {key: [d.isoformat() for d in bounds] for key, bounds in PERIODS.items()},
        "forward_status": "previously_evaluated_reconciliation",
        "training": "disabled_fixed_production_weights",
        "purchase": {"ticket": "3連複", "unit_yen": UNIT_YEN, "box_size": BOX_SIZE,
                     "type": "simulated_tickets", "operational_purchase_records": "not_provided"},
        "artifacts": artifacts,
        "output_sha256": {path.name: sha256_file(path) for path in sorted(output.iterdir())
                          if path.suffix in {".csv", ".json"} and path.is_file()},
    }
    atomic_json(complete, manifest)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    run(args.inventory.resolve(), args.output.resolve())


if __name__ == "__main__":
    main()
