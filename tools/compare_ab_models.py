"""Issue #32 のオフライン比較。python -m tools.compare_ab_models --help"""

import argparse
from collections import Counter, defaultdict
import csv
from datetime import date
import json
from pathlib import Path
import platform
import sys

import numpy as np
import pandas as pd

from model_registry import load_model_spec, normalized_weights_sha256, sha256_file, validate_effective_weights
from tokutyouryou_keisann.baseline import load_effective_weights_file
from tokutyouryou_keisann.config import CONFIG, FEATURE_WEIGHTS_SEED, FEAT_COLS, PROJECT_ROOT
from tokutyouryou_keisann.optimizer import optimize_placewise_weights
from tokutyouryou_keisann.scoring import compute_scores_with_optimizer_weights

from tools.ab_comparison_diagnostics import diagnostic_builders, git
from tools.ab_comparison_evidence import available_by_cutoff, snapshot_grade
from tools.ab_comparison_inputs import feature_missingness, load_races, pipeline, read_results, score, timestamp, training_set
from tools.ab_comparison_metrics import common_purchase, grouped_summary, purchase_decision, race_metrics, summarize, operational_summary

DEFAULT_PERIODS = {
    "train": ["2025-06-14", "2026-06-30"],
    "validation": ["2026-07-01", "2026-10-23"],
    "forward": ["2026-10-24", None],
}
ITERATION_KEYS = ("N_ITER_DEFAULT", "N_ITER_PLACE", "N_ITER_PLACE_SURFACE")
METHODS = {"A": "legacy", "B": "corrected", "C": "legacy", "D": "corrected"}
EMPTY_PAYOUTS = pd.DataFrame(columns=["rid_str", "払戻種別", "組番_norm", "払戻金_int"])


def read_json(path):
    def reject_constant(value):
        raise ValueError(f"{path}: JSONに非有限数 {value} は使用できません")
    return json.loads(path.read_text(encoding="utf-8"), parse_constant=reject_constant)


def save_json(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def save_csv(path, rows):
    with path.open("w", encoding="utf-8-sig", newline="") as output:
        if rows:
            writer = csv.DictWriter(output, fieldnames=list(dict.fromkeys(key for row in rows for key in row)))
            writer.writeheader()
            for row in rows:
                writer.writerow({key: json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else value
                                 for key, value in row.items()})


def periods_from(settings):
    if set(settings) != set(DEFAULT_PERIODS):
        raise ValueError("期間設定には train、validation、forward が必要です")
    parsed = {label: (date.fromisoformat(settings[label][0]),
                      date.fromisoformat(settings[label][1]) if settings[label][1] else None)
              for label in DEFAULT_PERIODS}
    train, valid, forward = parsed.values()
    if not (train[1] and valid[1] and train[0] <= train[1] < valid[0] <= valid[1] < forward[0]
            and (forward[1] is None or forward[0] <= forward[1])):
        raise ValueError("期間の順序が不正、または重複しています")
    return parsed


def integer_setting(config, key, default, minimum):
    value = config.get(key, default)
    if type(value) is not int or value < minimum:
        raise ValueError(f"{key} は {minimum} 以上の整数が必要です")
    return value


def baselines_from(config, root):
    configured = config.get("baselines", {"github": {"model_config": "config/production_model.json"}})
    if not configured:
        raise ValueError("少なくとも1つの基準モデルが必要です")
    records = {}
    for label, item in configured.items():
        model_root = (root / item["model_root"]).resolve() if "model_root" in item else PROJECT_ROOT
        spec = load_model_spec(model_root, item["model_config"])
        # 基準ごとの重みファイルは明示設定を検証して読み込む。
        weights = load_effective_weights_file(spec.resolved_weight_path)
        validate_effective_weights(spec, weights, scoring_model_version="legacy")
        training = item.get("training_period")
        if training is not None:
            start, end = map(date.fromisoformat, training)
            if end < start:
                raise ValueError(f"{label}: 基準モデルの学習期間が不正です")
        records[label] = (weights, {
            "model_name": spec.model_name,
            "model_config_sha256": sha256_file(spec.config_path),
            "weight_file": str(spec.resolved_weight_path),
            "weight_sha256": spec.file_sha256,
            "effective_weight_sha256": spec.effective_sha256,
            "training_period": training,
            "training_period_status": "declared" if training else "unknown",
        })
    return records


def fixed_candidates(path, label, evaluation_start, evaluation_ids):
    manifest = read_json(path)
    if manifest.get("schema_version") != 1:
        raise ValueError("学習済み候補のマニフェスト形式が不正です")
    imported, metadata = {}, {}
    for method in ("C", "D"):
        source = manifest["methods"][label][method]
        start, end = map(date.fromisoformat, source["training_period"])
        ids = source["training_race_ids"]
        if (not start <= end < evaluation_start or not ids or not isinstance(ids, list)
                or len(set(ids)) != len(ids) or len(ids) != source["training_races"]
                or set(ids) & evaluation_ids or source["validation"] is not (method == "D")):
            raise ValueError(f"{method}: 評価より前の学習期間・学習レース・特徴量方式を確認できません")
        weight_path = (path.parent / source["weight_file"]).resolve()
        if sha256_file(weight_path) != source["weight_sha256"]:
            raise ValueError(f"{method}: 学習済み重みファイルの SHA-256 が不一致です")
        payload = read_json(weight_path)
        weights = {}
        for row in payload:
            key = row["group"]
            key = tuple(key) if isinstance(key, list) else key
            if key in weights or set(row["weights"]) != set(FEAT_COLS):
                raise ValueError(f"{method}: 学習済み重みのグループ・特徴量が不正です")
            weights[key] = row["weights"]
        if "__default__" not in weights or normalized_weights_sha256(weights) != source["effective_weight_sha256"]:
            raise ValueError(f"{method}: 学習済み実効重みの SHA-256 が不一致です")
        imported[method] = weights
        metadata[method] = {**source, "weight_file": str(weight_path),
                            "training_code_sha": source.get("training_code_sha", source["code_sha"]),
                            "source_manifest_sha256": sha256_file(path)}
    if any(metadata["C"][key] != metadata["D"][key] for key in ("training_period", "training_race_ids")):
        raise ValueError("C・D の学習期間または学習レースが一致しません")
    return imported, metadata


def check_optimizer_parity(frame, weights):
    optimized_path = compute_scores_with_optimizer_weights(frame, weights)
    pipeline_path = score(frame, weights)
    if (not np.allclose(optimized_path["score_raw"], pipeline_path["score_raw"], atol=1e-9, rtol=0)
            or not np.array_equal(optimized_path["rank"], pipeline_path["rank"])):
        raise ValueError("最適化と本番スコア経路で順位またはスコアが一致しません")


def optimize(races, results, periods, code_sha, setting, output):
    saved = CONFIG.copy()
    trained, manifests = {}, {}
    try:
        CONFIG.update(setting)
        search = {key: value for key, value in CONFIG.items()
                  if key not in {"DATA_GLOB", "RESULTS_FILE"}}
        for method in ("C", "D"):
            mode = METHODS[method]
            frame, entries = training_set(races, results, periods["train"][1], mode)
            prepared = score(frame, {"__default__": FEATURE_WEIGHTS_SEED})
            prepared.attrs["production_score_arithmetic"] = True
            check_optimizer_parity(prepared, {"__default__": FEATURE_WEIGHTS_SEED})
            weights, groups = optimize_placewise_weights(prepared, entries, EMPTY_PAYOUTS)
            check_optimizer_parity(prepared, weights)
            # JSON keeps the optimizer's full-precision values, including per-surface groups.
            keys = sorted(weights, key=lambda key: (isinstance(key, tuple), key))
            payload = [{"group": key, "weights": weights[key]} for key in keys]
            path = output / f"weights_{method}.json"
            save_json(path, payload)
            trained[method] = weights
            groups.to_csv(output / f"optimizer_{method}.csv", index=False, encoding="utf-8-sig")
            manifests[method] = {
                "code_sha": code_sha, "weight_file": path.name, "weight_sha256": sha256_file(path),
                "effective_weight_sha256": normalized_weights_sha256(weights),
                "training_period": [d.isoformat() for d in periods["train"]],
                "training_races": int(frame["rid_str"].nunique()), "training_horses": len(frame),
                "training_race_ids": sorted(frame["rid_str"].unique()),
                "optimizer_pipeline_parity": "score_and_rank_passed_on_seed_and_candidate",
            }
    finally:
        CONFIG.clear()
        CONFIG.update(saved)
    return trained, manifests, search


def diagnosis(rid, race, fixed, stage, ticket_args, result, metadata):
    frames = race["frames"]
    variants = {"legacy": frames["legacy"], "corrected": frames["corrected"]}
    variants.update({key: frame for key, frame in frames.items() if key not in variants})
    scored = {key: score(frame, fixed).set_index("馬番").sort_index() for key, frame in variants.items()}
    comparisons = {
        "main_parity": ("legacy", "main"),
        "legacy_cache_effect": ("legacy", "main_no_cache"),
        "corrected_cache_effect": ("corrected_with_cache", "corrected"),
        **{key: (key, value["compare_to"]) for key, value in metadata.items() if "compare_to" in value},
    }
    rows = []
    for name, (candidate, reference) in comparisons.items():
        row = {"race_id": rid, "stage": stage, "comparison": name, "candidate": candidate, "reference": reference}
        if candidate not in scored or reference not in scored:
            rows.append({**row, "status": "roster_mismatch"})
            continue
        left, right = scored[candidate], scored[reference]
        changes = {}
        for col in FEAT_COLS:
            same = np.isclose(left[col].astype(float), right[col].astype(float), atol=1e-9, rtol=0, equal_nan=True)
            if not same.all():
                changes[col] = int((~same).sum())
        rank_changes = int(left["rank"].ne(right["rank"]).sum())
        score_changes = int((~np.isclose(left["score_raw"], right["score_raw"], atol=1e-9, rtol=0)).sum())
        values = {}
        if name.endswith(("D-04", "D-09")):
            for col in ("ta_n", "win_rate"):
                values[col] = {str(n): [None if pd.isna(before) else float(before),
                                        None if pd.isna(after) else float(after)]
                               for n, before, after in zip(left.index, right[col], left[col])}
        rows.append({
            **row, "horse_value_reference_then_candidate": values, "status": "different" if changes or rank_changes or score_changes else "equal",
            "changed_feature_cells": changes, "rank_changes": rank_changes, "score_changes": score_changes,
            "candidate_metrics": race_metrics(left.reset_index().sort_values("rank"), result, *ticket_args),
            "reference_metrics": race_metrics(right.reset_index().sort_values("rank"), result, *ticket_args),
        })
    return rows


def evaluate(races, results, baselines, trained, methods, ticket_args, metadata, assessment):
    race_rows, diagnostic_rows = defaultdict(list), []
    for label, (fixed, _) in baselines.items():
        for method in methods:
            race_rows[label, method] = []
        for rid, race in sorted(races.items(), key=lambda item: (item[1]["start"], item[0])):
            stage = race["stage"]
            result = results.get(rid)
            diagnostic_rows.extend(
                {"baseline": label, **row}
                for row in diagnosis(rid, race, fixed, stage, ticket_args, result, metadata)
            )
            if stage == "train":
                continue
            ordered = {method: score(race["frames"][mode], trained.get(method, fixed)).sort_values("rank")
                       for method, mode in methods.items()}
            decisions = {method: purchase_decision(frame, race, rid, assessment) for method, frame in ordered.items()}
            shared = common_purchase(decisions.values())
            method_metrics = {method: race_metrics(frame, result, *ticket_args) for method, frame in ordered.items()}
            for method, frame in ordered.items():
                row = {"baseline": label, "method": method, "race_id": rid, "stage": stage,
                       "start_at": race["start"].isoformat(), "month": race["start"].strftime("%Y-%m"),
                       "horses": len(frame), "purchase_decision": decisions[method], "common_purchase": shared,
                       "result_finalized_date": (timestamp(result["evidence"]["finalized_at"]).date().isoformat()
                                                 if result and result["evidence"]["finalized_at"] else None),
                       "result_evidence_grade": result["evidence"]["grade"] if result else None,
                       "result_available_by_date": result["evidence"]["available_by_date"] if result else None,
                       **method_metrics[method],
                       "model_popularity_band": method_metrics[method]["popularity_band"],
                       "popularity_band": method_metrics["A"]["popularity_band"]}
                race_rows[label, method].append(row)
    return race_rows, diagnostic_rows


def candidate_selection(summary, minimum, baselines):
    selected = {}
    for baseline in baselines:
        eligible = [row for row in summary if row["baseline"] == baseline and row["stage"] == "validation"
                    and row["filtered_settled_races"] >= minimum]
        ordered = sorted(eligible, key=lambda row: (-row["filtered_profit_yen"], -row["filtered_roi_pct"], row["method"]))
        selected[baseline] = {"method": ordered[0]["method"] if ordered else None,
                              "eligible_methods": [row["method"] for row in ordered]}
    return selected


def coverage_counts(races, results, periods, missing, exclusions, evidence):
    coverage = {}
    for stage in periods:
        rows = [(rid, race) for rid, race in races.items() if race["stage"] == stage]
        counts = missing[stage]
        for rid, race in rows:
            result = results.get(rid)
            if not result or not result["top"]:
                reason = "missing_result" if not result else "invalid_or_missing_finish_top3"
                if evidence.get(rid, {}).get("grade") == "D":
                    reason = "result_evidence_grade_D"
                counts[reason] += 1
                exclusions.append({"race_id": rid, "stage": stage, "reason": reason, "scope": "finish_metrics"})
            elif stage == "train" and not available_by_cutoff(result["evidence"], periods["train"][1]):
                counts["training_result_not_final_in_train_period"] += 1
                exclusions.append({"race_id": rid, "stage": stage, "reason": "training_result_not_final_in_train_period",
                                   "scope": "optimization"})
            for kind in ("単勝", "複勝", "3連複"):
                if not result or kind not in result["payouts"]:
                    counts["missing_payout_" + kind] += 1
                    exclusions.append({"race_id": rid, "stage": stage, "reason": "missing_payout_" + kind,
                                       "scope": "settlement"})
        coverage[stage] = {
            "loaded_races": len(rows), "loaded_horses": sum(len(race["frames"]["legacy"]) for _, race in rows),
            "missing": dict(counts),
            "feature_missing_cells": {mode: feature_missingness(races, stage, mode) for mode in ("legacy", "corrected")},
        }
    return coverage


def run(config_path, output):
    # race_purchase_filter の既存エントリーポイントと同じ import 解決。
    filter_path = str(PROJECT_ROOT / "race_purchase_filter")
    sys.path.append(filter_path)
    try:
        from race_purchase_filter import assessment
    finally:
        sys.path.remove(filter_path)

    root = config_path.parent
    config = read_json(config_path)
    if config.get("schema_version") != 1:
        raise ValueError("schema_version=1 の設定が必要です")
    periods = periods_from(config.get("periods", DEFAULT_PERIODS))
    unit = integer_setting(config, "unit_yen", 100, 100)
    box_size = integer_setting(config, "box_size", 5, 3)
    high = integer_setting(config, "top_payout_races", 3, 0)
    selection_min = integer_setting(config, "min_validation_settled_races", 30, 30)
    single_date = date.fromisoformat(config["evaluation_date"]) if "evaluation_date" in config else None
    stages = ("validation", "forward")
    if single_date:
        stages = tuple(stage for stage in stages if periods[stage][0] <= single_date
                       and (periods[stage][1] is None or single_date <= periods[stage][1]))
        if not stages:
            raise ValueError("evaluation_date は検証期間または前向き照合期間内の日付が必要です")
    weight_mode = config.get("weight_mode", "ab_only" if single_date else "optimize")
    if weight_mode not in {"ab_only", "optimize", "fixed"}:
        raise ValueError("weight_mode は ab_only、optimize、fixed のいずれかです")
    if ("candidate_manifest" in config) != (weight_mode == "fixed"):
        raise ValueError("candidate_manifest は weight_mode=fixed の場合に指定してください")
    methods = {key: mode for key, mode in METHODS.items() if weight_mode != "ab_only" or key in "AB"}
    if unit % 100 or box_size > 5:
        raise ValueError("unit_yen は100円単位、box_size は3〜5です")
    if pipeline.SCORING_MODEL_VERSION != "legacy":
        raise ValueError("重み比較では KEIBA_SCORING_MODEL_VERSION=legacy が必要です")
    code_sha = git(PROJECT_ROOT, "rev-parse", "HEAD").decode().strip()
    if git(PROJECT_ROOT, "status", "--porcelain", "--untracked-files=no").strip():
        raise ValueError("コードSHAを固定するため、追跡済みファイルの変更をコミットしてください")
    for path in [Path(__file__), *Path(__file__).parent.glob("ab_comparison_*.py")]:
        git(PROJECT_ROOT, "ls-files", "--error-unmatch", str(path))
    builders, diagnostics = diagnostic_builders(PROJECT_ROOT)
    baselines = baselines_from(config, root)
    snapshots_path = root / config["snapshots"]
    results_path = root / config["results"]
    snapshots, result_rows = read_json(snapshots_path), read_json(results_path)
    races, counts, missing, excluded = load_races(
        root, snapshots, periods, builders, evaluation_date=single_date, include_training=weight_mode == "optimize",
    )
    if single_date and not any(race["stage"] in stages for race in races.values()):
        raise ValueError("指定日に比較可能なレースがありません")
    evidence = {}
    starts = {row["race_id"]: timestamp(row["start_at"]) for row in snapshots}
    results = read_results(result_rows, races, evidence, starts=starts)
    coverage = coverage_counts(races, results, periods, missing, excluded, evidence)

    setting = config.get("optimizer_iterations", {})
    if set(setting) - set(ITERATION_KEYS) or any(type(v) is not int or v < 1 for v in setting.values()):
        raise ValueError("optimizer_iterations には既存の N_ITER_* を正整数で指定してください")

    # Manifest is written last and marks a completed run.
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise ValueError("出力先には空のディレクトリを指定してください")
    trained, weights_manifest, search = {}, {}, None
    if weight_mode == "optimize":
        trained, weights_manifest, search = optimize(races, results, periods, code_sha, setting, output)
    if weight_mode == "fixed":
        evaluation_start = single_date or periods["validation"][0]
        evaluation_ids = {row["race_id"] for row in snapshots
                          if timestamp(row["start_at"]).date() >= evaluation_start}
        trained, weights_manifest = fixed_candidates(
            (root / config["candidate_manifest"]).resolve(),
            config.get("candidate_manifest_baseline", "github"), evaluation_start, evaluation_ids,
        )
    rows, diagnostic_rows = evaluate(races, results, baselines, trained, methods, (unit, box_size), diagnostics, assessment)
    summary, breakdown = grouped_summary(rows, unit, high, stages)
    actual_path = root / config["operational_results"] if "operational_results" in config else None
    actual = operational_summary(rows, read_json(actual_path) if actual_path else [], stages)
    # 採用候補は検証期間内に確定した払戻だけで選ぶ。
    cutoff = periods["validation"][1]
    selection_ids = {rid for rid, result in results.items() if available_by_cutoff(result["evidence"], cutoff)}
    selection_summary = [
        {"baseline": label, "method": method, "stage": "validation",
         **summarize([r for r in method_rows if r["stage"] == "validation" and r["race_id"] in selection_ids], unit, high)}
        for (label, method), method_rows in rows.items()
    ]
    if single_date:
        selection_summary = []
    candidate = candidate_selection(selection_summary, selection_min, baselines)
    parity = Counter(item["status"] for item in diagnostic_rows if item["comparison"] == "main_parity")
    flat_rows = [row for method_rows in rows.values() for row in method_rows]
    for name, values in (("comparison", summary), ("validation_selection", selection_summary), ("breakdown", breakdown), ("races", flat_rows),
                         ("diagnostics", diagnostic_rows), ("exclusions", excluded), ("operational_comparison", actual)):
        save_json(output / f"{name}.json", values)
        save_csv(output / f"{name}.csv", values)
    methods_manifest = {}
    for label, (_, fixed) in baselines.items():
        methods_manifest[label] = {
            method: {**(weights_manifest.get(method) or fixed),
                     "code_sha": code_sha, "validation": mode == "corrected",
                     "feature_source_sha256": sha256_file(PROJECT_ROOT / "1_keibayosou_features.py")}
            for method, mode in methods.items()
        }
    manifest = {
        "schema_version": 1, "code_sha": code_sha, "methods": methods_manifest,
        "weight_mode": weight_mode, "evaluation_date": single_date.isoformat() if single_date else None,
        "notes": {
            "input_timestamp_evidence": "SHA-256の一致は内容を固定するもので、発走前取得の証明にはなりません。取得時刻は元の保存記録で確認が必要です。",
            "production_reproduction": "整形後の共通入力での一致は、当時の生入力や実行環境を含む現行モデルの完全再現を証明しません。",
            "selection_minimum": "精算済み30レースは候補選定の最低条件です。本番採用の十分条件ではありません。",
        },
        "runtime": {"python": platform.python_version(), "pandas": pd.__version__, "numpy": np.__version__},
        "periods": {key: [value.isoformat() if value else None for value in bounds] for key, bounds in periods.items()},
        "forward_status": "previously_evaluated_reconciliation",
        "inputs": {"config_sha256": sha256_file(config_path), "snapshots_sha256": sha256_file(snapshots_path),
                   "results_sha256": sha256_file(results_path), "snapshot_records": len(snapshots),
                   "result_records": len(result_rows), "result_records_without_usable_snapshot": len(results.keys() - races.keys()),
                   "snapshot_counts": dict(counts),
                   "snapshot_evidence_grades": {row["race_id"]: snapshot_grade(row) for row in snapshots},
                   "result_evidence": evidence,
                   "operational_results_sha256": sha256_file(actual_path) if actual_path else None},
        "coverage": coverage, "optimizer_config": search,
        "optimizer_seed_weight_sha256": normalized_weights_sha256({"__default__": FEATURE_WEIGHTS_SEED}),
        "scoring": {"model": "legacy", "alpha": pipeline.ALPHA, "extra_alpha": pipeline.EXTRA_ALPHA,
                    "dl_probability_blend": pipeline.DL_PROB_BLEND, "dl_rank_blend": pipeline.DL_RANK_BLEND,
                    "dl_score_bonus": pipeline.DL_SCORE_BONUS, "external_inputs": "hash-pinned pre-race archives only"},
        "purchase": {"filter_policy": assessment.policy_version(assessment.load_policy()),
                     "cohort": "intersection_of_buy_candidates_across_enabled_methods_per_baseline",
                     "own_cohort": "buy_candidates_per_method",
                     "operational": "recorded_actual_purchases_only" if actual_path else "no_ledger_supplied",
                     "ticket": "3連複", "box_size": box_size, "unit_yen": unit, "top_payout_races": high},
        "validation_selection": {"status": "disabled_single_day" if single_date else "evaluated", "sort": ["filtered_profit_yen DESC", "filtered_roi_pct DESC", "method ASC"],
                                 "result_finalized_date_max": cutoff.isoformat(), "result_availability_cutoff": cutoff.isoformat(), "min_settled_races": selection_min, "baselines": candidate},
        "diagnostics": {"variants": diagnostics, "main_parity_candidate": "legacy", "main_parity_tolerance": 1e-9,
                        "main_parity_counts": dict(parity)},
    }
    manifest["output_sha256"] = {path.name: sha256_file(path) for path in sorted(output.iterdir())}
    save_json(output / "manifest.json", manifest)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    manifest = run(args.config.resolve(), args.output.resolve())
    if not manifest["diagnostics"]["main_parity_counts"].get("equal") or any(
        key != "equal" for key in manifest["diagnostics"]["main_parity_counts"]
    ):
        parser.exit(2, "修正前 main との一致を確認できません。diagnostics.csv を確認してください。\n")


if __name__ == "__main__":
    main()
