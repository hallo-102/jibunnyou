from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from .adapters import export_canonical_csv, load_legacy_excel
from .backtest import summarize_bets
from .collectors.daily import collect_daily_dataset
from .collectors.netkeiba_results import collect_results_for_entries, save_results
from .config import load_settings
from .contracts import canonicalize, load_race_table
from .doctor import run_doctor
from .features import build_features
from .history import HistoryStore, build_training_dataset
from .legacy_history import load_legacy_history
from .orchestrator import run_pipeline
from .reporting import load_settled_files, write_report
from .results import evaluate_strategy_bets, load_results
from .t5_runtime import run_t5_runtime
from .training import train_lightgbm
from .validation import validate_races
from .walkforward import run_walkforward


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="keiba-v2")
    sub = parser.add_subparsers(dest="command", required=True)

    p_doctor = sub.add_parser("doctor", help="check V2 runtime, optional dependencies, model and history readiness")
    p_doctor.add_argument("--settings")

    p_validate = sub.add_parser("validate", help="validate input data")
    p_validate.add_argument("--input", required=True)
    p_validate.add_argument("--settings")

    p_import = sub.add_parser("import-legacy", help="read legacy Excel without modifying it and export canonical CSV")
    p_import.add_argument("--input", required=True)
    p_import.add_argument("--output", required=True)
    p_import.add_argument("--sheet", default="0")

    p_history = sub.add_parser("import-history", help="read legacy result CSV/Excel without modifying it and append to V2 history DB")
    p_history.add_argument("--input", required=True)
    p_history.add_argument("--date", help="YYYYMMDD fallback when source rows/filename do not contain a date")
    p_history.add_argument("--settings")

    p_collect = sub.add_parser("collect", help="collect same-day netkeiba entries, schedule and JRA odds")
    p_collect.add_argument("--date", required=True, help="YYYYMMDD")
    p_collect.add_argument("--settings")
    p_collect.add_argument("--show-browser", action="store_true")

    p_t5 = sub.add_parser("t5-runtime", help="run restart-safe T-5 SHADOW odds/prediction runtime")
    p_t5.add_argument("--input", required=True)
    p_t5.add_argument("--schedule", required=True)
    p_t5.add_argument("--date", required=True, help="YYYYMMDD")
    p_t5.add_argument("--settings")
    p_t5.add_argument("--show-browser", action="store_true")

    p_results = sub.add_parser("collect-results", help="collect netkeiba results and append horse history")
    p_results.add_argument("--entries", required=True, help="canonical/raw entries CSV containing source_race_id")
    p_results.add_argument("--date", required=True, help="YYYYMMDD")
    p_results.add_argument("--settings")

    p_training = sub.add_parser("build-training", help="build leakage-safe training rows from the V2 history DB")
    p_training.add_argument("--output")
    p_training.add_argument("--settings")

    p_train = sub.add_parser("train", help="train LightGBM from labeled canonical data")
    p_train.add_argument("--input", required=True)
    p_train.add_argument("--settings")

    p_walk = sub.add_parser("walkforward", help="chronological walk-forward accuracy/ROI evaluation")
    p_walk.add_argument("--input", required=True, help="training CSV")
    p_walk.add_argument("--splits", type=int, default=4)
    p_walk.add_argument("--output")
    p_walk.add_argument("--settings")

    p_run = sub.add_parser("run", help="run validation + history features + prediction + odds + race selection + SHADOW strategies")
    p_run.add_argument("--input", required=True)
    p_run.add_argument("--date", required=True, help="YYYYMMDD")
    p_run.add_argument("--combination-odds")
    p_run.add_argument("--settings")

    p_settle = sub.add_parser("settle", help="evaluate strategy tickets against race results")
    p_settle.add_argument("--bets", required=True, help="strategy_bets_*.json")
    p_settle.add_argument("--results", required=True)
    p_settle.add_argument("--payouts")
    p_settle.add_argument("--output")

    p_report = sub.add_parser("report", help="aggregate settled SHADOW tickets into Excel performance report")
    p_report.add_argument("--directory", default="data/output")
    p_report.add_argument("--output", default="data/output/performance_report.xlsx")
    return parser


def main() -> None:
    args = _build_parser().parse_args()

    if args.command == "doctor":
        report = run_doctor(args.settings)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        if not report["ok"]:
            raise SystemExit(2)
        return

    if args.command == "validate":
        settings = load_settings(args.settings)
        result = validate_races(canonicalize(load_race_table(args.input)), settings.section("validation"))
        if result.warnings:
            print("WARN:", *result.warnings, sep="\n- ")
        if not result.ok:
            print("ERROR:", *result.errors, sep="\n- ")
            raise SystemExit(2)
        print("OK: validation passed")
        return

    if args.command == "import-legacy":
        sheet: str | int | None = int(args.sheet) if str(args.sheet).isdigit() else args.sheet
        df = load_legacy_excel(args.input, sheet_name=sheet)
        dst = export_canonical_csv(df, args.output)
        print(f"OK: exported {len(df)} rows -> {dst}")
        return

    if args.command == "import-history":
        settings = load_settings(args.settings)
        history_path = settings.project_root / str(settings.section("app").get("history_db", "data/runtime/history.sqlite3"))
        rows = load_legacy_history(args.input, race_date=args.date)
        inserted = HistoryStore(history_path).upsert_runs(rows)
        print(f"OK: history_rows_upserted={inserted} source={Path(args.input).resolve()}")
        print(f"history_db={history_path}")
        return

    if args.command == "collect":
        settings = load_settings(args.settings)
        result = collect_daily_dataset(args.date, settings.project_root, headless=not args.show_browser)
        print(f"OK: canonical={result['canonical_path']}")
        print(f"schedule={result['schedule_path']}")
        print(f"combination_odds={result['combination_odds_path']}")
        return

    if args.command == "t5-runtime":
        results = run_t5_runtime(
            args.input, args.schedule, args.date, args.settings,
            headless=not args.show_browser,
        )
        summary = pd.DataFrame(results)
        print(summary.to_string(index=False) if not summary.empty else "No T-5 races processed")
        if not summary.empty and summary["status"].isin(["FAILED", "MISSED"]).any():
            raise SystemExit(3)
        return

    if args.command == "collect-results":
        settings = load_settings(args.settings)
        entries = pd.read_csv(args.entries, encoding="utf-8-sig", dtype={"source_race_id": str, "race_date": str})
        results, payouts = collect_results_for_entries(entries)
        result_path, payout_path = save_results(results, payouts, settings.project_root / "data" / "results", args.date)
        history_path = settings.project_root / str(settings.section("app").get("history_db", "data/runtime/history.sqlite3"))
        inserted = HistoryStore(history_path).upsert_runs(results)
        print(f"OK: results={result_path}")
        print(f"payouts={payout_path}")
        print(f"history_rows_upserted={inserted}")
        return

    if args.command == "build-training":
        settings = load_settings(args.settings)
        history_path = settings.project_root / str(settings.section("app").get("history_db", "data/runtime/history.sqlite3"))
        store = HistoryStore(history_path)
        dataset = build_training_dataset(store.load_all(), n_recent=int(settings.section("history").get("n_recent", 5)))
        output = Path(args.output) if args.output else settings.project_root / "data" / "training" / "training.csv"
        output.parent.mkdir(parents=True, exist_ok=True)
        dataset.to_csv(output, index=False, encoding="utf-8-sig")
        print(f"OK: training_rows={len(dataset)} -> {output}")
        return

    if args.command == "train":
        settings = load_settings(args.settings)
        df = build_features(canonicalize(load_race_table(args.input)))
        pred_cfg = settings.section("prediction")
        model_path = settings.project_root / str(pred_cfg.get("model_path", "data/runtime/model.txt"))
        result = train_lightgbm(
            df,
            feature_prefix=str(pred_cfg.get("feature_prefix", "feature_")),
            model_path=model_path,
            seed=int(settings.section("app").get("seed", 42)),
        )
        print(f"OK: model={result['model_path']}")
        print(result["metrics"])
        return

    if args.command == "walkforward":
        settings = load_settings(args.settings)
        df = pd.read_csv(args.input, encoding="utf-8-sig", dtype={"race_id": str, "race_date": str})
        pred_cfg = settings.section("prediction")
        odds_cfg = settings.section("odds")
        folds, summary = run_walkforward(
            df,
            feature_prefix=str(pred_cfg.get("feature_prefix", "feature_")),
            n_splits=int(args.splits),
            seed=int(settings.section("app").get("seed", 42)),
            min_ev=float(odds_cfg.get("min_expected_value", 1.05)),
            max_odds=float(odds_cfg.get("max_win_odds", 30.0)),
        )
        output = Path(args.output) if args.output else settings.project_root / "data" / "output" / "walkforward.csv"
        output.parent.mkdir(parents=True, exist_ok=True)
        folds.to_csv(output, index=False, encoding="utf-8-sig")
        print(f"OK: walkforward={output}")
        print(summary)
        return

    if args.command == "run":
        result = run_pipeline(Path(args.input), args.date, args.settings, args.combination_odds)
        print(f"OK: run_id={result['run_id']} races={result['metrics']['races']} horses={result['metrics']['horses']}")
        print(f"history_rows_used={result['metrics']['history_rows_used']}")
        print(f"buy_candidate_races={result['metrics']['buy_candidate_races']}")
        print(f"strategy_bets={result['metrics']['strategy_bets']} stake={result['metrics']['strategy_stake_yen']} yen")
        print(f"predictions={result['prediction_path']}")
        print(f"race_selection={result['race_selection_path']}")
        print(f"strategy={result['strategy_path']}")
        return

    if args.command == "settle":
        bets = pd.DataFrame(json.loads(Path(args.bets).read_text(encoding="utf-8")))
        results = load_results(args.results)
        payouts = pd.read_csv(args.payouts, encoding="utf-8-sig") if args.payouts else None
        settled = evaluate_strategy_bets(bets, results, payouts)
        summary = summarize_bets(settled)
        output = Path(args.output) if args.output else Path(args.bets).with_name(Path(args.bets).stem + "_settled.csv")
        settled.to_csv(output, index=False, encoding="utf-8-sig")
        print(f"OK: settled={output}")
        print(summary.to_dict())
        return

    if args.command == "report":
        settled = load_settled_files(args.directory)
        destination = write_report(settled, args.output)
        print(f"OK: report={destination}")
        return

    raise SystemExit(2)


if __name__ == "__main__":
    main()
