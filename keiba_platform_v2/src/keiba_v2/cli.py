from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from .adapters import export_canonical_csv, load_legacy_excel
from .backtest import summarize_bets
from .collectors.daily import collect_daily_dataset
from .config import load_settings
from .contracts import canonicalize, load_race_table
from .features import build_features
from .orchestrator import run_pipeline
from .results import evaluate_strategy_bets, load_results
from .training import train_lightgbm
from .validation import validate_races


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="keiba-v2")
    sub = parser.add_subparsers(dest="command", required=True)

    p_validate = sub.add_parser("validate", help="validate input data")
    p_validate.add_argument("--input", required=True)
    p_validate.add_argument("--settings")

    p_import = sub.add_parser("import-legacy", help="read legacy Excel without modifying it and export canonical CSV")
    p_import.add_argument("--input", required=True)
    p_import.add_argument("--output", required=True)
    p_import.add_argument("--sheet", default="0")

    p_collect = sub.add_parser("collect", help="collect same-day netkeiba entries and JRA odds")
    p_collect.add_argument("--date", required=True, help="YYYYMMDD")
    p_collect.add_argument("--settings")
    p_collect.add_argument("--show-browser", action="store_true")

    p_train = sub.add_parser("train", help="train LightGBM from labeled canonical data")
    p_train.add_argument("--input", required=True)
    p_train.add_argument("--settings")

    p_run = sub.add_parser("run", help="run validation + features + prediction + odds + race selection + SHADOW strategies")
    p_run.add_argument("--input", required=True)
    p_run.add_argument("--date", required=True, help="YYYYMMDD")
    p_run.add_argument("--combination-odds")
    p_run.add_argument("--settings")

    p_settle = sub.add_parser("settle", help="evaluate strategy tickets against race results")
    p_settle.add_argument("--bets", required=True, help="strategy_bets_YYYYMMDD.json")
    p_settle.add_argument("--results", required=True)
    p_settle.add_argument("--payouts")
    p_settle.add_argument("--output")
    return parser


def main() -> None:
    args = _build_parser().parse_args()

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

    if args.command == "collect":
        settings = load_settings(args.settings)
        result = collect_daily_dataset(args.date, settings.project_root, headless=not args.show_browser)
        print(f"OK: canonical={result['canonical_path']}")
        print(f"combination_odds={result['combination_odds_path']}")
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

    if args.command == "run":
        result = run_pipeline(Path(args.input), args.date, args.settings, args.combination_odds)
        print(f"OK: run_id={result['run_id']} races={result['metrics']['races']} horses={result['metrics']['horses']}")
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

    raise SystemExit(2)


if __name__ == "__main__":
    main()
