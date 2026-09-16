from __future__ import annotations

import argparse
from pathlib import Path

from .adapters import export_canonical_csv, load_legacy_excel
from .config import load_settings
from .contracts import canonicalize, load_race_table
from .features import build_features
from .orchestrator import run_pipeline
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

    p_train = sub.add_parser("train", help="train LightGBM from labeled canonical data")
    p_train.add_argument("--input", required=True)
    p_train.add_argument("--settings")

    p_run = sub.add_parser("run", help="run validation + features + prediction + odds + race selection + SHADOW strategies")
    p_run.add_argument("--input", required=True)
    p_run.add_argument("--date", required=True, help="YYYYMMDD")
    p_run.add_argument("--settings")
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
        sheet: str | int | None
        sheet = int(args.sheet) if str(args.sheet).isdigit() else args.sheet
        df = load_legacy_excel(args.input, sheet_name=sheet)
        dst = export_canonical_csv(df, args.output)
        print(f"OK: exported {len(df)} rows -> {dst}")
        return

    if args.command == "train":
        settings = load_settings(args.settings)
        df = canonicalize(load_race_table(args.input))
        df = build_features(df)
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
        result = run_pipeline(Path(args.input), args.date, args.settings)
        print(f"OK: races={result['metrics']['races']} horses={result['metrics']['horses']}")
        print(f"buy_candidate_races={result['metrics']['buy_candidate_races']}")
        print(f"strategy_bets={result['metrics']['strategy_bets']} stake={result['metrics']['strategy_stake_yen']} yen")
        print(f"predictions={result['prediction_path']}")
        print(f"race_selection={result['race_selection_path']}")
        print(f"strategy={result['strategy_path']}")
        return

    raise SystemExit(2)


if __name__ == "__main__":
    main()
