from __future__ import annotations

import argparse
from pathlib import Path

from .config import load_settings
from .contracts import load_race_table
from .orchestrator import run_pipeline
from .validation import validate_races


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="keiba-v2")
    sub = parser.add_subparsers(dest="command", required=True)

    p_validate = sub.add_parser("validate", help="validate input data")
    p_validate.add_argument("--input", required=True)
    p_validate.add_argument("--settings")

    p_run = sub.add_parser("run", help="run prediction + odds + shadow pipeline")
    p_run.add_argument("--input", required=True)
    p_run.add_argument("--date", required=True, help="YYYYMMDD")
    p_run.add_argument("--settings")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    if args.command == "validate":
        settings = load_settings(args.settings)
        result = validate_races(load_race_table(args.input), settings.section("validation"))
        if result.warnings:
            print("WARN:", *result.warnings, sep="\n- ")
        if not result.ok:
            print("ERROR:", *result.errors, sep="\n- ")
            raise SystemExit(2)
        print("OK: validation passed")
        return

    if args.command == "run":
        result = run_pipeline(Path(args.input), args.date, args.settings)
        print(f"OK: races={result['metrics']['races']} horses={result['metrics']['horses']}")
        print(f"shadow_bets={result['metrics']['shadow_bets']} stake={result['metrics']['shadow_stake_yen']} yen")
        print(f"predictions={result['prediction_path']}")
        print(f"odds_summary={result['odds_path']}")
        return

    raise SystemExit(2)


if __name__ == "__main__":
    main()
