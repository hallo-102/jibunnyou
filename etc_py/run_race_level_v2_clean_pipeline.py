# -*- coding: utf-8 -*-
"""
race_id 重複を除去したクリーン入力で v1/v2/confirmed を再検証する統合ランナー。

処理順:
1. clean_racedata_results_race_id.py
2. 00_Export_To_Excel_4_v2.py
3. enrich_entries_v2_market.py
4. compare_race_level_v1_v2.py
5. build_confirmed_race_level_v2.py
6. evaluate_confirmed_race_level_v2.py

元の racedata_results.xlsx / race_levels.xlsx は上書きしない。
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import List

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
MASTER = REPO_ROOT / "data" / "master"

CLEANER = HERE / "clean_racedata_results_race_id.py"
GENERATOR = HERE / "00_Export_To_Excel_4_v2.py"
ENRICH = HERE / "enrich_entries_v2_market.py"
COMPARE = HERE / "compare_race_level_v1_v2.py"
BUILD_CONF = HERE / "build_confirmed_race_level_v2.py"
EVAL_CONF = HERE / "evaluate_confirmed_race_level_v2.py"


def run(name: str, cmd: List[str]) -> None:
    print("\n" + "=" * 88)
    print(f"[PIPELINE] {name}")
    print("[CMD] " + " ".join(cmd))
    print("=" * 88)
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT))
    if proc.returncode != 0:
        raise SystemExit(f"{name} failed: returncode={proc.returncode}")


def main() -> None:
    parser = argparse.ArgumentParser(description="clean race-id + v2 validation pipeline")
    parser.add_argument("--input", type=Path, default=MASTER / "racedata_results.xlsx")
    parser.add_argument("--clean-input", type=Path, default=MASTER / "racedata_results_clean.xlsx")
    parser.add_argument("--race-levels", type=Path, default=MASTER / "race_levels_clean_v2.xlsx")
    parser.add_argument("--compare-out", type=Path, default=MASTER / "race_levels_clean_v1_v2_compare.xlsx")
    parser.add_argument("--confirmed-out", type=Path, default=MASTER / "confirmed_race_level_clean_v2_evaluation.xlsx")
    parser.add_argument("--audit", type=Path, default=MASTER / "race_id_clean_audit.csv")
    args = parser.parse_args()

    py = sys.executable
    src = args.input.resolve()
    clean_src = args.clean_input.resolve()
    race_levels = args.race_levels.resolve()
    compare_out = args.compare_out.resolve()
    confirmed_out = args.confirmed_out.resolve()
    audit = args.audit.resolve()

    run("01_clean_race_ids", [
        py, "-u", str(CLEANER),
        "--input", str(src),
        "--out", str(clean_src),
        "--audit", str(audit),
    ])

    run("02_generate_v1_v2", [
        py, "-u", str(GENERATOR),
        "--excel", str(clean_src),
        "--out", str(race_levels),
    ])

    run("03_enrich_v2_market", [
        py, "-u", str(ENRICH),
        "--input", str(race_levels),
    ])

    run("04_compare_v1_v2", [
        py, "-u", str(COMPARE),
        "--input", str(race_levels),
        "--out", str(compare_out),
    ])

    run("05_build_confirmed", [
        py, "-u", str(BUILD_CONF),
        "--input", str(race_levels),
    ])

    run("06_evaluate_confirmed", [
        py, "-u", str(EVAL_CONF),
        "--input", str(race_levels),
        "--out", str(confirmed_out),
    ])

    print("\n" + "=" * 88)
    print("[DONE] clean v2 validation pipeline completed")
    print(f"clean input   : {clean_src}")
    print(f"race levels   : {race_levels}")
    print(f"v1/v2 compare : {compare_out}")
    print(f"confirmed eval: {confirmed_out}")
    print(f"clean audit   : {audit}")
    print("=" * 88)


if __name__ == "__main__":
    main()
