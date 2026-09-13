# -*- coding: utf-8 -*-
"""
clean_racedata_results_race_id_v3.py を使う再検証パイプライン。

処理:
1. race_id開催日目に基づき正しいYYYYMMDDシートへ再配置
2. v1/v2再生成
3. entries_v2へodds/pop付与
4. v1 vs v2比較
5. confirmed race level生成
6. confirmed時系列ホールドアウト評価
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
MASTER = ROOT / "data" / "master"
PY = sys.executable


def run(name: str, *args: str) -> None:
    cmd = [PY, "-u", *args]
    print("\n" + "=" * 88)
    print(f"[PIPELINE-V3] {name}")
    print("[CMD] " + " ".join(cmd))
    print("=" * 88)
    p = subprocess.run(cmd, cwd=str(ROOT))
    if p.returncode != 0:
        raise SystemExit(f"{name} failed: returncode={p.returncode}")


def main() -> None:
    src = MASTER / "racedata_results.xlsx"
    clean = MASTER / "racedata_results_clean_v3.xlsx"
    audit = MASTER / "race_id_clean_v3_audit.csv"
    levels = MASTER / "race_levels_clean_v3.xlsx"
    compare = MASTER / "race_levels_clean_v3_v1_v2_compare.xlsx"
    eval_out = MASTER / "confirmed_race_level_clean_v3_evaluation.xlsx"

    run(
        "01_clean_and_reassign_race_ids",
        str(HERE / "clean_racedata_results_race_id_v3.py"),
        "--input", str(src),
        "--out", str(clean),
        "--audit", str(audit),
    )
    run(
        "02_generate_v1_v2",
        str(HERE / "00_Export_To_Excel_4_v2.py"),
        "--excel", str(clean),
        "--out", str(levels),
    )
    run(
        "03_enrich_market",
        str(HERE / "enrich_entries_v2_market.py"),
        "--input", str(levels),
    )
    run(
        "04_compare_v1_v2",
        str(HERE / "compare_race_level_v1_v2.py"),
        "--input", str(levels),
        "--out", str(compare),
    )
    run(
        "05_build_confirmed",
        str(HERE / "build_confirmed_race_level_v2.py"),
        "--input", str(levels),
    )
    run(
        "06_evaluate_confirmed",
        str(HERE / "evaluate_confirmed_race_level_v2.py"),
        "--input", str(levels),
        "--out", str(eval_out),
    )

    print("\n[PIPELINE-V3] ALL DONE")
    print(f"clean={clean}")
    print(f"levels={levels}")
    print(f"compare={compare}")
    print(f"confirmed_eval={eval_out}")
    print(f"audit={audit}")


if __name__ == "__main__":
    main()
