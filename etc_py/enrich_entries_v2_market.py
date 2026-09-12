# -*- coding: utf-8 -*-
"""
race_levels.xlsx の entries_v2 に entries 由来の odds / pop を追加する。

前提:
- clean_racedata_results_race_id.py で cross-sheet race_id 重複を除去した入力から
  00_Export_To_Excel_4_v2.py を実行していること。
- race_id + horse_id が一意でない場合は安全のため停止する。

目的:
confirmed_race_level_v2 の時系列評価で単勝ROIを正しく計算できるようにする。
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

KEYS = ["race_id", "horse_id"]


def norm_ids(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["race_id"] = out["race_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    out["horse_id"] = pd.to_numeric(out["horse_id"], errors="coerce").astype("Int64")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="entries_v2 に odds/pop を追加")
    parser.add_argument("--input", type=Path, default=Path("data/master/race_levels.xlsx"))
    args = parser.parse_args()
    path = args.input.resolve()
    if not path.exists():
        raise FileNotFoundError(path)

    xls = pd.ExcelFile(path, engine="openpyxl")
    required = {"entries", "entries_v2"}
    missing = required - set(xls.sheet_names)
    if missing:
        xls.close()
        raise ValueError(f"必要シート不足: {sorted(missing)}")

    v1 = norm_ids(pd.read_excel(xls, sheet_name="entries"))
    v2 = norm_ids(pd.read_excel(xls, sheet_name="entries_v2"))
    xls.close()

    for name, df in [("entries", v1), ("entries_v2", v2)]:
        if df.duplicated(KEYS).any():
            n = int(df.duplicated(KEYS, keep=False).sum())
            raise ValueError(
                f"{name}: race_id+horse_id が一意ではありません ({n} rows)。"
                "clean_racedata_results_race_id.py を先に実行してください。"
            )

    market_cols = [c for c in ["odds", "pop"] if c in v1.columns]
    if not market_cols:
        raise ValueError("entries に odds/pop がありません。")

    base = v1[KEYS + market_cols].copy()
    enriched = v2.drop(columns=[c for c in market_cols if c in v2.columns], errors="ignore").merge(
        base,
        on=KEYS,
        how="left",
        validate="one_to_one",
    )

    missing_odds = int(pd.to_numeric(enriched.get("odds"), errors="coerce").isna().sum()) if "odds" in enriched.columns else len(enriched)
    missing_pop = int(pd.to_numeric(enriched.get("pop"), errors="coerce").isna().sum()) if "pop" in enriched.columns else len(enriched)

    with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        enriched.to_excel(writer, sheet_name="entries_v2", index=False)

    print(f"[done] entries_v2 market enrichment rows={len(enriched)}")
    print(f"[verify] missing odds={missing_odds} / missing pop={missing_pop}")


if __name__ == "__main__":
    main()
