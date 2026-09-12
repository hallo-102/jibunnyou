# -*- coding: utf-8 -*-
"""
racedata_results.xlsx を走査し、race_id の衝突・重複を診断する。

目的:
- 同一 race_id が複数の日付シートに現れていないか
- 同一シート内で同一 race_id + horse_name が重複していないか
- race_id 単位で race_name / race_info が食い違っていないか

出力:
- race_id_collision_audit.xlsx
  * race_id_cross_sheet
  * race_horse_duplicates
  * race_metadata_conflicts
  * summary
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import pandas as pd


def norm_col(s: str) -> str:
    return str(s).replace(" ", "").replace("　", "").replace("\n", "").replace("\r", "")


def pick_col(df: pd.DataFrame, candidates: List[str]):
    lookup = {norm_col(c): c for c in df.columns}
    for c in candidates:
        k = norm_col(c)
        if k in lookup:
            return lookup[k]
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        default=str(Path(__file__).resolve().parents[1] / "data" / "master" / "racedata_results.xlsx"),
    )
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    src = Path(args.input)
    if not src.exists():
        raise FileNotFoundError(src)
    out = Path(args.out) if args.out else src.with_name("race_id_collision_audit.xlsx")

    xls = pd.ExcelFile(src, engine="openpyxl")
    rows = []
    for sheet in xls.sheet_names:
        s = str(sheet)
        if len(s) != 8 or not s.isdigit():
            continue
        df = pd.read_excel(xls, sheet_name=sheet)
        rid_col = pick_col(df, ["レースID", "ﾚｰｽID", "レースId", "レースＩＤ", "race_id"])
        horse_col = pick_col(df, ["馬名", "horse_name"])
        race_name_col = pick_col(df, ["レース名", "race_name"])
        race_info_col = pick_col(df, ["レース情報", "race_info"])
        number_col = pick_col(df, ["馬番", "馬 番", "number"])
        rank_col = pick_col(df, ["着順", "着 順", "順位", "rank"])
        if rid_col is None:
            continue
        for i, r in df.iterrows():
            rid = r.get(rid_col)
            if pd.isna(rid):
                continue
            rid_s = str(rid).strip().replace(".0", "")
            rows.append({
                "sheet_date": s,
                "source_row": int(i) + 2,
                "race_id": rid_s,
                "horse_name": None if horse_col is None else r.get(horse_col),
                "number": None if number_col is None else r.get(number_col),
                "rank": None if rank_col is None else r.get(rank_col),
                "race_name": None if race_name_col is None else r.get(race_name_col),
                "race_info": None if race_info_col is None else r.get(race_info_col),
            })
    xls.close()

    all_rows = pd.DataFrame(rows)
    if all_rows.empty:
        raise ValueError("日付シートから race_id を取得できませんでした。")

    # 1) 同一race_idが複数シート日に存在
    cross = []
    for rid, g in all_rows.groupby("race_id", sort=False):
        dates = sorted(g["sheet_date"].dropna().astype(str).unique().tolist())
        if len(dates) >= 2:
            cross.append({
                "race_id": rid,
                "sheet_count": len(dates),
                "sheet_dates": ",".join(dates),
                "row_count": len(g),
                "horse_count": g["horse_name"].nunique(dropna=True),
            })
    cross_df = pd.DataFrame(cross)

    # 2) 同一日・同一race_id・同一馬の重複
    horse_key = ["sheet_date", "race_id", "horse_name"]
    dup_mask = all_rows.duplicated(horse_key, keep=False)
    horse_dups = all_rows.loc[dup_mask].sort_values(horse_key + ["source_row"]).copy()

    # 3) race_id内でメタ情報が食い違う
    meta_conf = []
    for rid, g in all_rows.groupby("race_id", sort=False):
        race_names = sorted(set(str(x) for x in g["race_name"].dropna().tolist()))
        race_infos = sorted(set(str(x) for x in g["race_info"].dropna().tolist()))
        dates = sorted(set(str(x) for x in g["sheet_date"].dropna().tolist()))
        if len(race_names) > 1 or len(race_infos) > 1 or len(dates) > 1:
            meta_conf.append({
                "race_id": rid,
                "dates": ",".join(dates),
                "race_name_count": len(race_names),
                "race_names": " | ".join(race_names[:10]),
                "race_info_count": len(race_infos),
                "race_infos": " | ".join(race_infos[:10]),
                "row_count": len(g),
            })
    meta_df = pd.DataFrame(meta_conf)

    summary = pd.DataFrame([{
        "source_rows": len(all_rows),
        "unique_race_ids": all_rows["race_id"].nunique(),
        "cross_sheet_race_id_count": len(cross_df),
        "same_sheet_race_horse_duplicate_rows": len(horse_dups),
        "metadata_conflict_race_id_count": len(meta_df),
    }])

    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        summary.to_excel(writer, sheet_name="summary", index=False)
        cross_df.to_excel(writer, sheet_name="race_id_cross_sheet", index=False)
        horse_dups.to_excel(writer, sheet_name="race_horse_duplicates", index=False)
        meta_df.to_excel(writer, sheet_name="race_metadata_conflicts", index=False)

    print(f"[done] {out}")
    print(summary.to_string(index=False))
    if not cross_df.empty:
        print("\n[WARN] 同一race_idが複数日付シートに存在します。先頭20件:")
        print(cross_df.head(20).to_string(index=False))
    if not horse_dups.empty:
        print("\n[WARN] 同一日・同一race_id・同一馬の重複があります。先頭20件:")
        print(horse_dups.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
