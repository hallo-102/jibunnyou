# -*- coding: utf-8 -*-
"""Explicit forward rank comparison CLI. No changes to production ranking/bets."""
from __future__ import annotations

import argparse
import importlib
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

engine = importlib.import_module("1_keibayosou_forward_rank_comparison")
normalizer = importlib.import_module("1_keibayosou_conditional_rerank")


def export_excel_pre(excel: Path, destination: Path, race_date: str, generation: str) -> None:
    target = pd.read_excel(excel, sheet_name="TARGET", engine="openpyxl")
    now = pd.read_excel(excel, sheet_name="今走レース情報", engine="openpyxl")
    if "条件付き順位" not in target:
        raise ValueError("先に条件付き再ランキングをExcelへ書き出してください")
    target = normalizer._key(target)
    now = normalizer._key(now)
    race_name = {}
    for rid, group in now.groupby("rid_str"):
        row = group.iloc[0]
        race_name[rid] = str(row.get("クラス", row.get("レース種別", "")))
    races = []
    for rid, group in target.groupby("rid_str"):
        horses = []
        for _, h in group.iterrows():
            horses.append({
                "horse_number": int(h["馬番"]),
                "production_rank": int(h["rank"]),
                "conditional_rank": int(h["条件付き順位"]),
                "horse_name": str(h.get("馬名", "")),
            })
        races.append({"race_id": rid, "class": race_name.get(rid, ""), "horses": horses})
    recorded_at = datetime.now().astimezone().isoformat(timespec="seconds")
    engine.record_pre(destination, race_date=race_date, generation=generation,
                      recorded_at=recorded_at, races=races)
    print(json.dumps({"pre": str(destination), "races": len(races)}, ensure_ascii=False))


def import_result_csv(path: Path) -> dict[str, dict[int, dict]]:
    df = pd.read_csv(path, dtype={"race_id": str})
    required = {"race_id", "horse_number", "finish"}
    if not required.issubset(df.columns):
        raise ValueError(f"結果CSVには{sorted(required)}が必要")
    result: dict[str, dict[int, dict]] = {}
    for _, row in df.iterrows():
        rid = str(row["race_id"]).strip()
        number = int(row["horse_number"])
        values = {"finish": None if pd.isna(row["finish"]) else int(row["finish"])}
        for col in ("win_payout", "place_payout"):
            if col in df.columns:
                values[col] = None if pd.isna(row[col]) else int(row[col])
        bucket = result.setdefault(rid, {})
        if number in bucket:
            raise ValueError(f"レース{rid} 馬番{number}の結果が重複")
        bucket[number] = values
    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="command", required=True)
    pre = sub.add_parser("pre")
    pre.add_argument("--excel", type=Path, required=True)
    pre.add_argument("--date", required=True)
    pre.add_argument("--generation", required=True)
    pre.add_argument("--output", type=Path, required=True)
    post = sub.add_parser("post")
    post.add_argument("--pre", type=Path, required=True)
    post.add_argument("--results", type=Path, required=True)
    post.add_argument("--output", type=Path, required=True)
    summary = sub.add_parser("summary")
    summary.add_argument("--generation", required=True)
    summary.add_argument("posts", type=Path, nargs="+")
    args = ap.parse_args()
    if args.command == "pre":
        export_excel_pre(args.excel, args.output, args.date, args.generation)
    elif args.command == "post":
        engine.settle_post(args.pre, args.output, actual=import_result_csv(args.results),
                           finalized_at=datetime.now().astimezone().isoformat(timespec="seconds"))
        print(json.dumps({"post": str(args.output)}, ensure_ascii=False))
    else:
        print(json.dumps(engine.summarize_post(args.posts, generation=args.generation),
                         ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
