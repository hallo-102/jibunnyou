# -*- coding: utf-8 -*-
"""racedata_results.xlsx の race_id 一意性を検査する診断ツール。"""

from __future__ import annotations

import argparse
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_XLSX = BASE_DIR / "data" / "master" / "racedata_results.xlsx"


def _find_race_id_col(df: pd.DataFrame) -> str | None:
    aliases = {"レースID", "ﾚｰｽID", "レースId", "レースＩＤ", "race_id"}
    for col in df.columns:
        key = str(col).replace(" ", "").replace("　", "")
        if key in aliases:
            return str(col)
    return None


def inspect_race_ids(xlsx_path: Path) -> Tuple[Dict[str, List[str]], int]:
    if not xlsx_path.exists():
        raise FileNotFoundError(f"入力Excelが見つかりません: {xlsx_path}")

    xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
    dates_by_rid: Dict[str, set[str]] = defaultdict(set)
    row_count = 0
    try:
        date_sheets = sorted(
            [s for s in xls.sheet_names if re.fullmatch(r"\d{8}", str(s))],
            key=lambda s: int(str(s)),
        )
        for sheet in date_sheets:
            df = pd.read_excel(xls, sheet_name=sheet)
            if df.empty:
                continue
            race_col = _find_race_id_col(df)
            if race_col is None:
                print(f"[WARN] {sheet}: race_id列が見つかりません")
                continue
            values = (
                df[race_col]
                .dropna()
                .astype(str)
                .str.strip()
                .str.replace(r"\.0$", "", regex=True)
            )
            row_count += len(values)
            for rid in values.unique():
                if rid:
                    dates_by_rid[rid].add(str(sheet))
    finally:
        xls.close()

    conflicts = {
        rid: sorted(dates)
        for rid, dates in dates_by_rid.items()
        if len(dates) >= 2
    }
    return conflicts, row_count


def main() -> None:
    parser = argparse.ArgumentParser(
        description="racedata_results.xlsx の同一race_id×複数開催日を検査します"
    )
    parser.add_argument("--excel", default=str(DEFAULT_XLSX))
    parser.add_argument(
        "--report",
        default=None,
        help="重複一覧CSVの出力先。省略時は data/master/race_id_conflicts.csv",
    )
    args = parser.parse_args()

    xlsx_path = Path(args.excel).resolve()
    report_path = (
        Path(args.report).resolve()
        if args.report
        else xlsx_path.parent / "race_id_conflicts.csv"
    )

    print(f"[CHECK] 入力: {xlsx_path}")
    conflicts, row_count = inspect_race_ids(xlsx_path)

    if not conflicts:
        print(f"[OK] 同一race_idが複数日付に存在する問題はありません。確認行数={row_count}")
        if report_path.exists():
            report_path.unlink()
        return

    rows = []
    for rid, dates in sorted(conflicts.items()):
        rows.append(
            {
                "race_id": rid,
                "date_count": len(dates),
                "dates": "|".join(dates),
            }
        )
    report = pd.DataFrame(rows)
    report.to_csv(report_path, index=False, encoding="utf-8-sig")

    print(
        f"[ERROR] 同一race_idが複数開催日に存在します: {len(conflicts)} race_id"
    )
    for row in rows[:20]:
        print(
            f"  race_id={row['race_id']} date_count={row['date_count']} dates={row['dates']}"
        )
    if len(rows) > 20:
        print(f"  ... 残り {len(rows) - 20} 件")
    print(f"[REPORT] {report_path}")
    raise SystemExit(2)


if __name__ == "__main__":
    main()
