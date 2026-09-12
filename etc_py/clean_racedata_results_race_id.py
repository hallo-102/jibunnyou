# -*- coding: utf-8 -*-
"""
racedata_results.xlsx の cross-sheet race_id 重複を安全に除去する前処理。

方針:
- YYYYMMDD シートを日付昇順で処理する。
- 同じ race_id が後続シートに再登場した場合、race_name / race_info が同一なら
  最初に出現したシートだけを正として、後続シート側の同 race_id 行を除外する。
- race_name / race_info が食い違う場合は安全のため停止する。
- race_id が空の行はそのまま残す。
- 監査CSVを併せて出力する。

これは rating 計算前の入力クレンジング用であり、元ファイルは変更しない。
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

DATE_SHEET_RE = re.compile(r"^\d{8}$")

RACE_ID_ALIASES = ["レースID", "ﾚｰｽID", "レースId", "レースＩＤ", "レースＩｄ", "race_id"]
RACE_NAME_ALIASES = ["レース名", "race_name"]
RACE_INFO_ALIASES = ["レース情報", "race_info"]


def _find_col(df: pd.DataFrame, aliases: List[str]) -> Optional[str]:
    for c in aliases:
        if c in df.columns:
            return c
    normalized = {str(c).replace(" ", "").replace("　", ""): c for c in df.columns}
    for a in aliases:
        k = a.replace(" ", "").replace("　", "")
        if k in normalized:
            return normalized[k]
    return None


def _norm_text(v) -> str:
    if v is None or pd.isna(v):
        return ""
    return str(v).strip()


def _norm_race_id(v) -> str:
    if v is None or pd.isna(v):
        return ""
    s = str(v).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s


def clean_workbook(src: Path, dst: Path, audit_csv: Path) -> None:
    if not src.exists():
        raise FileNotFoundError(src)

    xls = pd.ExcelFile(src, engine="openpyxl")
    sheet_names = list(xls.sheet_names)
    date_sheets = sorted([s for s in sheet_names if DATE_SHEET_RE.match(str(s))])
    other_sheets = [s for s in sheet_names if s not in date_sheets]

    seen: Dict[str, Dict[str, str]] = {}
    cleaned: Dict[str, pd.DataFrame] = {}
    audit_rows: List[Dict] = []
    conflict_rows: List[Dict] = []

    print(f"[clean] input={src}")
    print(f"[clean] YYYYMMDD sheets={len(date_sheets)}")

    for sheet in date_sheets:
        df = pd.read_excel(xls, sheet_name=sheet)
        race_id_col = _find_col(df, RACE_ID_ALIASES)
        if race_id_col is None:
            cleaned[sheet] = df
            print(f"[clean][warn] {sheet}: race_id列なし。無変更で保持")
            continue

        race_name_col = _find_col(df, RACE_NAME_ALIASES)
        race_info_col = _find_col(df, RACE_INFO_ALIASES)

        race_ids = df[race_id_col].map(_norm_race_id)
        keep_mask = pd.Series(True, index=df.index)

        for rid in sorted(set(race_ids) - {""}):
            idx = df.index[race_ids == rid]
            if len(idx) == 0:
                continue

            race_name = ""
            race_info = ""
            if race_name_col is not None:
                vals = [_norm_text(v) for v in df.loc[idx, race_name_col].tolist() if _norm_text(v)]
                race_name = vals[0] if vals else ""
            if race_info_col is not None:
                vals = [_norm_text(v) for v in df.loc[idx, race_info_col].tolist() if _norm_text(v)]
                race_info = vals[0] if vals else ""

            if rid not in seen:
                seen[rid] = {
                    "sheet": str(sheet),
                    "race_name": race_name,
                    "race_info": race_info,
                }
                continue

            first = seen[rid]
            same_name = (not first["race_name"] or not race_name or first["race_name"] == race_name)
            same_info = (not first["race_info"] or not race_info or first["race_info"] == race_info)

            audit_rows.append({
                "race_id": rid,
                "kept_sheet": first["sheet"],
                "dropped_sheet": str(sheet),
                "dropped_rows": int(len(idx)),
                "same_race_name": bool(same_name),
                "same_race_info": bool(same_info),
                "race_name": race_name,
            })

            if not (same_name and same_info):
                conflict_rows.append({
                    "race_id": rid,
                    "first_sheet": first["sheet"],
                    "later_sheet": str(sheet),
                    "first_race_name": first["race_name"],
                    "later_race_name": race_name,
                    "first_race_info": first["race_info"],
                    "later_race_info": race_info,
                })
                continue

            keep_mask.loc[idx] = False

        before = len(df)
        after_df = df.loc[keep_mask].copy()
        cleaned[sheet] = after_df
        removed = before - len(after_df)
        if removed:
            print(f"[clean] {sheet}: removed_rows={removed}")

    for sheet in other_sheets:
        cleaned[sheet] = pd.read_excel(xls, sheet_name=sheet)
    xls.close()

    if conflict_rows:
        conflict_path = audit_csv.with_name(audit_csv.stem + "_CONFLICT.csv")
        pd.DataFrame(conflict_rows).to_csv(conflict_path, index=False, encoding="utf-8-sig")
        raise RuntimeError(
            f"同一race_idでrace_name/race_infoが食い違う重複が {len(conflict_rows)} 件あります。"
            f"安全のため停止しました: {conflict_path}"
        )

    dst.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(dst, engine="openpyxl") as writer:
        for sheet in sheet_names:
            cleaned[sheet].to_excel(writer, sheet_name=str(sheet)[:31], index=False)

    audit_df = pd.DataFrame(audit_rows)
    audit_csv.parent.mkdir(parents=True, exist_ok=True)
    audit_df.to_csv(audit_csv, index=False, encoding="utf-8-sig")

    dropped_races = int(len(audit_df))
    dropped_rows = int(audit_df["dropped_rows"].sum()) if not audit_df.empty else 0
    print(f"[clean] duplicated race_id groups removed={dropped_races}")
    print(f"[clean] rows removed={dropped_rows}")
    print(f"[clean] output={dst}")
    print(f"[clean] audit={audit_csv}")


def main() -> None:
    parser = argparse.ArgumentParser(description="racedata_results cross-sheet race_id cleaner")
    parser.add_argument("--input", type=Path, default=Path("data/master/racedata_results.xlsx"))
    parser.add_argument("--out", type=Path, default=Path("data/master/racedata_results_clean.xlsx"))
    parser.add_argument("--audit", type=Path, default=Path("data/master/race_id_clean_audit.csv"))
    args = parser.parse_args()
    clean_workbook(args.input.resolve(), args.out.resolve(), args.audit.resolve())


if __name__ == "__main__":
    main()
