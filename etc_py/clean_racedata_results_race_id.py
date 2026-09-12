# -*- coding: utf-8 -*-
"""
racedata_results.xlsx の cross-sheet race_id 重複を安全に除去する前処理。

方針:
- YYYYMMDD シートを日付昇順で読む。
- netkeiba/JRA系 race_id の構造
    YYYY + 場コード2桁 + 開催回2桁 + 開催日目2桁 + レース番号2桁
  を使い、同一 (year, place, meeting) 内で「開催日目」と実カレンダー日を対応付ける。
- 例: 同一開催で day=7/8 の両方が 8/15, 8/16 の両シートへ混入していても、
  開催全体の日付順と day 順から day7→8/15, day8→8/16 と解決する。
- race_id が複数シートにある場合、解決した正規シートだけ残す。
- 正規シートを解決できない場合は、race_name/race_info が実質同一なら最初のシートを残す。
- メタデータも異なり、正規シートも解決不能な場合だけ安全停止する。
- race_info 比較時は空白差を正規化する。
- race_id が空の行はそのまま残す。
- 元ファイルは変更しない。
"""
from __future__ import annotations

import argparse
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

DATE_SHEET_RE = re.compile(r"^\d{8}$")
RACE_ID_RE = re.compile(r"^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})$")

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


def _norm_compare_text(v) -> str:
    """race_info等の比較用。改行・全角空白・連続空白の差を無視する。"""
    s = _norm_text(v).replace("　", " ")
    return re.sub(r"\s+", " ", s).strip()


def _norm_race_id(v) -> str:
    if v is None or pd.isna(v):
        return ""
    s = str(v).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s


def _parse_race_id(rid: str) -> Optional[Tuple[int, int, int, int, int]]:
    m = RACE_ID_RE.match(rid)
    if not m:
        return None
    return tuple(int(x) for x in m.groups())  # year, place, meeting, day, race_no


def _first_nonempty(df: pd.DataFrame, idx, col: Optional[str]) -> str:
    if col is None:
        return ""
    for v in df.loc[idx, col].tolist():
        s = _norm_text(v)
        if s:
            return s
    return ""


def _build_meeting_day_map(
    sheet_frames: Dict[str, pd.DataFrame],
    sheet_rid_cols: Dict[str, str],
) -> Tuple[Dict[Tuple[int, int, int, int], str], pd.DataFrame]:
    """
    (year, place, meeting, day) -> 正規 YYYYMMDD sheet を推定する。

    同一開催について、観測された開催日目(day)とカレンダー日(sheet)をそれぞれ
    ソートし、件数が一致する場合に順番対応させる。
    これは同じ週末の両日データが双方のシートへ混入していても機能する。
    """
    meeting_days: Dict[Tuple[int, int, int], set] = defaultdict(set)
    meeting_sheets: Dict[Tuple[int, int, int], set] = defaultdict(set)

    for sheet, df in sheet_frames.items():
        rid_col = sheet_rid_cols.get(sheet)
        if not rid_col:
            continue
        for raw in df[rid_col].tolist():
            rid = _norm_race_id(raw)
            parsed = _parse_race_id(rid)
            if parsed is None:
                continue
            year, place, meeting, day, _ = parsed
            key = (year, place, meeting)
            meeting_days[key].add(day)
            meeting_sheets[key].add(str(sheet))

    mapping: Dict[Tuple[int, int, int, int], str] = {}
    audit_rows: List[Dict] = []

    for key in sorted(set(meeting_days) | set(meeting_sheets)):
        days = sorted(meeting_days.get(key, set()))
        sheets = sorted(meeting_sheets.get(key, set()))
        resolved = len(days) == len(sheets) and len(days) > 0

        if resolved:
            for day, sheet in zip(days, sheets):
                mapping[(key[0], key[1], key[2], day)] = sheet

        audit_rows.append({
            "year": key[0],
            "place_code": key[1],
            "meeting_no": key[2],
            "day_count": len(days),
            "sheet_count": len(sheets),
            "days": ",".join(str(x) for x in days),
            "sheets": ",".join(sheets),
            "mapping_resolved": bool(resolved),
        })

    return mapping, pd.DataFrame(audit_rows)


def clean_workbook(src: Path, dst: Path, audit_csv: Path) -> None:
    if not src.exists():
        raise FileNotFoundError(src)

    xls = pd.ExcelFile(src, engine="openpyxl")
    sheet_names = list(xls.sheet_names)
    date_sheets = sorted([str(s) for s in sheet_names if DATE_SHEET_RE.match(str(s))])
    other_sheets = [str(s) for s in sheet_names if str(s) not in date_sheets]

    print(f"[clean] input={src}")
    print(f"[clean] YYYYMMDD sheets={len(date_sheets)}")

    # 先に全日付シートを読む。開催日目→実日付の対応を全開催履歴から決めるため。
    sheet_frames: Dict[str, pd.DataFrame] = {}
    sheet_rid_cols: Dict[str, str] = {}
    sheet_name_cols: Dict[str, Optional[str]] = {}
    sheet_info_cols: Dict[str, Optional[str]] = {}

    for sheet in date_sheets:
        df = pd.read_excel(xls, sheet_name=sheet)
        sheet_frames[sheet] = df
        rid_col = _find_col(df, RACE_ID_ALIASES)
        if rid_col is not None:
            sheet_rid_cols[sheet] = rid_col
        sheet_name_cols[sheet] = _find_col(df, RACE_NAME_ALIASES)
        sheet_info_cols[sheet] = _find_col(df, RACE_INFO_ALIASES)

    day_map, meeting_map_audit = _build_meeting_day_map(sheet_frames, sheet_rid_cols)
    resolved_meetings = int(meeting_map_audit["mapping_resolved"].sum()) if not meeting_map_audit.empty else 0
    print(f"[clean] meeting-day mapping resolved={resolved_meetings}/{len(meeting_map_audit)}")

    # race_idごとの出現情報を収集。
    occurrences: Dict[str, List[Dict]] = defaultdict(list)
    for sheet in date_sheets:
        df = sheet_frames[sheet]
        rid_col = sheet_rid_cols.get(sheet)
        if rid_col is None:
            continue
        race_ids = df[rid_col].map(_norm_race_id)
        for rid in sorted(set(race_ids) - {""}):
            idx = df.index[race_ids == rid]
            occurrences[rid].append({
                "sheet": sheet,
                "idx": idx,
                "race_name": _first_nonempty(df, idx, sheet_name_cols.get(sheet)),
                "race_info": _first_nonempty(df, idx, sheet_info_cols.get(sheet)),
            })

    # race_idごとに正規シートを決定。
    canonical_sheet: Dict[str, str] = {}
    audit_rows: List[Dict] = []
    unresolved_conflicts: List[Dict] = []

    for rid, occs in occurrences.items():
        if len(occs) == 1:
            canonical_sheet[rid] = occs[0]["sheet"]
            continue

        parsed = _parse_race_id(rid)
        expected_sheet = None
        if parsed is not None:
            year, place, meeting, day, _ = parsed
            expected_sheet = day_map.get((year, place, meeting, day))

        occ_sheets = sorted(o["sheet"] for o in occs)
        resolution_method = ""
        chosen = None

        if expected_sheet in occ_sheets:
            chosen = expected_sheet
            resolution_method = "race_id_meeting_day_map"
        else:
            # day-mapが作れない場合のみメタデータ一致を見て最初のシートへfallback。
            names = {_norm_compare_text(o["race_name"]) for o in occs if _norm_compare_text(o["race_name"])}
            infos = {_norm_compare_text(o["race_info"]) for o in occs if _norm_compare_text(o["race_info"])}
            if len(names) <= 1 and len(infos) <= 1:
                chosen = occ_sheets[0]
                resolution_method = "metadata_same_keep_first"
            else:
                unresolved_conflicts.append({
                    "race_id": rid,
                    "sheets": ",".join(occ_sheets),
                    "expected_sheet": expected_sheet or "",
                    "race_names": " || ".join(sorted(names)),
                    "race_infos": " || ".join(sorted(infos)),
                })
                continue

        canonical_sheet[rid] = chosen

        for o in occs:
            same_name = _norm_compare_text(o["race_name"]) == _norm_compare_text(occs[0]["race_name"])
            same_info = _norm_compare_text(o["race_info"]) == _norm_compare_text(occs[0]["race_info"])
            audit_rows.append({
                "race_id": rid,
                "chosen_sheet": chosen,
                "observed_sheet": o["sheet"],
                "action": "KEEP" if o["sheet"] == chosen else "DROP",
                "rows": int(len(o["idx"])),
                "resolution_method": resolution_method,
                "expected_sheet": expected_sheet or "",
                "same_race_name_as_first": bool(same_name),
                "same_race_info_as_first": bool(same_info),
                "race_name": o["race_name"],
                "race_info": o["race_info"],
            })

    if unresolved_conflicts:
        conflict_path = audit_csv.with_name(audit_csv.stem + "_CONFLICT.csv")
        pd.DataFrame(unresolved_conflicts).to_csv(conflict_path, index=False, encoding="utf-8-sig")
        xls.close()
        raise RuntimeError(
            f"正規シートを自動解決できないrace_idが {len(unresolved_conflicts)} 件あります。"
            f"安全のため停止しました: {conflict_path}"
        )

    # 正規シート以外に存在するrace_id行を落とす。
    cleaned: Dict[str, pd.DataFrame] = {}
    total_removed = 0
    for sheet in date_sheets:
        df = sheet_frames[sheet]
        rid_col = sheet_rid_cols.get(sheet)
        if rid_col is None:
            cleaned[sheet] = df
            print(f"[clean][warn] {sheet}: race_id列なし。無変更で保持")
            continue

        race_ids = df[rid_col].map(_norm_race_id)
        keep_mask = pd.Series(True, index=df.index)
        for rid in sorted(set(race_ids) - {""}):
            target = canonical_sheet.get(rid)
            if target is not None and target != sheet:
                keep_mask.loc[df.index[race_ids == rid]] = False

        after_df = df.loc[keep_mask].copy()
        removed = len(df) - len(after_df)
        total_removed += removed
        cleaned[sheet] = after_df
        if removed:
            print(f"[clean] {sheet}: removed_rows={removed}")

    for sheet in other_sheets:
        cleaned[sheet] = pd.read_excel(xls, sheet_name=sheet)
    xls.close()

    dst.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(dst, engine="openpyxl") as writer:
        for sheet in sheet_names:
            key = str(sheet)
            cleaned[key].to_excel(writer, sheet_name=key[:31], index=False)

    audit_df = pd.DataFrame(audit_rows)
    audit_csv.parent.mkdir(parents=True, exist_ok=True)
    audit_df.to_csv(audit_csv, index=False, encoding="utf-8-sig")

    meeting_audit_path = audit_csv.with_name(audit_csv.stem + "_MEETING_MAP.csv")
    meeting_map_audit.to_csv(meeting_audit_path, index=False, encoding="utf-8-sig")

    duplicated_races = sum(1 for occs in occurrences.values() if len(occs) > 1)
    kept_race_ids = len(canonical_sheet)
    print(f"[clean] duplicated race_id groups resolved={duplicated_races}")
    print(f"[clean] unique race_ids kept={kept_race_ids}")
    print(f"[clean] rows removed={total_removed}")
    print(f"[clean] output={dst}")
    print(f"[clean] audit={audit_csv}")
    print(f"[clean] meeting_map_audit={meeting_audit_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="racedata_results cross-sheet race_id cleaner")
    parser.add_argument("--input", type=Path, default=Path("data/master/racedata_results.xlsx"))
    parser.add_argument("--out", type=Path, default=Path("data/master/racedata_results_clean.xlsx"))
    parser.add_argument("--audit", type=Path, default=Path("data/master/race_id_clean_audit.csv"))
    args = parser.parse_args()
    clean_workbook(args.input.resolve(), args.out.resolve(), args.audit.resolve())


if __name__ == "__main__":
    main()
