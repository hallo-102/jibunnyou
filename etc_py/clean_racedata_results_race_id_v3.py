# -*- coding: utf-8 -*-
"""
racedata_results.xlsx の cross-sheet race_id 混在を補正する v3 クリーナー。

目的:
- 同じ race_id が複数日付シートへ重複収録されたケースを解消する。
- さらに、開催7日目/8日目など複数日分が同一シートへ寄ってしまうケースで、
  race_id が示す開催日目を使って正しい YYYYMMDD シートへ再配置する。

重要:
- 元Excelは変更しない。
- race_id 構造: YYYY + 場コード2桁 + 開催回2桁 + 開催日目2桁 + レース番号2桁
- meeting-day の対応は、まず観測シート数=開催日目数なら単純対応。
- 観測シート数が不足する場合は、その開催の最初の観測日から、元ブックの
  YYYYMMDDシート列を開催日目数ぶん連続で取り、観測済みシートがすべて含まれる
  場合だけ補完対応する。
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
    return tuple(int(x) for x in m.groups())


def _norm_text(v) -> str:
    if v is None or pd.isna(v):
        return ""
    return re.sub(r"\s+", " ", str(v).replace("　", " ")).strip()


def _first_nonempty(df: pd.DataFrame, idx, col: Optional[str]) -> str:
    if col is None:
        return ""
    for v in df.loc[idx, col].tolist():
        s = _norm_text(v)
        if s:
            return s
    return ""


def build_meeting_map(
    date_sheets: List[str],
    frames: Dict[str, pd.DataFrame],
    rid_cols: Dict[str, str],
) -> Tuple[Dict[Tuple[int, int, int, int], str], pd.DataFrame]:
    meeting_days = defaultdict(set)
    meeting_sheets = defaultdict(set)

    for sheet in date_sheets:
        df = frames[sheet]
        rid_col = rid_cols.get(sheet)
        if rid_col is None:
            continue
        for raw in df[rid_col].tolist():
            rid = _norm_race_id(raw)
            p = _parse_race_id(rid)
            if p is None:
                continue
            year, place, meeting, day, _ = p
            key = (year, place, meeting)
            meeting_days[key].add(day)
            meeting_sheets[key].add(sheet)

    sheet_pos = {s: i for i, s in enumerate(date_sheets)}
    mapping: Dict[Tuple[int, int, int, int], str] = {}
    audit_rows = []

    for key in sorted(meeting_days):
        days = sorted(meeting_days[key])
        observed = sorted(meeting_sheets[key])
        candidates: List[str] = []
        method = ""

        if len(days) == len(observed) and days:
            candidates = observed
            method = "observed_count_match"
        elif days and observed:
            # 観測最初日から、元ブックの日付シートを開催日目数ぶん連続取得。
            start = sheet_pos[observed[0]]
            seq = date_sheets[start:start + len(days)]
            if len(seq) == len(days) and set(observed).issubset(set(seq)):
                candidates = seq
                method = "calendar_sequence_fill"

        resolved = len(candidates) == len(days) and len(days) > 0
        if resolved:
            for day, sheet in zip(days, candidates):
                mapping[(key[0], key[1], key[2], day)] = sheet

        audit_rows.append({
            "year": key[0],
            "place_code": key[1],
            "meeting_no": key[2],
            "day_count": len(days),
            "observed_sheet_count": len(observed),
            "days": ",".join(map(str, days)),
            "observed_sheets": ",".join(observed),
            "candidate_sheets": ",".join(candidates),
            "mapping_resolved": resolved,
            "resolution_method": method,
        })

    return mapping, pd.DataFrame(audit_rows)


def clean_workbook(src: Path, dst: Path, audit_csv: Path) -> None:
    if not src.exists():
        raise FileNotFoundError(src)

    xls = pd.ExcelFile(src, engine="openpyxl")
    sheet_names = [str(s) for s in xls.sheet_names]
    date_sheets = sorted([s for s in sheet_names if DATE_SHEET_RE.match(s)])
    other_sheets = [s for s in sheet_names if s not in date_sheets]

    frames: Dict[str, pd.DataFrame] = {}
    rid_cols: Dict[str, str] = {}
    name_cols: Dict[str, Optional[str]] = {}
    info_cols: Dict[str, Optional[str]] = {}

    for sheet in date_sheets:
        df = pd.read_excel(xls, sheet_name=sheet)
        frames[sheet] = df
        rc = _find_col(df, RACE_ID_ALIASES)
        if rc is not None:
            rid_cols[sheet] = rc
        name_cols[sheet] = _find_col(df, RACE_NAME_ALIASES)
        info_cols[sheet] = _find_col(df, RACE_INFO_ALIASES)

    meeting_map, meeting_audit = build_meeting_map(date_sheets, frames, rid_cols)
    unresolved = meeting_audit[~meeting_audit["mapping_resolved"]].copy()
    print(f"[clean-v3] meeting mapping resolved={int(meeting_audit['mapping_resolved'].sum())}/{len(meeting_audit)}")

    # race_id単位で全出現を集約
    occurrences = defaultdict(list)
    for sheet in date_sheets:
        df = frames[sheet]
        rid_col = rid_cols.get(sheet)
        if rid_col is None:
            continue
        ids = df[rid_col].map(_norm_race_id)
        for rid in sorted(set(ids) - {""}):
            idx = df.index[ids == rid]
            occurrences[rid].append({
                "sheet": sheet,
                "idx": idx,
                "race_name": _first_nonempty(df, idx, name_cols[sheet]),
                "race_info": _first_nonempty(df, idx, info_cols[sheet]),
            })

    # 出力用: 元シートをコピーし、race_idあり行はいったん除去してから正規シートへ戻す。
    output_frames: Dict[str, pd.DataFrame] = {}
    base_nonrace: Dict[str, pd.DataFrame] = {}
    for sheet in date_sheets:
        df = frames[sheet]
        rid_col = rid_cols.get(sheet)
        if rid_col is None:
            base_nonrace[sheet] = df.copy()
        else:
            ids = df[rid_col].map(_norm_race_id)
            base_nonrace[sheet] = df.loc[ids == ""].copy()
        output_frames[sheet] = base_nonrace[sheet].copy()

    audit_rows = []
    unresolved_rids = []

    for rid, occs in occurrences.items():
        p = _parse_race_id(rid)
        target = None
        method = ""
        if p is not None:
            year, place, meeting, day, _ = p
            target = meeting_map.get((year, place, meeting, day))
            if target is not None:
                method = "race_id_meeting_day_map"

        if target is None:
            # fallback: 完全同一メタデータなら最初の出現を採用
            names = {_norm_text(o["race_name"]) for o in occs if _norm_text(o["race_name"])}
            infos = {_norm_text(o["race_info"]) for o in occs if _norm_text(o["race_info"])}
            if len(names) <= 1 and len(infos) <= 1:
                target = sorted(o["sheet"] for o in occs)[0]
                method = "metadata_same_keep_first"
            else:
                unresolved_rids.append(rid)
                continue

        # targetシートに元コピーがあればそれを優先。なければ最初のコピーを移送。
        source_occ = next((o for o in occs if o["sheet"] == target), None)
        if source_occ is None:
            source_occ = sorted(occs, key=lambda o: o["sheet"])[0]
            method += "+moved_from_other_sheet"

        src_df = frames[source_occ["sheet"]]
        rows = src_df.loc[source_occ["idx"]].copy()
        output_frames[target] = pd.concat([output_frames[target], rows], ignore_index=True)

        for o in occs:
            audit_rows.append({
                "race_id": rid,
                "target_sheet": target,
                "source_sheet_used": source_occ["sheet"],
                "observed_sheet": o["sheet"],
                "resolution_method": method,
                "row_count": int(len(o["idx"])),
                "race_name": o["race_name"],
                "race_info": o["race_info"],
            })

    if unresolved_rids:
        conflict = audit_csv.with_name(audit_csv.stem + "_UNRESOLVED.csv")
        pd.DataFrame({"race_id": unresolved_rids}).to_csv(conflict, index=False, encoding="utf-8-sig")
        xls.close()
        raise RuntimeError(f"正規シートを解決できないrace_id={len(unresolved_rids)}。{conflict}")

    # race_id順に並べる。race_id空行は末尾に残る。
    for sheet in date_sheets:
        df = output_frames[sheet]
        rid_col = rid_cols.get(sheet)
        if rid_col is not None and rid_col in df.columns:
            ids = df[rid_col].map(_norm_race_id)
            with_rid = df.loc[ids != ""].copy()
            no_rid = df.loc[ids == ""].copy()
            if not with_rid.empty:
                with_rid["__rid_sort"] = with_rid[rid_col].map(_norm_race_id)
                with_rid = with_rid.sort_values("__rid_sort", kind="mergesort").drop(columns="__rid_sort")
            output_frames[sheet] = pd.concat([with_rid, no_rid], ignore_index=True)

    for sheet in other_sheets:
        output_frames[sheet] = pd.read_excel(xls, sheet_name=sheet)
    xls.close()

    dst.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(dst, engine="openpyxl") as writer:
        for sheet in sheet_names:
            output_frames[sheet].to_excel(writer, sheet_name=sheet[:31], index=False)

    audit_df = pd.DataFrame(audit_rows)
    audit_df.to_csv(audit_csv, index=False, encoding="utf-8-sig")
    meeting_path = audit_csv.with_name(audit_csv.stem + "_MEETING_MAP.csv")
    meeting_audit.to_csv(meeting_path, index=False, encoding="utf-8-sig")

    # 検証
    unique_rids = len(occurrences)
    duplicated = sum(1 for v in occurrences.values() if len(v) > 1)
    print(f"[clean-v3] unique race_ids={unique_rids}")
    print(f"[clean-v3] duplicated race_ids observed={duplicated}")
    print(f"[clean-v3] output={dst}")
    print(f"[clean-v3] audit={audit_csv}")
    print(f"[clean-v3] meeting_map={meeting_path}")
    if not unresolved.empty:
        print("[clean-v3][warn] unresolved meeting rows remain in audit, but individual race_id fallback succeeded where possible")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=Path("data/master/racedata_results.xlsx"))
    parser.add_argument("--out", type=Path, default=Path("data/master/racedata_results_clean.xlsx"))
    parser.add_argument("--audit", type=Path, default=Path("data/master/race_id_clean_audit.csv"))
    args = parser.parse_args()
    clean_workbook(args.input.resolve(), args.out.resolve(), args.audit.resolve())


if __name__ == "__main__":
    main()
