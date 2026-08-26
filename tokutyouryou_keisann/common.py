# -*- coding: utf-8 -*-
from __future__ import annotations

import math
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple, TypeAlias

import numpy as np
import pandas as pd

from .config import (
    CONFIG,
    EMPIRICAL_WEIGHT_SIGN_GUARD,
    FEAT_COLS,
    OPTIMIZER_FIXED_ZERO_FEATURES,
    PLACE_MAP,
    RACELEVEL_COLS,
)


WeightKey: TypeAlias = str | tuple[str, str]
WeightsMap: TypeAlias = Dict[WeightKey, Dict[str, float]]


RESULTS_EXCEL_HEADER = 0
RESULTS_EXCEL_SKIPROWS = None


class ResultsWorkbookSchemaError(ValueError):
    """結果Excelのヘッダーまたは必須列が安全に解釈できない場合の例外。"""


def _is_missing_result_header_part(value: Any) -> bool:
    """空欄・NaN・pandas生成のUnnamedを列名の無効要素として判定する。"""
    if value is None:
        return True
    if isinstance(value, str):
        text = value.strip()
        return not text or text.lower().startswith("unnamed:")
    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        return False
    return isinstance(missing, (bool, np.bool_)) and bool(missing)


def _normalize_result_column(value: Any, column_index: int) -> str:
    """結果Excelの列名を型に依存せず、空要素を除いた文字列へ変換する。"""
    if isinstance(value, (tuple, list)):
        parts = [
            str(part).strip()
            for part in value
            if not _is_missing_result_header_part(part)
        ]
        return " / ".join(parts) if parts else f"Unnamed: {column_index}"
    if _is_missing_result_header_part(value):
        return f"Unnamed: {column_index}"
    return str(value).strip()


def _format_result_columns_for_diagnostic(columns: Any) -> list[dict[str, str]]:
    """異常時だけ表示する列名の値・Python型一覧を作る。"""
    return [
        {"value": repr(column), "type": type(column).__name__}
        for column in list(columns)
    ]


def _normalize_result_columns(
    columns: Any,
    *,
    xlsx_path: str | Path,
    sheet_name: str,
) -> list[str]:
    """列名を正規化し、正規化後の重複を診断付きで拒否する。"""
    raw_columns = list(columns)
    normalized = [
        _normalize_result_column(column, index)
        for index, column in enumerate(raw_columns)
    ]
    positions: dict[str, list[int]] = {}
    for index, column in enumerate(normalized):
        positions.setdefault(column, []).append(index)
    duplicates = {
        column: indexes
        for column, indexes in positions.items()
        if len(indexes) > 1
    }
    if duplicates:
        raise ResultsWorkbookSchemaError(
            "結果Excelの正規化後の列名が重複しています。\n"
            f"Excelファイル={Path(xlsx_path).resolve()}\n"
            f"問題のシート={sheet_name}\n"
            f"重複列={duplicates}\n"
            f"実際の列と型={_format_result_columns_for_diagnostic(raw_columns)}\n"
            f"使用した読込指定=header={RESULTS_EXCEL_HEADER}, "
            f"skiprows={RESULTS_EXCEL_SKIPROWS}\n"
            "修正候補=Excelの1行目にある重複・空欄・結合ヘッダー、または出力元の列結合処理"
        )
    return normalized


def _results_schema_error(
    *,
    xlsx_path: str | Path,
    sheet_name: str,
    expected: dict[str, list[str]],
    actual_columns: Any,
    raw_columns: Any,
) -> ResultsWorkbookSchemaError:
    """必須列欠落時にファイル・シート・読込指定を含む例外を作る。"""
    return ResultsWorkbookSchemaError(
        "結果Excelの対象シートで必須列が不足しています。\n"
        f"Excelファイル={Path(xlsx_path).resolve()}\n"
        f"問題のシート={sheet_name}\n"
        f"期待した必須列={expected}\n"
        f"実際に取得した列={list(actual_columns)}\n"
        f"実際の列と型={_format_result_columns_for_diagnostic(raw_columns)}\n"
        f"使用した読込指定=header={RESULTS_EXCEL_HEADER}, "
        f"skiprows={RESULTS_EXCEL_SKIPROWS}\n"
        "修正候補=Excelの1行目、結合セル、出力元の列構成、またはheader/skiprows指定"
    )


def _norm_name(s: Any) -> str:
    if pd.isna(s):
        return ""
    s = str(s).strip()
    s = re.sub(r"\s+", "", s)
    return s


def _mean(s: pd.Series, default: float) -> float:
    v = pd.to_numeric(s, errors="coerce").mean()
    return default if math.isnan(v) else float(v)


def find_col(cols: Any, cand: List[str]) -> str | None:
    for c in cand:
        if c in cols:
            return c
    return None


def _calc_box_trifecta_points(n: int) -> int:
    return math.comb(n, 3)


def _normalize_combo(sv: Any) -> str:
    if pd.isna(sv):
        return ""
    parts = re.split(r"[^\d]+", str(sv))
    nums = [int(x) for x in parts if x.isdigit()]
    nums = sorted(nums)
    return "-".join(str(n) for n in nums) if nums else ""


def _yen_to_int(x: Any) -> int:
    if pd.isna(x):
        return 0
    s = re.sub(r"[^\d]", "", str(x))
    return int(s) if s else 0


def _clip_weight_by_name(name: str, value: float) -> float:
    if name in OPTIMIZER_FIXED_ZERO_FEATURES:
        return 0.0

    if name in RACELEVEL_COLS:
        lo = float(CONFIG["RACELEVEL_WEIGHT_MIN"])
        hi = float(CONFIG["RACELEVEL_WEIGHT_MAX"])
    else:
        lo = float(CONFIG["WEIGHT_MIN"])
        hi = float(CONFIG["WEIGHT_MAX"])
    clipped = max(lo, min(hi, float(value)))
    expected_sign = EMPIRICAL_WEIGHT_SIGN_GUARD.get(name)
    if expected_sign is None or clipped == 0.0:
        return clipped
    if (clipped > 0.0 and expected_sign < 0) or (clipped < 0.0 and expected_sign > 0):
        return abs(clipped) * float(expected_sign)
    return clipped


def _blend_weights(base_w: Dict[str, float], place_w: Dict[str, float], alpha: float) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for k in FEAT_COLS:
        bw = float(base_w.get(k, 0.0))
        pw = float(place_w.get(k, 0.0))
        v = (1.0 - alpha) * pw + alpha * bw
        out[k] = _clip_weight_by_name(k, v)
    return out


def _normalize_surface_name(value: Any) -> str:
    if pd.isna(value):
        return ""

    s = str(value).strip()
    if not s:
        return ""
    if "芝" in s:
        return "芝"
    if "ダ" in s:
        return "ダ"
    return ""


def _get_weights_for_place_surface(
    weights_map: WeightsMap,
    place: str,
    surface: str,
) -> Dict[str, float]:
    place = str(place or "").strip()
    surface = _normalize_surface_name(surface)
    place_surface_key = (place, surface)

    if place and surface and place_surface_key in weights_map and isinstance(weights_map[place_surface_key], dict):
        return weights_map[place_surface_key]
    if place and place in weights_map and isinstance(weights_map[place], dict):
        return weights_map[place]
    return weights_map.get("__default__", {})


def _get_weights_for_place(weights_map: WeightsMap, place: str) -> Dict[str, float]:
    return _get_weights_for_place_surface(weights_map, place, "")


@dataclass
class RaceMeta:
    rid_str: str
    date: str
    place_code: str
    place_name: str


def discover_files(pattern: str) -> List[str]:
    import glob

    files = glob.glob(pattern)

    def _ok(p: str) -> bool:
        for kw in CONFIG["EXCLUDE_KEYWORDS"]:
            if kw and kw in os.path.basename(p):
                return False
        return True

    files = [p for p in files if _ok(p)]

    # 同じ開催日の正規ファイルとバックアップ名を同時投入しない。
    files_by_date: Dict[str, List[str]] = {}
    files_without_date: List[str] = []
    for p in files:
        match = re.search(r"(\d{8})", os.path.basename(p))
        if match is None:
            files_without_date.append(p)
            continue
        files_by_date.setdefault(match.group(1), []).append(p)

    selected_files = list(files_without_date)
    for date_text, same_date_files in files_by_date.items():
        if len(same_date_files) == 1:
            selected_files.extend(same_date_files)
            continue

        canonical_name = f"馬の競走成績_{date_text}.xlsx"
        canonical_files = [
            p
            for p in same_date_files
            if os.path.basename(p) == canonical_name
        ]
        if len(canonical_files) != 1:
            actual_files = [str(Path(p).resolve()) for p in sorted(same_date_files)]
            raise RuntimeError(
                "同一日付の入力Excelが複数あり、正規ファイルを一意に選べません。\n"
                f"対象日付={date_text}\n"
                f"期待する正規ファイル名={canonical_name}\n"
                f"実際の候補={actual_files}"
            )

        selected = canonical_files[0]
        excluded = [
            str(Path(p).resolve())
            for p in same_date_files
            if p != selected
        ]
        print(
            "[WARN] 同一日付の入力Excel重複を除外 "
            f"date={date_text} selected={Path(selected).resolve()} "
            f"excluded={excluded}"
        )
        selected_files.append(selected)

    files = selected_files

    def _key(p: str) -> Tuple[int, str]:
        m = re.search(r"(\d{8})", os.path.basename(p))
        return (int(m.group(1)) if m else -1, p)

    return sorted(files, key=_key)


def resolve_duplicate_feature_races(
    frame: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """複数の入力日に存在する同一レースは、結果確定日と一致する版だけを残す。"""
    required_columns = {"rid_str", "馬番", "date", "source_file_name"}
    missing_columns = sorted(required_columns - set(frame.columns))
    if missing_columns:
        raise ValueError(
            "重複レース解決に必要な列がありません。"
            f" missing_columns={missing_columns}"
        )

    work = frame.copy()
    work["rid_str"] = work["rid_str"].fillna("").astype(str).str.strip()
    work["date"] = work["date"].fillna("").astype(str).str.strip()
    work["source_file_name"] = (
        work["source_file_name"].fillna("").astype(str).str.strip()
    )

    source_counts = work.groupby("rid_str", dropna=False)["source_file_name"].nunique()
    duplicate_rids = source_counts[source_counts.gt(1)].index.tolist()
    if not duplicate_rids:
        return work, pd.DataFrame(
            columns=[
                "rid_str",
                "result_date",
                "selected_source_file",
                "excluded_source_files",
                "excluded_rows",
            ]
        )

    drop_indices: list[Any] = []
    summary_rows: list[dict[str, Any]] = []
    for rid in duplicate_rids:
        race = work.loc[work["rid_str"].eq(rid)]
        result_dates = sorted(date for date in race["date"].unique() if date)
        if len(result_dates) != 1:
            raise RuntimeError(
                "重複レースの結果確定日を一意に特定できません。"
                f" rid_str={rid} result_dates={result_dates}"
            )
        result_date = result_dates[0]
        source_files = sorted(race["source_file_name"].unique().tolist())

        matching_sources = []
        for source_file in source_files:
            date_matches = re.findall(r"(\d{8})", os.path.basename(source_file))
            if result_date in date_matches:
                matching_sources.append(source_file)
        if len(matching_sources) != 1:
            raise RuntimeError(
                "重複レースで結果確定日と一致する入力版を一意に選べません。"
                f" rid_str={rid} result_date={result_date} "
                f"source_files={source_files} matching_sources={matching_sources}"
            )

        selected_source = matching_sources[0]
        selected = race.loc[race["source_file_name"].eq(selected_source)]
        if selected.duplicated(subset=["rid_str", "馬番"], keep=False).any():
            duplicate_keys = selected.loc[
                selected.duplicated(subset=["rid_str", "馬番"], keep=False),
                ["rid_str", "馬番", "source_file_name"],
            ].to_dict("records")
            raise RuntimeError(
                "採用対象の入力版内でレースID・馬番が重複しています。"
                f" duplicate_keys={duplicate_keys}"
            )

        excluded_mask = race["source_file_name"].ne(selected_source)
        excluded = race.loc[excluded_mask]
        drop_indices.extend(excluded.index.tolist())
        summary_rows.append(
            {
                "rid_str": rid,
                "result_date": result_date,
                "selected_source_file": selected_source,
                "excluded_source_files": ", ".join(
                    sorted(excluded["source_file_name"].unique().tolist())
                ),
                "excluded_rows": int(len(excluded)),
            }
        )

    resolved = work.drop(index=drop_indices).copy()
    remaining_duplicates = resolved.duplicated(
        subset=["rid_str", "馬番"], keep=False
    )
    if remaining_duplicates.any():
        duplicate_keys = resolved.loc[
            remaining_duplicates,
            ["rid_str", "馬番", "source_file_name"],
        ].to_dict("records")
        raise RuntimeError(
            "重複レース解決後もレースID・馬番が重複しています。"
            f" duplicate_keys={duplicate_keys}"
        )

    return resolved.reset_index(drop=True), pd.DataFrame(summary_rows)


def build_rid_to_date_map(results_xlsx: str) -> Dict[str, str]:
    if not os.path.exists(results_xlsx):
        return {}
    xls = pd.ExcelFile(results_xlsx)
    rid_to_date: Dict[str, str] = {}
    for sh in xls.sheet_names:
        if not (len(sh) == 8 and sh.isdigit()):
            continue
        try:
            df = pd.read_excel(results_xlsx, sheet_name=sh, engine="openpyxl")
        except Exception:
            continue
        if "レースID" not in df.columns:
            continue
        for rid in df["レースID"].astype(str).dropna().values:
            rid_to_date[str(rid)] = sh
    return rid_to_date


def parse_rid_meta(rid_str: str, rid_to_date: Dict[str, str]) -> RaceMeta:
    rid_str = str(rid_str)
    # rid_str は YYYYMMDD + 競馬場コード2桁 + レース番号2桁。
    place_code = rid_str[8:10] if len(rid_str) >= 10 else ""
    place_name = PLACE_MAP.get(place_code, "")
    date = rid_to_date.get(rid_str, "")
    if not date:
        date = rid_str[:8] if len(rid_str) >= 8 else ""
    return RaceMeta(rid_str=rid_str, date=date, place_code=place_code, place_name=place_name)


def _coalesce_merge_columns(df: pd.DataFrame, base_cols: List[str]) -> pd.DataFrame:
    out = df.copy()

    for base in base_cols:
        candidates = [c for c in [base, f"{base}_x", f"{base}_y"] if c in out.columns]
        if not candidates:
            continue

        merged: pd.Series | None = None
        for col in candidates:
            current = out[col].copy()
            if pd.api.types.is_object_dtype(current) or pd.api.types.is_string_dtype(current):
                empty_mask = current.notna() & current.astype(str).str.strip().eq("")
                current = current.mask(empty_mask, np.nan)

            if merged is None:
                merged = current
            else:
                merged = merged.combine_first(current)

        if merged is None:
            continue

        out[base] = merged
        drop_cols = [c for c in candidates if c != base]
        if drop_cols:
            out = out.drop(columns=drop_cols)

    return out


def load_results_all_sheets(xlsx_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not os.path.exists(xlsx_path):
        raise FileNotFoundError(f"結果ファイルが見つかりません: {xlsx_path}")

    entry_expected = {
        "レースID": ["レースID", "race_id"],
        "馬名": ["馬名", "馬 名"],
        "着順": ["着順", "着 順"],
    }
    payout_expected = {
        "レースID": ["レースID", "race_id"],
        "払戻種別": ["払戻種別", "券種", "式別", "種別"],
        "組番": ["組番", "組み合わせ", "馬番"],
        "払戻金": ["払戻金", "払戻", "配当", "払戻金(円)"],
    }
    entries_rows, payout_rows = [], []
    adopted_sheet_count = 0
    excluded_sheet_count = 0

    with pd.ExcelFile(xlsx_path, engine="openpyxl") as excel:
        sheet_names = list(excel.sheet_names)
        for sheet_name in sheet_names:
            df = pd.read_excel(
                excel,
                sheet_name=sheet_name,
                header=RESULTS_EXCEL_HEADER,
                skiprows=RESULTS_EXCEL_SKIPROWS,
            )
            if df is None or df.empty:
                excluded_sheet_count += 1
                continue

            worksheet = excel.book[sheet_name]
            raw_header_row = next(
                worksheet.iter_rows(
                    min_row=1,
                    max_row=1,
                    min_col=1,
                    max_col=len(df.columns),
                    values_only=True,
                ),
                tuple(df.columns),
            )
            raw_columns = list(raw_header_row)
            if len(raw_columns) != len(df.columns):
                raw_columns = list(df.columns)

            df2 = df.copy()
            df2.columns = _normalize_result_columns(
                raw_columns,
                xlsx_path=xlsx_path,
                sheet_name=sheet_name,
            )
            cols = list(df2.columns)

            race_id_col = find_col(cols, entry_expected["レースID"])
            horse_name_col = find_col(cols, entry_expected["馬名"])
            rank_col = find_col(cols, entry_expected["着順"])
            entry_markers_present = any(
                column is not None
                for column in (horse_name_col, rank_col)
            )
            entry_ready = all(
                column is not None
                for column in (race_id_col, horse_name_col, rank_col)
            )
            if entry_markers_present and not entry_ready:
                raise _results_schema_error(
                    xlsx_path=xlsx_path,
                    sheet_name=sheet_name,
                    expected=entry_expected,
                    actual_columns=cols,
                    raw_columns=raw_columns,
                )

            payout_type_col = find_col(cols, payout_expected["払戻種別"])
            combo_col = find_col(cols, payout_expected["組番"])
            payout_col = find_col(cols, payout_expected["払戻金"])
            payout_markers_present = any(
                column is not None
                for column in (payout_type_col, payout_col)
            )
            payout_ready = all(
                column is not None
                for column in (race_id_col, payout_type_col, combo_col, payout_col)
            )
            if payout_markers_present and not payout_ready:
                raise _results_schema_error(
                    xlsx_path=xlsx_path,
                    sheet_name=sheet_name,
                    expected=payout_expected,
                    actual_columns=cols,
                    raw_columns=raw_columns,
                )

            if not entry_ready and not payout_ready:
                excluded_sheet_count += 1
                continue
            adopted_sheet_count += 1

            if entry_ready:
                entry_columns = [race_id_col, horse_name_col]
                horse_number_col = find_col(cols, ["馬番", "馬 番"])
                if horse_number_col is not None:
                    entry_columns.append(horse_number_col)
                entry_columns.append(rank_col)
                sub = df2[entry_columns].copy()
                sub["rid_str"] = sub[race_id_col].astype(str)
                sub["name_norm"] = sub[horse_name_col].map(_norm_name)
                sub["着順_num"] = pd.to_numeric(sub[rank_col], errors="coerce")
                if horse_number_col is not None:
                    sub["馬番_int"] = pd.to_numeric(
                        sub[horse_number_col], errors="coerce"
                    ).astype("Int64")
                else:
                    sub["馬番_int"] = pd.Series([pd.NA] * len(sub), dtype="Int64")
                entries_rows.append(
                    sub[["rid_str", "name_norm", "着順_num", "馬番_int"]]
                )

            if payout_ready:
                subp = df2[[race_id_col, payout_type_col, combo_col, payout_col]].copy()
                subp["rid_str"] = subp[race_id_col].astype(str)
                subp["払戻種別"] = subp[payout_type_col].astype(str)
                subp["組番_norm"] = subp[combo_col].map(_normalize_combo)
                subp["払戻金_int"] = subp[payout_col].map(_yen_to_int)
                payout_rows.append(
                    subp[["rid_str", "払戻種別", "組番_norm", "払戻金_int"]]
                )

    df_entries = pd.concat(entries_rows, ignore_index=True) if entries_rows else pd.DataFrame(
        columns=["rid_str", "name_norm", "着順_num", "馬番_int"]
    )
    df_payout = pd.concat(payout_rows, ignore_index=True) if payout_rows else pd.DataFrame(
        columns=["rid_str", "払戻種別", "組番_norm", "払戻金_int"]
    )
    if df_entries.empty or df_payout.empty:
        missing_output = []
        if df_entries.empty:
            missing_output.append("結果エントリー")
        if df_payout.empty:
            missing_output.append("払戻")
        raise ResultsWorkbookSchemaError(
            "結果Excelから必要なデータを作成できませんでした。\n"
            f"Excelファイル={Path(xlsx_path).resolve()}\n"
            f"不足データ={missing_output}\n"
            f"全シート数={len(sheet_names)}, 採用シート数={adopted_sheet_count}, "
            f"除外シート数={excluded_sheet_count}\n"
            f"使用した読込指定=header={RESULTS_EXCEL_HEADER}, "
            f"skiprows={RESULTS_EXCEL_SKIPROWS}\n"
            "修正候補=対象シートの必須列、シート種別、またはExcel出力元"
        )
    print(
        "[INFO] 結果Excel読込 "
        f"path={Path(xlsx_path).resolve()} "
        f"sheets={len(sheet_names)} adopted={adopted_sheet_count} "
        f"excluded={excluded_sheet_count} entries={len(df_entries)} "
        f"payouts={len(df_payout)}"
    )
    return df_entries, df_payout


def load_race_levels_simple(xlsx_path: str | Path) -> pd.DataFrame:
    xlsx_path = str(xlsx_path)
    if not os.path.exists(xlsx_path):
        return pd.DataFrame(columns=["rid_str", "race_level"])

    try:
        rl = pd.read_excel(xlsx_path, sheet_name="race_levels", engine="openpyxl")
    except Exception:
        return pd.DataFrame(columns=["rid_str", "race_level"])

    if rl is None or rl.empty:
        return pd.DataFrame(columns=["rid_str", "race_level"])

    rl = rl.copy()

    if "rid_str" not in rl.columns and "race_id" in rl.columns:
        rl["rid_str"] = rl["race_id"].astype(str)
    elif "rid_str" in rl.columns:
        rl["rid_str"] = rl["rid_str"].astype(str)
    else:
        return pd.DataFrame(columns=["rid_str", "race_level"])

    if "pre_top5_mean" in rl.columns:
        rl["race_level"] = pd.to_numeric(rl["pre_top5_mean"], errors="coerce")
    elif "pre_mean" in rl.columns:
        rl["race_level"] = pd.to_numeric(rl["pre_mean"], errors="coerce")
    else:
        rl["race_level"] = np.nan

    return rl[["rid_str", "race_level"]].dropna(subset=["rid_str"]).copy()
