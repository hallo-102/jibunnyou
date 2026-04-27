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

from .config import CONFIG, EMPIRICAL_WEIGHT_SIGN_GUARD, FEAT_COLS, PLACE_MAP, RACELEVEL_COLS


WeightKey: TypeAlias = str | tuple[str, str]
WeightsMap: TypeAlias = Dict[WeightKey, Dict[str, float]]


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

    def _key(p: str) -> Tuple[int, str]:
        m = re.search(r"(\d{8})", os.path.basename(p))
        return (int(m.group(1)) if m else -1, p)

    return sorted(files, key=_key)


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
    place_code = rid_str[4:6] if len(rid_str) >= 6 else ""
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

    book = pd.read_excel(xlsx_path, sheet_name=None, engine="openpyxl")

    entries_rows, payout_rows = [], []
    for _, df in book.items():
        if df is None or df.empty:
            continue

        df2 = df.copy()
        df2.columns = [
            c if isinstance(c, str) else " / ".join([str(x) for x in c])
            for c in df2.columns
        ]
        cols = list(df2.columns)

        if "レースID" not in cols and "race_id" in cols:
            df2 = df2.rename(columns={"race_id": "レースID"})
        if "馬名" not in cols:
            c = find_col(cols, ["馬名", "馬 名"])
            if c:
                df2 = df2.rename(columns={c: "馬名"})

        if "レースID" in df2.columns and "馬名" in df2.columns:
            c_rank = find_col(df2.columns, ["着順", "着 順"])
            c_umaban = find_col(df2.columns, ["馬番", "馬 番"])
            if c_rank is not None:
                sub = df2[["レースID", "馬名"] + ([c_umaban] if c_umaban else []) + [c_rank]].copy()
                sub["rid_str"] = sub["レースID"].astype(str)
                sub["name_norm"] = sub["馬名"].map(_norm_name)
                sub["着順_num"] = pd.to_numeric(sub[c_rank], errors="coerce")
                if c_umaban:
                    sub["馬番_int"] = pd.to_numeric(sub[c_umaban], errors="coerce").astype("Int64")
                else:
                    sub["馬番_int"] = pd.Series([pd.NA] * len(sub), dtype="Int64")
                entries_rows.append(sub[["rid_str", "name_norm", "着順_num", "馬番_int"]])

        c_ptype = find_col(df2.columns, ["払戻種別", "券種", "式別", "種別"])
        c_combo = find_col(df2.columns, ["組番", "組み合わせ", "馬番"])
        c_pay = find_col(df2.columns, ["払戻金", "払戻", "配当", "払戻金(円)"])
        if c_ptype and c_combo and c_pay and ("レースID" in df2.columns):
            subp = df2[["レースID", c_ptype, c_combo, c_pay]].copy()
            subp["rid_str"] = subp["レースID"].astype(str)
            subp["払戻種別"] = subp[c_ptype].astype(str)
            subp["組番_norm"] = subp[c_combo].map(_normalize_combo)
            subp["払戻金_int"] = subp[c_pay].map(_yen_to_int)
            payout_rows.append(subp[["rid_str", "払戻種別", "組番_norm", "払戻金_int"]])

    df_entries = pd.concat(entries_rows, ignore_index=True) if entries_rows else pd.DataFrame(
        columns=["rid_str", "name_norm", "着順_num", "馬番_int"]
    )
    df_payout = pd.concat(payout_rows, ignore_index=True) if payout_rows else pd.DataFrame(
        columns=["rid_str", "払戻種別", "組番_norm", "払戻金_int"]
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
