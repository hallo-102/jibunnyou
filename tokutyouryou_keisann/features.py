# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
from functools import lru_cache
from typing import Any, Dict, Tuple

import pandas as pd

from keibayosou_features import build_calc_favorite_risk, build_features_from_excel
from keibayosou_loaders import load_base_time, load_odds_csv, load_race_levels
from keibayosou_utils import _to_int

from .common import _norm_name, _normalize_surface_name
from .config import BASE_TIME_XLSX, ODDS_CSV, RACE_LEVEL_XLSX, calc_extra_penalty, calc_rest_dist_risk


@lru_cache(maxsize=1)
def _load_pipeline_master_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    levels_df = load_race_levels(str(RACE_LEVEL_XLSX))
    base_time_df = load_base_time(str(BASE_TIME_XLSX))
    return levels_df, base_time_df


def _extract_raceday_from_path(xlsx_path: str) -> str:
    m = re.search(r"(\d{8})", os.path.basename(str(xlsx_path)))
    return m.group(1) if m else ""


def _build_pipeline_context_maps(
    merged: pd.DataFrame,
) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, int | None], Dict[str, int | None], Dict[str, Any], Dict[Tuple[str, int], int]]:
    place_map: Dict[str, str] = {}
    surface_map: Dict[str, str] = {}
    dist_map: Dict[str, int | None] = {}
    field_size_map: Dict[str, int | None] = {}
    baba_map: Dict[str, Any] = {}

    if "場所" in merged.columns:
        place_map = merged.groupby("rid_str")["場所"].first().to_dict()
    if "芝・ダ" in merged.columns:
        surface_map = merged.groupby("rid_str")["芝・ダ"].first().map(_normalize_surface_name).to_dict()
    elif "芝ダ" in merged.columns:
        surface_map = merged.groupby("rid_str")["芝ダ"].first().map(_normalize_surface_name).to_dict()

    for col in ["距離", "距離(m)", "距離 ", "Distance"]:
        if col in merged.columns:
            dist_map = (
                merged.groupby("rid_str")[col]
                .first()
                .apply(
                    lambda v: _to_int(re.search(r"(\d+)", str(v)).group(1))
                    if pd.notna(v) and re.search(r"(\d+)", str(v))
                    else None
                )
                .to_dict()
            )
            break

    for col in ["頭数", "頭 数", "field_size"]:
        if col in merged.columns:
            field_size_map = merged.groupby("rid_str")[col].first().apply(lambda v: _to_int(v)).to_dict()
            break

    for col in ["馬場状態", "馬場", "馬 場"]:
        if col in merged.columns:
            baba_map = merged.groupby("rid_str")[col].first().to_dict()
            break

    pop_map: Dict[Tuple[str, int], int] = {}
    pop_col = None
    for col in merged.columns:
        if "人気" in str(col):
            pop_col = col
            break
    if pop_col:
        pop_series = pd.to_numeric(merged[pop_col], errors="coerce")
        pop_map = {
            (str(rid), _to_int(uma)): _to_int(pop)
            for rid, uma, pop in zip(merged.get("rid_str"), merged.get("馬番"), pop_series)
            if _to_int(uma) is not None and _to_int(pop) is not None
        }

    return place_map, surface_map, dist_map, field_size_map, baba_map, pop_map


def build_features_from_one_file(xlsx_path: str) -> pd.DataFrame:
    levels_df, base_time_df = _load_pipeline_master_data()
    raceday = _extract_raceday_from_path(xlsx_path)

    try:
        odds_df = load_odds_csv(str(ODDS_CSV), raceday=raceday)
    except Exception:
        odds_df = pd.DataFrame()

    merged, feat_df = build_features_from_excel(
        str(xlsx_path),
        levels_df,
        base_time_df,
        odds_df,
        raceday=raceday,
    )
    if feat_df is None or feat_df.empty:
        return pd.DataFrame()

    feat_df = feat_df.copy()
    feat_df["rid_str"] = feat_df["rid_str"].astype(str)
    feat_df["name_norm"] = feat_df.get("馬名", pd.Series(index=feat_df.index, dtype=object)).astype(str).map(_norm_name)

    merged_local = merged.copy()
    merged_local["rid_str"] = merged_local.get("rid_str", pd.Series(index=merged_local.index, dtype=object)).astype(str)
    if "馬番" in merged_local.columns:
        merged_local["馬番"] = pd.to_numeric(merged_local["馬番"], errors="coerce").astype("Int64")
    if "馬番" in feat_df.columns:
        feat_df["馬番"] = pd.to_numeric(feat_df["馬番"], errors="coerce").astype("Int64")

    meta_cols = merged_local[["rid_str", "馬番"]].copy()
    meta_cols["place_name"] = merged_local["場所"] if "場所" in merged_local.columns else ""
    if "芝・ダ" in merged_local.columns:
        meta_cols["surface_name"] = merged_local["芝・ダ"].map(_normalize_surface_name)
    elif "芝ダ" in merged_local.columns:
        meta_cols["surface_name"] = merged_local["芝ダ"].map(_normalize_surface_name)
    else:
        meta_cols["surface_name"] = ""
    feat_df = pd.merge(feat_df, meta_cols.drop_duplicates(subset=["rid_str", "馬番"]), on=["rid_str", "馬番"], how="left")
    feat_df["place_name"] = feat_df.get("place_name", "").fillna("").astype(str)
    feat_df["surface_name"] = feat_df.get("surface_name", "").fillna("").map(_normalize_surface_name)

    place_map, surface_map, dist_map, field_size_map, baba_map, pop_map = _build_pipeline_context_maps(merged_local)
    calc_fav_risk = build_calc_favorite_risk(place_map, surface_map, dist_map, field_size_map, pop_map, baba_map)

    feat_df["favorite_risk"] = feat_df.apply(calc_fav_risk, axis=1)
    if calc_rest_dist_risk is None:
        feat_df["rest_dist_risk"] = 0.0
    else:
        feat_df["rest_dist_risk"] = feat_df.apply(calc_rest_dist_risk, axis=1)

    if calc_extra_penalty is None:
        feat_df["extra_penalty"] = 0.0
    else:
        feat_df["extra_penalty"] = feat_df.apply(
            lambda r: float(calc_extra_penalty(r, rest_dist_risk=r.get("rest_dist_risk"))),
            axis=1,
        )

    return feat_df
