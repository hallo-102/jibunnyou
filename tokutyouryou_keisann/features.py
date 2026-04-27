# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Tuple

import numpy as np
import pandas as pd

from keibayosou_features import build_calc_favorite_risk, build_features_from_excel
from keibayosou_loaders import load_base_time, load_odds_csv, load_race_levels
from keibayosou_utils import _to_int

from .common import _norm_name, _normalize_surface_name
from .config import (
    BASE_TIME_XLSX,
    CONFIG,
    ODDS_CSV,
    RACE_LEVEL_XLSX,
    calc_extra_penalty,
    calc_rest_dist_risk,
)

try:
    from keibayosou_config import DL_PROB_BLEND, DL_RANK_BLEND
except Exception:
    DL_PROB_BLEND = 0.35
    DL_RANK_BLEND = 0.0


@lru_cache(maxsize=1)
def _load_pipeline_master_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    levels_df = load_race_levels(str(RACE_LEVEL_XLSX))
    base_time_df = load_base_time(str(BASE_TIME_XLSX))
    return levels_df, base_time_df


@lru_cache(maxsize=1)
def _load_dl_model_cache():
    """
    本番予想と同じDLモデルを最適化用データ作成でも使う。
    学習は重いのでプロセス内で1回だけ実行する。
    """
    from keibayosou_best_import_roi_runner import (
        DL_FEATURE_COLS,
        TRAIN_XLSX,
        _build_training_dataframe,
        _train_model,
    )

    train_path = Path(str(CONFIG.get("RESULTS_FILE") or TRAIN_XLSX))
    if not train_path.exists():
        train_path = TRAIN_XLSX

    df_train = _build_training_dataframe(train_path)
    x = df_train[DL_FEATURE_COLS].to_numpy(dtype=np.float32)
    y = df_train["y"].to_numpy(dtype=np.float32)
    model, mean, std = _train_model(x, y)
    return model, mean, std


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


def _attach_dl_features(merged: pd.DataFrame, feat_df: pd.DataFrame) -> pd.DataFrame:
    """
    最適化用特徴量にも本番2回目相当のDL列を付与する。
    これにより optimizer 側の dl_score / dl_bonus 評価が中立値ではなくなる。
    """
    if merged is None or merged.empty or feat_df is None or feat_df.empty:
        return feat_df

    try:
        from keibayosou_best_import_roi_runner import _predict_dl_rank

        model, mean, std = _load_dl_model_cache()
        dl_df = _predict_dl_rank(model, mean, std, merged.copy())
    except Exception as e:
        print(f"[WARN] DL列生成に失敗したため中立値を使用します: {e}")
        out = feat_df.copy()
        out["dl_prob"] = 0.5
        out["dl_rank"] = np.nan
        out["dl_rank_score"] = 0.5
        out["dl_prob_score"] = 0.5
        out["dl_score"] = 0.5
        return out

    dl_join = merged[["rid_str", "馬番"]].copy()
    dl_join["頭数"] = merged["頭数"] if "頭数" in merged.columns else pd.NA
    dl_join["rid_str"] = dl_join["rid_str"].astype(str)
    dl_join["馬番"] = pd.to_numeric(dl_join["馬番"], errors="coerce").astype("Int64")
    dl_join = pd.merge(dl_join, dl_df, on=["rid_str", "馬番"], how="left")
    dl_join["dl_rank"] = pd.to_numeric(dl_join["dl_rank"], errors="coerce")
    dl_join["dl_prob"] = pd.to_numeric(dl_join["dl_prob"], errors="coerce")
    dl_join["頭数"] = pd.to_numeric(dl_join["頭数"], errors="coerce")

    out = feat_df.copy()
    out["rid_str"] = out["rid_str"].astype(str)
    out["馬番"] = pd.to_numeric(out["馬番"], errors="coerce").astype("Int64")
    out = pd.merge(out, dl_join, on=["rid_str", "馬番"], how="left")

    def _calc_dl_rank_score(row: pd.Series) -> float:
        rank_val = row.get("dl_rank")
        field_size = row.get("頭数")
        if pd.isna(rank_val) or pd.isna(field_size):
            return 0.5
        try:
            rank_f = float(rank_val)
            field_f = float(field_size)
        except Exception:
            return 0.5
        if field_f <= 1.0 or rank_f < 1.0 or rank_f > field_f:
            return 0.5
        return (field_f - rank_f) / (field_f - 1.0)

    out["dl_rank_score"] = out.apply(_calc_dl_rank_score, axis=1)
    out["dl_prob_score"] = pd.to_numeric(out.get("dl_prob"), errors="coerce")

    def _normalize_prob_within_race(s: pd.Series) -> pd.Series:
        x = pd.to_numeric(s, errors="coerce")
        if x.notna().sum() == 0:
            return pd.Series([0.5] * len(s), index=s.index, dtype=float)
        min_val = x.min(skipna=True)
        max_val = x.max(skipna=True)
        if pd.isna(min_val) or pd.isna(max_val) or max_val == min_val:
            return pd.Series([0.5] * len(s), index=s.index, dtype=float)
        return ((x - min_val) / (max_val - min_val)).fillna(0.5)

    out["dl_prob_score"] = out.groupby("rid_str")["dl_prob_score"].transform(_normalize_prob_within_race)
    out["dl_score"] = (
        pd.to_numeric(out["dl_prob_score"], errors="coerce").fillna(0.5) * float(DL_PROB_BLEND)
        + pd.to_numeric(out["dl_rank_score"], errors="coerce").fillna(0.5) * float(DL_RANK_BLEND)
    )
    return out.drop(columns=["頭数"], errors="ignore")


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
    feat_df = _attach_dl_features(merged_local, feat_df)

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
