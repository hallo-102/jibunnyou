# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Tuple

import numpy as np
import pandas as pd

from keibayosou_features import (
    _normalize_rid_series,
    _normalize_umaban_series,
    build_calc_favorite_risk,
    build_features_from_excel,
)
from keibayosou_loaders import load_base_time, load_odds_csv, load_race_levels
from keibayosou_utils import _to_int

from .common import _norm_name, _normalize_surface_name
from .config import (
    BASE_TIME_XLSX,
    CONFIG,
    FEAT_COLS,
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


# ============================================================
# マスタ読み込み
# ============================================================
@lru_cache(maxsize=1)
def _load_pipeline_master_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    本番予想コード側と同じ race_levels / base_time を読む。
    """
    levels_df = load_race_levels(str(RACE_LEVEL_XLSX))
    base_time_df = load_base_time(str(BASE_TIME_XLSX))
    return levels_df, base_time_df


@lru_cache(maxsize=1)
def _load_dl_model_cache():
    """
    本番予想と同じDLモデルを最適化用データ作成でも使う。
    学習は重いので、プロセス内で1回だけ実行する。
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


# ============================================================
# 小物関数
# ============================================================
def _extract_raceday_from_path(xlsx_path: str) -> str:
    """
    ファイル名から YYYYMMDD を取り出す。
    例：
      馬の競走成績_20260313.xlsx -> 20260313
    """
    m = re.search(r"(\d{8})", os.path.basename(str(xlsx_path)))
    return m.group(1) if m else ""


def _safe_series(df: pd.DataFrame, col: str, default: Any = pd.NA) -> pd.Series:
    """
    df[col] が無い場合でも落ちないように Series を返す。
    """
    if df is None or df.empty:
        return pd.Series(dtype=object)
    if col in df.columns:
        return df[col]
    return pd.Series([default] * len(df), index=df.index)


def _parse_distance_to_int(value: Any) -> int | None:
    """
    '芝1600' / 'ダ1800' / 1600 のような値から距離だけ取り出す。
    """
    if pd.isna(value):
        return None
    m = re.search(r"(\d{3,4})", str(value))
    if not m:
        return None
    return _to_int(m.group(1))


def _coalesce_after_merge(df: pd.DataFrame, base_cols: list[str]) -> pd.DataFrame:
    """
    merge 後に place_name_x / place_name_y のような列ができた場合、
    1本にまとめる。
    """
    out = df.copy()

    for base in base_cols:
        candidates = [c for c in [base, f"{base}_x", f"{base}_y"] if c in out.columns]
        if not candidates:
            continue

        merged = None
        for c in candidates:
            s = out[c]
            if merged is None:
                merged = s.copy()
            else:
                merged = merged.combine_first(s)

        out[base] = merged

        drop_cols = [c for c in candidates if c != base]
        if drop_cols:
            out = out.drop(columns=drop_cols)

    return out


def _ensure_optimizer_required_columns(feat_df: pd.DataFrame) -> pd.DataFrame:
    """
    optimizer / scoring 側で必要な列を必ずそろえる。
    FEAT_COLS にある列が欠けていると、最適化対象から漏れたりエラーになったりするため。
    """
    out = feat_df.copy()

    for col in FEAT_COLS:
        if col not in out.columns:
            out[col] = 0.0

    required_default_cols = {
        "rid_str": "",
        "馬番": pd.NA,
        "馬名": "",
        "name_norm": "",
        "place_name": "",
        "surface_name": "",
        "distance_m": np.nan,
        "field_size": np.nan,
        "popularity": np.nan,
        "race_class": "",
        "favorite_risk": 0.0,
        "rest_dist_risk": 0.0,
        "extra_penalty": 0.0,
        "dl_prob": 0.5,
        "dl_rank": np.nan,
        "dl_rank_score": 0.5,
        "dl_prob_score": 0.5,
        "dl_score": 0.5,
    }

    for col, default in required_default_cols.items():
        if col not in out.columns:
            out[col] = default

    out["rid_str"] = _normalize_rid_series(out["rid_str"])
    out["馬番"] = _normalize_umaban_series(out["馬番"])
    out["name_norm"] = out["name_norm"].fillna("").astype(str)
    out["place_name"] = out["place_name"].fillna("").astype(str).str.strip()
    out["surface_name"] = out["surface_name"].fillna("").map(_normalize_surface_name)

    for col in [
        "distance_m",
        "field_size",
        "popularity",
        "favorite_risk",
        "rest_dist_risk",
        "extra_penalty",
        "dl_prob",
        "dl_rank",
        "dl_rank_score",
        "dl_prob_score",
        "dl_score",
    ]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out["favorite_risk"] = out["favorite_risk"].fillna(0.0)
    out["rest_dist_risk"] = out["rest_dist_risk"].fillna(0.0)
    out["extra_penalty"] = out["extra_penalty"].fillna(0.0)
    out["dl_prob"] = out["dl_prob"].fillna(0.5)
    out["dl_rank_score"] = out["dl_rank_score"].fillna(0.5)
    out["dl_prob_score"] = out["dl_prob_score"].fillna(0.5)
    out["dl_score"] = out["dl_score"].fillna(0.5)

    return out


# ============================================================
# 本番 favorite_risk 用マップ作成
# ============================================================
def _build_pipeline_context_maps(
    merged: pd.DataFrame,
) -> Tuple[
    Dict[str, str],
    Dict[str, str],
    Dict[str, int | None],
    Dict[str, int | None],
    Dict[str, Any],
    Dict[Tuple[str, int], int],
]:
    """
    本番 build_calc_favorite_risk() に渡すマップを作る。
    """
    place_map: Dict[str, str] = {}
    surface_map: Dict[str, str] = {}
    dist_map: Dict[str, int | None] = {}
    field_size_map: Dict[str, int | None] = {}
    baba_map: Dict[str, Any] = {}

    work = merged.copy()
    if "rid_str" in work.columns:
        work["rid_str"] = _normalize_rid_series(work["rid_str"])
    elif "レースID" in work.columns:
        work["rid_str"] = _normalize_rid_series(work["レースID"])
    else:
        work["rid_str"] = ""

    if "馬番" in work.columns:
        work["馬番"] = _normalize_umaban_series(work["馬番"])

    if "場所" in work.columns:
        place_map = work.groupby("rid_str")["場所"].first().to_dict()

    if "芝・ダ" in work.columns:
        surface_map = work.groupby("rid_str")["芝・ダ"].first().map(_normalize_surface_name).to_dict()
    elif "芝ダ" in work.columns:
        surface_map = work.groupby("rid_str")["芝ダ"].first().map(_normalize_surface_name).to_dict()
    elif "コース" in work.columns:
        surface_map = work.groupby("rid_str")["コース"].first().map(_normalize_surface_name).to_dict()

    for col in ["距離", "距離(m)", "距離 ", "Distance", "コース"]:
        if col in work.columns:
            dist_map = (
                work.groupby("rid_str")[col]
                .first()
                .apply(_parse_distance_to_int)
                .to_dict()
            )
            break

    for col in ["頭数", "頭 数", "field_size"]:
        if col in work.columns:
            field_size_map = (
                work.groupby("rid_str")[col]
                .first()
                .apply(lambda v: _to_int(v))
                .to_dict()
            )
            break

    for col in ["馬場状態", "馬場", "馬 場"]:
        if col in work.columns:
            baba_map = work.groupby("rid_str")[col].first().to_dict()
            break

    pop_map: Dict[Tuple[str, int], int] = {}
    pop_col = None
    for col in work.columns:
        if "人気" in str(col):
            pop_col = col
            break

    if pop_col is not None and "馬番" in work.columns:
        pop_series = pd.to_numeric(work[pop_col], errors="coerce")
        for rid, uma, pop in zip(work["rid_str"], work["馬番"], pop_series):
            uma_i = _to_int(uma)
            pop_i = _to_int(pop)
            if uma_i is None or pop_i is None:
                continue
            pop_map[(str(rid), int(uma_i))] = int(pop_i)

    return place_map, surface_map, dist_map, field_size_map, baba_map, pop_map


# ============================================================
# DL特徴量付与
# ============================================================
def _attach_dl_features(merged: pd.DataFrame, feat_df: pd.DataFrame) -> pd.DataFrame:
    """
    最適化用特徴量にも、本番2回目相当のDL列を付与する。

    作る列：
      - dl_prob
      - dl_rank
      - dl_rank_score
      - dl_prob_score
      - dl_score
    """
    if merged is None or merged.empty or feat_df is None or feat_df.empty:
        return feat_df

    out = feat_df.copy()

    try:
        from keibayosou_best_import_roi_runner import _predict_dl_rank

        model, mean, std = _load_dl_model_cache()
        dl_df = _predict_dl_rank(model, mean, std, merged.copy())
    except Exception as e:
        print(f"[WARN] DL列生成に失敗したため中立値を使用します: {e}")
        out["dl_prob"] = 0.5
        out["dl_rank"] = np.nan
        out["dl_rank_score"] = 0.5
        out["dl_prob_score"] = 0.5
        out["dl_score"] = 0.5
        return out

    merged_key = merged.copy()
    if "rid_str" in merged_key.columns:
        merged_key["rid_str"] = _normalize_rid_series(merged_key["rid_str"])
    elif "レースID" in merged_key.columns:
        merged_key["rid_str"] = _normalize_rid_series(merged_key["レースID"])
    else:
        merged_key["rid_str"] = ""

    merged_key["馬番"] = _normalize_umaban_series(_safe_series(merged_key, "馬番"))
    merged_key["頭数"] = pd.to_numeric(_safe_series(merged_key, "頭数"), errors="coerce")

    dl_df = dl_df.copy()
    if "rid_str" in dl_df.columns:
        dl_df["rid_str"] = _normalize_rid_series(dl_df["rid_str"])
    elif "レースID" in dl_df.columns:
        dl_df["rid_str"] = _normalize_rid_series(dl_df["レースID"])
    else:
        dl_df["rid_str"] = ""

    dl_df["馬番"] = _normalize_umaban_series(_safe_series(dl_df, "馬番"))

    if "dl_rank" not in dl_df.columns:
        dl_df["dl_rank"] = np.nan
    if "dl_prob" not in dl_df.columns:
        dl_df["dl_prob"] = 0.5

    dl_use = dl_df[["rid_str", "馬番", "dl_rank", "dl_prob"]].copy()
    dl_use["dl_rank"] = pd.to_numeric(dl_use["dl_rank"], errors="coerce")
    dl_use["dl_prob"] = pd.to_numeric(dl_use["dl_prob"], errors="coerce")

    dl_join = merged_key[["rid_str", "馬番", "頭数"]].drop_duplicates(subset=["rid_str", "馬番"])
    dl_join = pd.merge(dl_join, dl_use, on=["rid_str", "馬番"], how="left")

    out["rid_str"] = _normalize_rid_series(out["rid_str"])
    out["馬番"] = _normalize_umaban_series(out["馬番"])

    # 既存DL列がある場合は一度落としてから付け直す
    out = out.drop(
        columns=["dl_prob", "dl_rank", "dl_rank_score", "dl_prob_score", "dl_score", "頭数"],
        errors="ignore",
    )
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

        return float((field_f - rank_f) / (field_f - 1.0))

    out["dl_rank_score"] = out.apply(_calc_dl_rank_score, axis=1)
    out["dl_prob"] = pd.to_numeric(out["dl_prob"], errors="coerce").fillna(0.5)

    def _normalize_prob_within_race(s: pd.Series) -> pd.Series:
        x = pd.to_numeric(s, errors="coerce")
        if x.notna().sum() == 0:
            return pd.Series([0.5] * len(s), index=s.index, dtype=float)

        min_val = x.min(skipna=True)
        max_val = x.max(skipna=True)

        if pd.isna(min_val) or pd.isna(max_val) or max_val == min_val:
            return pd.Series([0.5] * len(s), index=s.index, dtype=float)

        return ((x - min_val) / (max_val - min_val)).fillna(0.5)

    out["dl_prob_score"] = out.groupby("rid_str")["dl_prob"].transform(_normalize_prob_within_race)

    # 本番側の考え方に合わせる
    out["dl_score"] = (
        pd.to_numeric(out["dl_prob_score"], errors="coerce").fillna(0.5) * float(DL_PROB_BLEND)
        + pd.to_numeric(out["dl_rank_score"], errors="coerce").fillna(0.5) * float(DL_RANK_BLEND)
    )

    return out.drop(columns=["頭数"], errors="ignore")


# ============================================================
# メイン：1ファイルから特徴量作成
# ============================================================
def build_features_from_one_file(xlsx_path: str) -> pd.DataFrame:
    """
    最適化コード側の特徴量作成入口。

    重要：
    ここでは独自特徴量を作らず、本番予想コード側の
    build_features_from_excel() を使う。

    これにより、
      - avg_finish
      - avg_pop
      - dist_diff
      - days_off
      - recent3_*
      - master_rating_*
      - cond_*
      - last3f_context_value
      - time_idx_context_value
    など、本番側と同じ特徴量を使える。
    """
    levels_df, base_time_df = _load_pipeline_master_data()
    raceday = _extract_raceday_from_path(xlsx_path)

    try:
        odds_df = load_odds_csv(str(ODDS_CSV), raceday=raceday)
    except Exception as e:
        print(f"[WARN] オッズCSV読み込み失敗: {e}")
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

    if merged is None or merged.empty:
        return pd.DataFrame()

    feat_df = feat_df.copy()
    merged_local = merged.copy()

    # ----------------------------
    # rid_str / 馬番 正規化
    # ----------------------------
    if "rid_str" in feat_df.columns:
        feat_df["rid_str"] = _normalize_rid_series(feat_df["rid_str"])
    elif "レースID" in feat_df.columns:
        feat_df["rid_str"] = _normalize_rid_series(feat_df["レースID"])
    else:
        feat_df["rid_str"] = ""

    if "馬番" in feat_df.columns:
        feat_df["馬番"] = _normalize_umaban_series(feat_df["馬番"])
    else:
        feat_df["馬番"] = pd.Series([pd.NA] * len(feat_df), index=feat_df.index, dtype="Int64")

    if "rid_str" in merged_local.columns:
        merged_local["rid_str"] = _normalize_rid_series(merged_local["rid_str"])
    elif "レースID" in merged_local.columns:
        merged_local["rid_str"] = _normalize_rid_series(merged_local["レースID"])
    else:
        merged_local["rid_str"] = ""

    if "馬番" in merged_local.columns:
        merged_local["馬番"] = _normalize_umaban_series(merged_local["馬番"])
    else:
        merged_local["馬番"] = pd.Series([pd.NA] * len(merged_local), index=merged_local.index, dtype="Int64")

    # ----------------------------
    # 馬名正規化
    # ----------------------------
    if "馬名" not in feat_df.columns:
        feat_df["馬名"] = ""

    feat_df["name_norm"] = feat_df["馬名"].astype(str).map(_norm_name)

    # ----------------------------
    # meta列を merged から付与
    # ----------------------------
    meta_cols = merged_local[["rid_str", "馬番"]].copy()

    meta_cols["place_name"] = _safe_series(merged_local, "場所", "")
    if "芝・ダ" in merged_local.columns:
        meta_cols["surface_name"] = merged_local["芝・ダ"].map(_normalize_surface_name)
    elif "芝ダ" in merged_local.columns:
        meta_cols["surface_name"] = merged_local["芝ダ"].map(_normalize_surface_name)
    elif "コース" in merged_local.columns:
        meta_cols["surface_name"] = merged_local["コース"].map(_normalize_surface_name)
    else:
        meta_cols["surface_name"] = ""

    if "距離" in merged_local.columns:
        meta_cols["distance_m"] = merged_local["距離"].map(_parse_distance_to_int)
    elif "コース" in merged_local.columns:
        meta_cols["distance_m"] = merged_local["コース"].map(_parse_distance_to_int)
    else:
        meta_cols["distance_m"] = np.nan

    meta_cols["field_size"] = _safe_series(merged_local, "頭数", pd.NA)
    meta_cols["popularity"] = _safe_series(merged_local, "人気", pd.NA)

    if "クラス" in merged_local.columns:
        meta_cols["race_class"] = merged_local["クラス"]
    elif "レース名" in merged_local.columns:
        meta_cols["race_class"] = merged_local["レース名"]
    else:
        meta_cols["race_class"] = ""

    meta_cols = meta_cols.drop_duplicates(subset=["rid_str", "馬番"])

    feat_df = pd.merge(
        feat_df,
        meta_cols,
        on=["rid_str", "馬番"],
        how="left",
        suffixes=("", "_meta"),
    )

    # merge後の重複列整理
    feat_df = _coalesce_after_merge(
        feat_df,
        [
            "place_name",
            "surface_name",
            "distance_m",
            "field_size",
            "popularity",
            "race_class",
        ],
    )

    feat_df["place_name"] = feat_df.get("place_name", "").fillna("").astype(str).str.strip()
    feat_df["surface_name"] = feat_df.get("surface_name", "").fillna("").map(_normalize_surface_name)
    feat_df["distance_m"] = pd.to_numeric(feat_df.get("distance_m"), errors="coerce")
    feat_df["field_size"] = pd.to_numeric(feat_df.get("field_size"), errors="coerce")
    feat_df["popularity"] = pd.to_numeric(feat_df.get("popularity"), errors="coerce")
    feat_df["race_class"] = feat_df.get("race_class", "").fillna("").astype(str)

    # ----------------------------
    # DL列を本番2回目相当に寄せる
    # ----------------------------
    feat_df = _attach_dl_features(merged_local, feat_df)

    # ----------------------------
    # favorite_risk / rest_dist_risk / extra_penalty
    # ----------------------------
    place_map, surface_map, dist_map, field_size_map, baba_map, pop_map = _build_pipeline_context_maps(merged_local)
    calc_fav_risk = build_calc_favorite_risk(
        place_map,
        surface_map,
        dist_map,
        field_size_map,
        pop_map,
        baba_map,
    )

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

    # ----------------------------
    # optimizer/scoring 側に必要な列を最終保証
    # ----------------------------
    feat_df = _ensure_optimizer_required_columns(feat_df)

    return feat_df