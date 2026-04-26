# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd

from .common import (
    WeightsMap,
    _normalize_surface_name,
)
from .config import (
    CONFIG,
    FEAT_COLS,
    PEN_CLOSE_LOSS_APPLY_WINRATE_MAX,
    PEN_CLOSE_LOSS_FINISH_TH,
    PEN_CLOSE_LOSS_K,
    PEN_CLOSE_LOSS_MARGIN_TH,
    PEN_FAV_POP_APPLY_WINRATE_MAX,
    PEN_FAV_POP_K,
    PEN_FAV_POP_TH,
    PEN_GOOD_LOSER_APPLY_WINRATE_MAX,
    PEN_GOOD_LOSER_H,
    PEN_GOOD_LOSER_K,
    PEN_GOOD_LOSER_L,
    PEN_TA_N_CAP,
    PEN_TA_N_K,
    PIPE_ALPHA,
    PIPE_EXTRA_ALPHA,
    calc_extra_penalty,
    calc_rest_dist_risk,
)

# 本番 pipeline と optimizer 側の score 式を揃えるために使う
try:
    from keibayosou_config import DL_SCORE_BONUS
except Exception:
    DL_SCORE_BONUS = 10.0


def summarize_stability(
    total_bets: int,
    total_success: float,
    total_invest: int,
    total_return: int,
    details: Dict[str, Dict[str, Any]],
) -> Dict[str, float]:
    """
    top5命中率専用の要約指標
    """
    roi = (total_return / total_invest) if total_invest else 0.0
    sr = (total_success / total_bets) if total_bets else 0.0

    hit_points = [float(v.get("top5_hit_points", 0.0)) for v in details.values() if int(v.get("evaluated", 0)) == 1]
    complete_flags = [float(v.get("top3_complete", 0.0)) for v in details.values() if int(v.get("evaluated", 0)) == 1]
    win_in_top5_flags = [float(v.get("win_in_top5", 0.0)) for v in details.values() if int(v.get("evaluated", 0)) == 1]
    place_capture_rates = [float(v.get("place_capture_rate", 0.0)) for v in details.values() if int(v.get("evaluated", 0)) == 1]
    rank1_win_flags = [float(v.get("rank1_win", 0.0)) for v in details.values() if int(v.get("evaluated", 0)) == 1]
    rank1_place_flags = [float(v.get("rank1_place", 0.0)) for v in details.values() if int(v.get("evaluated", 0)) == 1]
    rank1_dl_scores = [float(v.get("rank1_dl_score", 0.5)) for v in details.values() if int(v.get("evaluated", 0)) == 1]

    max_points = (
        float(CONFIG["TOP5_HIT_W_FIRST"])
        + float(CONFIG["TOP5_HIT_W_SECOND"])
        + float(CONFIG["TOP5_HIT_W_THIRD"])
    )

    top5_point_rate = (float(np.mean(hit_points)) / max_points) if hit_points and max_points > 0 else 0.0
    top3_complete_rate = float(np.mean(complete_flags)) if complete_flags else 0.0
    win_in_top5_rate = float(np.mean(win_in_top5_flags)) if win_in_top5_flags else 0.0
    place_in_top5_rate = float(np.mean(place_capture_rates)) if place_capture_rates else 0.0
    rank1_win_rate = float(np.mean(rank1_win_flags)) if rank1_win_flags else 0.0
    rank1_place_rate = float(np.mean(rank1_place_flags)) if rank1_place_flags else 0.0
    rank1_avg_dl_score = float(np.mean(rank1_dl_scores)) if rank1_dl_scores else 0.5

    return {
        "roi": float(roi),
        "success_rate": float(sr),
        "n_bets": float(total_bets),
        "n_success": float(total_success),
        "top5_point_rate": float(top5_point_rate),
        "top3_complete_rate": float(top3_complete_rate),
        "win_in_top5_rate": float(win_in_top5_rate),
        "place_in_top5_rate": float(place_in_top5_rate),
        "rank1_win_rate": float(rank1_win_rate),
        "rank1_place_rate": float(rank1_place_rate),
        "rank1_avg_dl_score": float(rank1_avg_dl_score),
    }


def calc_objective_score(stab: Dict[str, float]) -> float:
    """
    top5命中率専用の目的関数
    """
    point_rate = float(stab.get("top5_point_rate", 0.0))
    complete_rate = float(stab.get("top3_complete_rate", 0.0))
    win_rate = float(stab.get("win_in_top5_rate", 0.0))
    place_rate = float(stab.get("place_in_top5_rate", 0.0))
    rank1_place_rate = float(stab.get("rank1_place_rate", 0.0))
    rank1_win_rate = float(stab.get("rank1_win_rate", 0.0))
    n_bets = float(stab.get("n_bets", 0.0))

    score = 0.0
    score += float(CONFIG["OBJ_W_TOP5_POINT_RATE"]) * point_rate
    score += float(CONFIG["OBJ_W_TOP3_COMPLETE_RATE"]) * complete_rate
    score += float(CONFIG["OBJ_W_WIN_IN_TOP5_RATE"]) * win_rate
    score += float(CONFIG["OBJ_W_PLACE_IN_TOP5_RATE"]) * place_rate

    # 予想1位が3着内に入ることを、目的関数の明示的な評価項目として扱う
    score += float(CONFIG.get("OBJ_W_RANK1_PLACE_RATE", 0.85)) * rank1_place_rate
    score += float(CONFIG.get("OBJ_W_RANK1_WIN_RATE", 0.10)) * rank1_win_rate

    min_bets = float(CONFIG["OBJ_MIN_BETS"])
    max_bets = float(CONFIG["OBJ_MAX_BETS"])

    if n_bets < min_bets:
        shortage_ratio = (min_bets - n_bets) / max(min_bets, 1.0)
        score -= float(CONFIG["OBJ_LOW_BETS_PENALTY"]) * shortage_ratio
    else:
        score += float(CONFIG["OBJ_W_COVERAGE"]) * min(n_bets / max(max_bets, 1.0), 1.0)

    if n_bets > max_bets:
        excess_ratio = (n_bets - max_bets) / max(max_bets, 1.0)
        score -= float(CONFIG["OBJ_HIGH_BETS_PENALTY"]) * excess_ratio

    return float(score)


def better_by_objective(
    a_obj: float,
    a_stab: Dict[str, float],
    b_obj: float,
    b_stab: Dict[str, float],
) -> bool:
    eps = 1e-9
    if a_obj > b_obj + eps:
        return True
    if abs(a_obj - b_obj) <= eps:
        if a_stab["top5_point_rate"] > b_stab["top5_point_rate"] + eps:
            return True
        if abs(a_stab["top5_point_rate"] - b_stab["top5_point_rate"]) <= eps:
            if a_stab["top3_complete_rate"] > b_stab["top3_complete_rate"] + eps:
                return True
            if abs(a_stab["top3_complete_rate"] - b_stab["top3_complete_rate"]) <= eps:
                if a_stab["win_in_top5_rate"] > b_stab["win_in_top5_rate"] + eps:
                    return True
                if abs(a_stab["win_in_top5_rate"] - b_stab["win_in_top5_rate"]) <= eps:
                    if a_stab["place_in_top5_rate"] > b_stab["place_in_top5_rate"] + eps:
                        return True
                    if abs(a_stab["place_in_top5_rate"] - b_stab["place_in_top5_rate"]) <= eps:
                        if a_stab.get("rank1_place_rate", 0.0) > b_stab.get("rank1_place_rate", 0.0) + eps:
                            return True
                        if abs(a_stab.get("rank1_place_rate", 0.0) - b_stab.get("rank1_place_rate", 0.0)) <= eps:
                            if a_stab.get("rank1_win_rate", 0.0) > b_stab.get("rank1_win_rate", 0.0) + eps:
                                return True
    return False


@dataclass
class EvalContext:
    feat_df: pd.DataFrame
    feature_matrix: np.ndarray
    rid_array: np.ndarray
    name_norm_array: np.ndarray
    place_array: np.ndarray
    surface_array: np.ndarray
    favorite_risk: np.ndarray
    rest_dist_risk: np.ndarray
    extra_penalty_fixed: Optional[np.ndarray]
    extra_penalty_seed: np.ndarray
    extra_penalty_missing_mask: np.ndarray
    avg_pop: np.ndarray
    avg_finish: np.ndarray
    ta_n: np.ndarray
    avg_margin: np.ndarray
    win_rate: np.ndarray
    dl_score: np.ndarray
    dl_prob: np.ndarray
    dl_rank: np.ndarray
    dl_bonus: np.ndarray
    race_ids: list[str]
    race_indices: Dict[str, np.ndarray]
    place_indices: Dict[str, np.ndarray]
    place_surface_indices: Dict[tuple[str, str], np.ndarray]
    actual_top3_names: Dict[str, list[str]]
    actual_top3_nums: Dict[str, list[int]]
    pay_map: Dict[str, Dict[str, int]]
    place_by_rid: Dict[str, str]
    surface_by_rid: Dict[str, str]


def _first_present_numeric_frame(df: pd.DataFrame, keys: list[str]) -> np.ndarray:
    result = pd.Series(np.nan, index=df.index, dtype=float)
    for key in keys:
        if key not in df.columns:
            continue
        current = pd.to_numeric(df[key], errors="coerce")
        result = result.where(result.notna(), current)
    return result.to_numpy(dtype=float, copy=False)


def _normalize_score_array(values: np.ndarray) -> np.ndarray:
    if values.size == 0:
        return values
    mean_val = float(values.mean())
    if values.size <= 1:
        return np.full(values.shape, 50.0, dtype=float)
    std_val = float(values.std(ddof=1))
    if (not np.isfinite(std_val)) or std_val == 0.0:
        return np.full(values.shape, 50.0, dtype=float)
    z = (values - mean_val) / std_val
    return np.clip(50.0 + 10.0 * z, 0.0, 100.0)


def _dense_rank_desc(values: np.ndarray) -> np.ndarray:
    if values.size == 0:
        return np.array([], dtype=int)
    unique_desc = np.sort(np.unique(values))[::-1]
    rank_map = {float(v): i + 1 for i, v in enumerate(unique_desc.tolist())}
    return np.array([rank_map[float(v)] for v in values], dtype=int)


def _weight_vector(weights: Dict[str, float]) -> np.ndarray:
    return np.array([float(weights.get(col, 0.0)) for col in FEAT_COLS], dtype=float)


def _calc_rest_dist_risk_fallback_vector(feat_df: pd.DataFrame) -> np.ndarray:
    win_rate = _first_present_numeric_frame(feat_df, ["win_rate", "f_win_rate"])
    days_off = _first_present_numeric_frame(feat_df, ["days_off", "f_rest_days"])
    dist_diff = np.abs(_first_present_numeric_frame(feat_df, ["dist_diff", "f_diff_distance"]))
    cond1 = (days_off >= 140.0) & (dist_diff >= 200.0)
    cond2 = (days_off >= 180.0) & (dist_diff >= 100.0)
    return (((win_rate <= 0.20) & (cond1 | cond2))).astype(float)


def _calc_extra_penalty_formula_vector(
    avg_pop: np.ndarray,
    avg_finish: np.ndarray,
    ta_n: np.ndarray,
    avg_margin: np.ndarray,
    win_rate: np.ndarray,
    rest_dist_risk: np.ndarray,
) -> np.ndarray:
    penalty = np.zeros_like(rest_dist_risk, dtype=float)

    cond_win_fav = np.isnan(win_rate) | (win_rate <= float(PEN_FAV_POP_APPLY_WINRATE_MAX))
    mask_fav = np.isfinite(avg_pop) & cond_win_fav & (avg_pop <= float(PEN_FAV_POP_TH))
    penalty[mask_fav] += (float(PEN_FAV_POP_TH) - avg_pop[mask_fav]) * float(PEN_FAV_POP_K)

    cond_win_good = np.isnan(win_rate) | (win_rate <= float(PEN_GOOD_LOSER_APPLY_WINRATE_MAX))
    mask_good = (
        np.isfinite(avg_finish)
        & cond_win_good
        & (avg_finish >= float(PEN_GOOD_LOSER_L))
        & (avg_finish <= float(PEN_GOOD_LOSER_H))
    )
    width = (float(PEN_GOOD_LOSER_H) - float(PEN_GOOD_LOSER_L)) / 2.0
    if width > 0.0:
        center = (float(PEN_GOOD_LOSER_L) + float(PEN_GOOD_LOSER_H)) / 2.0
        closeness = 1.0 - np.minimum(1.0, np.abs(avg_finish - center) / width)
        closeness = np.clip(closeness, 0.0, None)
        penalty[mask_good] += closeness[mask_good] * float(PEN_GOOD_LOSER_K)

    mask_ta = np.isfinite(ta_n) & (ta_n > float(PEN_TA_N_CAP))
    penalty[mask_ta] += (ta_n[mask_ta] - float(PEN_TA_N_CAP)) * float(PEN_TA_N_K)

    mask_close = (
        np.isfinite(avg_finish)
        & np.isfinite(avg_margin)
        & (np.isnan(win_rate) | (win_rate <= float(PEN_CLOSE_LOSS_APPLY_WINRATE_MAX)))
        & (avg_finish >= float(PEN_CLOSE_LOSS_FINISH_TH))
        & (avg_margin <= float(PEN_CLOSE_LOSS_MARGIN_TH))
    )
    penalty[mask_close] += float(PEN_CLOSE_LOSS_K)

    penalty += rest_dist_risk
    return np.clip(penalty, 0.0, None)


def _safe_numeric_series_from_df(df: pd.DataFrame, col_name: str, default_value: float = np.nan) -> pd.Series:
    """
    列が無い / None / scalar / 重複列(DataFrame) でも安全に 1本の Series にする
    """
    if col_name not in df.columns:
        return pd.Series([default_value] * len(df), index=df.index, dtype=float)

    obj = df[col_name]

    if isinstance(obj, pd.Series):
        return pd.to_numeric(obj, errors="coerce")

    if isinstance(obj, pd.DataFrame):
        if obj.shape[1] == 0:
            return pd.Series([default_value] * len(df), index=df.index, dtype=float)
        s = pd.to_numeric(obj.iloc[:, 0], errors="coerce")
        for i in range(1, obj.shape[1]):
            s2 = pd.to_numeric(obj.iloc[:, i], errors="coerce")
            s = s.where(s.notna(), s2)
        return s

    # scalar になってしまった場合
    try:
        v = pd.to_numeric(pd.Series([obj] * len(df), index=df.index), errors="coerce")
        return v
    except Exception:
        return pd.Series([default_value] * len(df), index=df.index, dtype=float)


def build_eval_context(
    df_feat: pd.DataFrame,
    df_res_entries: pd.DataFrame,
    df_res_payout: pd.DataFrame,
) -> EvalContext:
    feat_df = df_feat.copy()

    for c in FEAT_COLS:
        if c not in feat_df.columns:
            feat_df[c] = 0.0
    if "place_name" not in feat_df.columns:
        feat_df["place_name"] = ""
    if "surface_name" not in feat_df.columns:
        feat_df["surface_name"] = ""
    if "name_norm" not in feat_df.columns:
        feat_df["name_norm"] = ""

    feat_df["rid_str"] = feat_df["rid_str"].astype(str)
    feat_df["place_name"] = feat_df["place_name"].fillna("").astype(str).str.strip()
    feat_df["surface_name"] = feat_df["surface_name"].fillna("").map(_normalize_surface_name)
    feat_df["name_norm"] = feat_df["name_norm"].fillna("").astype(str)

    feature_matrix = feat_df[FEAT_COLS].apply(pd.to_numeric, errors="coerce").fillna(0.0).to_numpy(dtype=float)
    rid_array = feat_df["rid_str"].to_numpy(dtype=object, copy=False)
    name_norm_array = feat_df["name_norm"].to_numpy(dtype=object, copy=False)
    place_array = feat_df["place_name"].to_numpy(dtype=object, copy=False)
    surface_array = feat_df["surface_name"].to_numpy(dtype=object, copy=False)

    favorite_series = (
        pd.to_numeric(feat_df["favorite_risk"], errors="coerce")
        if "favorite_risk" in feat_df.columns
        else pd.Series(0.0, index=feat_df.index, dtype=float)
    )
    favorite_risk = favorite_series.fillna(0.0).to_numpy(dtype=float)

    if "rest_dist_risk" in feat_df.columns:
        rest_dist_risk = pd.to_numeric(feat_df["rest_dist_risk"], errors="coerce").to_numpy(dtype=float)
    else:
        rest_dist_risk = np.full(len(feat_df), np.nan, dtype=float)
    rest_missing = np.isnan(rest_dist_risk)
    if rest_missing.any():
        if calc_rest_dist_risk is None:
            rest_dist_risk[rest_missing] = _calc_rest_dist_risk_fallback_vector(feat_df)[rest_missing]
        else:
            computed_rest = feat_df.apply(calc_rest_dist_risk, axis=1).to_numpy(dtype=float)
            rest_dist_risk[rest_missing] = computed_rest[rest_missing]
    rest_dist_risk = np.nan_to_num(rest_dist_risk, nan=0.0)

    avg_pop = _first_present_numeric_frame(feat_df, ["avg_pop", "f_pop_mean"])
    avg_finish = _first_present_numeric_frame(feat_df, ["avg_finish", "f_finish_mean"])
    ta_n = _first_present_numeric_frame(feat_df, ["ta_n", "f_race_count"])
    avg_margin = _first_present_numeric_frame(feat_df, ["avg_margin"])
    win_rate = _first_present_numeric_frame(feat_df, ["win_rate", "f_win_rate"])

    extra_penalty_fixed: Optional[np.ndarray] = None
    extra_penalty_seed = np.zeros(len(feat_df), dtype=float)
    extra_penalty_missing_mask = np.ones(len(feat_df), dtype=bool)
    if "extra_penalty" in feat_df.columns:
        extra_penalty_seed = pd.to_numeric(feat_df["extra_penalty"], errors="coerce").to_numpy(dtype=float)
        extra_penalty_missing_mask = np.isnan(extra_penalty_seed)
    if calc_extra_penalty is not None:
        extra_penalty_fixed = np.nan_to_num(extra_penalty_seed.copy(), nan=0.0)
        if extra_penalty_missing_mask.any():
            computed_extra = _calc_extra_penalty_formula_vector(
                avg_pop=avg_pop,
                avg_finish=avg_finish,
                ta_n=ta_n,
                avg_margin=avg_margin,
                win_rate=win_rate,
                rest_dist_risk=rest_dist_risk,
            )
            extra_penalty_fixed[extra_penalty_missing_mask] = computed_extra[extra_penalty_missing_mask]
    else:
        extra_penalty_seed = np.nan_to_num(extra_penalty_seed, nan=0.0)

    # ------------------------------------------------------------
    # DL列は無ければ中立値を使う
    # ここが今回のエラー修正ポイント
    # ------------------------------------------------------------
    dl_score_series = _safe_numeric_series_from_df(feat_df, "dl_score", default_value=0.5).fillna(0.5)
    dl_prob_series = _safe_numeric_series_from_df(feat_df, "dl_prob", default_value=0.5).fillna(0.5)
    dl_rank_series = _safe_numeric_series_from_df(feat_df, "dl_rank", default_value=np.nan)

    dl_bonus_series = (dl_score_series.astype(float) - 0.5) * float(DL_SCORE_BONUS)

    race_indices = {
        str(rid): np.asarray(idx, dtype=int)
        for rid, idx in feat_df.groupby("rid_str", sort=True).indices.items()
    }
    race_ids = sorted(race_indices.keys())

    place_indices = {
        str(place): np.asarray(idx, dtype=int)
        for place, idx in feat_df.groupby("place_name", sort=False).indices.items()
        if str(place)
    }
    place_surface_indices = {
        (str(place), str(surface)): np.asarray(idx, dtype=int)
        for (place, surface), idx in feat_df.groupby(["place_name", "surface_name"], sort=False).indices.items()
        if str(place) and str(surface)
    }

    place_by_rid = (
        feat_df[["rid_str", "place_name"]]
        .drop_duplicates(subset=["rid_str"])
        .set_index("rid_str")["place_name"]
        .to_dict()
    )
    surface_by_rid = (
        feat_df[["rid_str", "surface_name"]]
        .drop_duplicates(subset=["rid_str"])
        .set_index("rid_str")["surface_name"]
        .to_dict()
    )

    top3 = (
        df_res_entries[df_res_entries["着順_num"].isin([1, 2, 3])]
        .sort_values(["rid_str", "着順_num"])
        .groupby("rid_str")
    )
    actual_top3_names = top3["name_norm"].apply(list).to_dict()
    actual_top3_nums = top3["馬番_int"].apply(lambda s: [int(x) for x in s.dropna().tolist()]).to_dict()

    pay_map: Dict[str, Dict[str, int]] = {}
    if not df_res_payout.empty:
        pay_target = df_res_payout[df_res_payout["払戻種別"].astype(str).str.contains(CONFIG["BET_TYPE"], na=False)]
        for rid, g in pay_target.groupby("rid_str"):
            pay_map[str(rid)] = {row["組番_norm"]: int(row["払戻金_int"]) for _, row in g.iterrows()}

    return EvalContext(
        feat_df=feat_df,
        feature_matrix=feature_matrix,
        rid_array=rid_array,
        name_norm_array=name_norm_array,
        place_array=place_array,
        surface_array=surface_array,
        favorite_risk=favorite_risk,
        rest_dist_risk=rest_dist_risk,
        extra_penalty_fixed=extra_penalty_fixed,
        extra_penalty_seed=extra_penalty_seed,
        extra_penalty_missing_mask=extra_penalty_missing_mask,
        avg_pop=avg_pop,
        avg_finish=avg_finish,
        ta_n=ta_n,
        avg_margin=avg_margin,
        win_rate=win_rate,
        dl_score=dl_score_series.to_numpy(dtype=float, copy=False),
        dl_prob=dl_prob_series.to_numpy(dtype=float, copy=False),
        dl_rank=dl_rank_series.to_numpy(dtype=float, copy=False),
        dl_bonus=dl_bonus_series.to_numpy(dtype=float, copy=False),
        race_ids=race_ids,
        race_indices=race_indices,
        place_indices=place_indices,
        place_surface_indices=place_surface_indices,
        actual_top3_names=actual_top3_names,
        actual_top3_nums=actual_top3_nums,
        pay_map=pay_map,
        place_by_rid=place_by_rid,
        surface_by_rid=surface_by_rid,
    )


def _compute_total_raw_from_context(context: EvalContext, weights_map: WeightsMap) -> np.ndarray:
    default_weights = weights_map.get("__default__", {})
    total_raw = context.feature_matrix @ _weight_vector(default_weights)

    for place_name, idx in context.place_indices.items():
        place_weights = weights_map.get(place_name)
        if isinstance(place_weights, dict):
            total_raw[idx] = context.feature_matrix[idx] @ _weight_vector(place_weights)

    for place_surface_key, idx in context.place_surface_indices.items():
        place_surface_weights = weights_map.get(place_surface_key)
        if isinstance(place_surface_weights, dict):
            total_raw[idx] = context.feature_matrix[idx] @ _weight_vector(place_surface_weights)

    return total_raw


def _compute_extra_penalty_from_context(context: EvalContext, total_raw: np.ndarray) -> np.ndarray:
    if context.extra_penalty_fixed is not None:
        return context.extra_penalty_fixed.copy()
    return context.extra_penalty_seed.copy()


def _compute_score_rank_from_context(
    context: EvalContext,
    total_raw: np.ndarray,
    extra_penalty: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    # 本番 pipeline に合わせて dl_bonus も反映
    total = (
        total_raw
        + context.dl_bonus
        - PIPE_ALPHA * context.favorite_risk
        - PIPE_EXTRA_ALPHA * extra_penalty
    )

    score = np.zeros_like(total, dtype=float)
    rank = np.zeros(total.shape[0], dtype=int)

    for rid in context.race_ids:
        idx = context.race_indices[rid]
        race_total = total[idx]
        race_score = _normalize_score_array(race_total)
        score[idx] = np.round(race_score, 2)
        rank[idx] = _dense_rank_desc(score[idx])

    return total, score, rank


def compute_scores_with_optimizer_weights(
    df_feat: pd.DataFrame,
    weights_map: WeightsMap,
    eval_context: Optional[EvalContext] = None,
) -> pd.DataFrame:
    context = eval_context or build_eval_context(
        df_feat=df_feat,
        df_res_entries=pd.DataFrame(columns=["rid_str", "name_norm", "着順_num", "馬番_int"]),
        df_res_payout=pd.DataFrame(columns=["rid_str", "払戻種別", "組番_norm", "払戻金_int"]),
    )

    out = context.feat_df.copy()
    total_raw = _compute_total_raw_from_context(context, weights_map)
    extra_penalty = _compute_extra_penalty_from_context(context, total_raw)
    total, score, rank = _compute_score_rank_from_context(context, total_raw, extra_penalty)

    out["favorite_risk"] = context.favorite_risk
    out["rest_dist_risk"] = context.rest_dist_risk
    out["extra_penalty"] = extra_penalty
    out["dl_score"] = context.dl_score
    out["dl_prob"] = context.dl_prob
    out["dl_rank"] = context.dl_rank
    out["dl_bonus"] = context.dl_bonus
    out["total_raw"] = total_raw
    out["total"] = total
    out["score"] = score
    out["rank"] = rank
    return out


def eval_success_and_roi(
    weights_map: WeightsMap,
    df_feat: pd.DataFrame,
    df_res_entries: pd.DataFrame,
    df_res_payout: pd.DataFrame,
    eval_context: Optional[EvalContext] = None,
) -> Tuple[float, int, int, int, Dict[str, Dict[str, Any]], Dict[str, float]]:
    """
    関数名は既存互換のためそのまま
    中身は top5命中率専用評価
    """
    if df_feat is None or df_feat.empty:
        empty_stab = summarize_stability(0, 0.0, 0, 0, {})
        return 0.0, 0, 0, 0, {}, empty_stab

    context = eval_context or build_eval_context(df_feat, df_res_entries, df_res_payout)
    total_raw = _compute_total_raw_from_context(context, weights_map)
    extra_penalty = _compute_extra_penalty_from_context(context, total_raw)
    _, score, _ = _compute_score_rank_from_context(context, total_raw, extra_penalty)

    total_points = 0.0
    total_races = 0
    total_invest = 0
    total_return = 0
    details: Dict[str, Dict[str, Any]] = {}

    w1 = float(CONFIG["TOP5_HIT_W_FIRST"])
    w2 = float(CONFIG["TOP5_HIT_W_SECOND"])
    w3 = float(CONFIG["TOP5_HIT_W_THIRD"])

    for rid in context.race_ids:
        idx = context.race_indices[rid]
        race_scores = score[idx]
        race_names = context.name_norm_array[idx]

        order = np.lexsort((race_names.astype(str), -race_scores))
        pred_names = [str(name) for name in race_names[order[: int(CONFIG["TOP_K"])]]]

        pred_top_idx = int(idx[order[0]]) if len(order) >= 1 else -1
        rank1_name = str(race_names[order[0]]) if len(order) >= 1 else ""
        rank1_dl_score = float(context.dl_score[pred_top_idx]) if pred_top_idx >= 0 else 0.5

        act_names = context.actual_top3_names.get(rid, [])
        act_nums = context.actual_top3_nums.get(rid, [])
        if len(act_names) < 3 or len(act_nums) < 3:
            continue

        total_races += 1

        hit1 = 1 if act_names[0] in pred_names else 0
        hit2 = 1 if act_names[1] in pred_names else 0
        hit3 = 1 if act_names[2] in pred_names else 0

        rank1_is_win = 1 if (len(act_names) >= 1 and rank1_name == str(act_names[0])) else 0
        rank1_is_place = 1 if rank1_name in set(str(x) for x in act_names[:3]) else 0

        hit_points = hit1 * w1 + hit2 * w2 + hit3 * w3
        complete = 1 if (hit1 and hit2 and hit3) else 0
        capture_rate = (hit1 + hit2 + hit3) / 3.0

        total_points += hit_points

        details[rid] = {
            "place_name": str(context.place_by_rid.get(rid, "") or ""),
            "surface_name": _normalize_surface_name(context.surface_by_rid.get(rid, "")),
            "pred_top5_names": pred_names,
            "actual_top3_names": act_names,
            "actual_top3_nums": "-".join(str(n) for n in act_nums[:3]),
            "rank1_name": rank1_name,
            "rank1_win": int(rank1_is_win),
            "rank1_place": int(rank1_is_place),
            "rank1_dl_score": float(rank1_dl_score),
            "evaluated": 1,
            "top5_hit_points": float(hit_points),
            "top3_complete": int(complete),
            "win_in_top5": int(hit1),
            "place_capture_rate": float(capture_rate),
            "hit1": int(hit1),
            "hit2": int(hit2),
            "hit3": int(hit3),
            "success": int(complete),
            "pay_yen": 0,
            "invest_yen": 0,
            "payout_missing": 0,
            "pay_raw_yen": 0,
            "pay_capped_yen": 0,
            "score_gap": 0.0,
            "skip_by_gap": 0,
        }

    stab = summarize_stability(total_races, total_points, total_invest, total_return, details)
    return total_points, total_races, total_invest, total_return, details, stab
