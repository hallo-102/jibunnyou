from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from .contracts import canonicalize


def _minmax(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce").astype(float).replace([np.inf, -np.inf], np.nan)
    lo = s.min(skipna=True)
    hi = s.max(skipna=True)
    if pd.isna(lo) or pd.isna(hi) or hi <= lo:
        return pd.Series(0.5, index=s.index, dtype=float)
    return (s - lo) / (hi - lo)


def _fallback_score(df: pd.DataFrame, cfg: dict) -> pd.Series:
    feature_prefix = str(cfg.get("feature_prefix", "feature_"))
    weights = cfg.get("fallback_weights", {}) or {}
    score = pd.Series(0.0, index=df.index, dtype=float)
    used = 0
    for col in df.columns:
        if not str(col).startswith(feature_prefix):
            continue
        values = _minmax(df[col]).fillna(0.5)
        weight = float(weights.get(col, 1.0))
        score = score + values * weight
        used += 1
    if used == 0:
        odds = pd.to_numeric(df["win_odds"], errors="coerce").replace([np.inf, -np.inf], np.nan)
        implied = 1.0 / odds.clip(lower=1.01)
        score = implied.groupby(df["race_id"]).transform(_minmax)
    return score.replace([np.inf, -np.inf], np.nan).fillna(0.0).astype(float)


def _try_lightgbm_predict(df: pd.DataFrame, cfg: dict, project_root: Path) -> pd.Series | None:
    model_path = project_root / str(cfg.get("model_path", "data/runtime/model.txt"))
    if not model_path.exists():
        return None
    try:
        import lightgbm as lgb
    except ImportError:
        return None

    booster = lgb.Booster(model_file=str(model_path))
    names = booster.feature_name()
    missing = [c for c in names if c not in df.columns]
    if missing:
        return None
    x = (
        df[names]
        .apply(pd.to_numeric, errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0.0)
    )
    pred = pd.Series(booster.predict(x), index=df.index, dtype=float)
    return pred.replace([np.inf, -np.inf], np.nan).fillna(0.0)


def predict(df: pd.DataFrame, cfg: dict, project_root: Path) -> pd.DataFrame:
    out = canonicalize(df)
    model_score = _try_lightgbm_predict(out, cfg, project_root)
    out["prediction_source"] = "lightgbm" if model_score is not None else "fallback"
    out["prediction_score"] = model_score if model_score is not None else _fallback_score(out, cfg)
    out["prediction_score"] = out["prediction_score"].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    out["pred_rank"] = (
        out.groupby("race_id")["prediction_score"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    top_k = int(cfg.get("top_k", 5))
    out["pred_top_k"] = out["pred_rank"].le(top_k)
    return out.sort_values(["race_id", "pred_rank", "horse_no"]).reset_index(drop=True)
