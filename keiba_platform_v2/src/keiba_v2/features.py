from __future__ import annotations

import numpy as np
import pandas as pd


def _num(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
    if col not in df.columns:
        return pd.Series(default, index=df.index, dtype=float)
    return pd.to_numeric(df[col], errors="coerce").fillna(default).astype(float)


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Build stable, leakage-aware race features from canonical columns.

    Existing feature_* columns are preserved. When source columns are available,
    additional normalized features are generated. Missing source columns do not
    abort the pipeline; this allows gradual migration from legacy Excel inputs.
    """
    out = df.copy()

    if "finish_avg" in out.columns and "feature_avg_finish" not in out.columns:
        out["feature_avg_finish"] = _num(out, "finish_avg", 99.0)
    if "pop_avg" in out.columns and "feature_avg_pop" not in out.columns:
        out["feature_avg_pop"] = _num(out, "pop_avg", 99.0)
    if "last3f_avg" in out.columns and "feature_avg_last3f" not in out.columns:
        out["feature_avg_last3f"] = _num(out, "last3f_avg", 99.0)
    if "win_rate" in out.columns and "feature_win_rate" not in out.columns:
        out["feature_win_rate"] = _num(out, "win_rate", 0.0)
    if "fast_score" in out.columns and "feature_fast_score" not in out.columns:
        out["feature_fast_score"] = _num(out, "fast_score", 0.0)
    if "avg_score" in out.columns and "feature_avg_score" not in out.columns:
        out["feature_avg_score"] = _num(out, "avg_score", 0.0)
    if "leg_type_suitability" in out.columns and "feature_leg_type_suitability" not in out.columns:
        out["feature_leg_type_suitability"] = _num(out, "leg_type_suitability", 0.0)

    odds = _num(out, "win_odds", np.nan).clip(lower=1.01)
    implied = (1.0 / odds).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    out["market_implied_prob"] = implied
    denom = implied.groupby(out["race_id"]).transform("sum").replace(0.0, np.nan)
    out["market_share"] = (implied / denom).fillna(0.0)
    out["market_rank"] = (
        out.groupby("race_id")["win_odds"].rank(method="first", ascending=True).astype("Int64")
    )

    if "days_off" in out.columns:
        d = _num(out, "days_off", 0.0).clip(lower=0.0)
        out["feature_days_off_log"] = np.log1p(d)
    if "distance" in out.columns and "last_distance" in out.columns:
        out["feature_distance_change"] = (_num(out, "distance") - _num(out, "last_distance")).abs()
    if "body_weight_change" in out.columns:
        out["feature_body_weight_change_abs"] = _num(out, "body_weight_change").abs()

    feature_cols = [c for c in out.columns if str(c).startswith("feature_")]
    for col in feature_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce").replace([np.inf, -np.inf], np.nan)
        med = out.groupby("race_id")[col].transform("median")
        out[col] = out[col].fillna(med).fillna(0.0)

    return out
