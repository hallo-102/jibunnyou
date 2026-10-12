from __future__ import annotations

import numpy as np
import pandas as pd


def analyze_odds(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    rows: list[dict] = []
    for race_id, g in df.groupby("race_id", sort=True):
        race = g.sort_values("win_odds").copy()
        odds = pd.to_numeric(race["win_odds"], errors="coerce").astype(float)
        implied = (1.0 / odds.clip(lower=1.01)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        total = float(implied.sum())
        support = implied / total if total > 0 else implied
        top3_concentration = float(support.iloc[:3].sum()) if len(support) >= 3 else float(support.sum())

        ratios: list[float] = []
        values = odds.tolist()
        for i in range(len(values) - 1):
            a, b = values[i], values[i + 1]
            ratios.append(float(b / a) if a > 0 else 0.0)
        max_gap = max(ratios) if ratios else 0.0
        max_gap_position = ratios.index(max_gap) + 1 if ratios else 0

        best = race.iloc[0]
        second = race.iloc[1] if len(race) > 1 else best
        second_ratio = float(second["win_odds"] / best["win_odds"]) if float(best["win_odds"]) > 0 else 0.0

        rows.append({
            "race_id": str(race_id),
            "field_size": int(len(race)),
            "favorite_odds": float(best["win_odds"]),
            "second_favorite_odds": float(second["win_odds"]),
            "second_ratio": second_ratio,
            "top3_concentration": top3_concentration,
            "max_gap_ratio": float(max_gap),
            "max_gap_position": int(max_gap_position),
            "concentration_watch": top3_concentration >= float(cfg.get("top3_concentration_watch", 0.60)),
            "gap_watch": max_gap >= float(cfg.get("gap_watch", 1.50)),
        })
    return pd.DataFrame(rows)


def _normalize_model_probability(scores: pd.Series, race_ids: pd.Series) -> pd.Series:
    scores = pd.to_numeric(scores, errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
    result = pd.Series(0.0, index=scores.index, dtype=float)
    for race_id, idx in race_ids.groupby(race_ids).groups.items():
        s = scores.loc[idx].astype(float)
        if len(s) == 0:
            continue
        if (s >= 0).all() and (s <= 1).all() and float(s.sum()) > 0:
            p = s / float(s.sum())
        else:
            shifted = s - float(s.max())
            exp = np.exp(shifted.clip(lower=-50, upper=50))
            denom = float(exp.sum())
            p = exp / denom if denom > 0 else pd.Series(1.0 / len(s), index=s.index)
        result.loc[idx] = p
    return result


def add_expected_value(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    out = df.copy()
    out["model_win_prob"] = _normalize_model_probability(out["prediction_score"], out["race_id"])
    odds = pd.to_numeric(out["win_odds"], errors="coerce").fillna(0.0)
    out["expected_value"] = out["model_win_prob"] * odds
    min_ev = float(cfg.get("min_expected_value", 1.05))
    max_odds = float(cfg.get("max_win_odds", 30.0))
    out["value_candidate"] = (out["expected_value"] >= min_ev) & (odds <= max_odds)
    return out
