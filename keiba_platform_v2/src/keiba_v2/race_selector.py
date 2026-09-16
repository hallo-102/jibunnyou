from __future__ import annotations

import pandas as pd


def select_value_races(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """Rank races by value quality without forcing a fixed number of bets."""
    rows: list[dict] = []
    min_ev = float(cfg.get("min_race_ev", 1.08))
    min_edge = float(cfg.get("min_edge", 0.03))
    max_candidates = int(cfg.get("max_value_candidates_per_race", 4))

    for race_id, g in df.groupby("race_id", sort=True):
        gg = g.copy()
        if "expected_value" not in gg.columns:
            gg["expected_value"] = 0.0
        if "model_win_prob" not in gg.columns:
            gg["model_win_prob"] = 0.0
        if "market_implied_prob" not in gg.columns:
            gg["market_implied_prob"] = 0.0

        gg["edge"] = gg["model_win_prob"] - gg["market_implied_prob"]
        cand = gg[(gg["expected_value"] >= min_ev) & (gg["edge"] >= min_edge)].copy()
        cand = cand.sort_values(["expected_value", "edge"], ascending=False).head(max_candidates)

        best_ev = float(cand["expected_value"].max()) if not cand.empty else 0.0
        best_edge = float(cand["edge"].max()) if not cand.empty else 0.0
        score = best_ev * 0.7 + best_edge * 3.0 * 0.3 if not cand.empty else 0.0
        rows.append({
            "race_id": str(race_id),
            "value_candidate_count": int(len(cand)),
            "best_expected_value": best_ev,
            "best_edge": best_edge,
            "race_value_score": float(score),
            "buy_candidate": bool(len(cand) > 0),
        })

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(["buy_candidate", "race_value_score"], ascending=[False, False]).reset_index(drop=True)
