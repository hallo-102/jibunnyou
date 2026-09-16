from __future__ import annotations

import pandas as pd


def select_value_races(
    df: pd.DataFrame,
    cfg: dict,
    combination_ev: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Rank all races by ticket-level value, then select at most N races.

    A race may qualify through WIN value, combination value (QUINELLA/TRIO), or
    both. The daily cap is not a quota: fewer qualifying races means fewer bets.
    """
    rows: list[dict] = []
    min_ev = float(cfg.get("min_race_ev", 1.08))
    min_edge = float(cfg.get("min_edge", 0.03))
    min_combo_ev = float(cfg.get("min_combination_ev", min_ev))
    max_candidates = int(cfg.get("max_value_candidates_per_race", 4))
    max_buy_races = max(0, int(cfg.get("max_buy_races_per_day", 5)))

    combo = combination_ev.copy() if combination_ev is not None else pd.DataFrame()
    if not combo.empty:
        combo["race_id"] = combo["race_id"].astype(str)
        combo["expected_value"] = pd.to_numeric(combo["expected_value"], errors="coerce").fillna(0.0)
        combo["model_hit_prob"] = pd.to_numeric(combo.get("model_hit_prob"), errors="coerce").fillna(0.0)
        combo["odds"] = pd.to_numeric(combo.get("odds"), errors="coerce").fillna(0.0)
        combo["market_hit_prob"] = (1.0 / combo["odds"].where(combo["odds"] > 0)).fillna(0.0)
        combo["combo_edge"] = combo["model_hit_prob"] - combo["market_hit_prob"]

    for race_id, g in df.groupby("race_id", sort=True):
        gg = g.copy()
        if "expected_value" not in gg.columns:
            gg["expected_value"] = 0.0
        if "model_win_prob" not in gg.columns:
            gg["model_win_prob"] = 0.0
        if "market_implied_prob" not in gg.columns:
            gg["market_implied_prob"] = 0.0

        gg["edge"] = gg["model_win_prob"] - gg["market_implied_prob"]
        win_cand = gg[(gg["expected_value"] >= min_ev) & (gg["edge"] >= min_edge)].copy()
        win_cand = win_cand.sort_values(["expected_value", "edge"], ascending=False).head(max_candidates)
        best_win_ev = float(win_cand["expected_value"].max()) if not win_cand.empty else 0.0
        best_win_edge = float(win_cand["edge"].max()) if not win_cand.empty else 0.0
        win_score = best_win_ev * 0.7 + best_win_edge * 3.0 * 0.3 if not win_cand.empty else 0.0

        if combo.empty:
            combo_cand = pd.DataFrame()
        else:
            combo_cand = combo[(combo["race_id"] == str(race_id)) & (combo["expected_value"] >= min_combo_ev)].copy()
            combo_cand = combo_cand.sort_values("expected_value", ascending=False).head(max_candidates)
        best_combo_ev = float(combo_cand["expected_value"].max()) if not combo_cand.empty else 0.0
        best_combo_edge = float(combo_cand["combo_edge"].max()) if not combo_cand.empty else 0.0
        combo_score = best_combo_ev if not combo_cand.empty else 0.0

        win_ok = not win_cand.empty
        combo_ok = not combo_cand.empty
        if win_ok and combo_ok:
            source = "BOTH"
        elif win_ok:
            source = "WIN"
        elif combo_ok:
            source = "COMBINATION"
        else:
            source = "NONE"

        rows.append({
            "race_id": str(race_id),
            "value_candidate_count": int(len(win_cand)),
            "combination_candidate_count": int(len(combo_cand)),
            "best_expected_value": best_win_ev,
            "best_edge": best_win_edge,
            "best_combination_ev": best_combo_ev,
            "best_combination_edge": best_combo_edge,
            "race_value_score": float(max(win_score, combo_score)),
            "qualifying_source": source,
            "buy_candidate": bool(win_ok or combo_ok),
        })

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    out = out.sort_values(
        ["buy_candidate", "race_value_score", "best_combination_ev", "best_expected_value"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)
    out["selected_for_day"] = False
    qualifying = out.index[out["buy_candidate"]].tolist()[:max_buy_races]
    if qualifying:
        out.loc[qualifying, "selected_for_day"] = True
    out["selection_rank"] = pd.NA
    selected_indexes = out.index[out["selected_for_day"]].tolist()
    for rank, idx in enumerate(selected_indexes, start=1):
        out.at[idx, "selection_rank"] = rank
    return out
