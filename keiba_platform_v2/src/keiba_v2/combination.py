from __future__ import annotations

from itertools import permutations

import pandas as pd


def _selection_numbers(selection: str) -> tuple[int, ...]:
    return tuple(sorted(int(x) for x in str(selection).replace(" ", "").split("-") if x))


def unordered_topk_probability(prob_by_horse: dict[int, float], selection: tuple[int, ...]) -> float:
    """Approximate unordered top-k probability with a Plackett-Luce sequence.

    model_win_prob values are treated as positive strengths normalized within a
    race. The probability of every ordering of the requested horses occupying
    the first k places is summed. This is an approximation and must be validated
    by walk-forward ROI before any production use.
    """
    if not selection:
        return 0.0
    strengths = {int(k): max(float(v), 1e-12) for k, v in prob_by_horse.items()}
    if any(h not in strengths for h in selection):
        return 0.0
    total_strength = sum(strengths.values())
    if total_strength <= 0:
        return 0.0

    total_prob = 0.0
    for order in permutations(selection):
        remaining = total_strength
        p = 1.0
        for horse_no in order:
            s = strengths[horse_no]
            if remaining <= 0:
                p = 0.0
                break
            p *= s / remaining
            remaining -= s
        total_prob += p
    return float(min(max(total_prob, 0.0), 1.0))


def add_combination_expected_value(
    runner_predictions: pd.DataFrame,
    combination_odds: pd.DataFrame,
) -> pd.DataFrame:
    required_runner = {"race_id", "horse_no", "model_win_prob"}
    required_combo = {"race_id", "bet_type", "selection", "odds"}
    missing_r = required_runner - set(runner_predictions.columns)
    missing_c = required_combo - set(combination_odds.columns)
    if missing_r:
        raise ValueError(f"runner predictions missing columns: {sorted(missing_r)}")
    if missing_c:
        raise ValueError(f"combination odds missing columns: {sorted(missing_c)}")

    probs_by_race: dict[str, dict[int, float]] = {}
    for race_id, g in runner_predictions.groupby("race_id"):
        probs_by_race[str(race_id)] = {
            int(row["horse_no"]): float(row["model_win_prob"])
            for _, row in g.dropna(subset=["horse_no"]).iterrows()
        }

    out = combination_odds.copy()
    out["race_id"] = out["race_id"].astype(str)
    out["bet_type"] = out["bet_type"].astype(str).str.upper()
    out["odds"] = pd.to_numeric(out["odds"], errors="coerce")
    probabilities: list[float] = []
    normalized: list[str] = []

    for _, row in out.iterrows():
        selection = _selection_numbers(str(row["selection"]))
        normalized.append("-".join(map(str, selection)))
        probs = probs_by_race.get(str(row["race_id"]), {})
        expected_len = 2 if row["bet_type"] == "QUINELLA" else 3 if row["bet_type"] == "TRIO" else 0
        if expected_len == 0 or len(selection) != expected_len:
            probabilities.append(0.0)
        else:
            probabilities.append(unordered_topk_probability(probs, selection))

    out["selection"] = normalized
    out["model_hit_prob"] = probabilities
    out["expected_value"] = out["model_hit_prob"] * out["odds"].fillna(0.0)
    return out
