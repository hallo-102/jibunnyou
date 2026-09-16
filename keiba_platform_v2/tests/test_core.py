from __future__ import annotations

import pandas as pd

from keiba_v2.odds import add_expected_value, analyze_odds
from keiba_v2.prediction import predict
from keiba_v2.shadow import build_shadow_bets
from keiba_v2.validation import validate_races


def sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "race_id": ["202609190101"] * 6,
            "horse_no": [1, 2, 3, 4, 5, 6],
            "horse_name": ["A", "B", "C", "D", "E", "F"],
            "win_odds": [2.0, 4.0, 6.0, 10.0, 15.0, 20.0],
            "feature_win_rate": [0.4, 0.3, 0.2, 0.15, 0.10, 0.05],
            "feature_avg_score": [90, 80, 75, 70, 60, 50],
        }
    )


def test_validation_passes():
    result = validate_races(sample_df(), {"min_horses_per_race": 5, "max_horses_per_race": 18})
    assert result.ok


def test_validation_detects_duplicate():
    df = pd.concat([sample_df(), sample_df().iloc[[0]]], ignore_index=True)
    result = validate_races(df, {"min_horses_per_race": 5, "max_horses_per_race": 18})
    assert not result.ok
    assert any("duplicate" in x for x in result.errors)


def test_prediction_odds_and_shadow(tmp_path):
    prediction_cfg = {
        "top_k": 5,
        "feature_prefix": "feature_",
        "model_path": "data/runtime/none.txt",
        "fallback_weights": {"feature_win_rate": 0.4, "feature_avg_score": 0.6},
    }
    pred = predict(sample_df(), prediction_cfg, tmp_path)
    assert pred["pred_rank"].min() == 1
    assert pred["pred_rank"].max() == 6

    odds_cfg = {"min_expected_value": 0.5, "max_win_odds": 30.0, "top3_concentration_watch": 0.6, "gap_watch": 1.5}
    enriched = add_expected_value(pred, odds_cfg)
    summary = analyze_odds(enriched, odds_cfg)
    assert len(summary) == 1
    assert summary.iloc[0]["field_size"] == 6

    bets = build_shadow_bets(
        enriched,
        {"unit_yen": 100, "max_points_per_race": 5, "max_race_stake_yen": 500, "daily_stake_limit_yen": 5000},
    )
    assert len(bets) <= 5
    assert sum(b.stake_yen for b in bets) <= 500
