from __future__ import annotations

import pandas as pd

from keiba_v2.backtest import summarize_bets
from keiba_v2.features import build_features
from keiba_v2.odds import add_expected_value, analyze_odds
from keiba_v2.prediction import predict
from keiba_v2.race_selector import select_value_races
from keiba_v2.results import evaluate_strategy_bets
from keiba_v2.shadow import build_shadow_bets
from keiba_v2.storage import RunStore
from keiba_v2.strategies import build_strategy_bets, cap_bets
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


def test_prediction_odds_selection_and_strategies(tmp_path):
    featured = build_features(sample_df())
    assert "market_implied_prob" in featured.columns
    assert "market_rank" in featured.columns

    prediction_cfg = {
        "top_k": 5,
        "feature_prefix": "feature_",
        "model_path": "data/runtime/none.txt",
        "fallback_weights": {"feature_win_rate": 0.4, "feature_avg_score": 0.6},
    }
    pred = predict(featured, prediction_cfg, tmp_path)
    assert pred["pred_rank"].min() == 1
    assert pred["pred_rank"].max() == 6

    odds_cfg = {"min_expected_value": 0.5, "max_win_odds": 30.0, "top3_concentration_watch": 0.6, "gap_watch": 1.5}
    enriched = add_expected_value(pred, odds_cfg)
    summary = analyze_odds(enriched, odds_cfg)
    assert len(summary) == 1
    assert summary.iloc[0]["field_size"] == 6

    selection = select_value_races(enriched, {"min_race_ev": 0.5, "min_edge": -1.0, "max_value_candidates_per_race": 4})
    assert len(selection) == 1
    assert bool(selection.iloc[0]["buy_candidate"])

    strategy_cfg = {
        "unit_yen": 100, "min_expected_value": 0.5,
        "max_win_bets_per_race": 2, "max_quinella_points_per_race": 3,
        "max_trio_points_per_race": 5, "max_points_per_race": 13,
        "max_race_stake_yen": 1300, "daily_stake_limit_yen": 5000,
    }
    strategy_bets = cap_bets(build_strategy_bets(enriched, strategy_cfg), strategy_cfg)
    assert len(strategy_bets) <= 13
    assert sum(b.stake_yen for b in strategy_bets) <= 1300

    bets = build_shadow_bets(
        enriched,
        {"unit_yen": 100, "max_points_per_race": 5, "max_race_stake_yen": 500, "daily_stake_limit_yen": 5000},
    )
    assert len(bets) <= 5


def test_strategy_settlement_and_backtest():
    bets = pd.DataFrame([
        {"race_id": "R1", "bet_type": "WIN", "selection": "1", "stake_yen": 100, "reason": "test", "expected_value": 1.2},
        {"race_id": "R1", "bet_type": "TRIO", "selection": "1-2-3", "stake_yen": 100, "reason": "test", "expected_value": None},
    ])
    results = pd.DataFrame([
        {"race_id": "R1", "horse_no": 1, "finish_position": 1, "win_payout_yen_per_100": 250},
        {"race_id": "R1", "horse_no": 2, "finish_position": 2, "win_payout_yen_per_100": 0},
        {"race_id": "R1", "horse_no": 3, "finish_position": 3, "win_payout_yen_per_100": 0},
    ])
    payouts = pd.DataFrame([
        {"race_id": "R1", "bet_type": "TRIO", "selection": "3-1-2", "payout_yen_per_100": 800},
    ])
    settled = evaluate_strategy_bets(bets, results, payouts)
    summary = summarize_bets(settled)
    assert summary.stake_yen == 200
    assert summary.return_yen == 1050
    assert summary.profit_yen == 850


def test_run_store(tmp_path):
    store = RunStore(tmp_path / "runtime.sqlite3")
    store.start_run("run1", "20260919")
    store.save_strategy_bets("run1", [{
        "race_id": "R1", "bet_type": "WIN", "selection": "1",
        "stake_yen": 100, "reason": "test", "expected_value": 1.2,
    }])
    store.finish_run("run1", "SUCCESS", {"races": 1})
    with store.connect() as conn:
        row = conn.execute("SELECT status FROM runs WHERE run_id='run1'").fetchone()
        assert row["status"] == "SUCCESS"
