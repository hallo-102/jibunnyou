from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from tools.compare_best_weights_20260817 import (
    _official_popularity_to_numeric,
    _select_official_popularity_column,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "best_weights_profit_comparison_20260817"


def test_official_popularity_prefers_primary_result_column() -> None:
    """払戻側の文字列人気より、主結果側の数値人気を優先する。"""
    columns = ["レースID", "馬 番", "人 気", "人気"]

    assert _select_official_popularity_column(columns) == "人 気"


def test_official_popularity_parses_numeric_and_text_values() -> None:
    """数値人気と「N人気」形式を同じ数値へ正規化する。"""
    actual = _official_popularity_to_numeric(
        pd.Series([1, "2人気", " 10人気 ", None, "-"])
    )

    expected = pd.Series([1, 2, 10, pd.NA, pd.NA], dtype="Int64")
    pd.testing.assert_series_equal(actual, expected)


def test_comparison_uses_identical_race_and_horse_universe() -> None:
    """旧・新モデルが同一期間で同じレース数・馬数を評価したことを確認する。"""
    summary = pd.read_csv(OUTPUT_DIR / "prediction_summary.csv")
    base_periods = summary[summary["period"].isin(["TRAIN", "VALID", "TEST", "ALL"])]

    for _, period_rows in base_periods.groupby("period"):
        assert len(period_rows) == 2
        assert period_rows["race_count"].nunique() == 1
        assert period_rows["horse_count"].nunique() == 1

    paired_finish = pd.read_csv(OUTPUT_DIR / "paired_average_finish.csv")
    test_finish = paired_finish[paired_finish["period"].eq("TEST")].iloc[0]
    assert test_finish["common_race_count"] == 1064
    assert test_finish["new_minus_old"] == pytest.approx(0.1710526316)


def test_test_ratio_counts_reproduce_requested_ratios() -> None:
    """ユーザー指定の3比率を絶対件数から再現する。"""
    audit = json.loads((OUTPUT_DIR / "comparison_audit.json").read_text(encoding="utf-8"))
    explanation = audit["test_ratio_explanation"]

    assert explanation["rank1_place"]["old_count"] == 582
    assert explanation["rank1_place"]["new_count"] == 561
    assert explanation["rank1_place"]["ratio"] == pytest.approx(0.9639, abs=0.00005)
    assert explanation["top5_point"]["old_points"] == 4278
    assert explanation["top5_point"]["new_points"] == 4248
    assert explanation["top5_point"]["ratio"] == pytest.approx(0.9930, abs=0.00005)
    assert explanation["top3_complete"]["old_count"] == 291
    assert explanation["top3_complete"]["new_count"] == 296
    assert explanation["top3_complete"]["ratio"] == pytest.approx(1.0172, abs=0.00005)


def test_comparison_audit_rejects_prediction_leakage() -> None:
    """結果・払戻・確定オッズがスコアまたは順位へ入っていないことを確認する。"""
    audit = json.loads((OUTPUT_DIR / "comparison_audit.json").read_text(encoding="utf-8"))

    assert audit["feature_input"]["same_dataframe_object_used_for_both_models"] is True
    assert audit["results_data_flow"]["prediction_score_uses_results_or_payout"] is False
    assert audit["leakage_audit"]["final_odds_used_for_score_or_rank"] is False
    assert audit["leakage_audit"]["future_or_same_date_past_races_excluded"] is True
    assert audit["leakage_audit"]["future_or_same_date_ratings_excluded"] is True
    assert audit["leakage_audit"]["dl_result_training_has_ranking_effect"] is False
