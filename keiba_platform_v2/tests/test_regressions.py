from __future__ import annotations

import numpy as np
import pandas as pd

from keiba_v2.collectors.daily import merge_entries_and_odds
from keiba_v2.legacy_history import load_legacy_history
from keiba_v2.race_selector import select_value_races
from keiba_v2.strategies import StrategyBet, cap_bets
from keiba_v2.training import _chronological_race_split, _parse_race_dates
from keiba_v2.walkforward import _date_folds, _normalize_race_probabilities, _sanitize_features


def test_legacy_history_keeps_race_id_and_copies_source_race_id(tmp_path):
    source = tmp_path / "results_20260919.xlsx"
    pd.DataFrame({
        "レースID": ["202609190101", "202609190101"],
        "馬名": ["A", "B"],
        "馬番": [1, 2],
        "着順": [1, 2],
        "人気": [2, 1],
        "単勝": [4.0, 2.0],
    }).to_excel(source, index=False)

    out = load_legacy_history(source)
    assert "race_id" in out.columns
    assert "source_race_id" in out.columns
    assert out["race_id"].tolist() == ["202609190101", "202609190101"]
    assert out["source_race_id"].tolist() == ["202609190101", "202609190101"]
    assert out["horse_id"].str.startswith("NAME:").all()


def test_legacy_history_uses_sheet_date_and_reads_all_result_sheets(tmp_path):
    source = tmp_path / "racedata_results_clean_v3.xlsx"
    with pd.ExcelWriter(source) as writer:
        pd.DataFrame({
            "レースID": ["R1", "R1"],
            "着 順": [1, 2],
            "馬 番": [1, 2],
            "馬名": ["A", "B"],
            "人 気": [1, 2],
            "単勝 オッズ": [2.0, 4.0],
            "後3F": [34.1, 34.8],
        }).to_excel(writer, sheet_name="20260912", index=False)
        pd.DataFrame({
            "レースID": ["R2", "R2"],
            "着 順": [1, 2],
            "馬 番": [3, 4],
            "馬名": ["C", "D"],
            "人 気": [2, 1],
            "単勝 オッズ": [5.0, 1.8],
            "後3F": [35.0, 34.4],
        }).to_excel(writer, sheet_name="20260913", index=False)
        pd.DataFrame({"foo": [1]}).to_excel(writer, sheet_name="metadata", index=False)

    out = load_legacy_history(source)
    assert len(out) == 4
    assert set(out["race_date"].astype(str)) == {"20260912", "20260913"}
    assert set(out["race_id"]) == {"R1", "R2"}
    assert "last3f" in out.columns
    assert out["last3f"].notna().all()


def test_jra_odds_are_authoritative_when_entries_contain_old_odds():
    entries = pd.DataFrame({
        "race_id": ["R1", "R1"],
        "horse_no": [1, 2],
        "horse_name": ["A", "B"],
        "win_odds": [999.0, 999.0],
    })
    odds = pd.DataFrame({
        "race_id": ["R1", "R1"],
        "horse_no": [1, 2],
        "horse_name": ["A", "B"],
        "win_odds": [2.5, 4.0],
        "place_odds": ["1.2-1.5", "1.8-2.2"],
    })
    merged = merge_entries_and_odds(entries, odds)
    assert "win_odds_x" not in merged.columns
    assert "win_odds_y" not in merged.columns
    assert merged["win_odds"].tolist() == [2.5, 4.0]


def test_daily_stake_cap_respects_existing_t5_stake():
    bets = [
        StrategyBet("R1", "WIN", "1", 100, "test", 1.2),
        StrategyBet("R1", "WIN", "2", 100, "test", 1.1),
    ]
    cfg = {
        "max_points_per_race": 13,
        "max_race_stake_yen": 1300,
        "daily_stake_limit_yen": 5000,
    }
    selected = cap_bets(bets, cfg, existing_daily_stake_yen=4900)
    assert len(selected) == 1
    assert selected[0].stake_yen == 100

    selected_none = cap_bets(bets, cfg, existing_daily_stake_yen=5000)
    assert selected_none == []


def test_race_selection_never_forces_more_than_five_and_accepts_combo_only_value():
    rows = []
    combo_rows = []
    for i in range(1, 8):
        race_id = f"R{i}"
        rows.extend([
            {"race_id": race_id, "horse_no": 1, "expected_value": 0.95, "model_win_prob": 0.4, "market_implied_prob": 0.4},
            {"race_id": race_id, "horse_no": 2, "expected_value": 0.90, "model_win_prob": 0.3, "market_implied_prob": 0.3},
        ])
        combo_rows.append({
            "race_id": race_id,
            "bet_type": "TRIO",
            "selection": "1-2-3",
            "odds": 20.0,
            "model_hit_prob": 0.06 + i * 0.001,
            "expected_value": 1.20 + i * 0.01,
        })
    selected = select_value_races(
        pd.DataFrame(rows),
        {"min_race_ev": 1.08, "min_edge": 0.03, "max_buy_races_per_day": 5},
        pd.DataFrame(combo_rows),
    )
    assert int(selected["selected_for_day"].sum()) == 5
    assert set(selected.loc[selected["selected_for_day"], "qualifying_source"]) == {"COMBINATION"}


def test_race_selection_can_end_with_zero_races():
    runners = pd.DataFrame([
        {"race_id": "R1", "horse_no": 1, "expected_value": 0.9, "model_win_prob": 0.4, "market_implied_prob": 0.4},
        {"race_id": "R1", "horse_no": 2, "expected_value": 0.8, "model_win_prob": 0.3, "market_implied_prob": 0.3},
    ])
    selected = select_value_races(runners, {"min_race_ev": 1.08, "min_edge": 0.03, "max_buy_races_per_day": 5})
    assert int(selected["selected_for_day"].sum()) == 0


def _dated_rows() -> pd.DataFrame:
    rows = []
    for day in range(1, 7):
        date = f"202609{day:02d}"
        for race_no in (1, 2):
            race_id = f"{date}_T_{race_no:02d}R"
            for horse_no in (1, 2):
                rows.append({
                    "race_id": race_id,
                    "race_date": date,
                    "horse_no": horse_no,
                    "is_winner": int(horse_no == 1),
                    "win_odds": 2.0 + horse_no,
                    "feature_x": float(horse_no),
                })
    return pd.DataFrame(rows)


def test_train_validation_never_split_same_race_date():
    train, valid = _chronological_race_split(_dated_rows(), valid_fraction=0.33)
    train_dates = set(train["race_date"].astype(str))
    valid_dates = set(valid["race_date"].astype(str))
    assert train_dates.isdisjoint(valid_dates)
    assert max(train_dates) < min(valid_dates)


def test_training_date_parser_handles_numeric_yyyymmdd():
    values = pd.Series([20260901, 20260902, 20260903, 20260904, 20260905, 20260906])
    parsed = _parse_race_dates(values)
    assert parsed.dt.strftime("%Y%m%d").tolist() == [str(v) for v in values.tolist()]

    df = _dated_rows().copy()
    df["race_date"] = df["race_date"].astype(int)
    train, valid = _chronological_race_split(df, valid_fraction=0.33)
    assert pd.to_numeric(train["race_date"]).max() < pd.to_numeric(valid["race_date"]).min()


def test_walkforward_fold_dates_are_strictly_forward():
    df = _dated_rows()
    folds = _date_folds(df, n_splits=2)
    assert folds
    for train_dates, test_dates in folds:
        assert set(train_dates).isdisjoint(set(test_dates))
        assert max(train_dates) < min(test_dates)


def test_walkforward_sanitizes_nan_and_infinite_features():
    df = pd.DataFrame({
        "feature_x": [1.0, np.nan, np.inf, -np.inf],
        "feature_y": ["2.5", "bad", 3.0, None],
    })
    out = _sanitize_features(df, ["feature_x", "feature_y"])
    assert np.isfinite(out.to_numpy(dtype=float)).all()
    assert float(out.iloc[1, 1]) == 0.0


def test_walkforward_probability_normalization_never_returns_non_finite_values():
    raw = pd.Series([0.8, np.nan, np.inf, -np.inf, 0.0, 0.0])
    races = pd.Series(["R1", "R1", "R1", "R1", "R2", "R2"])
    out = _normalize_race_probabilities(raw, races)
    assert np.isfinite(out.to_numpy(dtype=float)).all()
    assert abs(float(out[races == "R1"].sum()) - 1.0) < 1e-12
    assert abs(float(out[races == "R2"].sum()) - 1.0) < 1e-12
    assert out[races == "R2"].tolist() == [0.5, 0.5]
