from __future__ import annotations

import pandas as pd

from keiba_v2.collectors.daily import merge_entries_and_odds
from keiba_v2.legacy_history import load_legacy_history
from keiba_v2.strategies import StrategyBet, cap_bets
from keiba_v2.training import _chronological_race_split
from keiba_v2.walkforward import _date_folds


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
    assert out["horse_id"].str.startswith("legacy:").all()


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


def test_walkforward_fold_dates_are_strictly_forward():
    df = _dated_rows()
    folds = _date_folds(df, n_splits=2)
    assert folds
    for train_dates, test_dates in folds:
        assert set(train_dates).isdisjoint(set(test_dates))
        assert max(train_dates) < min(test_dates)
