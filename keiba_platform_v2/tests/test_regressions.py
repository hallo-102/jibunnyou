from __future__ import annotations

import pandas as pd

from keiba_v2.legacy_history import load_legacy_history
from keiba_v2.strategies import StrategyBet, cap_bets


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
