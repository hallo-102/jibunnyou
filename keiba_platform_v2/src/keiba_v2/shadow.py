from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class ShadowBet:
    race_id: str
    horse_no: int
    bet_type: str
    stake_yen: int
    win_odds: float
    expected_value: float
    pred_rank: int


def build_shadow_bets(df: pd.DataFrame, cfg: dict) -> list[ShadowBet]:
    unit = int(cfg.get("unit_yen", 100))
    max_points = int(cfg.get("max_points_per_race", 5))
    max_race_stake = int(cfg.get("max_race_stake_yen", 500))
    daily_limit = int(cfg.get("daily_stake_limit_yen", 5000))

    bets: list[ShadowBet] = []
    daily_total = 0
    for race_id, g in df.groupby("race_id", sort=True):
        candidates = g[g["value_candidate"]].copy()
        candidates = candidates.sort_values(["expected_value", "prediction_score"], ascending=[False, False])
        race_total = 0
        points = 0
        for _, row in candidates.iterrows():
            if points >= max_points:
                break
            if race_total + unit > max_race_stake:
                break
            if daily_total + unit > daily_limit:
                return bets
            bets.append(
                ShadowBet(
                    race_id=str(race_id),
                    horse_no=int(row["horse_no"]),
                    bet_type="WIN",
                    stake_yen=unit,
                    win_odds=float(row["win_odds"]),
                    expected_value=float(row["expected_value"]),
                    pred_rank=int(row["pred_rank"]),
                )
            )
            points += 1
            race_total += unit
            daily_total += unit
    return bets


def append_shadow_log(bets: list[ShadowBet], output_path: Path, race_date: str) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).isoformat()
    with output_path.open("a", encoding="utf-8") as f:
        for bet in bets:
            payload = {
                "event": "SHADOW_PLAN",
                "race_date": race_date,
                "created_at_utc": now,
                **asdict(bet),
            }
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
