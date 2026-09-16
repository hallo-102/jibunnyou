from __future__ import annotations

from dataclasses import asdict, dataclass
from itertools import combinations

import pandas as pd


@dataclass(frozen=True)
class StrategyBet:
    race_id: str
    bet_type: str
    selection: str
    stake_yen: int
    reason: str
    expected_value: float | None = None

    def to_dict(self) -> dict:
        return asdict(self)


def _top_value_horses(g: pd.DataFrame, min_ev: float) -> pd.DataFrame:
    x = g[g.get("expected_value", pd.Series(0.0, index=g.index)) >= min_ev].copy()
    return x.sort_values(["expected_value", "pred_rank"], ascending=[False, True])


def build_strategy_bets(df: pd.DataFrame, cfg: dict) -> list[StrategyBet]:
    """Generate SHADOW-only strategy candidates for win, quinella, and trio.

    Combination tickets are deliberately conservative: they are generated only
    from horses already passing the model/value gate. Combination EV is marked
    unknown until exact combination odds are supplied by an odds adapter.
    """
    unit = int(cfg.get("unit_yen", 100))
    min_ev = float(cfg.get("min_expected_value", 1.08))
    max_win = int(cfg.get("max_win_bets_per_race", 2))
    max_quinella = int(cfg.get("max_quinella_points_per_race", 3))
    max_trio = int(cfg.get("max_trio_points_per_race", 5))
    bets: list[StrategyBet] = []

    for race_id, g in df.groupby("race_id", sort=True):
        values = _top_value_horses(g, min_ev).head(max(max_win, 5))
        for _, row in values.head(max_win).iterrows():
            bets.append(StrategyBet(
                race_id=str(race_id),
                bet_type="WIN",
                selection=str(int(row["horse_no"])),
                stake_yen=unit,
                reason=f"EV={float(row['expected_value']):.3f}, rank={int(row['pred_rank'])}",
                expected_value=float(row["expected_value"]),
            ))

        horse_nos = [int(x) for x in values["horse_no"].dropna().tolist()]
        for pair in list(combinations(horse_nos[:4], 2))[:max_quinella]:
            bets.append(StrategyBet(
                race_id=str(race_id), bet_type="QUINELLA",
                selection=f"{pair[0]}-{pair[1]}", stake_yen=unit,
                reason="model/value gated pair", expected_value=None,
            ))
        for trio in list(combinations(horse_nos[:5], 3))[:max_trio]:
            bets.append(StrategyBet(
                race_id=str(race_id), bet_type="TRIO",
                selection="-".join(map(str, sorted(trio))), stake_yen=unit,
                reason="model/value gated trio", expected_value=None,
            ))
    return bets


def cap_bets(bets: list[StrategyBet], cfg: dict) -> list[StrategyBet]:
    max_points = int(cfg.get("max_points_per_race", 13))
    max_stake = int(cfg.get("max_race_stake_yen", 1300))
    daily_limit = int(cfg.get("daily_stake_limit_yen", 5000))
    selected: list[StrategyBet] = []
    daily = 0
    per_race: dict[str, tuple[int, int]] = {}

    for bet in bets:
        pts, stake = per_race.get(bet.race_id, (0, 0))
        if pts + 1 > max_points or stake + bet.stake_yen > max_stake:
            continue
        if daily + bet.stake_yen > daily_limit:
            break
        selected.append(bet)
        per_race[bet.race_id] = (pts + 1, stake + bet.stake_yen)
        daily += bet.stake_yen
    return selected
