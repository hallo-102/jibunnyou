from __future__ import annotations

from dataclasses import asdict, dataclass

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


def build_strategy_bets(
    df: pd.DataFrame,
    cfg: dict,
    combination_ev: pd.DataFrame | None = None,
) -> list[StrategyBet]:
    """Generate SHADOW strategy candidates using EV gates for every ticket."""
    unit = int(cfg.get("unit_yen", 100))
    min_ev = float(cfg.get("min_expected_value", 1.08))
    max_win = int(cfg.get("max_win_bets_per_race", 2))
    max_quinella = int(cfg.get("max_quinella_points_per_race", 3))
    max_trio = int(cfg.get("max_trio_points_per_race", 5))
    bets: list[StrategyBet] = []

    for race_id, g in df.groupby("race_id", sort=True):
        values = _top_value_horses(g, min_ev)
        for _, row in values.head(max_win).iterrows():
            bets.append(StrategyBet(
                race_id=str(race_id),
                bet_type="WIN",
                selection=str(int(row["horse_no"])),
                stake_yen=unit,
                reason=f"EV={float(row['expected_value']):.3f}, rank={int(row['pred_rank'])}",
                expected_value=float(row["expected_value"]),
            ))

    if combination_ev is not None and not combination_ev.empty:
        c = combination_ev.copy()
        c["bet_type"] = c["bet_type"].astype(str).str.upper()
        c["expected_value"] = pd.to_numeric(c["expected_value"], errors="coerce").fillna(0.0)
        c = c[c["expected_value"] >= min_ev].sort_values("expected_value", ascending=False)
        for race_id, g in c.groupby("race_id", sort=True):
            for bet_type, limit in (("QUINELLA", max_quinella), ("TRIO", max_trio)):
                subset = g[g["bet_type"] == bet_type].head(limit)
                for _, row in subset.iterrows():
                    bets.append(StrategyBet(
                        race_id=str(race_id),
                        bet_type=bet_type,
                        selection=str(row["selection"]),
                        stake_yen=unit,
                        reason=(
                            f"EV={float(row['expected_value']):.3f}, "
                            f"p={float(row.get('model_hit_prob', 0.0)):.4f}, "
                            f"odds={float(row.get('odds', 0.0)):.1f}"
                        ),
                        expected_value=float(row["expected_value"]),
                    ))

    return sorted(
        bets,
        key=lambda b: (b.race_id, -(b.expected_value or 0.0), b.bet_type, b.selection),
    )


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
