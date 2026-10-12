from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class BacktestSummary:
    races: int
    bets: int
    stake_yen: int
    return_yen: int
    profit_yen: int
    roi: float
    hit_rate: float
    max_drawdown_yen: int

    def to_dict(self) -> dict:
        return asdict(self)


def summarize_bets(bets: pd.DataFrame) -> BacktestSummary:
    if bets.empty:
        return BacktestSummary(0, 0, 0, 0, 0, 0.0, 0.0, 0)
    x = bets.copy()
    x["stake_yen"] = pd.to_numeric(x.get("stake_yen", 0), errors="coerce").fillna(0).astype(int)
    x["return_yen"] = pd.to_numeric(x.get("return_yen", 0), errors="coerce").fillna(0).astype(int)
    x["profit_yen"] = x["return_yen"] - x["stake_yen"]
    equity = x["profit_yen"].cumsum()
    running_peak = equity.cummax().clip(lower=0)
    drawdown = running_peak - equity
    stake = int(x["stake_yen"].sum())
    ret = int(x["return_yen"].sum())
    hits = int((x["return_yen"] > 0).sum())
    return BacktestSummary(
        races=int(x["race_id"].astype(str).nunique()) if "race_id" in x.columns else 0,
        bets=int(len(x)), stake_yen=stake, return_yen=ret,
        profit_yen=ret - stake,
        roi=(ret / stake) if stake > 0 else 0.0,
        hit_rate=(hits / len(x)) if len(x) else 0.0,
        max_drawdown_yen=int(drawdown.max()) if len(drawdown) else 0,
    )


def chronological_folds(df: pd.DataFrame, date_col: str = "race_date", n_splits: int = 4) -> list[tuple[pd.DataFrame, pd.DataFrame]]:
    if date_col not in df.columns:
        raise ValueError(f"missing date column: {date_col}")
    x = df.copy()
    x[date_col] = pd.to_datetime(x[date_col], errors="coerce")
    x = x.dropna(subset=[date_col]).sort_values(date_col)
    dates = np.array(sorted(x[date_col].dt.normalize().unique()))
    if len(dates) < n_splits + 1:
        return []
    chunks = np.array_split(dates, n_splits + 1)
    folds: list[tuple[pd.DataFrame, pd.DataFrame]] = []
    for i in range(1, len(chunks)):
        train_dates = np.concatenate(chunks[:i])
        test_dates = chunks[i]
        train = x[x[date_col].dt.normalize().isin(train_dates)].copy()
        test = x[x[date_col].dt.normalize().isin(test_dates)].copy()
        if not train.empty and not test.empty:
            folds.append((train, test))
    return folds
