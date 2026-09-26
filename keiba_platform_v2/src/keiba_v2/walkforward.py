from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class FoldResult:
    fold: int
    train_dates: int
    test_dates: int
    train_races: int
    test_races: int
    train_last_date: str
    test_first_date: str
    top1_win_rate: float
    top3_win_rate: float
    top5_win_rate: float
    win_bets: int
    win_stake_yen: int
    win_return_yen: int
    win_roi: float

    def to_dict(self) -> dict:
        return asdict(self)


def _sanitize_features(df: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    """Return numeric finite features safe for LightGBM."""
    return (
        df[feature_cols]
        .apply(pd.to_numeric, errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0.0)
    )


def _normalize_race_probabilities(raw_prob: pd.Series, race_id: pd.Series) -> pd.Series:
    """Normalize finite non-negative scores within each race without propagating NaN/inf."""
    safe = pd.to_numeric(raw_prob, errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
    safe = safe.clip(lower=0.0)
    denom = safe.groupby(race_id).transform("sum")
    normalized = safe.div(denom.where(denom > 0.0))

    # A degenerate race (all scores invalid/zero) falls back to equal probability.
    group_size = race_id.groupby(race_id).transform("size").astype(float)
    fallback = 1.0 / group_size
    return normalized.replace([np.inf, -np.inf], np.nan).fillna(fallback)


def _date_folds(df: pd.DataFrame, n_splits: int) -> list[tuple[list[pd.Timestamp], list[pd.Timestamp]]]:
    work = df.copy()
    work["_race_date"] = pd.to_datetime(work["race_date"], errors="coerce").dt.normalize()
    if work["_race_date"].isna().any():
        raise ValueError("walk-forward race_date contains invalid values")
    dates = sorted(work["_race_date"].unique().tolist())
    if len(dates) < n_splits + 2:
        raise ValueError(f"not enough distinct race dates for walk-forward: dates={len(dates)}, n_splits={n_splits}")
    chunks = [list(x) for x in np.array_split(np.array(dates, dtype=object), n_splits + 1)]
    folds: list[tuple[list[pd.Timestamp], list[pd.Timestamp]]] = []
    for i in range(1, len(chunks)):
        train_dates = [pd.Timestamp(v) for chunk in chunks[:i] for v in chunk]
        test_dates = [pd.Timestamp(v) for v in chunks[i]]
        if train_dates and test_dates:
            folds.append((train_dates, test_dates))
    return folds


def run_walkforward(
    df: pd.DataFrame,
    *,
    feature_prefix: str = "feature_",
    n_splits: int = 4,
    seed: int = 42,
    min_ev: float = 1.05,
    max_odds: float = 30.0,
    unit_yen: int = 100,
) -> tuple[pd.DataFrame, dict]:
    try:
        import lightgbm as lgb
    except ImportError as exc:
        raise RuntimeError("install optional ML dependencies: pip install -e .[ml]") from exc

    required = {"race_id", "race_date", "horse_no", "is_winner", "win_odds"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"walk-forward data missing columns: {sorted(missing)}")
    feature_cols = [c for c in df.columns if str(c).startswith(feature_prefix)]
    if not feature_cols:
        raise ValueError(f"no features with prefix {feature_prefix}")

    work = df.copy()
    work["race_id"] = work["race_id"].astype(str)
    work["_race_date"] = pd.to_datetime(work["race_date"], errors="coerce").dt.normalize()
    if work["_race_date"].isna().any():
        raise ValueError("walk-forward race_date contains invalid values")
    work["is_winner"] = pd.to_numeric(work["is_winner"], errors="coerce").fillna(0).astype(int)
    work["win_odds"] = pd.to_numeric(work["win_odds"], errors="coerce").replace([np.inf, -np.inf], np.nan)
    folds = _date_folds(work, n_splits)
    results: list[FoldResult] = []

    for fold_no, (train_dates, test_dates) in enumerate(folds, start=1):
        train = work[work["_race_date"].isin(train_dates)].copy()
        test = work[work["_race_date"].isin(test_dates)].copy()
        if train["is_winner"].nunique() < 2 or test["is_winner"].sum() == 0:
            continue
        if train["_race_date"].max() >= test["_race_date"].min():
            raise RuntimeError("walk-forward leakage detected: train date overlaps test date")

        x_train = _sanitize_features(train, feature_cols)
        y_train = train["is_winner"]
        pos = max(1, int(y_train.sum()))
        neg = max(1, int((1 - y_train).sum()))
        model = lgb.LGBMClassifier(
            n_estimators=600,
            learning_rate=0.03,
            num_leaves=31,
            min_child_samples=30,
            subsample=0.9,
            colsample_bytree=0.9,
            reg_lambda=1.0,
            scale_pos_weight=neg / pos,
            random_state=seed + fold_no,
            n_jobs=-1,
            verbosity=-1,
        )
        model.fit(x_train, y_train)
        x_test = _sanitize_features(test, feature_cols)
        test["raw_prob"] = pd.Series(model.predict_proba(x_test)[:, 1], index=test.index, dtype=float)
        test["model_win_prob"] = _normalize_race_probabilities(test["raw_prob"], test["race_id"])
        test["pred_rank"] = (
            test.groupby("race_id")["model_win_prob"]
            .rank(method="first", ascending=False)
            .astype("Int64")
        )

        winners = test[test["is_winner"] == 1]
        race_count = int(test["race_id"].nunique())
        top1 = float((winners["pred_rank"] == 1).sum() / race_count) if race_count else 0.0
        top3 = float((winners["pred_rank"] <= 3).sum() / race_count) if race_count else 0.0
        top5 = float((winners["pred_rank"] <= 5).sum() / race_count) if race_count else 0.0

        test["expected_value"] = test["model_win_prob"] * test["win_odds"].fillna(0.0)
        bets = test[(test["expected_value"] >= min_ev) & (test["win_odds"] <= max_odds)].copy()
        stake = int(len(bets) * unit_yen)
        returns = int(round(float((bets["is_winner"] * bets["win_odds"].fillna(0.0) * unit_yen).sum())))
        roi = returns / stake if stake else 0.0

        results.append(FoldResult(
            fold=fold_no,
            train_dates=int(train["_race_date"].nunique()),
            test_dates=int(test["_race_date"].nunique()),
            train_races=int(train["race_id"].nunique()),
            test_races=race_count,
            train_last_date=str(train["_race_date"].max().date()),
            test_first_date=str(test["_race_date"].min().date()),
            top1_win_rate=top1,
            top3_win_rate=top3,
            top5_win_rate=top5,
            win_bets=int(len(bets)),
            win_stake_yen=stake,
            win_return_yen=returns,
            win_roi=float(roi),
        ))

    fold_df = pd.DataFrame([r.to_dict() for r in results])
    if fold_df.empty:
        summary = {"folds": 0, "test_dates": 0, "test_races": 0, "win_bets": 0, "win_roi": 0.0}
    else:
        total_stake = int(fold_df["win_stake_yen"].sum())
        total_return = int(fold_df["win_return_yen"].sum())
        summary = {
            "folds": int(len(fold_df)),
            "test_dates": int(fold_df["test_dates"].sum()),
            "test_races": int(fold_df["test_races"].sum()),
            "avg_top1_win_rate": float(fold_df["top1_win_rate"].mean()),
            "avg_top3_win_rate": float(fold_df["top3_win_rate"].mean()),
            "avg_top5_win_rate": float(fold_df["top5_win_rate"].mean()),
            "win_bets": int(fold_df["win_bets"].sum()),
            "win_stake_yen": total_stake,
            "win_return_yen": total_return,
            "win_roi": float(total_return / total_stake) if total_stake else 0.0,
            "profitable_folds": int((fold_df["win_roi"] > 1.0).sum()),
        }
    return fold_df, summary
