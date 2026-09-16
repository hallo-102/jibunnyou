from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class FoldResult:
    fold: int
    train_races: int
    test_races: int
    top1_win_rate: float
    top3_win_rate: float
    top5_win_rate: float
    win_bets: int
    win_stake_yen: int
    win_return_yen: int
    win_roi: float

    def to_dict(self) -> dict:
        return asdict(self)


def _race_folds(df: pd.DataFrame, n_splits: int) -> list[tuple[list[str], list[str]]]:
    work = df.copy()
    work["race_date"] = pd.to_datetime(work["race_date"], errors="coerce")
    races = (
        work.groupby("race_id", as_index=False)["race_date"]
        .min()
        .dropna()
        .sort_values(["race_date", "race_id"])
    )
    ids = races["race_id"].astype(str).tolist()
    if len(ids) < n_splits + 4:
        raise ValueError(f"not enough races for walk-forward: races={len(ids)}, n_splits={n_splits}")
    chunks = [list(x) for x in np.array_split(np.array(ids, dtype=object), n_splits + 1)]
    folds: list[tuple[list[str], list[str]]] = []
    for i in range(1, len(chunks)):
        train_ids = [str(v) for chunk in chunks[:i] for v in chunk]
        test_ids = [str(v) for v in chunks[i]]
        if train_ids and test_ids:
            folds.append((train_ids, test_ids))
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
    work["is_winner"] = pd.to_numeric(work["is_winner"], errors="coerce").fillna(0).astype(int)
    work["win_odds"] = pd.to_numeric(work["win_odds"], errors="coerce")
    folds = _race_folds(work, n_splits)
    results: list[FoldResult] = []

    for fold_no, (train_ids, test_ids) in enumerate(folds, start=1):
        train = work[work["race_id"].isin(train_ids)].copy()
        test = work[work["race_id"].isin(test_ids)].copy()
        if train["is_winner"].nunique() < 2 or test["is_winner"].sum() == 0:
            continue

        x_train = train[feature_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)
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
        x_test = test[feature_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)
        test["raw_prob"] = model.predict_proba(x_test)[:, 1]
        denom = test.groupby("race_id")["raw_prob"].transform("sum").replace(0.0, np.nan)
        test["model_win_prob"] = (test["raw_prob"] / denom).fillna(0.0)
        test["pred_rank"] = test.groupby("race_id")["model_win_prob"].rank(method="first", ascending=False).astype(int)

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
            train_races=int(train["race_id"].nunique()),
            test_races=race_count,
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
        summary = {"folds": 0, "test_races": 0, "win_bets": 0, "win_roi": 0.0}
    else:
        total_stake = int(fold_df["win_stake_yen"].sum())
        total_return = int(fold_df["win_return_yen"].sum())
        summary = {
            "folds": int(len(fold_df)),
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
