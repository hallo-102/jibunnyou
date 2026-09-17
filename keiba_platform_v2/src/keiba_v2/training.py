from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


def _chronological_race_split(data: pd.DataFrame, valid_fraction: float = 0.2) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split on complete race dates so no target-day result can leak into training."""
    x = data.copy()
    if "race_date" not in x.columns:
        raise ValueError("training data requires race_date for leakage-safe split")
    x["_race_date"] = pd.to_datetime(x["race_date"], errors="coerce")
    if x["_race_date"].isna().any():
        raise ValueError("race_date contains invalid values")

    dates = sorted(x["_race_date"].dt.normalize().unique().tolist())
    if len(dates) < 5:
        raise ValueError("at least 5 distinct race dates are required for chronological train/validation split")

    valid_count = max(1, int(round(len(dates) * valid_fraction)))
    valid_count = min(valid_count, len(dates) - 1)
    valid_dates = set(dates[-valid_count:])
    train = x[~x["_race_date"].dt.normalize().isin(valid_dates)].drop(columns=["_race_date"])
    valid = x[x["_race_date"].dt.normalize().isin(valid_dates)].drop(columns=["_race_date"])
    if train.empty or valid.empty:
        raise ValueError("chronological split produced an empty train or validation set")
    return train, valid


def _sanitize_features(df: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    return (
        df[feature_cols]
        .apply(pd.to_numeric, errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0.0)
    )


def train_lightgbm(df: pd.DataFrame, feature_prefix: str, model_path: Path, seed: int = 42) -> dict:
    try:
        import lightgbm as lgb
        from sklearn.metrics import log_loss, roc_auc_score
    except ImportError as exc:
        raise RuntimeError("install optional ML dependencies: pip install -e .[ml]") from exc

    if "is_winner" not in df.columns:
        raise ValueError("training data requires is_winner column (0/1)")
    if "race_id" not in df.columns:
        raise ValueError("training data requires race_id")
    if "race_date" not in df.columns:
        raise ValueError("training data requires race_date")
    feature_cols = [c for c in df.columns if str(c).startswith(feature_prefix)]
    if not feature_cols:
        raise ValueError(f"no training features with prefix: {feature_prefix}")

    data = df.copy()
    data["race_id"] = data["race_id"].astype(str)
    data["is_winner"] = pd.to_numeric(data["is_winner"], errors="coerce")
    data = data[data["is_winner"].isin([0, 1])].copy()
    if data.empty or data["is_winner"].nunique() < 2:
        raise ValueError("training data must contain winners and non-winners")

    train, valid = _chronological_race_split(data)
    if train["is_winner"].nunique() < 2 or valid["is_winner"].nunique() < 2:
        raise ValueError("train and validation must each contain winners and non-winners")

    x_train = _sanitize_features(train, feature_cols)
    y_train = train["is_winner"].astype(int)
    x_valid = _sanitize_features(valid, feature_cols)
    y_valid = valid["is_winner"].astype(int)

    positives = max(1, int(y_train.sum()))
    negatives = max(1, int((1 - y_train).sum()))
    scale_pos_weight = negatives / positives

    model = lgb.LGBMClassifier(
        n_estimators=1000,
        learning_rate=0.02,
        num_leaves=31,
        min_child_samples=30,
        subsample=0.9,
        colsample_bytree=0.9,
        reg_lambda=1.0,
        scale_pos_weight=scale_pos_weight,
        random_state=seed,
        n_jobs=-1,
    )
    model.fit(
        x_train,
        y_train,
        eval_set=[(x_valid, y_valid)],
        callbacks=[lgb.early_stopping(80, verbose=False)],
    )
    prob = np.asarray(model.predict_proba(x_valid)[:, 1], dtype=float)
    if not np.isfinite(prob).all():
        raise RuntimeError("LightGBM validation prediction contains non-finite values")

    model_path.parent.mkdir(parents=True, exist_ok=True)
    model.booster_.save_model(str(model_path))

    train_dates = pd.to_datetime(train["race_date"], errors="coerce")
    valid_dates = pd.to_datetime(valid["race_date"], errors="coerce")
    metrics = {
        "train_rows": int(len(train)),
        "valid_rows": int(len(valid)),
        "train_races": int(train["race_id"].nunique()),
        "valid_races": int(valid["race_id"].nunique()),
        "train_dates": int(train_dates.dt.normalize().nunique()),
        "valid_dates": int(valid_dates.dt.normalize().nunique()),
        "train_last_date": str(train_dates.max().date()),
        "valid_first_date": str(valid_dates.min().date()),
        "valid_logloss": float(log_loss(y_valid, prob, labels=[0, 1])),
        "best_iteration": int(model.best_iteration_ or model.n_estimators),
    }
    if y_valid.nunique() == 2:
        metrics["valid_auc"] = float(roc_auc_score(y_valid, prob))

    manifest = {
        "feature_cols": feature_cols,
        "metrics": metrics,
        "seed": seed,
        "split": "chronological_by_whole_race_date",
    }
    manifest_path = model_path.with_suffix(model_path.suffix + ".json")
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"model_path": model_path, "manifest_path": manifest_path, "feature_cols": feature_cols, "metrics": metrics}
