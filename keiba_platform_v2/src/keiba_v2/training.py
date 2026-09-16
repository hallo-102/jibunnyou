from __future__ import annotations

from pathlib import Path

import pandas as pd


def train_lightgbm(df: pd.DataFrame, feature_prefix: str, model_path: Path, seed: int = 42) -> dict:
    try:
        import lightgbm as lgb
        from sklearn.metrics import log_loss, roc_auc_score
    except ImportError as exc:
        raise RuntimeError("install optional ML dependencies: pip install -e .[ml]") from exc

    if "is_winner" not in df.columns:
        raise ValueError("training data requires is_winner column (0/1)")
    feature_cols = [c for c in df.columns if str(c).startswith(feature_prefix)]
    if not feature_cols:
        raise ValueError(f"no training features with prefix: {feature_prefix}")

    data = df.copy()
    data["is_winner"] = pd.to_numeric(data["is_winner"], errors="coerce")
    data = data[data["is_winner"].isin([0, 1])].copy()
    if data.empty or data["is_winner"].nunique() < 2:
        raise ValueError("training data must contain winners and non-winners")

    if "race_date" in data.columns:
        data = data.sort_values(["race_date", "race_id", "horse_no"])
    elif "race_id" in data.columns:
        data = data.sort_values(["race_id", "horse_no"])

    split = max(1, int(len(data) * 0.8))
    if split >= len(data):
        split = len(data) - 1
    train = data.iloc[:split]
    valid = data.iloc[split:]

    x_train = train[feature_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    y_train = train["is_winner"].astype(int)
    x_valid = valid[feature_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    y_valid = valid["is_winner"].astype(int)

    model = lgb.LGBMClassifier(
        n_estimators=500,
        learning_rate=0.03,
        num_leaves=31,
        subsample=0.9,
        colsample_bytree=0.9,
        random_state=seed,
    )
    model.fit(x_train, y_train, eval_set=[(x_valid, y_valid)], callbacks=[lgb.early_stopping(50, verbose=False)])
    prob = model.predict_proba(x_valid)[:, 1]

    model_path.parent.mkdir(parents=True, exist_ok=True)
    model.booster_.save_model(str(model_path))

    metrics = {
        "train_rows": int(len(train)),
        "valid_rows": int(len(valid)),
        "valid_logloss": float(log_loss(y_valid, prob, labels=[0, 1])),
    }
    if y_valid.nunique() == 2:
        metrics["valid_auc"] = float(roc_auc_score(y_valid, prob))
    return {"model_path": model_path, "feature_cols": feature_cols, "metrics": metrics}
