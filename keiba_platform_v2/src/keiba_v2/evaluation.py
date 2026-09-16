from __future__ import annotations

import pandas as pd


def evaluate_predictions(predictions: pd.DataFrame, results: pd.DataFrame) -> dict:
    required = {"race_id", "horse_no", "finish_position"}
    missing = required - set(results.columns)
    if missing:
        raise ValueError(f"results missing columns: {sorted(missing)}")

    merged = predictions.merge(
        results[["race_id", "horse_no", "finish_position"]],
        on=["race_id", "horse_no"],
        how="inner",
        validate="one_to_one",
    )
    if merged.empty:
        raise ValueError("no matching prediction/result rows")

    merged["finish_position"] = pd.to_numeric(merged["finish_position"], errors="coerce")
    winners = merged[merged["finish_position"] == 1].copy()
    races = int(merged["race_id"].nunique())
    top1_hits = int((winners["pred_rank"] == 1).sum())
    top3_hits = int((winners["pred_rank"] <= 3).sum())
    top5_hits = int((winners["pred_rank"] <= 5).sum())

    return {
        "races": races,
        "top1_win_rate": top1_hits / races if races else 0.0,
        "top3_win_rate": top3_hits / races if races else 0.0,
        "top5_win_rate": top5_hits / races if races else 0.0,
    }


def evaluate_shadow_roi(shadow_log: pd.DataFrame, results: pd.DataFrame) -> dict:
    required_results = {"race_id", "horse_no", "finish_position", "win_payout_yen_per_100"}
    missing = required_results - set(results.columns)
    if missing:
        raise ValueError(f"results missing ROI columns: {sorted(missing)}")

    if shadow_log.empty:
        return {"bets": 0, "invest_yen": 0, "return_yen": 0, "profit_yen": 0, "roi": 0.0}

    bets = shadow_log.copy()
    bets["horse_no"] = pd.to_numeric(bets["horse_no"], errors="coerce").astype("Int64")
    merged = bets.merge(results, on=["race_id", "horse_no"], how="left")
    invest = int(pd.to_numeric(merged["stake_yen"], errors="coerce").fillna(0).sum())

    is_win = pd.to_numeric(merged["finish_position"], errors="coerce").eq(1)
    payout_per_100 = pd.to_numeric(merged["win_payout_yen_per_100"], errors="coerce").fillna(0)
    stake = pd.to_numeric(merged["stake_yen"], errors="coerce").fillna(0)
    returns = (payout_per_100 * (stake / 100.0)).where(is_win, 0.0)
    return_yen = int(round(float(returns.sum())))
    profit = return_yen - invest
    roi = (return_yen / invest * 100.0) if invest else 0.0
    return {
        "bets": int(len(merged)),
        "invest_yen": invest,
        "return_yen": return_yen,
        "profit_yen": profit,
        "roi": roi,
    }
