# -*- coding: utf-8 -*-
"""
entries_confirmed_v2 を使い、confirmed race level が pre_rating_v2 の予測力を
本当に改善するか時系列ホールドアウトで検証する。

TRAIN(古い70%)だけで alpha を選択し、TEST(新しい30%)で固定評価する。
alpha選択にROIは使わない。順位予測力のみで選び、ROIはTESTで確認する。
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

import pandas as pd

STAKE_YEN = 100
ALPHA_GRID = [0.0, 0.25, 0.50, 0.75, 1.0, 1.25, 1.50, 2.0, 3.0]
TRAIN_RATIO = 0.70


def to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def load_data(path: Path) -> pd.DataFrame:
    xls = pd.ExcelFile(path, engine="openpyxl")
    if "entries_confirmed_v2" not in xls.sheet_names:
        raise ValueError("entries_confirmed_v2 がありません。先に build_confirmed_race_level_v2.py を実行してください。")
    if "races" not in xls.sheet_names:
        raise ValueError("races シートがありません。")

    e = pd.read_excel(xls, sheet_name="entries_confirmed_v2")
    races = pd.read_excel(xls, sheet_name="races")
    xls.close()

    for df in (e, races):
        df["race_id"] = df["race_id"].astype(str).str.replace(r"\.0$", "", regex=True)

    keep = [c for c in ["race_id", "date", "class", "place", "distance", "ground"] if c in races.columns]
    e = e.merge(races[keep].drop_duplicates("race_id"), on="race_id", how="left")

    for c in ["rank", "odds", "pop", "pre_rating_v2", "prev_confirmation_adjustment_v2", "prev_confirmed_race_level_v2"]:
        if c in e.columns:
            e[c] = to_num(e[c])

    e["prev_confirmation_adjustment_v2"] = e["prev_confirmation_adjustment_v2"].fillna(0.0)
    e["date_ts"] = pd.to_datetime(e["date"].astype(str).str.replace(r"\.0$", "", regex=True), format="%Y%m%d", errors="coerce")
    e = e[e["rank"].notna() & e["pre_rating_v2"].notna() & e["date_ts"].notna()].copy()
    return e


def split_by_time(df: pd.DataFrame):
    dates = sorted(df["date_ts"].dropna().unique())
    if len(dates) < 2:
        raise ValueError("時系列分割に必要な開催日数が不足しています。")
    cut_idx = max(1, min(len(dates) - 1, int(len(dates) * TRAIN_RATIO)))
    cutoff = dates[cut_idx]
    train = df[df["date_ts"] < cutoff].copy()
    test = df[df["date_ts"] >= cutoff].copy()
    return train, test, pd.Timestamp(cutoff)


def score_races(df: pd.DataFrame, alpha: float) -> Dict[str, float]:
    work = df.copy()
    work["model_score"] = work["pre_rating_v2"] + alpha * work["prev_confirmation_adjustment_v2"]

    races = 0
    top1_win = 0
    top1_place = 0
    top3_winner = 0
    top3_complete = 0
    spearman_values: List[float] = []
    stake = 0.0
    payout = 0.0

    for _, g0 in work.groupby("race_id"):
        g = g0[g0["rank"].notna() & g0["model_score"].notna()].copy()
        if len(g) < 2:
            continue
        g = g.sort_values(["model_score"], ascending=False).reset_index(drop=True)
        races += 1

        first = g.iloc[0]
        if int(first["rank"]) == 1:
            top1_win += 1
            odds = first.get("odds")
            if pd.notna(odds):
                payout += STAKE_YEN * float(odds)
        if int(first["rank"]) <= 3:
            top1_place += 1
        stake += STAKE_YEN

        top3 = g.head(3)
        if (top3["rank"] == 1).any():
            top3_winner += 1
        actual_top3_ids = set(g[g["rank"] <= 3].index.tolist())
        pred_top3_ids = set(top3.index.tolist())
        if len(actual_top3_ids) >= 3 and actual_top3_ids.issubset(pred_top3_ids):
            top3_complete += 1

        pred_rank = g["model_score"].rank(ascending=False, method="average")
        actual_rank = g["rank"].rank(ascending=True, method="average")
        corr = pred_rank.corr(actual_rank, method="pearson")
        if pd.notna(corr):
            # pred_rank小=強い、actual_rank小=好走なので正相関が良い
            spearman_values.append(float(corr))

    if races == 0:
        return {"races": 0}

    mean_spearman = sum(spearman_values) / len(spearman_values) if spearman_values else 0.0
    win_rate = top1_win / races
    place_rate = top1_place / races
    top3_winner_rate = top3_winner / races
    top3_complete_rate = top3_complete / races
    roi = payout / stake * 100.0 if stake > 0 else 0.0

    # alpha選択は的中・順位性能のみ。ROIを最適化対象にしない。
    objective = (0.50 * mean_spearman) + (0.25 * win_rate) + (0.25 * top3_winner_rate)

    return {
        "races": races,
        "alpha": alpha,
        "top1_win_rate": win_rate,
        "top1_place_rate": place_rate,
        "top3_contains_winner_rate": top3_winner_rate,
        "top3_complete_rate": top3_complete_rate,
        "mean_spearman": mean_spearman,
        "win_roi_pct": roi,
        "objective": objective,
    }


def by_year(df: pd.DataFrame, alpha: float) -> pd.DataFrame:
    rows = []
    d = df.copy()
    d["year"] = d["date_ts"].dt.year
    for y, g in d.groupby("year"):
        r = score_races(g, alpha)
        r["year"] = int(y)
        rows.append(r)
    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(Path(__file__).resolve().parents[1] / "data" / "master" / "race_levels.xlsx"))
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    path = Path(args.input)
    out = Path(args.out) if args.out else path.with_name("confirmed_race_level_v2_evaluation.xlsx")

    df = load_data(path)
    train, test, cutoff = split_by_time(df)

    train_rows = [score_races(train, a) for a in ALPHA_GRID]
    train_eval = pd.DataFrame(train_rows)
    valid_train = train_eval[train_eval["races"] > 0].copy()
    if valid_train.empty:
        raise ValueError("TRAIN評価対象レースがありません。")

    best_row = valid_train.sort_values(["objective", "mean_spearman", "top1_win_rate"], ascending=False).iloc[0]
    best_alpha = float(best_row["alpha"])

    baseline_train = score_races(train, 0.0)
    best_train = score_races(train, best_alpha)
    baseline_test = score_races(test, 0.0)
    best_test = score_races(test, best_alpha)

    summary = pd.DataFrame([
        {"split": "TRAIN_BASE", **baseline_train},
        {"split": "TRAIN_CONFIRMED", **best_train},
        {"split": "TEST_BASE", **baseline_test},
        {"split": "TEST_CONFIRMED", **best_test},
    ])

    decision = "KEEP_V2_BASE"
    if best_alpha != 0.0:
        sp_gain = best_test.get("mean_spearman", 0.0) - baseline_test.get("mean_spearman", 0.0)
        win_gain = best_test.get("top1_win_rate", 0.0) - baseline_test.get("top1_win_rate", 0.0)
        top3_gain = best_test.get("top3_contains_winner_rate", 0.0) - baseline_test.get("top3_contains_winner_rate", 0.0)
        # TESTで順位相関が悪化せず、勝率/Top3のどちらかが改善した時だけ採用候補。
        if sp_gain >= -0.002 and (win_gain > 0.0 or top3_gain > 0.0):
            decision = "ADOPT_CONFIRMED_FEATURE_CANDIDATE"
        else:
            decision = "REJECT_CONFIRMED_FEATURE"

    decision_df = pd.DataFrame([{
        "cutoff_date": cutoff.strftime("%Y-%m-%d"),
        "train_ratio": TRAIN_RATIO,
        "best_alpha_from_train": best_alpha,
        "decision": decision,
        "test_spearman_gain": best_test.get("mean_spearman", 0.0) - baseline_test.get("mean_spearman", 0.0),
        "test_top1_win_gain": best_test.get("top1_win_rate", 0.0) - baseline_test.get("top1_win_rate", 0.0),
        "test_top3_winner_gain": best_test.get("top3_contains_winner_rate", 0.0) - baseline_test.get("top3_contains_winner_rate", 0.0),
        "test_roi_gain_pct_point": best_test.get("win_roi_pct", 0.0) - baseline_test.get("win_roi_pct", 0.0),
    }])

    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        decision_df.to_excel(writer, sheet_name="decision", index=False)
        summary.to_excel(writer, sheet_name="summary", index=False)
        train_eval.to_excel(writer, sheet_name="train_alpha_search", index=False)
        by_year(train, best_alpha).to_excel(writer, sheet_name="train_by_year", index=False)
        by_year(test, best_alpha).to_excel(writer, sheet_name="test_by_year", index=False)

    print(f"[done] {out}")
    print(f"[result] cutoff={cutoff.date()} best_alpha={best_alpha:.2f} decision={decision}")
    print("[TEST BASE]", baseline_test)
    print("[TEST CONF]", best_test)


if __name__ == "__main__":
    main()
