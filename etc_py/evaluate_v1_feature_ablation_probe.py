# -*- coding: utf-8 -*-
"""
v1 pre_rating を固定し、候補特徴量を1つずつ加えた時に予測力が改善するかを
時系列ホールドアウトで高速スクリーニングする。

これは「最終rating実装」ではなく feature probe。
ここで効いた特徴量だけを次段階で v1 rating 更新ロジックへ正式移植する。

候補:
- DISTANCE_BAND: pre_distance_rating_v2 - pre_overall_rating_v2
- PREV_TIME: 前走の time_vs_master_per_1000m（小さいほど良い）
- PREV_MARGIN: 前走の gap_from_winner_per_1000m（小さいほど良い）
- WEIGHT: 今回斤量 - レース平均斤量

リーク防止:
- pre_* rating は各レース直前値のみ。
- PREV_TIME / PREV_MARGIN は同馬の過去走のみ。
- 標準化平均/標準偏差と alpha 選択は TRAIN(古い70%) のみ。
- TEST(新しい30%) は固定評価。
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

TRAIN_RATIO = 0.70
STAKE_YEN = 100
ALPHA_GRID = [-20.0, -15.0, -10.0, -7.5, -5.0, -2.5, 0.0, 2.5, 5.0, 7.5, 10.0, 15.0, 20.0]
FEATURES = ["DISTANCE_BAND", "PREV_TIME", "PREV_MARGIN", "WEIGHT"]


def num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def norm_id_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.replace(r"\.0$", "", regex=True)


def load_data(path: Path) -> pd.DataFrame:
    xls = pd.ExcelFile(path, engine="openpyxl")
    required = {"entries", "entries_v2", "ratings_history_v2", "races"}
    missing = required - set(xls.sheet_names)
    if missing:
        xls.close()
        raise ValueError(f"必要シート不足: {sorted(missing)}")

    v1 = pd.read_excel(xls, sheet_name="entries")
    v2 = pd.read_excel(xls, sheet_name="entries_v2")
    hist = pd.read_excel(xls, sheet_name="ratings_history_v2")
    races = pd.read_excel(xls, sheet_name="races")
    xls.close()

    for df in (v1, v2, hist, races):
        if "race_id" in df.columns:
            df["race_id"] = norm_id_series(df["race_id"])
    for df in (v1, v2, hist):
        if "horse_id" in df.columns:
            df["horse_id"] = num(df["horse_id"]).astype("Int64")

    req_v1 = {"race_id", "horse_id", "rank", "odds", "pop", "weight", "pre_rating"}
    req_v2 = {"race_id", "horse_id", "pre_overall_rating_v2", "pre_distance_rating_v2"}
    req_h = {"race_id", "horse_id", "date", "time_vs_master_per_1000m", "gap_from_winner_per_1000m"}
    for name, df, req in [("entries", v1, req_v1), ("entries_v2", v2, req_v2), ("ratings_history_v2", hist, req_h)]:
        miss = req - set(df.columns)
        if miss:
            raise ValueError(f"{name} 必須列不足: {sorted(miss)}")

    # 一意性はv3 clean済みが前提。
    for name, df in [("entries", v1), ("entries_v2", v2), ("ratings_history_v2", hist)]:
        if df.duplicated(["race_id", "horse_id"]).any():
            raise ValueError(f"{name}: race_id+horse_id 重複あり。v3 clean版を使用してください。")

    keep1 = ["race_id", "horse_id", "rank", "odds", "pop", "weight", "pre_rating"]
    keep2 = ["race_id", "horse_id", "pre_overall_rating_v2", "pre_distance_rating_v2"]
    d = v1[keep1].merge(v2[keep2], on=["race_id", "horse_id"], how="inner", validate="one_to_one")

    race_cols = [c for c in ["race_id", "date", "start_time", "place", "class", "ground", "distance", "race_name"] if c in races.columns]
    race_meta = races[race_cols].drop_duplicates("race_id")
    d = d.merge(race_meta, on="race_id", how="left", validate="many_to_one")

    d["date_ts"] = pd.to_datetime(d["date"].astype(str).str.replace(r"\.0$", "", regex=True), format="%Y%m%d", errors="coerce")
    d["rank"] = num(d["rank"])
    d["odds"] = num(d["odds"])
    d["pop"] = num(d["pop"])
    d["weight"] = num(d["weight"])
    d["pre_rating"] = num(d["pre_rating"])
    d["pre_overall_rating_v2"] = num(d["pre_overall_rating_v2"])
    d["pre_distance_rating_v2"] = num(d["pre_distance_rating_v2"])

    # 今回レース前に既に存在する距離帯特化シグナル。
    d["DISTANCE_BAND"] = d["pre_distance_rating_v2"] - d["pre_overall_rating_v2"]

    # 今回斤量の相対値。斤量そのものの水準ではなく同一レース内差を見る。
    d["race_mean_weight"] = d.groupby("race_id")["weight"].transform("mean")
    d["WEIGHT"] = d["weight"] - d["race_mean_weight"]

    # ratings_history_v2を馬ごとに時系列ソートし、過去走値を1行shift。
    h = hist[["race_id", "horse_id", "date", "time_vs_master_per_1000m", "gap_from_winner_per_1000m"]].copy()
    h["date_ts"] = pd.to_datetime(h["date"].astype(str).str.replace(r"\.0$", "", regex=True), format="%Y%m%d", errors="coerce")
    h["time_vs_master_per_1000m"] = num(h["time_vs_master_per_1000m"])
    h["gap_from_winner_per_1000m"] = num(h["gap_from_winner_per_1000m"])
    h = h.sort_values(["horse_id", "date_ts", "race_id"], kind="mergesort")
    h["PREV_TIME"] = h.groupby("horse_id")["time_vs_master_per_1000m"].shift(1)
    h["PREV_MARGIN"] = h.groupby("horse_id")["gap_from_winner_per_1000m"].shift(1)
    prev = h[["race_id", "horse_id", "PREV_TIME", "PREV_MARGIN"]]
    d = d.merge(prev, on=["race_id", "horse_id"], how="left", validate="one_to_one")

    # 小さい方が良い特徴は符号を反転し、「大きいほど良い」へ統一。
    d["PREV_TIME"] = -num(d["PREV_TIME"])
    d["PREV_MARGIN"] = -num(d["PREV_MARGIN"])

    d = d[d["rank"].notna() & (d["rank"] > 0) & d["pre_rating"].notna() & d["date_ts"].notna()].copy()
    return d


def split_by_time(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Timestamp]:
    race_dates = df[["race_id", "date_ts"]].drop_duplicates("race_id").sort_values(["date_ts", "race_id"])
    n = len(race_dates)
    if n < 20:
        raise ValueError("時系列分割に必要なレース数が不足しています。")
    cut_idx = max(1, min(n - 1, int(n * TRAIN_RATIO)))
    test_ids = set(race_dates.iloc[cut_idx:]["race_id"].astype(str))
    train_ids = set(race_dates.iloc[:cut_idx]["race_id"].astype(str))
    cutoff = race_dates.iloc[cut_idx]["date_ts"]
    return df[df["race_id"].isin(train_ids)].copy(), df[df["race_id"].isin(test_ids)].copy(), cutoff


def fit_standardizer(train: pd.DataFrame, feature: str) -> Tuple[float, float]:
    s = num(train[feature]).dropna()
    if s.empty:
        return 0.0, 1.0
    mean = float(s.mean())
    std = float(s.std(ddof=0))
    if not np.isfinite(std) or std <= 1e-12:
        std = 1.0
    return mean, std


def add_score(df: pd.DataFrame, feature: str, alpha: float, mean: float, std: float) -> pd.DataFrame:
    out = df.copy()
    x = (num(out[feature]) - mean) / std
    # 欠損は「情報なし」として補正0。
    x = x.fillna(0.0).clip(-3.0, 3.0)
    out["probe_z"] = x
    out["score"] = num(out["pre_rating"]) + float(alpha) * x
    return out


def evaluate(df: pd.DataFrame, feature: str, alpha: float, mean: float, std: float) -> Dict[str, float]:
    scored = add_score(df, feature, alpha, mean, std)
    rows: List[Dict] = []
    for rid, g0 in scored.groupby("race_id", sort=False):
        g = g0.sort_values(["score", "horse_id"], ascending=[False, True], kind="mergesort")
        if g.empty:
            continue
        top1 = g.iloc[0]
        top3 = g.head(3)
        winners = set(g.loc[g["rank"] == 1, "horse_id"].dropna().astype(int))
        pred3 = set(top3["horse_id"].dropna().astype(int))
        actual3 = set(g.loc[g["rank"] <= 3, "horse_id"].dropna().astype(int))
        spearman = g["score"].corr(-g["rank"], method="spearman") if g["score"].nunique() >= 2 and g["rank"].nunique() >= 2 else np.nan
        win = int(float(top1["rank"]) == 1.0)
        place = int(float(top1["rank"]) <= 3.0)
        odds = float(top1["odds"]) if pd.notna(top1["odds"]) else np.nan
        ret = STAKE_YEN * odds if win and np.isfinite(odds) and odds > 0 else 0.0
        rows.append({
            "top1_win": win,
            "top1_place": place,
            "top3_contains_winner": int(bool(pred3 & winners)),
            "top3_complete": int(bool(actual3) and actual3.issubset(pred3)),
            "spearman": spearman,
            "stake": STAKE_YEN,
            "return": ret,
        })
    r = pd.DataFrame(rows)
    if r.empty:
        return {"races": 0, "alpha": alpha}
    stake = float(r["stake"].sum())
    ret = float(r["return"].sum())
    return {
        "races": int(len(r)),
        "alpha": float(alpha),
        "top1_win_rate": float(r["top1_win"].mean()),
        "top1_place_rate": float(r["top1_place"].mean()),
        "top3_contains_winner_rate": float(r["top3_contains_winner"].mean()),
        "top3_complete_rate": float(r["top3_complete"].mean()),
        "mean_spearman": float(r["spearman"].mean()),
        "win_roi_pct": float(ret / stake * 100.0) if stake else np.nan,
        "win_profit_yen": float(ret - stake),
    }


def train_objective(row: pd.Series, baseline: pd.Series) -> float:
    """ROIへ過学習しないよう、alpha選択は予測性能だけで行う。"""
    win_gain = float(row["top1_win_rate"] - baseline["top1_win_rate"])
    top3_gain = float(row["top3_contains_winner_rate"] - baseline["top3_contains_winner_rate"])
    place_gain = float(row["top1_place_rate"] - baseline["top1_place_rate"])
    sp_gain = float(row["mean_spearman"] - baseline["mean_spearman"])
    return 0.40 * win_gain + 0.30 * top3_gain + 0.20 * place_gain + 0.10 * sp_gain


def decide(test_base: Dict[str, float], test_feat: Dict[str, float]) -> Tuple[str, Dict[str, float]]:
    gains = {
        "test_top1_win_gain": test_feat["top1_win_rate"] - test_base["top1_win_rate"],
        "test_top1_place_gain": test_feat["top1_place_rate"] - test_base["top1_place_rate"],
        "test_top3_winner_gain": test_feat["top3_contains_winner_rate"] - test_base["top3_contains_winner_rate"],
        "test_spearman_gain": test_feat["mean_spearman"] - test_base["mean_spearman"],
        "test_roi_gain_pct_point": test_feat["win_roi_pct"] - test_base["win_roi_pct"],
    }
    # 採用候補: 勝率またはTop3捕捉が改善し、もう一方が大幅悪化せず、Spearmanも大幅悪化しない。
    if (
        (gains["test_top1_win_gain"] > 0 or gains["test_top3_winner_gain"] > 0)
        and gains["test_top1_win_gain"] >= -0.002
        and gains["test_top3_winner_gain"] >= -0.002
        and gains["test_spearman_gain"] >= -0.003
    ):
        return "KEEP_FOR_FORMAL_ABLATION", gains
    return "REJECT_FEATURE_SIGNAL", gains


def main() -> None:
    p = argparse.ArgumentParser(description="v1 feature ablation probe")
    p.add_argument("--input", type=Path, default=Path("data/master/race_levels_clean_v3.xlsx"))
    p.add_argument("--out", type=Path, default=None)
    args = p.parse_args()

    path = args.input.resolve()
    if not path.exists():
        raise FileNotFoundError(path)
    out = args.out.resolve() if args.out else path.with_name("v1_feature_ablation_probe.xlsx")

    df = load_data(path)
    train, test, cutoff = split_by_time(df)

    all_train_search = []
    summary_rows = []
    decision_rows = []
    coverage_rows = []

    # ベースラインは特徴量に依存しない。
    base_mean, base_std = 0.0, 1.0
    # 任意特徴でalpha=0なら同じscore=v1。DISTANCE_BANDを仮置き。
    train_base = evaluate(train, "DISTANCE_BAND", 0.0, base_mean, base_std)
    test_base = evaluate(test, "DISTANCE_BAND", 0.0, base_mean, base_std)
    summary_rows.append({"feature": "V1_BASE", "split": "TRAIN", **train_base})
    summary_rows.append({"feature": "V1_BASE", "split": "TEST", **test_base})

    for feature in FEATURES:
        mean, std = fit_standardizer(train, feature)
        train_rows = []
        for alpha in ALPHA_GRID:
            r = evaluate(train, feature, alpha, mean, std)
            r["feature"] = feature
            train_rows.append(r)
        te = pd.DataFrame(train_rows)
        base_row = te.loc[te["alpha"] == 0.0].iloc[0]
        te["objective"] = te.apply(lambda row: train_objective(row, base_row), axis=1)
        te["train_mean"] = mean
        te["train_std"] = std
        all_train_search.append(te)

        # objective最大。同点時はalpha絶対値が小さい方を選ぶ。
        te["abs_alpha"] = te["alpha"].abs()
        best = te.sort_values(["objective", "top1_win_rate", "top3_contains_winner_rate", "abs_alpha"], ascending=[False, False, False, True]).iloc[0]
        alpha = float(best["alpha"])

        train_best = evaluate(train, feature, alpha, mean, std)
        test_best = evaluate(test, feature, alpha, mean, std)
        summary_rows.append({"feature": feature, "split": "TRAIN", **train_best})
        summary_rows.append({"feature": feature, "split": "TEST", **test_best})

        decision, gains = decide(test_base, test_best)
        decision_rows.append({
            "feature": feature,
            "best_alpha_from_train": alpha,
            "decision": decision,
            **gains,
        })

        coverage_rows.append({
            "feature": feature,
            "train_nonnull_rate": float(train[feature].notna().mean()),
            "test_nonnull_rate": float(test[feature].notna().mean()),
            "train_mean": mean,
            "train_std": std,
        })

    train_search = pd.concat(all_train_search, ignore_index=True)
    summary = pd.DataFrame(summary_rows)
    decisions = pd.DataFrame(decision_rows)
    coverage = pd.DataFrame(coverage_rows)

    # 参考ランキング: TESTで勝率・Top3捕捉・ROIのv1比改善を表示。
    ranking = decisions.copy()
    ranking["screen_score"] = (
        0.45 * ranking["test_top1_win_gain"]
        + 0.35 * ranking["test_top3_winner_gain"]
        + 0.10 * ranking["test_top1_place_gain"]
        + 0.10 * ranking["test_spearman_gain"]
    )
    ranking = ranking.sort_values("screen_score", ascending=False)

    readme = pd.DataFrame([
        {"item": "input", "value": str(path)},
        {"item": "cutoff", "value": cutoff.strftime("%Y-%m-%d")},
        {"item": "train_ratio", "value": TRAIN_RATIO},
        {"item": "purpose", "value": "feature signal screening; not final production rating"},
        {"item": "alpha selection", "value": "TRAIN prediction metrics only; ROI excluded from selection"},
        {"item": "PREV_TIME/PREV_MARGIN", "value": "previous start only; no current/future race result leakage"},
        {"item": "next step", "value": "formal rating-engine ablation only for KEEP_FOR_FORMAL_ABLATION features"},
    ])

    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        decisions.to_excel(writer, sheet_name="decision", index=False)
        ranking.to_excel(writer, sheet_name="ranking", index=False)
        summary.to_excel(writer, sheet_name="summary", index=False)
        train_search.to_excel(writer, sheet_name="train_alpha_search", index=False)
        coverage.to_excel(writer, sheet_name="coverage", index=False)
        readme.to_excel(writer, sheet_name="README", index=False)

    print(f"[done] {out}")
    print(f"[split] cutoff={cutoff.date()} train_races={train['race_id'].nunique()} test_races={test['race_id'].nunique()}")
    print("\n=== decision ===")
    print(decisions.to_string(index=False))
    print("\n=== ranking ===")
    print(ranking[["feature", "best_alpha_from_train", "decision", "screen_score", "test_top1_win_gain", "test_top3_winner_gain", "test_roi_gain_pct_point"]].to_string(index=False))


if __name__ == "__main__":
    main()
