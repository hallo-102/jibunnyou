# -*- coding: utf-8 -*-
"""
Formal v1 margin candidate の walk-forward 安定性評価。

目的:
- 1回の70/30 holdoutだけでなく、時系列を複数の連続foldに分けて
  V1_BASE と V1_MARGIN_FORMAL を比較する。
- チューニングは行わない。既に固定済みの2モデルをそのまま比較する。
- 重点指標は top1勝率 / top3勝馬捕捉 / 単勝ROI。

使い方:
  python -u etc_py/evaluate_v1_margin_walkforward.py \
    --baseline data/master/race_levels_clean_v3.xlsx \
    --candidate data/master/race_levels_v1_margin.xlsx \
    --out data/master/v1_margin_walkforward.xlsx
"""
from __future__ import annotations

import argparse
from pathlib import Path
import math

import numpy as np
import pandas as pd

N_FOLDS = 5
MIN_RACES_PER_FOLD = 300


def _norm_ids(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["race_id"] = out["race_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    out["horse_id"] = pd.to_numeric(out["horse_id"], errors="coerce").astype("Int64")
    return out


def _load(path: Path, label: str) -> pd.DataFrame:
    xls = pd.ExcelFile(path, engine="openpyxl")
    need = {"entries", "races"}
    missing = need - set(xls.sheet_names)
    if missing:
        xls.close()
        raise ValueError(f"{label}: 必要シート不足 {sorted(missing)}")

    e = _norm_ids(pd.read_excel(xls, sheet_name="entries"))
    r = pd.read_excel(xls, sheet_name="races")
    xls.close()

    r["race_id"] = r["race_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    if "date" not in r.columns:
        raise ValueError(f"{label}: races.date がありません")

    cols = ["race_id", "date"]
    merged = e.merge(r[cols], on="race_id", how="left", validate="many_to_one")
    merged["date"] = pd.to_datetime(merged["date"].astype(str), format="%Y%m%d", errors="coerce")

    required = ["race_id", "horse_id", "rank", "pre_rating", "odds", "date"]
    missing_cols = [c for c in required if c not in merged.columns]
    if missing_cols:
        raise ValueError(f"{label}: 必要列不足 {missing_cols}")

    merged["rank"] = pd.to_numeric(merged["rank"], errors="coerce")
    merged["pre_rating"] = pd.to_numeric(merged["pre_rating"], errors="coerce")
    merged["odds"] = pd.to_numeric(merged["odds"], errors="coerce")
    return merged


def _spearman_one(g: pd.DataFrame) -> float:
    x = g["pre_rating"].rank(method="average", ascending=True)
    y = (-g["rank"]).rank(method="average", ascending=True)
    if x.nunique() < 2 or y.nunique() < 2:
        return np.nan
    return float(x.corr(y, method="pearson"))


def _metrics(df: pd.DataFrame) -> dict:
    rows = []
    spears = []
    for rid, g0 in df.groupby("race_id", sort=False):
        g = g0.dropna(subset=["pre_rating", "rank"]).copy()
        if g.empty:
            continue
        g = g.sort_values(["pre_rating", "horse_id"], ascending=[False, True])
        top1 = g.iloc[0]
        top3 = g.head(3)
        actual_top3 = set(g.loc[g["rank"].isin([1, 2, 3]), "horse_id"].tolist())
        top3_ids = set(top3["horse_id"].tolist())

        odds = float(top1["odds"]) if pd.notna(top1["odds"]) else np.nan
        stake = 100.0
        ret = odds * 100.0 if (pd.notna(odds) and int(top1["rank"]) == 1) else 0.0

        rows.append({
            "top1_win": int(top1["rank"] == 1),
            "top1_place": int(top1["rank"] <= 3),
            "top3_winner": int((top3["rank"] == 1).any()),
            "top3_complete": int(len(actual_top3) == 3 and actual_top3.issubset(top3_ids)),
            "stake": stake,
            "return": ret,
        })
        s = _spearman_one(g)
        if not math.isnan(s):
            spears.append(s)

    m = pd.DataFrame(rows)
    if m.empty:
        return {
            "races": 0,
            "top1_win_rate": np.nan,
            "top1_place_rate": np.nan,
            "top3_contains_winner_rate": np.nan,
            "top3_complete_rate": np.nan,
            "mean_spearman": np.nan,
            "win_roi_pct": np.nan,
            "win_profit_yen": np.nan,
        }

    stake = float(m["stake"].sum())
    ret = float(m["return"].sum())
    return {
        "races": int(len(m)),
        "top1_win_rate": float(m["top1_win"].mean()),
        "top1_place_rate": float(m["top1_place"].mean()),
        "top3_contains_winner_rate": float(m["top3_winner"].mean()),
        "top3_complete_rate": float(m["top3_complete"].mean()),
        "mean_spearman": float(np.mean(spears)) if spears else np.nan,
        "win_roi_pct": (ret / stake * 100.0) if stake else np.nan,
        "win_profit_yen": ret - stake,
    }


def _align(base: pd.DataFrame, cand: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    keys = ["race_id", "horse_id"]
    bkeys = set(map(tuple, base[keys].astype(str).to_numpy()))
    ckeys = set(map(tuple, cand[keys].astype(str).to_numpy()))
    common = bkeys & ckeys
    if not common:
        raise ValueError("baseline/candidate の共通出走がありません")

    common_df = pd.DataFrame(list(common), columns=keys)
    common_df["horse_id"] = pd.to_numeric(common_df["horse_id"], errors="coerce").astype("Int64")
    b = base.merge(common_df, on=keys, how="inner")
    c = cand.merge(common_df, on=keys, how="inner")
    return b, c


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--baseline", type=Path, required=True)
    p.add_argument("--candidate", type=Path, required=True)
    p.add_argument("--out", type=Path, default=Path("data/master/v1_margin_walkforward.xlsx"))
    args = p.parse_args()

    base = _load(args.baseline.resolve(), "baseline")
    cand = _load(args.candidate.resolve(), "candidate")
    base, cand = _align(base, cand)

    race_dates = (
        base[["race_id", "date"]]
        .drop_duplicates("race_id")
        .dropna(subset=["date"])
        .sort_values(["date", "race_id"])
        .reset_index(drop=True)
    )
    if race_dates.empty:
        raise ValueError("有効なrace dateがありません")

    # race単位で連続foldを作る。各foldは純粋な将来区間で、モデル自体の再学習はしない。
    fold_ids = np.array_split(race_dates["race_id"].to_numpy(), N_FOLDS)
    detail_rows = []

    for i, ids in enumerate(fold_ids, start=1):
        if len(ids) < MIN_RACES_PER_FOLD:
            continue
        ids_set = set(map(str, ids))
        b = base[base["race_id"].astype(str).isin(ids_set)].copy()
        c = cand[cand["race_id"].astype(str).isin(ids_set)].copy()
        mb = _metrics(b)
        mc = _metrics(c)

        date_min = race_dates.loc[race_dates["race_id"].isin(ids), "date"].min()
        date_max = race_dates.loc[race_dates["race_id"].isin(ids), "date"].max()

        row = {
            "fold": i,
            "date_start": date_min,
            "date_end": date_max,
            **{f"base_{k}": v for k, v in mb.items()},
            **{f"margin_{k}": v for k, v in mc.items()},
        }
        for k in [
            "top1_win_rate", "top1_place_rate", "top3_contains_winner_rate",
            "top3_complete_rate", "mean_spearman", "win_roi_pct", "win_profit_yen"
        ]:
            row[f"gain_{k}"] = mc[k] - mb[k]
        detail_rows.append(row)

    detail = pd.DataFrame(detail_rows)
    if detail.empty:
        raise ValueError("walk-forward foldを作れませんでした")

    # 安定性判定: 主要3指標のうち2つ以上が改善したfoldを「勝ち」とする。
    major_cols = [
        "gain_top1_win_rate",
        "gain_top3_contains_winner_rate",
        "gain_win_roi_pct",
    ]
    detail["major_improve_count"] = sum((detail[c] > 0).astype(int) for c in major_cols)
    detail["major_win_fold"] = detail["major_improve_count"] >= 2

    win_folds = int(detail["major_win_fold"].sum())
    total_folds = int(len(detail))
    avg_win_gain = float(detail["gain_top1_win_rate"].mean())
    avg_top3_gain = float(detail["gain_top3_contains_winner_rate"].mean())
    avg_roi_gain = float(detail["gain_win_roi_pct"].mean())
    avg_spear_gain = float(detail["gain_mean_spearman"].mean())

    if win_folds >= math.ceil(total_folds * 0.6) and avg_win_gain > 0 and avg_top3_gain > 0:
        decision = "ROBUST_MARGIN_CANDIDATE"
    elif win_folds <= math.floor(total_folds * 0.4) and (avg_win_gain <= 0 or avg_top3_gain <= 0):
        decision = "REJECT_MARGIN_ROBUSTNESS"
    else:
        decision = "HOLD_MARGIN_MORE_DATA"

    summary = pd.DataFrame([{
        "decision": decision,
        "folds": total_folds,
        "major_win_folds": win_folds,
        "major_win_rate": win_folds / total_folds,
        "avg_top1_win_gain": avg_win_gain,
        "avg_top3_winner_gain": avg_top3_gain,
        "avg_roi_gain_pct_point": avg_roi_gain,
        "avg_spearman_gain": avg_spear_gain,
    }])

    out = args.out.resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        summary.to_excel(w, sheet_name="SUMMARY", index=False)
        detail.to_excel(w, sheet_name="FOLDS", index=False)

    print(f"[done] {out}")
    print(summary.to_string(index=False))
    print("\n=== FOLDS ===")
    show_cols = [
        "fold", "date_start", "date_end", "base_races",
        "gain_top1_win_rate", "gain_top3_contains_winner_rate",
        "gain_win_roi_pct", "gain_mean_spearman", "major_win_fold"
    ]
    print(detail[show_cols].to_string(index=False))


if __name__ == "__main__":
    main()
