# -*- coding: utf-8 -*-
"""
現行v1と、着差学習を強化したformal candidateを時系列holdoutで比較する。

前提:
- baseline: race_levels_clean_v3.xlsx
- candidate: race_levels_v1_margin.xlsx
- 両方とも同じ racedata_results_clean_v3.xlsx 由来

評価:
- 古い70% = TRAIN表示用
- 新しい30% = TEST最終判定
- alpha調整なし。candidateは完全固定ルール。
- 1位勝率 / Top3勝馬捕捉 / 単勝ROIを優先する。
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

STAKE_YEN = 100
TRAIN_RATIO = 0.70
KEYS = ["race_id", "horse_id"]


def _norm(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["race_id"] = out["race_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    out["horse_id"] = pd.to_numeric(out["horse_id"], errors="coerce").astype("Int64")
    return out


def load_entries(path: Path, rating_name: str) -> pd.DataFrame:
    xls = pd.ExcelFile(path, engine="openpyxl")
    if "entries" not in xls.sheet_names or "races" not in xls.sheet_names:
        xls.close()
        raise ValueError(f"{path}: entries/races シートがありません")
    e = _norm(pd.read_excel(xls, sheet_name="entries"))
    r = pd.read_excel(xls, sheet_name="races")
    xls.close()

    r["race_id"] = r["race_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    req = {"race_id", "horse_id", "rank", "odds", "pop", "pre_rating"}
    miss = req - set(e.columns)
    if miss:
        raise ValueError(f"{path}: entries列不足 {sorted(miss)}")
    if e.duplicated(KEYS).any():
        raise ValueError(f"{path}: race_id+horse_id 重複あり")

    e = e.rename(columns={"pre_rating": rating_name})
    race_cols = [c for c in ["race_id", "date", "start_time", "place", "class", "ground", "distance", "baba", "race_name"] if c in r.columns]
    meta = r[race_cols].drop_duplicates("race_id")
    e = e.merge(meta, on="race_id", how="left", validate="many_to_one")

    e["rank"] = pd.to_numeric(e["rank"], errors="coerce")
    e["odds"] = pd.to_numeric(e["odds"], errors="coerce")
    e["pop"] = pd.to_numeric(e["pop"], errors="coerce")
    e[rating_name] = pd.to_numeric(e[rating_name], errors="coerce")
    e = e[e["rank"].notna() & (e["rank"] > 0) & e[rating_name].notna()].copy()
    return e


def race_metrics(entries: pd.DataFrame, rating_col: str, model: str) -> pd.DataFrame:
    rows: List[Dict] = []
    for rid, g0 in entries.groupby("race_id", sort=False):
        g = g0.sort_values([rating_col, "horse_id"], ascending=[False, True], kind="mergesort")
        if g.empty:
            continue
        top1 = g.iloc[0]
        top3 = g.head(3)
        actual_top3 = set(g.loc[g["rank"] <= 3, "horse_id"].dropna().astype(int))
        pred_top3 = set(top3["horse_id"].dropna().astype(int))
        winner = set(g.loc[g["rank"] == 1, "horse_id"].dropna().astype(int))
        spearman = float("nan")
        if g[rating_col].nunique() >= 2 and g["rank"].nunique() >= 2:
            spearman = g[rating_col].corr(-g["rank"], method="spearman")
        win = int(float(top1["rank"]) == 1.0)
        place = int(float(top1["rank"]) <= 3.0)
        odds = float(top1["odds"]) if pd.notna(top1["odds"]) else float("nan")
        ret = STAKE_YEN * odds if win and pd.notna(odds) and odds > 0 else 0.0
        row = {
            "model": model,
            "race_id": str(rid),
            "top1_horse_id": int(top1["horse_id"]),
            "top1_win": win,
            "top1_place": place,
            "top3_contains_winner": int(bool(pred_top3 & winner)),
            "top3_complete": int(bool(actual_top3) and actual_top3.issubset(pred_top3)),
            "spearman": spearman,
            "stake_yen": STAKE_YEN,
            "return_yen": ret,
        }
        for c in ["date", "start_time", "place", "class", "ground", "distance", "baba", "race_name"]:
            if c in g.columns:
                row[c] = g[c].iloc[0]
        rows.append(row)
    return pd.DataFrame(rows)


def summarize(detail: pd.DataFrame, model: str, split: str) -> Dict:
    g = detail[detail["model"] == model]
    if g.empty:
        return {"split": split, "model": model, "races": 0}
    stake = float(g["stake_yen"].sum())
    ret = float(g["return_yen"].sum())
    return {
        "split": split,
        "model": model,
        "races": int(len(g)),
        "top1_win_rate": float(g["top1_win"].mean()),
        "top1_place_rate": float(g["top1_place"].mean()),
        "top3_contains_winner_rate": float(g["top3_contains_winner"].mean()),
        "top3_complete_rate": float(g["top3_complete"].mean()),
        "mean_spearman": float(g["spearman"].mean()),
        "win_roi_pct": float(ret / stake * 100.0) if stake > 0 else float("nan"),
        "win_profit_yen": float(ret - stake),
    }


def main() -> None:
    p = argparse.ArgumentParser(description="v1 vs formal margin holdout evaluator")
    p.add_argument("--baseline", type=Path, default=Path("data/master/race_levels_clean_v3.xlsx"))
    p.add_argument("--candidate", type=Path, default=Path("data/master/race_levels_v1_margin.xlsx"))
    p.add_argument("--out", type=Path, default=Path("data/master/v1_margin_formal_evaluation.xlsx"))
    args = p.parse_args()

    base = load_entries(args.baseline.resolve(), "rating_base")
    cand = load_entries(args.candidate.resolve(), "rating_margin")

    keep_cand = KEYS + ["rating_margin"]
    merged = base.merge(cand[keep_cand], on=KEYS, how="inner", validate="one_to_one")
    if merged.empty:
        raise ValueError("比較可能データが0件")

    race_dates = merged[["race_id", "date"]].drop_duplicates().copy()
    race_dates["date_num"] = pd.to_numeric(race_dates["date"].astype(str).str.replace(r"\.0$", "", regex=True), errors="coerce")
    race_dates = race_dates.sort_values(["date_num", "race_id"], kind="mergesort").reset_index(drop=True)
    cut = max(1, min(len(race_dates) - 1, int(len(race_dates) * TRAIN_RATIO)))
    train_ids = set(race_dates.iloc[:cut]["race_id"])
    test_ids = set(race_dates.iloc[cut:]["race_id"])
    cutoff_date = race_dates.iloc[cut]["date"]

    base_detail = race_metrics(merged, "rating_base", "V1_BASE")
    margin_detail = race_metrics(merged, "rating_margin", "V1_MARGIN_FORMAL")
    detail = pd.concat([base_detail, margin_detail], ignore_index=True)
    detail["split"] = detail["race_id"].map(lambda x: "TRAIN" if x in train_ids else ("TEST" if x in test_ids else "OTHER"))

    rows = []
    for split in ["TRAIN", "TEST"]:
        d = detail[detail["split"] == split]
        for model in ["V1_BASE", "V1_MARGIN_FORMAL"]:
            rows.append(summarize(d, model, split))
    summary = pd.DataFrame(rows)

    test = summary[summary["split"] == "TEST"].set_index("model")
    b = test.loc["V1_BASE"]
    m = test.loc["V1_MARGIN_FORMAL"]
    gains = {
        "top1_win_gain": float(m["top1_win_rate"] - b["top1_win_rate"]),
        "top1_place_gain": float(m["top1_place_rate"] - b["top1_place_rate"]),
        "top3_winner_gain": float(m["top3_contains_winner_rate"] - b["top3_contains_winner_rate"]),
        "spearman_gain": float(m["mean_spearman"] - b["mean_spearman"]),
        "roi_gain_pct_point": float(m["win_roi_pct"] - b["win_roi_pct"]),
    }

    predictive_primary_improved = sum([
        gains["top1_win_gain"] > 0,
        gains["top3_winner_gain"] > 0,
    ])
    nonworse = sum([
        gains["top1_win_gain"] >= -0.002,
        gains["top3_winner_gain"] >= -0.002,
        gains["top1_place_gain"] >= -0.002,
        gains["spearman_gain"] >= -0.002,
    ])

    if predictive_primary_improved >= 1 and nonworse >= 3 and gains["roi_gain_pct_point"] >= -1.0:
        decision = "ADOPT_MARGIN_CANDIDATE"
    elif gains["top1_win_gain"] < -0.005 or gains["top3_winner_gain"] < -0.005:
        decision = "REJECT_MARGIN_CANDIDATE"
    else:
        decision = "HOLD_MARGIN_MORE_DATA"

    decision_df = pd.DataFrame([{
        "cutoff_date": cutoff_date,
        "train_races": len(train_ids),
        "test_races": len(test_ids),
        "decision": decision,
        **gains,
    }])

    # 1位評価が変わったレースだけの直接対決
    bdet = base_detail[["race_id", "top1_horse_id", "top1_win", "top1_place", "return_yen"]].rename(columns={
        "top1_horse_id": "base_top1_horse_id", "top1_win": "base_win", "top1_place": "base_place", "return_yen": "base_return_yen"
    })
    mdet = margin_detail[["race_id", "top1_horse_id", "top1_win", "top1_place", "return_yen"]].rename(columns={
        "top1_horse_id": "margin_top1_horse_id", "top1_win": "margin_win", "top1_place": "margin_place", "return_yen": "margin_return_yen"
    })
    h2h = bdet.merge(mdet, on="race_id", validate="one_to_one")
    h2h["different_top1"] = h2h["base_top1_horse_id"] != h2h["margin_top1_horse_id"]
    h2h["split"] = h2h["race_id"].map(lambda x: "TRAIN" if x in train_ids else "TEST")
    h2h = h2h[h2h["different_top1"]].copy()

    params = pd.DataFrame([
        {"item": "baseline", "value": str(args.baseline.resolve())},
        {"item": "candidate", "value": str(args.candidate.resolve())},
        {"item": "train_ratio", "value": TRAIN_RATIO},
        {"item": "selection", "value": "candidate固定。alpha tuningなし"},
        {"item": "priority", "value": "top1 win / top3 winner / ROI > place > Spearman"},
    ])

    out = args.out.resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        decision_df.to_excel(writer, sheet_name="decision", index=False)
        summary.to_excel(writer, sheet_name="summary", index=False)
        h2h.to_excel(writer, sheet_name="head_to_head", index=False)
        detail.to_excel(writer, sheet_name="race_detail", index=False)
        params.to_excel(writer, sheet_name="README", index=False)

    print(f"[done] {out}")
    print(f"[result] cutoff={cutoff_date} decision={decision}")
    print("\n=== TEST ===")
    print(summary[summary["split"] == "TEST"].to_string(index=False))
    print("\n=== GAINS ===")
    for k, v in gains.items():
        print(f"{k}={v:+.6f}")


if __name__ == "__main__":
    main()
