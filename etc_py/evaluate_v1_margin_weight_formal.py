# -*- coding: utf-8 -*-
"""
V1_MARGIN_FORMAL に今回斤量のレース内相対差だけを足す正式アブレーション。

score = pre_rating_margin + beta * (weight - race_mean_weight)

- weight はレース前に確定しているためリークなし。
- beta は古い70% TRAINのみで選択。
- ROIはbeta選択に使わず、TEST確認専用。
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

import pandas as pd

TRAIN_RATIO = 0.70
STAKE_YEN = 100
BETA_GRID = [-6.0, -4.0, -3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 4.0, 6.0]


def _norm_ids(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["race_id"] = out["race_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    out["horse_id"] = pd.to_numeric(out["horse_id"], errors="coerce").astype("Int64")
    return out


def load_data(path: Path) -> pd.DataFrame:
    xls = pd.ExcelFile(path, engine="openpyxl")
    if "entries" not in xls.sheet_names or "races" not in xls.sheet_names:
        xls.close()
        raise ValueError("entries/races シートがありません")
    e = _norm_ids(pd.read_excel(xls, sheet_name="entries"))
    r = pd.read_excel(xls, sheet_name="races")
    xls.close()

    r["race_id"] = r["race_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    need = {"race_id", "horse_id", "rank", "odds", "pre_rating", "weight"}
    miss = need - set(e.columns)
    if miss:
        raise ValueError(f"entries列不足: {sorted(miss)}")
    if e.duplicated(["race_id", "horse_id"]).any():
        raise ValueError("race_id+horse_id 重複あり")

    meta_cols = [c for c in ["race_id", "date", "start_time", "place", "class", "ground", "distance", "baba", "race_name"] if c in r.columns]
    e = e.merge(r[meta_cols].drop_duplicates("race_id"), on="race_id", how="left", validate="many_to_one")

    for c in ["rank", "odds", "pre_rating", "weight"]:
        e[c] = pd.to_numeric(e[c], errors="coerce")
    e = e[e["rank"].notna() & (e["rank"] > 0) & e["pre_rating"].notna()].copy()

    race_mean_w = e.groupby("race_id")["weight"].transform("mean")
    e["weight_diff"] = e["weight"] - race_mean_w
    e["weight_diff"] = e["weight_diff"].fillna(0.0)
    return e


def race_detail(df: pd.DataFrame, score_col: str, model: str) -> pd.DataFrame:
    rows: List[Dict] = []
    for rid, g0 in df.groupby("race_id", sort=False):
        g = g0.sort_values([score_col, "horse_id"], ascending=[False, True], kind="mergesort")
        if g.empty:
            continue
        top1 = g.iloc[0]
        top3 = g.head(3)
        winner_ids = set(g.loc[g["rank"] == 1, "horse_id"].dropna().astype(int))
        actual_top3 = set(g.loc[g["rank"] <= 3, "horse_id"].dropna().astype(int))
        pred_top3 = set(top3["horse_id"].dropna().astype(int))
        win = int(float(top1["rank"]) == 1.0)
        place = int(float(top1["rank"]) <= 3.0)
        odds = float(top1["odds"]) if pd.notna(top1["odds"]) else float("nan")
        ret = STAKE_YEN * odds if win and pd.notna(odds) and odds > 0 else 0.0
        spear = float("nan")
        if g[score_col].nunique() >= 2 and g["rank"].nunique() >= 2:
            spear = g[score_col].corr(-g["rank"], method="spearman")
        row = {
            "model": model,
            "race_id": str(rid),
            "top1_horse_id": int(top1["horse_id"]),
            "top1_win": win,
            "top1_place": place,
            "top3_contains_winner": int(bool(pred_top3 & winner_ids)),
            "top3_complete": int(bool(actual_top3) and actual_top3.issubset(pred_top3)),
            "spearman": spear,
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


def objective(row: Dict) -> float:
    # ROIは意図的に含めない。
    return (
        0.40 * row["top1_win_rate"]
        + 0.30 * row["top3_contains_winner_rate"]
        + 0.20 * row["top1_place_rate"]
        + 0.10 * row["mean_spearman"]
    )


def main() -> None:
    p = argparse.ArgumentParser(description="formal weight ablation on v1 margin")
    p.add_argument("--input", type=Path, default=Path("data/master/race_levels_v1_margin.xlsx"))
    p.add_argument("--out", type=Path, default=Path("data/master/v1_margin_weight_formal_evaluation.xlsx"))
    args = p.parse_args()

    df = load_data(args.input.resolve())
    race_dates = df[["race_id", "date"]].drop_duplicates().copy()
    race_dates["date_num"] = pd.to_numeric(race_dates["date"].astype(str).str.replace(r"\.0$", "", regex=True), errors="coerce")
    race_dates = race_dates.sort_values(["date_num", "race_id"], kind="mergesort").reset_index(drop=True)
    cut = max(1, min(len(race_dates)-1, int(len(race_dates) * TRAIN_RATIO)))
    train_ids = set(race_dates.iloc[:cut]["race_id"])
    test_ids = set(race_dates.iloc[cut:]["race_id"])
    cutoff_date = race_dates.iloc[cut]["date"]

    train = df[df["race_id"].isin(train_ids)].copy()
    test = df[df["race_id"].isin(test_ids)].copy()

    search_rows = []
    best_beta = 0.0
    best_obj = float("-inf")
    for beta in BETA_GRID:
        tmp = train.copy()
        tmp["score"] = tmp["pre_rating"] + beta * tmp["weight_diff"]
        det = race_detail(tmp, "score", f"beta={beta:g}")
        s = summarize(det, f"beta={beta:g}", "TRAIN")
        s["beta"] = beta
        s["objective"] = objective(s)
        search_rows.append(s)
        if s["objective"] > best_obj:
            best_obj = s["objective"]
            best_beta = beta

    base_test = test.copy()
    base_test["score"] = base_test["pre_rating"]
    cand_test = test.copy()
    cand_test["score"] = cand_test["pre_rating"] + best_beta * cand_test["weight_diff"]

    base_det = race_detail(base_test, "score", "V1_MARGIN")
    cand_det = race_detail(cand_test, "score", "V1_MARGIN_WEIGHT")
    base_sum = summarize(base_det, "V1_MARGIN", "TEST")
    cand_sum = summarize(cand_det, "V1_MARGIN_WEIGHT", "TEST")
    summary = pd.DataFrame([base_sum, cand_sum])

    gains = {
        "top1_win_gain": cand_sum["top1_win_rate"] - base_sum["top1_win_rate"],
        "top1_place_gain": cand_sum["top1_place_rate"] - base_sum["top1_place_rate"],
        "top3_winner_gain": cand_sum["top3_contains_winner_rate"] - base_sum["top3_contains_winner_rate"],
        "top3_complete_gain": cand_sum["top3_complete_rate"] - base_sum["top3_complete_rate"],
        "spearman_gain": cand_sum["mean_spearman"] - base_sum["mean_spearman"],
        "roi_gain_pct_point": cand_sum["win_roi_pct"] - base_sum["win_roi_pct"],
    }

    primary_improved = int(gains["top1_win_gain"] > 0) + int(gains["top3_winner_gain"] > 0)
    severe_harm = gains["top1_win_gain"] < -0.005 or gains["top3_winner_gain"] < -0.005
    if best_beta != 0 and primary_improved >= 1 and gains["roi_gain_pct_point"] >= -1.0 and gains["top1_place_gain"] >= -0.003:
        decision = "ADOPT_WEIGHT_ON_MARGIN_CANDIDATE"
    elif severe_harm or best_beta == 0:
        decision = "REJECT_WEIGHT_ON_MARGIN"
    else:
        decision = "HOLD_WEIGHT_ON_MARGIN_MORE_DATA"

    decision_df = pd.DataFrame([{
        "cutoff_date": cutoff_date,
        "train_races": len(train_ids),
        "test_races": len(test_ids),
        "best_beta_rating_per_kg": best_beta,
        "decision": decision,
        **gains,
    }])

    search_df = pd.DataFrame(search_rows).sort_values("objective", ascending=False)
    detail = pd.concat([base_det, cand_det], ignore_index=True)

    out = args.out.resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        decision_df.to_excel(w, sheet_name="decision", index=False)
        summary.to_excel(w, sheet_name="summary", index=False)
        search_df.to_excel(w, sheet_name="train_beta_search", index=False)
        detail.to_excel(w, sheet_name="race_detail", index=False)
        pd.DataFrame([
            {"item": "input", "value": str(args.input.resolve())},
            {"item": "formula", "value": "pre_rating_margin + beta * (weight - race_mean_weight)"},
            {"item": "beta_grid", "value": ",".join(str(x) for x in BETA_GRID)},
            {"item": "selection", "value": "TRAIN predictive metrics only; ROI excluded"},
        ]).to_excel(w, sheet_name="README", index=False)

    print(f"[done] {out}")
    print(f"[result] cutoff={cutoff_date} best_beta={best_beta:+.2f} decision={decision}")
    print("\n=== TEST ===")
    print(summary.to_string(index=False))
    print("\n=== GAINS ===")
    for k, v in gains.items():
        print(f"{k}={v:+.6f}")


if __name__ == "__main__":
    main()
