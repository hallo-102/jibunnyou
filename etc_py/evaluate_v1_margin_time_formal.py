# -*- coding: utf-8 -*-
"""
Compare three fixed models on the same chronological 70/30 split:
- V1_BASE
- V1_MARGIN_FORMAL
- V1_MARGIN_TIME_FORMAL

No alpha tuning. The margin+time candidate must beat or at least preserve the already-promising
margin model on the primary betting-oriented metrics. ROI is confirmation, not a tuning target.
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

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

    for c in ["rank", "odds", "pop", rating_name]:
        e[c] = pd.to_numeric(e[c], errors="coerce")
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


def gain_row(test: pd.DataFrame, left: str, right: str, label: str) -> Dict:
    a = test.loc[left]
    b = test.loc[right]
    return {
        "comparison": label,
        "top1_win_gain": float(b["top1_win_rate"] - a["top1_win_rate"]),
        "top1_place_gain": float(b["top1_place_rate"] - a["top1_place_rate"]),
        "top3_winner_gain": float(b["top3_contains_winner_rate"] - a["top3_contains_winner_rate"]),
        "top3_complete_gain": float(b["top3_complete_rate"] - a["top3_complete_rate"]),
        "spearman_gain": float(b["mean_spearman"] - a["mean_spearman"]),
        "roi_gain_pct_point": float(b["win_roi_pct"] - a["win_roi_pct"]),
    }


def main() -> None:
    p = argparse.ArgumentParser(description="v1 vs margin vs margin+time formal evaluator")
    p.add_argument("--baseline", type=Path, default=Path("data/master/race_levels_clean_v3.xlsx"))
    p.add_argument("--margin", type=Path, default=Path("data/master/race_levels_v1_margin.xlsx"))
    p.add_argument("--candidate", type=Path, default=Path("data/master/race_levels_v1_margin_time.xlsx"))
    p.add_argument("--out", type=Path, default=Path("data/master/v1_margin_time_formal_evaluation.xlsx"))
    args = p.parse_args()

    base = load_entries(args.baseline.resolve(), "rating_base")
    margin = load_entries(args.margin.resolve(), "rating_margin")
    cand = load_entries(args.candidate.resolve(), "rating_margin_time")

    merged = base.merge(margin[KEYS + ["rating_margin"]], on=KEYS, how="inner", validate="one_to_one")
    merged = merged.merge(cand[KEYS + ["rating_margin_time"]], on=KEYS, how="inner", validate="one_to_one")
    if merged.empty:
        raise ValueError("比較可能データが0件")

    race_dates = merged[["race_id", "date"]].drop_duplicates().copy()
    race_dates["date_num"] = pd.to_numeric(race_dates["date"].astype(str).str.replace(r"\.0$", "", regex=True), errors="coerce")
    race_dates = race_dates.sort_values(["date_num", "race_id"], kind="mergesort").reset_index(drop=True)
    cut = max(1, min(len(race_dates) - 1, int(len(race_dates) * TRAIN_RATIO)))
    train_ids = set(race_dates.iloc[:cut]["race_id"])
    test_ids = set(race_dates.iloc[cut:]["race_id"])
    cutoff_date = race_dates.iloc[cut]["date"]

    specs = [
        ("rating_base", "V1_BASE"),
        ("rating_margin", "V1_MARGIN_FORMAL"),
        ("rating_margin_time", "V1_MARGIN_TIME_FORMAL"),
    ]
    details = [race_metrics(merged, col, name) for col, name in specs]
    detail = pd.concat(details, ignore_index=True)
    detail["split"] = detail["race_id"].map(lambda x: "TRAIN" if x in train_ids else ("TEST" if x in test_ids else "OTHER"))

    rows = []
    for split in ["TRAIN", "TEST"]:
        d = detail[detail["split"] == split]
        for _, name in specs:
            rows.append(summarize(d, name, split))
    summary = pd.DataFrame(rows)

    test = summary[summary["split"] == "TEST"].set_index("model")
    gains = pd.DataFrame([
        gain_row(test, "V1_BASE", "V1_MARGIN_FORMAL", "MARGIN_MINUS_BASE"),
        gain_row(test, "V1_MARGIN_FORMAL", "V1_MARGIN_TIME_FORMAL", "MARGIN_TIME_MINUS_MARGIN"),
        gain_row(test, "V1_BASE", "V1_MARGIN_TIME_FORMAL", "MARGIN_TIME_MINUS_BASE"),
    ])

    g = gains[gains["comparison"] == "MARGIN_TIME_MINUS_MARGIN"].iloc[0]
    primary_improved = int(g["top1_win_gain"] > 0) + int(g["top3_winner_gain"] > 0)
    primary_nonworse = g["top1_win_gain"] >= -0.002 and g["top3_winner_gain"] >= -0.002
    roi_ok = g["roi_gain_pct_point"] >= -1.0
    # Since margin's known weakness is rank-order correlation, prefer time addition that does not worsen it further.
    spearman_ok = g["spearman_gain"] >= -0.002

    if primary_improved >= 1 and primary_nonworse and roi_ok and spearman_ok:
        decision = "ADOPT_TIME_ON_MARGIN_CANDIDATE"
    elif g["top1_win_gain"] < -0.005 or g["top3_winner_gain"] < -0.005 or g["roi_gain_pct_point"] < -3.0:
        decision = "REJECT_TIME_ON_MARGIN"
    else:
        decision = "HOLD_TIME_ON_MARGIN_MORE_DATA"

    decision_df = pd.DataFrame([{
        "cutoff_date": cutoff_date,
        "train_races": len(train_ids),
        "test_races": len(test_ids),
        "decision": decision,
        **{f"mt_vs_m_{k}": v for k, v in g.drop(labels=["comparison"]).to_dict().items()},
    }])

    out = args.out.resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        decision_df.to_excel(writer, sheet_name="decision", index=False)
        summary.to_excel(writer, sheet_name="summary", index=False)
        gains.to_excel(writer, sheet_name="gains", index=False)
        detail.to_excel(writer, sheet_name="race_detail", index=False)
        pd.DataFrame([
            {"item": "baseline", "value": str(args.baseline.resolve())},
            {"item": "margin", "value": str(args.margin.resolve())},
            {"item": "margin_time", "value": str(args.candidate.resolve())},
            {"item": "selection", "value": "all models fixed; no alpha tuning"},
            {"item": "priority", "value": "top1 win / top3 winner / ROI; Spearman must not materially worsen vs margin"},
        ]).to_excel(writer, sheet_name="README", index=False)

    print(f"[done] {out}")
    print(f"[result] cutoff={cutoff_date} decision={decision}")
    print("\n=== TEST ===")
    print(summary[summary["split"] == "TEST"].to_string(index=False))
    print("\n=== GAINS ===")
    print(gains.to_string(index=False))


if __name__ == "__main__":
    main()
