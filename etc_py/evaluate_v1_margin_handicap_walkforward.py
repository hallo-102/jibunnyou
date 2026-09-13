# -*- coding: utf-8 -*-
"""
V1_MARGIN + HANDICAP_REL の expanding walk-forward 検証。

目的:
- ハンデ戦だけで相対斤量を使う補正が、期間をずらしても安定して効くか確認する。
- 各foldで未来データを使わず、過去データだけでbetaを選択して次期間に固定する。
- beta選択にROIは使わない。

使い方:
  python -u etc_py/evaluate_v1_margin_handicap_walkforward.py \
      --input data/master/race_levels_v1_margin.xlsx \
      --raw data/master/racedata_results_clean_v3.xlsx
"""
from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd

INITIAL_TRAIN_RATIO = 0.30
N_FOLDS = 5
STAKE_YEN = 100
BETA_GRID = [-8.0, -6.0, -4.0, -3.0, -2.0, -1.0, -0.5, 0.0, 0.5, 1.0, 2.0, 3.0, 4.0, 6.0, 8.0]


def _load_context_module():
    path = Path(__file__).with_name("evaluate_v1_margin_weight_context.py")
    spec = importlib.util.spec_from_file_location("margin_weight_context_for_walkforward", path)
    if spec is None or spec.loader is None:
        raise ImportError(path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


ctxmod = _load_context_module()


def summarize(detail: pd.DataFrame, model: str, scope: str, fold: int) -> Dict:
    g = detail[detail["model"] == model].copy()
    if scope == "HANDICAP":
        g = g[g["weight_type"] == "HANDICAP"]
    if g.empty:
        return {
            "fold": fold,
            "scope": scope,
            "model": model,
            "races": 0,
            "top1_win_rate": float("nan"),
            "top1_place_rate": float("nan"),
            "top3_contains_winner_rate": float("nan"),
            "top3_complete_rate": float("nan"),
            "mean_spearman": float("nan"),
            "win_roi_pct": float("nan"),
            "win_profit_yen": float("nan"),
        }
    stake = float(g["stake_yen"].sum())
    ret = float(g["return_yen"].sum())
    return {
        "fold": fold,
        "scope": scope,
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
    # ROIは選択に使わない。
    return (
        0.40 * row["top1_win_rate"]
        + 0.30 * row["top3_contains_winner_rate"]
        + 0.20 * row["top1_place_rate"]
        + 0.10 * row["mean_spearman"]
    )


def choose_beta(train: pd.DataFrame) -> tuple[float, pd.DataFrame]:
    rows: List[Dict] = []
    best_beta = 0.0
    best_obj = float("-inf")

    for beta in BETA_GRID:
        tmp = train.copy()
        tmp["score"] = tmp["pre_rating"] + beta * tmp["HANDICAP_REL"]
        det = ctxmod.race_detail(tmp, "score", f"beta={beta:g}")
        s = summarize(det, f"beta={beta:g}", "ALL", 0)
        obj = objective(s)
        row = {"beta": beta, "objective": obj, **s}
        rows.append(row)
        if obj > best_obj:
            best_obj = obj
            best_beta = beta

    return best_beta, pd.DataFrame(rows).sort_values("objective", ascending=False)


def main() -> None:
    p = argparse.ArgumentParser(description="walk-forward v1 margin + handicap relative weight")
    p.add_argument("--input", type=Path, default=Path("data/master/race_levels_v1_margin.xlsx"))
    p.add_argument("--raw", type=Path, default=Path("data/master/racedata_results_clean_v3.xlsx"))
    p.add_argument("--out", type=Path, default=Path("data/master/v1_margin_handicap_walkforward.xlsx"))
    args = p.parse_args()

    e = ctxmod.load_margin_entries(args.input.resolve())
    raw = ctxmod.load_raw_context(args.raw.resolve())
    df = ctxmod.add_features(e, raw)

    race_dates = (
        df[["race_id", "date_num"]]
        .drop_duplicates()
        .sort_values(["date_num", "race_id"], kind="mergesort")
        .reset_index(drop=True)
    )
    n = len(race_dates)
    if n < 100:
        raise ValueError(f"レース数が少なすぎます: {n}")

    initial_end = max(1, int(n * INITIAL_TRAIN_RATIO))
    remain = n - initial_end
    fold_size = max(1, remain // N_FOLDS)

    summary_rows: List[Dict] = []
    gain_rows: List[Dict] = []
    beta_rows: List[Dict] = []
    search_rows: List[pd.DataFrame] = []
    detail_rows: List[pd.DataFrame] = []

    for fold in range(1, N_FOLDS + 1):
        train_end = initial_end + (fold - 1) * fold_size
        test_start = train_end
        test_end = n if fold == N_FOLDS else min(n, test_start + fold_size)
        if test_start >= n or test_end <= test_start:
            continue

        train_ids = set(race_dates.iloc[:train_end]["race_id"])
        test_ids = set(race_dates.iloc[test_start:test_end]["race_id"])
        train = df[df["race_id"].isin(train_ids)].copy()
        test = df[df["race_id"].isin(test_ids)].copy()

        beta, search_df = choose_beta(train)
        search_df.insert(0, "fold", fold)
        search_rows.append(search_df)

        base = test.copy()
        base["score"] = base["pre_rating"]
        cand = test.copy()
        cand["score"] = cand["pre_rating"] + beta * cand["HANDICAP_REL"]

        bdet = ctxmod.race_detail(base, "score", "V1_MARGIN")
        cdet = ctxmod.race_detail(cand, "score", "V1_MARGIN_HANDICAP")
        bdet["fold"] = fold
        cdet["fold"] = fold
        detail_rows.extend([bdet, cdet])

        train_start_date = race_dates.iloc[0]["date_num"]
        train_end_date = race_dates.iloc[train_end - 1]["date_num"]
        test_start_date = race_dates.iloc[test_start]["date_num"]
        test_end_date = race_dates.iloc[test_end - 1]["date_num"]
        beta_rows.append({
            "fold": fold,
            "train_races": len(train_ids),
            "test_races": len(test_ids),
            "train_start_date": train_start_date,
            "train_end_date": train_end_date,
            "test_start_date": test_start_date,
            "test_end_date": test_end_date,
            "best_beta": beta,
            "train_handicap_races": int(train.loc[train["weight_type"] == "HANDICAP", "race_id"].nunique()),
            "test_handicap_races": int(test.loc[test["weight_type"] == "HANDICAP", "race_id"].nunique()),
        })

        for scope in ["ALL", "HANDICAP"]:
            bs = summarize(bdet, "V1_MARGIN", scope, fold)
            cs = summarize(cdet, "V1_MARGIN_HANDICAP", scope, fold)
            summary_rows.extend([bs, cs])
            gain_rows.append({
                "fold": fold,
                "scope": scope,
                "best_beta": beta,
                "races": cs["races"],
                "top1_win_gain": cs["top1_win_rate"] - bs["top1_win_rate"],
                "top1_place_gain": cs["top1_place_rate"] - bs["top1_place_rate"],
                "top3_winner_gain": cs["top3_contains_winner_rate"] - bs["top3_contains_winner_rate"],
                "top3_complete_gain": cs["top3_complete_rate"] - bs["top3_complete_rate"],
                "spearman_gain": cs["mean_spearman"] - bs["mean_spearman"],
                "roi_gain_pct_point": cs["win_roi_pct"] - bs["win_roi_pct"],
            })

    summary_df = pd.DataFrame(summary_rows)
    gains_df = pd.DataFrame(gain_rows)
    beta_df = pd.DataFrame(beta_rows)
    searches_df = pd.concat(search_rows, ignore_index=True) if search_rows else pd.DataFrame()
    detail_df = pd.concat(detail_rows, ignore_index=True) if detail_rows else pd.DataFrame()

    hg = gains_df[gains_df["scope"] == "HANDICAP"].copy()
    valid = hg.dropna(subset=["top1_win_gain", "top3_winner_gain", "roi_gain_pct_point"])
    positive_primary = int(((valid["top1_win_gain"] > 0) | (valid["top3_winner_gain"] > 0)).sum())
    positive_roi = int((valid["roi_gain_pct_point"] > 0).sum())
    nonnegative_primary = int(((valid["top1_win_gain"] >= 0) & (valid["top3_winner_gain"] >= 0)).sum())
    nvalid = len(valid)

    mean_win_gain = float(valid["top1_win_gain"].mean()) if nvalid else float("nan")
    mean_top3_gain = float(valid["top3_winner_gain"].mean()) if nvalid else float("nan")
    mean_roi_gain = float(valid["roi_gain_pct_point"].mean()) if nvalid else float("nan")
    mean_place_gain = float(valid["top1_place_gain"].mean()) if nvalid else float("nan")

    if (
        nvalid >= 4
        and positive_primary >= 3
        and nonnegative_primary >= 3
        and mean_win_gain >= 0
        and mean_top3_gain >= -0.002
        and mean_roi_gain >= -1.0
    ):
        decision = "ROBUST_HANDICAP_WEIGHT_CANDIDATE"
    elif (
        nvalid >= 3
        and (mean_win_gain < -0.005 or mean_top3_gain < -0.005)
        and positive_primary <= 1
    ):
        decision = "REJECT_HANDICAP_WEIGHT_ROBUSTNESS"
    else:
        decision = "HOLD_HANDICAP_WEIGHT_MORE_DATA"

    decision_df = pd.DataFrame([{
        "decision": decision,
        "valid_folds": nvalid,
        "positive_primary_folds": positive_primary,
        "nonnegative_primary_folds": nonnegative_primary,
        "positive_roi_folds": positive_roi,
        "mean_handicap_top1_win_gain": mean_win_gain,
        "mean_handicap_top1_place_gain": mean_place_gain,
        "mean_handicap_top3_winner_gain": mean_top3_gain,
        "mean_handicap_roi_gain_pct_point": mean_roi_gain,
        "initial_train_ratio": INITIAL_TRAIN_RATIO,
        "folds": N_FOLDS,
    }])

    out = args.out.resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        decision_df.to_excel(w, sheet_name="decision", index=False)
        beta_df.to_excel(w, sheet_name="fold_beta", index=False)
        gains_df.to_excel(w, sheet_name="fold_gains", index=False)
        summary_df.to_excel(w, sheet_name="fold_summary", index=False)
        searches_df.to_excel(w, sheet_name="beta_search", index=False)
        detail_df.to_excel(w, sheet_name="race_detail", index=False)
        pd.DataFrame([
            {"item": "formula", "value": "pre_rating_margin + beta * HANDICAP_REL"},
            {"item": "HANDICAP_REL", "value": "HANDICAP race only: weight - race mean weight; otherwise 0"},
            {"item": "walk_forward", "value": "30% initial train + 5 expanding test folds"},
            {"item": "beta_selection", "value": "past data only; predictive metrics; ROI excluded"},
            {"item": "beta_grid", "value": ",".join(str(x) for x in BETA_GRID)},
        ]).to_excel(w, sheet_name="README", index=False)

    print(f"[done] {out}")
    print(f"[result] decision={decision}")
    print("\n=== FOLD BETAS ===")
    print(beta_df.to_string(index=False))
    print("\n=== HANDICAP GAINS ===")
    print(hg.to_string(index=False))
    print("\n=== ROBUSTNESS ===")
    print(decision_df.to_string(index=False))


if __name__ == "__main__":
    main()
