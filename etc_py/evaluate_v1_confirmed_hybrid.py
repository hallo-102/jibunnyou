# -*- coding: utf-8 -*-
"""
v1 の pre_rating に confirmed race level のリーク防止補正だけを加えた
ハイブリッドを時系列ホールドアウトで評価する。

狙い:
- v2丸ごとは v1 より上位馬選択が弱かった。
- confirmed_race_level は v2 に加えると小幅改善した。
- そこで v1 の良さを残したまま confirmed だけ移植する価値があるかを検証する。

方法:
1. race_levels*.xlsx の entries(v1) と entries_confirmed_v2 を race_id+horse_id で結合。
2. 古い70%のレース日を TRAIN、新しい30%を TEST に分離。
3. TRAIN のみで alpha を探索。
       hybrid_rating = pre_rating + alpha * prev_confirmation_adjustment_v2
4. alpha選択にROIは使わない。
5. TEST に alpha を固定し、v1_base と v1+confirmed を比較。
6. 参考として v2_base / v2+confirmed も同一TESTで比較する。

未来情報:
entries_confirmed_v2.prev_confirmation_adjustment_v2 は、その出走日より前に
判明していた後続成績だけで作られていることを前提とする。
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

STAKE_YEN = 100
TRAIN_RATIO = 0.70
ALPHA_GRID = [0.0, 0.25, 0.50, 0.75, 1.00, 1.50, 2.00, 2.50, 3.00, 4.00, 5.00]
KEYS = ["race_id", "horse_id"]


def to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def norm_ids(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["race_id"] = out["race_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    out["horse_id"] = to_num(out["horse_id"]).astype("Int64")
    return out


def load_data(path: Path) -> pd.DataFrame:
    xls = pd.ExcelFile(path, engine="openpyxl")
    required = {"entries", "entries_confirmed_v2", "races"}
    missing = required - set(xls.sheet_names)
    if missing:
        xls.close()
        raise ValueError(f"必要シート不足: {sorted(missing)}")

    v1 = norm_ids(pd.read_excel(xls, sheet_name="entries"))
    conf = norm_ids(pd.read_excel(xls, sheet_name="entries_confirmed_v2"))
    races = pd.read_excel(xls, sheet_name="races")
    races["race_id"] = races["race_id"].astype(str).str.replace(r"\.0$", "", regex=True)

    for name, df in [("entries", v1), ("entries_confirmed_v2", conf)]:
        if df.duplicated(KEYS).any():
            n = int(df.duplicated(KEYS, keep=False).sum())
            xls.close()
            raise ValueError(f"{name}: race_id+horse_id 重複あり ({n} rows)")

    v1_cols = ["race_id", "horse_id", "rank", "odds", "pop", "pre_rating"]
    conf_cols = [
        "race_id", "horse_id", "pre_rating_v2", "prev_confirmation_adjustment_v2",
        "prev_confirmation_confidence", "prev_followup_starts", "prev_followup_coverage",
    ]
    for c in v1_cols:
        if c not in v1.columns:
            xls.close()
            raise ValueError(f"entries に必須列がありません: {c}")
    for c in ["race_id", "horse_id", "pre_rating_v2", "prev_confirmation_adjustment_v2"]:
        if c not in conf.columns:
            xls.close()
            raise ValueError(f"entries_confirmed_v2 に必須列がありません: {c}")

    conf_cols = [c for c in conf_cols if c in conf.columns]
    df = v1[v1_cols].merge(conf[conf_cols], on=KEYS, how="inner", validate="one_to_one")

    race_cols = ["race_id", "date", "place", "class", "ground", "distance", "race_name"]
    race_cols = [c for c in race_cols if c in races.columns]
    meta = races[race_cols].drop_duplicates("race_id")
    df = df.merge(meta, on="race_id", how="left", validate="many_to_one")
    xls.close()

    for c in ["rank", "odds", "pop", "pre_rating", "pre_rating_v2", "prev_confirmation_adjustment_v2"]:
        df[c] = to_num(df[c])
    df["prev_confirmation_adjustment_v2"] = df["prev_confirmation_adjustment_v2"].fillna(0.0)
    df["date"] = df["date"].astype(str).str.replace(r"\.0$", "", regex=True)
    df["date_ts"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")

    df = df[
        df["rank"].notna()
        & (df["rank"] > 0)
        & df["pre_rating"].notna()
        & df["date_ts"].notna()
    ].copy()
    return df


def split_by_time(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Timestamp]:
    dates = sorted(df["date_ts"].dropna().unique())
    if len(dates) < 10:
        raise ValueError("日付数が少なすぎて70/30分割できません。")
    idx = max(1, min(len(dates) - 1, int(len(dates) * TRAIN_RATIO)))
    cutoff = pd.Timestamp(dates[idx])
    train = df[df["date_ts"] < cutoff].copy()
    test = df[df["date_ts"] >= cutoff].copy()
    return train, test, cutoff


def score_races(df: pd.DataFrame, rating_kind: str, alpha: float = 0.0) -> Dict[str, float]:
    rows: List[Dict] = []

    for race_id, g0 in df.groupby("race_id", sort=False):
        g = g0.copy()
        if rating_kind == "v1":
            g["score"] = g["pre_rating"]
        elif rating_kind == "v1_confirmed":
            g["score"] = g["pre_rating"] + alpha * g["prev_confirmation_adjustment_v2"]
        elif rating_kind == "v2":
            g["score"] = g["pre_rating_v2"]
        elif rating_kind == "v2_confirmed":
            g["score"] = g["pre_rating_v2"] + alpha * g["prev_confirmation_adjustment_v2"]
        else:
            raise ValueError(rating_kind)

        g = g[g["score"].notna()].copy()
        if g.empty:
            continue
        g = g.sort_values(["score", "horse_id"], ascending=[False, True], kind="mergesort")
        top1 = g.iloc[0]
        top3 = g.head(3)

        actual_top3 = set(g.loc[g["rank"] <= 3, "horse_id"].dropna().astype(int).tolist())
        pred_top3 = set(top3["horse_id"].dropna().astype(int).tolist())
        winners = set(g.loc[g["rank"] == 1, "horse_id"].dropna().astype(int).tolist())

        if g["score"].nunique(dropna=True) >= 2 and g["rank"].nunique(dropna=True) >= 2:
            spearman = g["score"].corr(-g["rank"], method="spearman")
        else:
            spearman = float("nan")

        top1_win = int(float(top1["rank"]) == 1.0)
        top1_place = int(float(top1["rank"]) <= 3.0)
        odds = float(top1["odds"]) if pd.notna(top1["odds"]) else float("nan")
        ret = STAKE_YEN * odds if top1_win and pd.notna(odds) and odds > 0 else 0.0

        rows.append({
            "race_id": str(race_id),
            "top1_win": top1_win,
            "top1_place": top1_place,
            "top3_contains_winner": int(bool(pred_top3 & winners)),
            "top3_complete": int(bool(actual_top3) and actual_top3.issubset(pred_top3)),
            "spearman": spearman,
            "return_yen": ret,
        })

    d = pd.DataFrame(rows)
    if d.empty:
        return {
            "races": 0, "top1_win_rate": float("nan"), "top1_place_rate": float("nan"),
            "top3_contains_winner_rate": float("nan"), "top3_complete_rate": float("nan"),
            "mean_spearman": float("nan"), "win_roi_pct": float("nan"), "objective": float("nan"),
        }

    n = len(d)
    win = float(d["top1_win"].mean())
    place = float(d["top1_place"].mean())
    top3 = float(d["top3_contains_winner"].mean())
    complete = float(d["top3_complete"].mean())
    sp = float(d["spearman"].mean())
    roi = float(d["return_yen"].sum() / (STAKE_YEN * n) * 100.0)
    objective = (0.50 * sp) + (0.25 * win) + (0.25 * top3)

    return {
        "races": int(n),
        "top1_win_rate": win,
        "top1_place_rate": place,
        "top3_contains_winner_rate": top3,
        "top3_complete_rate": complete,
        "mean_spearman": sp,
        "win_roi_pct": roi,
        "objective": objective,
    }


def by_year(df: pd.DataFrame, kind: str, alpha: float) -> pd.DataFrame:
    rows = []
    d = df.copy()
    d["year"] = d["date_ts"].dt.year
    for year, g in d.groupby("year"):
        r = score_races(g, kind, alpha)
        r["year"] = int(year)
        r["model"] = kind
        r["alpha"] = alpha
        rows.append(r)
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="v1 + confirmed hybrid 時系列評価")
    parser.add_argument("--input", type=Path, default=Path("data/master/race_levels_clean_v3.xlsx"))
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    path = args.input.resolve()
    if not path.exists():
        raise FileNotFoundError(path)
    out = args.out.resolve() if args.out else path.with_name("v1_confirmed_hybrid_evaluation.xlsx")

    df = load_data(path)
    train, test, cutoff = split_by_time(df)

    alpha_rows = []
    for alpha in ALPHA_GRID:
        r = score_races(train, "v1_confirmed", alpha)
        r["alpha"] = alpha
        alpha_rows.append(r)
    alpha_df = pd.DataFrame(alpha_rows)
    valid = alpha_df[alpha_df["races"] > 0].copy()
    if valid.empty:
        raise ValueError("TRAIN評価対象がありません。")

    best = valid.sort_values(
        ["objective", "mean_spearman", "top1_win_rate", "top3_contains_winner_rate"],
        ascending=False,
    ).iloc[0]
    best_alpha = float(best["alpha"])

    # TESTではalpha固定。ROIでalphaを選んでいないことが重要。
    models = [
        ("V1_BASE", "v1", 0.0),
        ("V1_CONFIRMED", "v1_confirmed", best_alpha),
        ("V2_BASE", "v2", 0.0),
        ("V2_CONFIRMED_SAME_ALPHA", "v2_confirmed", best_alpha),
    ]
    summary_rows = []
    for split_name, split_df in [("TRAIN", train), ("TEST", test)]:
        for model_name, kind, alpha in models:
            r = score_races(split_df, kind, alpha)
            summary_rows.append({"split": split_name, "model": model_name, "alpha": alpha, **r})
    summary = pd.DataFrame(summary_rows)

    test_s = summary[summary["split"] == "TEST"].set_index("model")
    base = test_s.loc["V1_BASE"]
    hyb = test_s.loc["V1_CONFIRMED"]
    gains = {
        "test_spearman_gain": float(hyb["mean_spearman"] - base["mean_spearman"]),
        "test_top1_win_gain": float(hyb["top1_win_rate"] - base["top1_win_rate"]),
        "test_top1_place_gain": float(hyb["top1_place_rate"] - base["top1_place_rate"]),
        "test_top3_winner_gain": float(hyb["top3_contains_winner_rate"] - base["top3_contains_winner_rate"]),
        "test_roi_gain_pct_point": float(hyb["win_roi_pct"] - base["win_roi_pct"]),
    }

    # 本番候補判定: 順位相関をほぼ悪化させず、勝率/Top3のどちらかが改善。
    decision = "KEEP_V1_BASE"
    if best_alpha != 0.0:
        if gains["test_spearman_gain"] >= -0.002 and (
            gains["test_top1_win_gain"] > 0.0 or gains["test_top3_winner_gain"] > 0.0
        ):
            decision = "ADOPT_V1_CONFIRMED_CANDIDATE"
        else:
            decision = "REJECT_V1_CONFIRMED"

    decision_df = pd.DataFrame([{
        "cutoff_date": cutoff.strftime("%Y-%m-%d"),
        "train_ratio": TRAIN_RATIO,
        "best_alpha_from_train": best_alpha,
        "decision": decision,
        **gains,
    }])

    yearly = pd.concat([
        by_year(test, "v1", 0.0),
        by_year(test, "v1_confirmed", best_alpha),
    ], ignore_index=True)

    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        decision_df.to_excel(writer, sheet_name="decision", index=False)
        summary.to_excel(writer, sheet_name="summary", index=False)
        alpha_df.to_excel(writer, sheet_name="train_alpha_search", index=False)
        yearly.to_excel(writer, sheet_name="test_by_year", index=False)

    print(f"[done] {out}")
    print(f"[result] cutoff={cutoff.date()} best_alpha={best_alpha:.2f} decision={decision}")
    print("\n=== TEST ===")
    print(summary[summary["split"] == "TEST"].to_string(index=False))
    print("\n=== GAINS: V1_CONFIRMED - V1_BASE ===")
    for k, v in gains.items():
        print(f"{k}={v:+.6f}")


if __name__ == "__main__":
    main()
