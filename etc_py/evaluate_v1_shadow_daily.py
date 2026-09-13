# -*- coding: utf-8 -*-
"""
旧V1 / V1_MARGIN / V1_MARGIN+HANDICAP の日次シャドー比較。

目的:
- 新規開催日が増えるたびに3モデルを同じレースで比較する。
- 履歴は追記せず、入力3ブックの共通データから毎回再構築する。
  これにより同日二重記録を防ぐ。
- 結果未確定レース（勝馬が存在しないレース）は評価対象外。
- ROIは従来評価と同じく、各レースで予測1位へ単勝100円を賭けた近似値。

使い方:
  python -u etc_py/evaluate_v1_shadow_daily.py \
      --baseline data/master/race_levels_clean_v3.xlsx \
      --margin data/master/race_levels_v1_margin.xlsx \
      --candidate data/master/race_levels_v1_margin_handicap_prod_candidate.xlsx

特定日だけ確認:
  ... --date 20260912
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

KEYS = ["race_id", "horse_id"]
STAKE_YEN = 100


def norm_rid(v) -> str:
    s = str(v).strip()
    return s[:-2] if s.endswith(".0") else s


def load_book(path: Path, model: str) -> pd.DataFrame:
    xls = pd.ExcelFile(path, engine="openpyxl")
    if "entries" not in xls.sheet_names or "races" not in xls.sheet_names:
        xls.close()
        raise ValueError(f"{path}: entries/races シートがありません")

    e = pd.read_excel(xls, sheet_name="entries")
    r = pd.read_excel(xls, sheet_name="races")
    xls.close()

    for req in ["race_id", "horse_id", "rank", "odds"]:
        if req not in e.columns:
            raise ValueError(f"{path}: entriesに {req} がありません")

    score_col = "pre_rating_prod_candidate" if model == "V1_MARGIN_HANDICAP" else "pre_rating"
    if score_col not in e.columns:
        raise ValueError(f"{path}: entriesに {score_col} がありません")

    e = e.copy()
    e["race_id"] = e["race_id"].map(norm_rid)
    e["horse_id"] = pd.to_numeric(e["horse_id"], errors="coerce").astype("Int64")
    e["rank"] = pd.to_numeric(e["rank"], errors="coerce")
    e["odds"] = pd.to_numeric(e["odds"], errors="coerce")
    e[score_col] = pd.to_numeric(e[score_col], errors="coerce")

    if e.duplicated(KEYS).any():
        dup = int(e.duplicated(KEYS).sum())
        raise ValueError(f"{path}: race_id+horse_id 重複 {dup}件")

    r = r.copy()
    r["race_id"] = r["race_id"].map(norm_rid)
    race_cols = [c for c in [
        "race_id", "date", "start_time", "place", "class", "ground",
        "distance", "baba", "race_name"
    ] if c in r.columns]
    meta = r[race_cols].drop_duplicates("race_id")
    e = e.merge(meta, on="race_id", how="left", validate="many_to_one")

    keep = KEYS + ["rank", "odds", score_col] + [c for c in race_cols if c != "race_id"]
    if model == "V1_MARGIN_HANDICAP":
        for c in ["prod_weight_type", "prod_handicap_rel"]:
            if c in e.columns:
                keep.append(c)

    out = e[keep].copy()
    out = out.rename(columns={score_col: f"score_{model}"})
    return out


def build_common(base: pd.DataFrame, margin: pd.DataFrame, cand: pd.DataFrame) -> pd.DataFrame:
    # 結果・メタ情報はbaseline側を正とし、他2モデルは評価scoreだけ結合する。
    m = margin[KEYS + ["score_V1_MARGIN"]].copy()
    ccols = KEYS + ["score_V1_MARGIN_HANDICAP"]
    for c in ["prod_weight_type", "prod_handicap_rel"]:
        if c in cand.columns:
            ccols.append(c)
    c = cand[ccols].copy()

    merged = base.merge(m, on=KEYS, how="inner", validate="one_to_one")
    merged = merged.merge(c, on=KEYS, how="inner", validate="one_to_one")
    if merged.empty:
        raise ValueError("3モデルで比較可能な共通データが0件です")

    # 日付は数値化して固定。欠損日付は評価不能。
    if "date" not in merged.columns:
        raise ValueError("racesに date 列がありません")
    merged["date_num"] = pd.to_numeric(
        merged["date"].astype(str).str.replace(r"\.0$", "", regex=True),
        errors="coerce",
    ).astype("Int64")
    merged = merged[merged["date_num"].notna()].copy()
    return merged


def complete_result_race_ids(df: pd.DataFrame) -> set[str]:
    # 少なくとも勝馬が1頭存在するレースだけ評価対象。
    winners = df[df["rank"] == 1].groupby("race_id")["horse_id"].nunique()
    valid = winners[winners == 1].index
    return set(str(x) for x in valid)


def race_detail(df: pd.DataFrame, score_col: str, model: str) -> pd.DataFrame:
    rows: List[Dict] = []
    for rid, g0 in df.groupby("race_id", sort=False):
        g = g0[g0[score_col].notna()].copy()
        if g.empty:
            continue

        winner_rows = g[g["rank"] == 1]
        if len(winner_rows) != 1:
            continue

        g = g.sort_values([score_col, "horse_id"], ascending=[False, True], kind="mergesort")
        top1 = g.iloc[0]
        top3 = g.head(3)
        winner_id = int(winner_rows.iloc[0]["horse_id"])
        actual_top3 = set(g.loc[g["rank"].between(1, 3, inclusive="both"), "horse_id"].dropna().astype(int))
        pred_top3 = set(top3["horse_id"].dropna().astype(int))

        win = int(float(top1["rank"]) == 1.0)
        place = int(float(top1["rank"]) <= 3.0)
        odds = float(top1["odds"]) if pd.notna(top1["odds"]) else float("nan")
        ret = STAKE_YEN * odds if win and pd.notna(odds) and odds > 0 else 0.0

        spear = float("nan")
        valid_rank = g["rank"].notna() & (g["rank"] > 0)
        gs = g.loc[valid_rank]
        if len(gs) >= 2 and gs[score_col].nunique() >= 2 and gs["rank"].nunique() >= 2:
            spear = gs[score_col].corr(-gs["rank"], method="spearman")

        row = {
            "model": model,
            "race_id": str(rid),
            "date": int(top1["date_num"]),
            "top1_horse_id": int(top1["horse_id"]),
            "winner_horse_id": winner_id,
            "top1_win": win,
            "top1_place": place,
            "top3_contains_winner": int(winner_id in pred_top3),
            "top3_complete": int(len(actual_top3) >= 3 and actual_top3.issubset(pred_top3)),
            "spearman": spear,
            "stake_yen": STAKE_YEN,
            "return_yen": ret,
            "top1_odds": odds,
        }
        for c in ["start_time", "place", "class", "ground", "distance", "baba", "race_name"]:
            if c in g.columns:
                row[c] = g[c].iloc[0]
        if "prod_weight_type" in g.columns:
            vals = g["prod_weight_type"].dropna().astype(str)
            row["weight_type"] = vals.iloc[0] if not vals.empty else "OTHER"
        else:
            row["weight_type"] = "OTHER"
        rows.append(row)
    return pd.DataFrame(rows)


def summarize(g: pd.DataFrame, scope: str) -> Dict:
    if scope == "HANDICAP":
        g = g[g["weight_type"] == "HANDICAP"]
    if g.empty:
        return {
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
        "races": int(len(g)),
        "top1_win_rate": float(g["top1_win"].mean()),
        "top1_place_rate": float(g["top1_place"].mean()),
        "top3_contains_winner_rate": float(g["top3_contains_winner"].mean()),
        "top3_complete_rate": float(g["top3_complete"].mean()),
        "mean_spearman": float(g["spearman"].mean()),
        "win_roi_pct": float(ret / stake * 100.0) if stake > 0 else float("nan"),
        "win_profit_yen": float(ret - stake),
    }


def build_summaries(detail: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    daily_rows: List[Dict] = []
    for date, dg in detail.groupby("date", sort=True):
        for model in ["V1_BASE", "V1_MARGIN", "V1_MARGIN_HANDICAP"]:
            mg = dg[dg["model"] == model]
            for scope in ["ALL", "HANDICAP"]:
                daily_rows.append({"date": int(date), "model": model, "scope": scope, **summarize(mg, scope)})

    cumulative_rows: List[Dict] = []
    for model in ["V1_BASE", "V1_MARGIN", "V1_MARGIN_HANDICAP"]:
        mg = detail[detail["model"] == model]
        for scope in ["ALL", "HANDICAP"]:
            cumulative_rows.append({"model": model, "scope": scope, **summarize(mg, scope)})

    return pd.DataFrame(daily_rows), pd.DataFrame(cumulative_rows)


def build_changed_top1(detail: pd.DataFrame) -> pd.DataFrame:
    base = detail[detail["model"] == "V1_BASE"][["race_id", "date", "top1_horse_id", "winner_horse_id", "weight_type"]].rename(
        columns={"top1_horse_id": "v1_top1"}
    )
    margin = detail[detail["model"] == "V1_MARGIN"][["race_id", "top1_horse_id"]].rename(
        columns={"top1_horse_id": "margin_top1"}
    )
    cand = detail[detail["model"] == "V1_MARGIN_HANDICAP"][["race_id", "top1_horse_id"]].rename(
        columns={"top1_horse_id": "candidate_top1"}
    )
    out = base.merge(margin, on="race_id", validate="one_to_one").merge(cand, on="race_id", validate="one_to_one")
    out["v1_to_margin_changed"] = out["v1_top1"] != out["margin_top1"]
    out["margin_to_candidate_changed"] = out["margin_top1"] != out["candidate_top1"]
    out["candidate_correct"] = out["candidate_top1"] == out["winner_horse_id"]
    out["margin_correct"] = out["margin_top1"] == out["winner_horse_id"]
    out["v1_correct"] = out["v1_top1"] == out["winner_horse_id"]
    return out[out["v1_to_margin_changed"] | out["margin_to_candidate_changed"]].sort_values(["date", "race_id"])


def main() -> None:
    p = argparse.ArgumentParser(description="daily shadow comparison: V1 vs MARGIN vs MARGIN+HANDICAP")
    p.add_argument("--baseline", type=Path, default=Path("data/master/race_levels_clean_v3.xlsx"))
    p.add_argument("--margin", type=Path, default=Path("data/master/race_levels_v1_margin.xlsx"))
    p.add_argument("--candidate", type=Path, default=Path("data/master/race_levels_v1_margin_handicap_prod_candidate.xlsx"))
    p.add_argument("--date", type=int, default=None, help="YYYYMMDD。省略時は共通全期間")
    p.add_argument("--out", type=Path, default=Path("data/master/v1_shadow_daily_comparison.xlsx"))
    args = p.parse_args()

    for path in [args.baseline, args.margin, args.candidate]:
        if not path.resolve().exists():
            raise FileNotFoundError(path.resolve())

    base = load_book(args.baseline.resolve(), "V1_BASE")
    margin = load_book(args.margin.resolve(), "V1_MARGIN")
    cand = load_book(args.candidate.resolve(), "V1_MARGIN_HANDICAP")
    df = build_common(base, margin, cand)

    valid_races = complete_result_race_ids(df)
    df = df[df["race_id"].isin(valid_races)].copy()
    if args.date is not None:
        df = df[df["date_num"] == int(args.date)].copy()
        if df.empty:
            raise ValueError(f"date={args.date}: 結果確定済みの共通レースがありません")

    details = []
    for model, score_col in [
        ("V1_BASE", "score_V1_BASE"),
        ("V1_MARGIN", "score_V1_MARGIN"),
        ("V1_MARGIN_HANDICAP", "score_V1_MARGIN_HANDICAP"),
    ]:
        details.append(race_detail(df, score_col, model))
    detail = pd.concat(details, ignore_index=True)
    if detail.empty:
        raise ValueError("評価可能レースが0件です")

    daily, cumulative = build_summaries(detail)
    changed = build_changed_top1(detail)

    common_races = int(detail[detail["model"] == "V1_BASE"]["race_id"].nunique())
    common_dates = sorted(detail["date"].dropna().astype(int).unique().tolist())
    quality = pd.DataFrame([
        {"item": "common_evaluated_races", "value": common_races},
        {"item": "first_date", "value": common_dates[0] if common_dates else ""},
        {"item": "last_date", "value": common_dates[-1] if common_dates else ""},
        {"item": "date_filter", "value": args.date if args.date is not None else "ALL"},
        {"item": "stake_rule", "value": "100 yen on each model top1 per evaluated race"},
        {"item": "return_rule", "value": "if top1 wins: odds * 100 yen; otherwise 0"},
        {"item": "result_filter", "value": "exactly one rank==1 in common race"},
    ])

    config = pd.DataFrame([
        {"model": "V1_BASE", "score": "baseline entries.pre_rating"},
        {"model": "V1_MARGIN", "score": "margin entries.pre_rating"},
        {"model": "V1_MARGIN_HANDICAP", "score": "candidate entries.pre_rating_prod_candidate"},
    ])

    out = args.out.resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        cumulative.to_excel(w, sheet_name="cumulative_summary", index=False)
        daily.to_excel(w, sheet_name="daily_summary", index=False)
        changed.to_excel(w, sheet_name="changed_top1", index=False)
        detail.to_excel(w, sheet_name="race_detail", index=False)
        quality.to_excel(w, sheet_name="data_quality", index=False)
        config.to_excel(w, sheet_name="model_config", index=False)

    print(f"[done] {out}")
    print(f"[evaluated] races={common_races} dates={len(common_dates)}")
    print("\n=== CUMULATIVE ALL ===")
    print(cumulative[cumulative["scope"] == "ALL"].to_string(index=False))
    print("\n=== CUMULATIVE HANDICAP ===")
    print(cumulative[cumulative["scope"] == "HANDICAP"].to_string(index=False))
    if common_dates:
        latest = common_dates[-1]
        print(f"\n=== LATEST DATE {latest} ===")
        print(daily[(daily["date"] == latest) & (daily["scope"] == "ALL")].to_string(index=False))


if __name__ == "__main__":
    main()
