# -*- coding: utf-8 -*-
"""
V1_MARGIN_FORMAL に対して斤量情報を文脈別に分解して検証する。

特徴量:
- RAW_REL      : 今回斤量 - 同レース平均斤量
- WEIGHT_CHANGE: 今回斤量 - 前走斤量
- HANDICAP_REL : ハンデ戦のみ、今回斤量 - 同レース平均斤量
- AGESEX_REL   : 今回斤量 - 同レース同年齢・同性別グループ平均斤量
                 同グループが1頭のみの場合は0

重量条件:
- race_info から ハンデ / 別定 / 定量 / 馬齢 / OTHER を判定

方針:
- すべてレース前に判明している情報だけを使う。
- beta は古い70% TRAINだけで選択。
- ROIはbeta選択に使わず、TEST確認専用。
- 各特徴量を単独で評価し、重量条件別TESTも出力する。
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

TRAIN_RATIO = 0.70
STAKE_YEN = 100
BETA_GRID = [-8.0, -6.0, -4.0, -3.0, -2.0, -1.0, -0.5, 0.0, 0.5, 1.0, 2.0, 3.0, 4.0, 6.0, 8.0]
FEATURES = ["RAW_REL", "WEIGHT_CHANGE", "HANDICAP_REL", "AGESEX_REL"]


def _norm_rid(v) -> str:
    s = str(v).strip()
    return s[:-2] if s.endswith(".0") else s


def _norm_ids(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["race_id"] = out["race_id"].map(_norm_rid)
    out["horse_id"] = pd.to_numeric(out["horse_id"], errors="coerce").astype("Int64")
    return out


def _find_col(df: pd.DataFrame, aliases: List[str]) -> Optional[str]:
    for c in aliases:
        if c in df.columns:
            return c
    norm = {str(c).replace(" ", "").replace("　", ""): c for c in df.columns}
    for a in aliases:
        key = a.replace(" ", "").replace("　", "")
        if key in norm:
            return norm[key]
    return None


def _weight_type(text) -> str:
    if text is None or pd.isna(text):
        return "OTHER"
    s = str(text)
    if "ハンデ" in s:
        return "HANDICAP"
    if "別定" in s:
        return "BETTEI"
    if "定量" in s:
        return "TEIRYO"
    if "馬齢" in s:
        return "BAREI"
    return "OTHER"


def _parse_sex_age(v) -> Tuple[str, Optional[int], str]:
    if v is None or pd.isna(v):
        return "", None, "UNKNOWN"
    s = str(v).strip()
    sex = ""
    for token in ["牡", "牝", "セ", "騙"]:
        if token in s:
            sex = "セ" if token in ("セ", "騙") else token
            break
    m = re.search(r"(\d+)", s)
    age = int(m.group(1)) if m else None
    group = f"{sex}{age}" if sex and age is not None else "UNKNOWN"
    return sex, age, group


def load_margin_entries(path: Path) -> pd.DataFrame:
    xls = pd.ExcelFile(path, engine="openpyxl")
    need_sheets = {"entries", "races", "horses"}
    missing = need_sheets - set(xls.sheet_names)
    if missing:
        xls.close()
        raise ValueError(f"{path}: 必要シート不足 {sorted(missing)}")
    e = _norm_ids(pd.read_excel(xls, sheet_name="entries"))
    r = pd.read_excel(xls, sheet_name="races")
    h = pd.read_excel(xls, sheet_name="horses")
    xls.close()

    r["race_id"] = r["race_id"].map(_norm_rid)
    h["id"] = pd.to_numeric(h["id"], errors="coerce").astype("Int64")
    h = h.rename(columns={"id": "horse_id", "name": "horse_name"})

    req = {"race_id", "horse_id", "rank", "odds", "pre_rating", "weight"}
    miss = req - set(e.columns)
    if miss:
        raise ValueError(f"entries列不足: {sorted(miss)}")
    if e.duplicated(["race_id", "horse_id"]).any():
        raise ValueError("race_id+horse_id 重複あり")

    meta_cols = [c for c in ["race_id", "date", "start_time", "place", "class", "ground", "distance", "baba", "race_name"] if c in r.columns]
    e = e.merge(r[meta_cols].drop_duplicates("race_id"), on="race_id", how="left", validate="many_to_one")
    e = e.merge(h[["horse_id", "horse_name"]].drop_duplicates("horse_id"), on="horse_id", how="left", validate="many_to_one")

    for c in ["rank", "odds", "pre_rating", "weight"]:
        e[c] = pd.to_numeric(e[c], errors="coerce")
    e = e[e["rank"].notna() & (e["rank"] > 0) & e["pre_rating"].notna()].copy()
    e["date_num"] = pd.to_numeric(e["date"].astype(str).str.replace(r"\.0$", "", regex=True), errors="coerce")
    return e


def load_raw_context(raw_path: Path) -> pd.DataFrame:
    xls = pd.ExcelFile(raw_path, engine="openpyxl")
    rows: List[pd.DataFrame] = []
    for sheet in sorted([str(s) for s in xls.sheet_names if re.fullmatch(r"\d{8}", str(s))]):
        df = pd.read_excel(xls, sheet_name=sheet)
        rid_col = _find_col(df, ["レースID", "ﾚｰｽID", "レースId", "レースＩＤ", "race_id"])
        horse_col = _find_col(df, ["馬名", "horse_name"])
        sexage_col = _find_col(df, ["性齢", "sex_age"])
        info_col = _find_col(df, ["レース情報", "race_info"])
        if rid_col is None or horse_col is None:
            continue
        out = pd.DataFrame({
            "race_id": df[rid_col].map(_norm_rid),
            "horse_name": df[horse_col].astype(str).str.strip(),
            "sex_age": df[sexage_col] if sexage_col is not None else None,
            "race_info": df[info_col] if info_col is not None else None,
            "raw_sheet": sheet,
        })
        out = out[(out["race_id"] != "") & out["horse_name"].notna() & (out["horse_name"] != "") & (out["horse_name"].str.lower() != "nan")]
        rows.append(out)
    xls.close()
    if not rows:
        raise ValueError("raw workbookからrace_id/馬名を取得できませんでした")
    ctx = pd.concat(rows, ignore_index=True)
    # 同じrace_id+horse_nameが複数行あっても、sex_age/race_infoの最初の非空値を採る。
    def first_nonempty(s: pd.Series):
        for v in s:
            if v is not None and not pd.isna(v) and str(v).strip() and str(v).strip().lower() != "nan":
                return v
        return None
    ctx = (ctx.groupby(["race_id", "horse_name"], as_index=False)
              .agg({"sex_age": first_nonempty, "race_info": first_nonempty, "raw_sheet": "first"}))
    ctx["weight_type"] = ctx["race_info"].map(_weight_type)
    parsed = ctx["sex_age"].map(_parse_sex_age)
    ctx["sex"] = parsed.map(lambda x: x[0])
    ctx["age"] = parsed.map(lambda x: x[1])
    ctx["age_sex_group"] = parsed.map(lambda x: x[2])
    return ctx


def add_features(e: pd.DataFrame, ctx: pd.DataFrame) -> pd.DataFrame:
    df = e.merge(
        ctx[["race_id", "horse_name", "sex_age", "weight_type", "sex", "age", "age_sex_group"]],
        on=["race_id", "horse_name"], how="left", validate="one_to_one"
    )
    df["weight_type"] = df["weight_type"].fillna("OTHER")
    df["age_sex_group"] = df["age_sex_group"].fillna("UNKNOWN")

    race_mean = df.groupby("race_id")["weight"].transform("mean")
    df["RAW_REL"] = (df["weight"] - race_mean).fillna(0.0)
    df["HANDICAP_REL"] = df["RAW_REL"].where(df["weight_type"] == "HANDICAP", 0.0)

    group_count = df.groupby(["race_id", "age_sex_group"])["weight"].transform("count")
    group_mean = df.groupby(["race_id", "age_sex_group"])["weight"].transform("mean")
    df["AGESEX_REL"] = (df["weight"] - group_mean).where((group_count >= 2) & (df["age_sex_group"] != "UNKNOWN"), 0.0).fillna(0.0)

    # 前走斤量は同一馬の時系列だけで作る。今回行より未来は参照しない。
    order = df.sort_values(["horse_id", "date_num", "race_id"], kind="mergesort").index
    ordered = df.loc[order].copy()
    ordered["prev_weight"] = ordered.groupby("horse_id")["weight"].shift(1)
    ordered["WEIGHT_CHANGE"] = (ordered["weight"] - ordered["prev_weight"]).fillna(0.0)
    df.loc[ordered.index, "prev_weight"] = ordered["prev_weight"]
    df.loc[ordered.index, "WEIGHT_CHANGE"] = ordered["WEIGHT_CHANGE"]
    df["WEIGHT_CHANGE"] = df["WEIGHT_CHANGE"].fillna(0.0)
    return df


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
        rows.append({
            "model": model,
            "race_id": str(rid),
            "weight_type": g["weight_type"].iloc[0] if "weight_type" in g.columns else "OTHER",
            "top1_horse_id": int(top1["horse_id"]),
            "top1_win": win,
            "top1_place": place,
            "top3_contains_winner": int(bool(pred_top3 & winner_ids)),
            "top3_complete": int(bool(actual_top3) and actual_top3.issubset(pred_top3)),
            "spearman": spear,
            "stake_yen": STAKE_YEN,
            "return_yen": ret,
        })
    return pd.DataFrame(rows)


def summarize(detail: pd.DataFrame, model: str, split: str, weight_type: Optional[str] = None) -> Dict:
    g = detail[detail["model"] == model]
    if weight_type is not None:
        g = g[g["weight_type"] == weight_type]
    if g.empty:
        return {"split": split, "weight_type": weight_type or "ALL", "model": model, "races": 0}
    stake = float(g["stake_yen"].sum())
    ret = float(g["return_yen"].sum())
    return {
        "split": split,
        "weight_type": weight_type or "ALL",
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
    return (
        0.40 * row["top1_win_rate"]
        + 0.30 * row["top3_contains_winner_rate"]
        + 0.20 * row["top1_place_rate"]
        + 0.10 * row["mean_spearman"]
    )


def main() -> None:
    p = argparse.ArgumentParser(description="context-aware weight ablation on v1 margin")
    p.add_argument("--input", type=Path, default=Path("data/master/race_levels_v1_margin.xlsx"))
    p.add_argument("--raw", type=Path, default=Path("data/master/racedata_results_clean_v3.xlsx"))
    p.add_argument("--out", type=Path, default=Path("data/master/v1_margin_weight_context_evaluation.xlsx"))
    args = p.parse_args()

    e = load_margin_entries(args.input.resolve())
    ctx = load_raw_context(args.raw.resolve())
    df = add_features(e, ctx)

    race_dates = df[["race_id", "date_num"]].drop_duplicates().sort_values(["date_num", "race_id"], kind="mergesort").reset_index(drop=True)
    cut = max(1, min(len(race_dates)-1, int(len(race_dates) * TRAIN_RATIO)))
    train_ids = set(race_dates.iloc[:cut]["race_id"])
    test_ids = set(race_dates.iloc[cut:]["race_id"])
    cutoff_date = race_dates.iloc[cut]["date_num"]
    train = df[df["race_id"].isin(train_ids)].copy()
    test = df[df["race_id"].isin(test_ids)].copy()

    train_search_rows: List[Dict] = []
    decisions: List[Dict] = []
    summary_rows: List[Dict] = []
    all_details: List[pd.DataFrame] = []

    base_test = test.copy()
    base_test["score"] = base_test["pre_rating"]
    base_det = race_detail(base_test, "score", "V1_MARGIN")
    all_details.append(base_det)
    base_sum = summarize(base_det, "V1_MARGIN", "TEST")
    summary_rows.append(base_sum)

    for feature in FEATURES:
        best_beta = 0.0
        best_obj = float("-inf")
        for beta in BETA_GRID:
            tmp = train.copy()
            tmp["score"] = tmp["pre_rating"] + beta * tmp[feature]
            det = race_detail(tmp, "score", f"{feature}_beta={beta:g}")
            s = summarize(det, f"{feature}_beta={beta:g}", "TRAIN")
            s["feature"] = feature
            s["beta"] = beta
            s["objective"] = objective(s)
            train_search_rows.append(s)
            if s["objective"] > best_obj:
                best_obj = s["objective"]
                best_beta = beta

        cand = test.copy()
        cand["score"] = cand["pre_rating"] + best_beta * cand[feature]
        model = f"V1_MARGIN+{feature}"
        det = race_detail(cand, "score", model)
        all_details.append(det)
        csum = summarize(det, model, "TEST")
        summary_rows.append(csum)

        gains = {
            "top1_win_gain": csum["top1_win_rate"] - base_sum["top1_win_rate"],
            "top1_place_gain": csum["top1_place_rate"] - base_sum["top1_place_rate"],
            "top3_winner_gain": csum["top3_contains_winner_rate"] - base_sum["top3_contains_winner_rate"],
            "top3_complete_gain": csum["top3_complete_rate"] - base_sum["top3_complete_rate"],
            "spearman_gain": csum["mean_spearman"] - base_sum["mean_spearman"],
            "roi_gain_pct_point": csum["win_roi_pct"] - base_sum["win_roi_pct"],
        }
        primary = int(gains["top1_win_gain"] > 0) + int(gains["top3_winner_gain"] > 0)
        if best_beta == 0:
            decision = "REJECT_SIGNAL"
        elif primary >= 1 and gains["roi_gain_pct_point"] >= -1.0 and gains["top1_place_gain"] >= -0.003:
            decision = "KEEP_SIGNAL"
        elif gains["top1_win_gain"] < -0.005 or gains["top3_winner_gain"] < -0.005:
            decision = "REJECT_SIGNAL"
        else:
            decision = "HOLD_SIGNAL"
        decisions.append({
            "feature": feature,
            "best_beta_rating_per_kg": best_beta,
            "decision": decision,
            **gains,
        })

    detail = pd.concat(all_details, ignore_index=True)
    summary = pd.DataFrame(summary_rows)
    decision_df = pd.DataFrame(decisions)
    search_df = pd.DataFrame(train_search_rows).sort_values(["feature", "objective"], ascending=[True, False])

    by_type_rows: List[Dict] = []
    weight_types = ["HANDICAP", "BETTEI", "TEIRYO", "BAREI", "OTHER"]
    for wt in weight_types:
        by_type_rows.append(summarize(base_det, "V1_MARGIN", "TEST", wt))
        for feature in FEATURES:
            model = f"V1_MARGIN+{feature}"
            det = detail[detail["model"] == model]
            by_type_rows.append(summarize(det, model, "TEST", wt))
    by_type = pd.DataFrame(by_type_rows)

    coverage = pd.DataFrame([{
        "rows": len(df),
        "races": df["race_id"].nunique(),
        "sex_age_missing_rows": int(df["sex_age"].isna().sum()),
        "weight_type_counts": str(df[["race_id", "weight_type"]].drop_duplicates()["weight_type"].value_counts().to_dict()),
        "prev_weight_available_rows": int(df["prev_weight"].notna().sum()),
        "agesex_nonzero_rows": int((df["AGESEX_REL"].abs() > 1e-12).sum()),
    }])

    out = args.out.resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        decision_df.to_excel(w, sheet_name="decision", index=False)
        summary.to_excel(w, sheet_name="summary", index=False)
        by_type.to_excel(w, sheet_name="test_by_weight_type", index=False)
        search_df.to_excel(w, sheet_name="train_beta_search", index=False)
        detail.to_excel(w, sheet_name="race_detail", index=False)
        coverage.to_excel(w, sheet_name="coverage", index=False)
        pd.DataFrame([
            {"item": "input", "value": str(args.input.resolve())},
            {"item": "raw", "value": str(args.raw.resolve())},
            {"item": "features", "value": ", ".join(FEATURES)},
            {"item": "selection", "value": "TRAIN predictive metrics only; ROI excluded"},
            {"item": "note", "value": "斤量条件は race_info から ハンデ/別定/定量/馬齢/OTHER を判定"},
        ]).to_excel(w, sheet_name="README", index=False)

    print(f"[done] {out}")
    print(f"[result] cutoff={int(cutoff_date) if pd.notna(cutoff_date) else cutoff_date}")
    print("\n=== DECISIONS ===")
    print(decision_df.to_string(index=False))
    print("\n=== TEST SUMMARY ===")
    print(summary.to_string(index=False))
    print("\n=== COVERAGE ===")
    print(coverage.to_string(index=False))


if __name__ == "__main__":
    main()
