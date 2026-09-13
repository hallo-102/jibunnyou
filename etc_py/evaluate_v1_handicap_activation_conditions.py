# -*- coding: utf-8 -*-
"""
V1_MARGIN + HANDICAP_REL の「発動条件」を時系列 holdout で検証する。

目的:
- ハンデ戦で MARGIN と MARGIN+HANDICAP の1位馬が変わるレースだけに注目し、
  どんな時に補正による1位変更を受け入れるべきかをTRAINだけで選ぶ。
- 最終30% TESTでは、選ばれたルールを固定して評価する。
- ROIはルール選択に使わず、TEST確認専用。

入力:
- race_levels_v1_margin_handicap_prod_candidate.xlsx

比較:
1. V1_MARGIN                : 補正を使わない
2. ALWAYS_HANDICAP          : ハンデ戦では常に補正を使う
3. GATED_HANDICAP           : TRAINで選んだ条件の時だけ補正による1位変更を採用

候補条件:
- new_rel >= threshold
- rel_advantage >= threshold
- prod_advantage >= threshold
- new_pop <= threshold
- new_odds <= threshold
- new_rel + prod_advantage の2条件

重要:
- 変更しないレースでは3方式の1位馬は同じ。
- ルール選択では勝率/3着内率を使い、ROIは使わない。
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

STAKE_YEN = 100
TRAIN_RATIO = 0.70
MIN_TRAIN_ACTIVATIONS = 20


def norm_rid(v) -> str:
    s = str(v).strip()
    return s[:-2] if s.endswith(".0") else s


def load_data(path: Path) -> pd.DataFrame:
    xls = pd.ExcelFile(path, engine="openpyxl")
    if "entries" not in xls.sheet_names or "races" not in xls.sheet_names:
        xls.close()
        raise ValueError(f"{path}: entries/races シートがありません")
    e = pd.read_excel(xls, sheet_name="entries")
    r = pd.read_excel(xls, sheet_name="races")
    xls.close()

    required = {
        "race_id", "horse_id", "rank", "odds", "pop", "pre_rating",
        "pre_rating_prod_candidate", "prod_handicap_rel", "prod_weight_type",
    }
    missing = required - set(e.columns)
    if missing:
        raise ValueError(f"{path}: entries列不足 {sorted(missing)}")

    e = e.copy()
    e["race_id"] = e["race_id"].map(norm_rid)
    e["horse_id"] = pd.to_numeric(e["horse_id"], errors="coerce").astype("Int64")
    for c in ["rank", "odds", "pop", "pre_rating", "pre_rating_prod_candidate", "prod_handicap_rel"]:
        e[c] = pd.to_numeric(e[c], errors="coerce")

    if e.duplicated(["race_id", "horse_id"]).any():
        raise ValueError("race_id+horse_id 重複あり")

    r = r.copy()
    r["race_id"] = r["race_id"].map(norm_rid)
    race_cols = [c for c in [
        "race_id", "date", "start_time", "place", "class", "ground",
        "distance", "baba", "race_name",
    ] if c in r.columns]
    e = e.merge(
        r[race_cols].drop_duplicates("race_id"),
        on="race_id",
        how="left",
        validate="many_to_one",
    )

    if "date" not in e.columns:
        raise ValueError("racesに date 列がありません")
    e["date_num"] = pd.to_numeric(
        e["date"].astype(str).str.replace(r"\.0$", "", regex=True),
        errors="coerce",
    ).astype("Int64")

    e = e[
        e["date_num"].notna()
        & e["rank"].notna()
        & (e["rank"] > 0)
        & e["pre_rating"].notna()
        & e["pre_rating_prod_candidate"].notna()
    ].copy()

    return e


def build_race_rows(e: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict] = []

    for rid, g0 in e.groupby("race_id", sort=False):
        g = g0.copy()
        winners = g[g["rank"] == 1]
        if len(winners) != 1:
            continue

        margin_ranked = g.sort_values(
            ["pre_rating", "horse_id"],
            ascending=[False, True],
            kind="mergesort",
        )
        prod_ranked = g.sort_values(
            ["pre_rating_prod_candidate", "horse_id"],
            ascending=[False, True],
            kind="mergesort",
        )
        if margin_ranked.empty or prod_ranked.empty:
            continue

        old = margin_ranked.iloc[0]
        new = prod_ranked.iloc[0]
        winner_id = int(winners.iloc[0]["horse_id"])
        changed = int(old["horse_id"]) != int(new["horse_id"])

        row = {
            "race_id": str(rid),
            "date": int(g["date_num"].iloc[0]),
            "weight_type": str(g["prod_weight_type"].iloc[0]),
            "winner_horse_id": winner_id,
            "margin_top1": int(old["horse_id"]),
            "candidate_top1": int(new["horse_id"]),
            "changed": bool(changed),
            "margin_win": int(int(old["horse_id"]) == winner_id),
            "candidate_win": int(int(new["horse_id"]) == winner_id),
            "margin_place": int(float(old["rank"]) <= 3.0),
            "candidate_place": int(float(new["rank"]) <= 3.0),
            "margin_return_yen": (
                STAKE_YEN * float(old["odds"])
                if int(old["horse_id"]) == winner_id and pd.notna(old["odds"]) and float(old["odds"]) > 0
                else 0.0
            ),
            "candidate_return_yen": (
                STAKE_YEN * float(new["odds"])
                if int(new["horse_id"]) == winner_id and pd.notna(new["odds"]) and float(new["odds"]) > 0
                else 0.0
            ),
            "old_rel": float(old["prod_handicap_rel"]) if pd.notna(old["prod_handicap_rel"]) else 0.0,
            "new_rel": float(new["prod_handicap_rel"]) if pd.notna(new["prod_handicap_rel"]) else 0.0,
            "old_pre": float(old["pre_rating"]),
            "new_pre": float(new["pre_rating"]),
            "old_prod": float(old["pre_rating_prod_candidate"]),
            "new_prod": float(new["pre_rating_prod_candidate"]),
            "old_pop": float(old["pop"]) if pd.notna(old["pop"]) else float("nan"),
            "new_pop": float(new["pop"]) if pd.notna(new["pop"]) else float("nan"),
            "old_odds": float(old["odds"]) if pd.notna(old["odds"]) else float("nan"),
            "new_odds": float(new["odds"]) if pd.notna(new["odds"]) else float("nan"),
        }
        row["rel_advantage"] = row["new_rel"] - row["old_rel"]
        row["pre_rating_gap_new_minus_old"] = row["new_pre"] - row["old_pre"]
        row["prod_advantage"] = row["new_prod"] - row["old_prod"]

        for c in ["place", "class", "ground", "distance", "baba", "race_name"]:
            if c in g.columns:
                row[c] = g[c].iloc[0]
        rows.append(row)

    return pd.DataFrame(rows)


def make_rules() -> List[Tuple[str, Dict]]:
    rules: List[Tuple[str, Dict]] = [("NEVER", {"kind": "never"}), ("ALWAYS", {"kind": "always"})]

    for t in [0.0, 0.5, 1.0, 1.5, 2.0, 2.5]:
        rules.append((f"NEW_REL>={t:g}", {"kind": "new_rel", "t": t}))
    for t in [0.5, 1.0, 1.5, 2.0, 2.5, 3.0]:
        rules.append((f"REL_ADV>={t:g}", {"kind": "rel_adv", "t": t}))
    for t in [0.0, 0.5, 1.0, 2.0, 3.0, 5.0]:
        rules.append((f"PROD_ADV>={t:g}", {"kind": "prod_adv", "t": t}))
    for t in [3, 5, 8, 10]:
        rules.append((f"NEW_POP<={t}", {"kind": "new_pop", "t": float(t)}))
    for t in [5.0, 10.0, 20.0, 40.0]:
        rules.append((f"NEW_ODDS<={t:g}", {"kind": "new_odds", "t": t}))

    for rel_t in [0.5, 1.0, 1.5, 2.0]:
        for prod_t in [0.5, 1.0, 2.0, 3.0]:
            rules.append((
                f"NEW_REL>={rel_t:g}&PROD_ADV>={prod_t:g}",
                {"kind": "rel_prod", "rel_t": rel_t, "prod_t": prod_t},
            ))
    return rules


def activation_mask(df: pd.DataFrame, spec: Dict) -> pd.Series:
    changed = df["changed"].fillna(False)

    kind = spec["kind"]
    if kind == "never":
        cond = pd.Series(False, index=df.index)
    elif kind == "always":
        cond = pd.Series(True, index=df.index)
    elif kind == "new_rel":
        cond = df["new_rel"] >= float(spec["t"])
    elif kind == "rel_adv":
        cond = df["rel_advantage"] >= float(spec["t"])
    elif kind == "prod_adv":
        cond = df["prod_advantage"] >= float(spec["t"])
    elif kind == "new_pop":
        cond = df["new_pop"].notna() & (df["new_pop"] <= float(spec["t"]))
    elif kind == "new_odds":
        cond = df["new_odds"].notna() & (df["new_odds"] <= float(spec["t"]))
    elif kind == "rel_prod":
        cond = (
            (df["new_rel"] >= float(spec["rel_t"]))
            & (df["prod_advantage"] >= float(spec["prod_t"]))
        )
    else:
        raise ValueError(f"unknown rule kind: {kind}")

    return changed & cond.fillna(False)


def evaluate_rule(df: pd.DataFrame, name: str, spec: Dict, split: str) -> Dict:
    mask = activation_mask(df, spec)

    win = df["margin_win"].copy()
    place = df["margin_place"].copy()
    ret = df["margin_return_yen"].copy()

    win.loc[mask] = df.loc[mask, "candidate_win"]
    place.loc[mask] = df.loc[mask, "candidate_place"]
    ret.loc[mask] = df.loc[mask, "candidate_return_yen"]

    stake = float(len(df) * STAKE_YEN)
    total_ret = float(ret.sum())

    changed = df["changed"].fillna(False)
    changed_count = int(changed.sum())
    activation_count = int(mask.sum())

    return {
        "split": split,
        "rule": name,
        "races": int(len(df)),
        "changed_races": changed_count,
        "activations": activation_count,
        "activation_rate_of_changed": (
            activation_count / changed_count if changed_count > 0 else float("nan")
        ),
        "top1_win_rate": float(win.mean()) if len(df) else float("nan"),
        "top1_place_rate": float(place.mean()) if len(df) else float("nan"),
        "win_roi_pct": (total_ret / stake * 100.0) if stake > 0 else float("nan"),
        "win_profit_yen": total_ret - stake,
    }


def objective(row: Dict) -> float:
    return 0.65 * row["top1_win_rate"] + 0.35 * row["top1_place_rate"]


def bin_summary(changed_train: pd.DataFrame, changed_test: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict] = []

    configs = [
        ("new_rel", [-999, 0, 0.5, 1.0, 1.5, 2.0, 999]),
        ("rel_advantage", [-999, 0.5, 1.0, 1.5, 2.0, 2.5, 999]),
        ("prod_advantage", [-999, 0.5, 1.0, 2.0, 3.0, 5.0, 999]),
        ("new_pop", [0, 3, 5, 8, 10, 99]),
        ("new_odds", [0, 5, 10, 20, 40, 9999]),
    ]

    for feature, bins in configs:
        for split_name, src in [("TRAIN", changed_train), ("TEST", changed_test)]:
            if src.empty:
                continue
            work = src.copy()
            work["_bin"] = pd.cut(work[feature], bins=bins, include_lowest=True, duplicates="drop")
            for b, g in work.groupby("_bin", observed=True):
                if g.empty:
                    continue
                candidate_better = (
                    (g["candidate_win"] > g["margin_win"])
                    | ((g["candidate_win"] == g["margin_win"]) & (g["candidate_place"] > g["margin_place"]))
                )
                margin_better = (
                    (g["margin_win"] > g["candidate_win"])
                    | ((g["margin_win"] == g["candidate_win"]) & (g["margin_place"] > g["candidate_place"]))
                )
                rows.append({
                    "split": split_name,
                    "feature": feature,
                    "bin": str(b),
                    "races": int(len(g)),
                    "candidate_win_rate": float(g["candidate_win"].mean()),
                    "margin_win_rate": float(g["margin_win"].mean()),
                    "candidate_place_rate": float(g["candidate_place"].mean()),
                    "margin_place_rate": float(g["margin_place"].mean()),
                    "candidate_better_races": int(candidate_better.sum()),
                    "margin_better_races": int(margin_better.sum()),
                })
    return pd.DataFrame(rows)


def main() -> None:
    p = argparse.ArgumentParser(description="formal activation-gate test for handicap weight correction")
    p.add_argument(
        "--input",
        type=Path,
        default=Path("data/master/race_levels_v1_margin_handicap_prod_candidate.xlsx"),
    )
    p.add_argument(
        "--out",
        type=Path,
        default=Path("data/master/v1_handicap_activation_conditions.xlsx"),
    )
    args = p.parse_args()

    src = args.input.resolve()
    if not src.exists():
        raise FileNotFoundError(src)

    e = load_data(src)
    race_df = build_race_rows(e)
    race_df = race_df[race_df["weight_type"] == "HANDICAP"].copy()
    if race_df.empty:
        raise ValueError("HANDICAPレースが0件です")

    race_df = race_df.sort_values(["date", "race_id"], kind="mergesort").reset_index(drop=True)
    cut = max(1, min(len(race_df) - 1, int(len(race_df) * TRAIN_RATIO)))
    train = race_df.iloc[:cut].copy()
    test = race_df.iloc[cut:].copy()
    cutoff_date = int(test.iloc[0]["date"])

    search_rows: List[Dict] = []
    rule_specs = make_rules()

    for name, spec in rule_specs:
        row = evaluate_rule(train, name, spec, "TRAIN")
        row["objective"] = objective(row)
        row["eligible"] = (
            name in ("NEVER", "ALWAYS")
            or row["activations"] >= MIN_TRAIN_ACTIVATIONS
        )
        search_rows.append(row)

    search_df = pd.DataFrame(search_rows)
    eligible = search_df[search_df["eligible"]].copy()
    if eligible.empty:
        raise ValueError("TRAINで有効なルール候補がありません")

    eligible = eligible.sort_values(
        ["objective", "activations", "rule"],
        ascending=[False, False, True],
        kind="mergesort",
    )
    best_name = str(eligible.iloc[0]["rule"])
    spec_map = {name: spec for name, spec in rule_specs}
    best_spec = spec_map[best_name]

    test_rows = [
        evaluate_rule(test, "MARGIN_ONLY", {"kind": "never"}, "TEST"),
        evaluate_rule(test, "ALWAYS_HANDICAP", {"kind": "always"}, "TEST"),
        evaluate_rule(test, f"GATED:{best_name}", best_spec, "TEST"),
    ]
    test_summary = pd.DataFrame(test_rows)

    base = test_summary[test_summary["rule"] == "MARGIN_ONLY"].iloc[0]
    always = test_summary[test_summary["rule"] == "ALWAYS_HANDICAP"].iloc[0]
    gated = test_summary[test_summary["rule"].str.startswith("GATED:")].iloc[0]

    gains = {
        "gated_vs_margin_win_gain": float(gated["top1_win_rate"] - base["top1_win_rate"]),
        "gated_vs_margin_place_gain": float(gated["top1_place_rate"] - base["top1_place_rate"]),
        "gated_vs_margin_roi_gain_pct_point": float(gated["win_roi_pct"] - base["win_roi_pct"]),
        "gated_vs_always_win_gain": float(gated["top1_win_rate"] - always["top1_win_rate"]),
        "gated_vs_always_place_gain": float(gated["top1_place_rate"] - always["top1_place_rate"]),
        "gated_vs_always_roi_gain_pct_point": float(gated["win_roi_pct"] - always["win_roi_pct"]),
    }

    if (
        gains["gated_vs_margin_win_gain"] > 0
        and gains["gated_vs_margin_place_gain"] >= 0
        and gains["gated_vs_always_win_gain"] >= 0
        and gains["gated_vs_always_place_gain"] >= -0.005
    ):
        decision = "ADOPT_GATED_HANDICAP_RULE"
    elif (
        gains["gated_vs_margin_win_gain"] < -0.005
        or gains["gated_vs_margin_place_gain"] < -0.01
    ):
        decision = "REJECT_GATED_RULE_KEEP_EXISTING"
    else:
        decision = "HOLD_GATED_RULE_MORE_DATA"

    decision_df = pd.DataFrame([{
        "decision": decision,
        "cutoff_date": cutoff_date,
        "train_races": len(train),
        "test_races": len(test),
        "train_changed_races": int(train["changed"].sum()),
        "test_changed_races": int(test["changed"].sum()),
        "best_train_rule": best_name,
        "best_train_objective": float(eligible.iloc[0]["objective"]),
        **gains,
    }])

    changed_train = train[train["changed"]].copy()
    changed_test = test[test["changed"]].copy()
    bins = bin_summary(changed_train, changed_test)

    changed_detail = pd.concat([
        changed_train.assign(split="TRAIN"),
        changed_test.assign(split="TEST"),
    ], ignore_index=True)

    out = args.out.resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        decision_df.to_excel(w, sheet_name="decision", index=False)
        test_summary.to_excel(w, sheet_name="test_summary", index=False)
        search_df.sort_values(
            ["eligible", "objective", "activations"],
            ascending=[False, False, False],
        ).to_excel(w, sheet_name="train_rule_search", index=False)
        bins.to_excel(w, sheet_name="changed_bins", index=False)
        changed_detail.to_excel(w, sheet_name="changed_detail", index=False)
        pd.DataFrame([
            {"item": "selection", "value": "TRAIN only; ROI excluded from rule selection"},
            {"item": "train_ratio", "value": TRAIN_RATIO},
            {"item": "min_train_activations", "value": MIN_TRAIN_ACTIVATIONS},
            {"item": "target", "value": "HANDICAP races only"},
            {"item": "baseline", "value": "V1_MARGIN"},
            {"item": "always_candidate", "value": "V1_MARGIN + beta4 HANDICAP_REL"},
        ]).to_excel(w, sheet_name="README", index=False)

    print(f"[done] {out}")
    print(f"[result] cutoff={cutoff_date} best_rule={best_name} decision={decision}")
    print("\n=== TEST ===")
    print(test_summary.to_string(index=False))
    print("\n=== GAINS ===")
    for k, v in gains.items():
        print(f"{k}={v:+.6f}")


if __name__ == "__main__":
    main()
