# -*- coding: utf-8 -*-
"""
V1_MARGIN + HANDICAP_REL の完全アウト・オブ・サンプル監視。

固定仕様:
- OOS開始日: 2026-09-13（既定。--start-dateで変更可能）
- 比較: V1_MARGIN vs V1_MARGIN_HANDICAP_PROD_CANDIDATE
- 入力は候補Excel 1冊だけ。pre_rating と pre_rating_prod_candidate を同一レースで比較する。
- 結果確定済み（勝馬が1頭だけ存在）のレースだけを評価する。
- 毎回入力から再構築し、追記しないため同日・同レースの二重計上を防ぐ。
- ハンデ戦100Rまではモデルを自動変更せず、COLLECT_MORE_OOS とする。
- ROIは各レース予測1位への単勝100円の近似値。
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

import pandas as pd

DEFAULT_START_DATE = 20260913
TARGET_HANDICAP_RACES = 100
STAKE_YEN = 100


def norm_rid(v) -> str:
    s = str(v).strip()
    return s[:-2] if s.endswith(".0") else s


def load_candidate(path: Path) -> pd.DataFrame:
    xls = pd.ExcelFile(path, engine="openpyxl")
    required_sheets = {"entries", "races"}
    missing_sheets = required_sheets - set(xls.sheet_names)
    if missing_sheets:
        xls.close()
        raise ValueError(f"{path}: 必要シート不足 {sorted(missing_sheets)}")

    e = pd.read_excel(xls, sheet_name="entries")
    r = pd.read_excel(xls, sheet_name="races")
    xls.close()

    required = {
        "race_id", "horse_id", "rank", "odds", "pre_rating",
        "pre_rating_prod_candidate", "prod_weight_type", "prod_handicap_rel",
    }
    missing = required - set(e.columns)
    if missing:
        raise ValueError(f"{path}: entries列不足 {sorted(missing)}")

    e = e.copy()
    e["race_id"] = e["race_id"].map(norm_rid)
    e["horse_id"] = pd.to_numeric(e["horse_id"], errors="coerce").astype("Int64")
    for c in ["rank", "odds", "pre_rating", "pre_rating_prod_candidate", "prod_handicap_rel"]:
        e[c] = pd.to_numeric(e[c], errors="coerce")

    if e.duplicated(["race_id", "horse_id"]).any():
        dup = int(e.duplicated(["race_id", "horse_id"]).sum())
        raise ValueError(f"race_id+horse_id 重複あり: {dup}件")

    r = r.copy()
    if "race_id" not in r.columns or "date" not in r.columns:
        raise ValueError("racesに race_id/date がありません")
    r["race_id"] = r["race_id"].map(norm_rid)
    race_cols = [c for c in [
        "race_id", "date", "start_time", "place", "class", "ground",
        "distance", "baba", "race_name",
    ] if c in r.columns]
    meta = r[race_cols].drop_duplicates("race_id")
    e = e.merge(meta, on="race_id", how="left", validate="many_to_one")

    e["date_num"] = pd.to_numeric(
        e["date"].astype(str).str.replace(r"\.0$", "", regex=True),
        errors="coerce",
    ).astype("Int64")
    return e


def build_race_detail(e: pd.DataFrame, start_date: int) -> pd.DataFrame:
    src = e[
        e["date_num"].notna()
        & (e["date_num"] >= int(start_date))
        & e["rank"].notna()
        & (e["rank"] > 0)
        & e["pre_rating"].notna()
        & e["pre_rating_prod_candidate"].notna()
    ].copy()

    rows: List[Dict] = []
    for rid, g0 in src.groupby("race_id", sort=False):
        g = g0.copy()
        winners = g[g["rank"] == 1]
        if len(winners) != 1:
            continue

        margin_ranked = g.sort_values(
            ["pre_rating", "horse_id"],
            ascending=[False, True],
            kind="mergesort",
        )
        cand_ranked = g.sort_values(
            ["pre_rating_prod_candidate", "horse_id"],
            ascending=[False, True],
            kind="mergesort",
        )
        if margin_ranked.empty or cand_ranked.empty:
            continue

        old = margin_ranked.iloc[0]
        new = cand_ranked.iloc[0]
        winner = winners.iloc[0]
        winner_id = int(winner["horse_id"])
        old_id = int(old["horse_id"])
        new_id = int(new["horse_id"])

        old_win = int(old_id == winner_id)
        new_win = int(new_id == winner_id)
        old_place = int(float(old["rank"]) <= 3.0)
        new_place = int(float(new["rank"]) <= 3.0)

        old_odds = float(old["odds"]) if pd.notna(old["odds"]) else float("nan")
        new_odds = float(new["odds"]) if pd.notna(new["odds"]) else float("nan")
        old_return = STAKE_YEN * old_odds if old_win and pd.notna(old_odds) and old_odds > 0 else 0.0
        new_return = STAKE_YEN * new_odds if new_win and pd.notna(new_odds) and new_odds > 0 else 0.0

        row = {
            "date": int(g["date_num"].iloc[0]),
            "race_id": str(rid),
            "weight_type": str(g["prod_weight_type"].iloc[0]),
            "winner_horse_id": winner_id,
            "margin_top1_horse_id": old_id,
            "candidate_top1_horse_id": new_id,
            "top1_changed": old_id != new_id,
            "margin_top1_win": old_win,
            "candidate_top1_win": new_win,
            "margin_top1_place": old_place,
            "candidate_top1_place": new_place,
            "margin_top1_odds": old_odds,
            "candidate_top1_odds": new_odds,
            "margin_return_yen": old_return,
            "candidate_return_yen": new_return,
            "margin_pre_rating": float(old["pre_rating"]),
            "candidate_base_pre_rating": float(new["pre_rating"]),
            "candidate_prod_rating": float(new["pre_rating_prod_candidate"]),
            "candidate_handicap_rel": float(new["prod_handicap_rel"]),
        }
        for c in ["start_time", "place", "class", "ground", "distance", "baba", "race_name"]:
            if c in g.columns:
                row[c] = g[c].iloc[0]
        rows.append(row)

    if not rows:
        return pd.DataFrame(columns=[
            "date", "race_id", "weight_type", "winner_horse_id",
            "margin_top1_horse_id", "candidate_top1_horse_id", "top1_changed",
            "margin_top1_win", "candidate_top1_win", "margin_top1_place",
            "candidate_top1_place", "margin_return_yen", "candidate_return_yen",
        ])
    return pd.DataFrame(rows).sort_values(["date", "race_id"], kind="mergesort").reset_index(drop=True)


def model_summary(detail: pd.DataFrame, model: str, scope: str) -> Dict:
    g = detail.copy()
    if scope == "HANDICAP":
        g = g[g["weight_type"] == "HANDICAP"]

    if model == "V1_MARGIN":
        win_col = "margin_top1_win"
        place_col = "margin_top1_place"
        ret_col = "margin_return_yen"
    elif model == "V1_MARGIN_HANDICAP":
        win_col = "candidate_top1_win"
        place_col = "candidate_top1_place"
        ret_col = "candidate_return_yen"
    else:
        raise ValueError(model)

    if g.empty:
        return {
            "model": model,
            "scope": scope,
            "races": 0,
            "top1_win_rate": float("nan"),
            "top1_place_rate": float("nan"),
            "win_roi_pct": float("nan"),
            "win_profit_yen": 0.0,
        }

    stake = float(len(g) * STAKE_YEN)
    ret = float(g[ret_col].sum())
    return {
        "model": model,
        "scope": scope,
        "races": int(len(g)),
        "top1_win_rate": float(g[win_col].mean()),
        "top1_place_rate": float(g[place_col].mean()),
        "win_roi_pct": float(ret / stake * 100.0) if stake > 0 else float("nan"),
        "win_profit_yen": float(ret - stake),
    }


def build_daily(detail: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict] = []
    if detail.empty:
        return pd.DataFrame()
    for date, dg in detail.groupby("date", sort=True):
        for model in ["V1_MARGIN", "V1_MARGIN_HANDICAP"]:
            for scope in ["ALL", "HANDICAP"]:
                rows.append({"date": int(date), **model_summary(dg, model, scope)})
    return pd.DataFrame(rows)


def build_changed(detail: pd.DataFrame) -> pd.DataFrame:
    if detail.empty:
        return detail.copy()
    g = detail[detail["top1_changed"]].copy()
    if g.empty:
        return g
    g["candidate_change_better_win"] = g["candidate_top1_win"] > g["margin_top1_win"]
    g["margin_change_better_win"] = g["margin_top1_win"] > g["candidate_top1_win"]
    g["candidate_change_better_place"] = g["candidate_top1_place"] > g["margin_top1_place"]
    g["margin_change_better_place"] = g["margin_top1_place"] > g["candidate_top1_place"]
    return g


def build_progress(detail: pd.DataFrame, start_date: int) -> pd.DataFrame:
    h = detail[detail["weight_type"] == "HANDICAP"].copy() if not detail.empty else detail.copy()
    changed = h[h["top1_changed"]].copy() if not h.empty else h.copy()

    h_races = int(len(h))
    remaining = max(TARGET_HANDICAP_RACES - h_races, 0)

    margin_win = float(h["margin_top1_win"].mean()) if h_races else float("nan")
    cand_win = float(h["candidate_top1_win"].mean()) if h_races else float("nan")
    margin_place = float(h["margin_top1_place"].mean()) if h_races else float("nan")
    cand_place = float(h["candidate_top1_place"].mean()) if h_races else float("nan")

    if h_races:
        stake = float(h_races * STAKE_YEN)
        margin_roi = float(h["margin_return_yen"].sum() / stake * 100.0)
        cand_roi = float(h["candidate_return_yen"].sum() / stake * 100.0)
    else:
        margin_roi = float("nan")
        cand_roi = float("nan")

    changed_n = int(len(changed))
    cand_change_wins = int((changed["candidate_top1_win"] > changed["margin_top1_win"]).sum()) if changed_n else 0
    margin_change_wins = int((changed["margin_top1_win"] > changed["candidate_top1_win"]).sum()) if changed_n else 0

    if h_races == 0:
        status = "WAITING_FOR_OOS_RESULTS"
    elif h_races < TARGET_HANDICAP_RACES:
        status = "COLLECT_MORE_OOS"
    else:
        win_gain = cand_win - margin_win
        place_gain = cand_place - margin_place
        roi_gain = cand_roi - margin_roi
        if win_gain >= 0 and place_gain >= 0 and roi_gain >= 0:
            status = "READY_FOR_PRODUCTION_REVIEW"
        elif win_gain < 0 and roi_gain < 0:
            status = "REVIEW_CANDIDATE"
        else:
            status = "HOLD_AND_KEEP_MONITORING"

    first_date = int(detail["date"].min()) if not detail.empty else None
    latest_date = int(detail["date"].max()) if not detail.empty else None

    return pd.DataFrame([{
        "status": status,
        "oos_start_date": int(start_date),
        "first_evaluated_date": first_date,
        "latest_evaluated_date": latest_date,
        "completed_all_races": int(len(detail)),
        "completed_handicap_races": h_races,
        "target_handicap_races": TARGET_HANDICAP_RACES,
        "remaining_handicap_races": remaining,
        "changed_handicap_races": changed_n,
        "candidate_change_win_improvements": cand_change_wins,
        "margin_change_win_improvements": margin_change_wins,
        "net_changed_win_delta": cand_change_wins - margin_change_wins,
        "margin_handicap_win_rate": margin_win,
        "candidate_handicap_win_rate": cand_win,
        "win_rate_gain": cand_win - margin_win if h_races else float("nan"),
        "margin_handicap_place_rate": margin_place,
        "candidate_handicap_place_rate": cand_place,
        "place_rate_gain": cand_place - margin_place if h_races else float("nan"),
        "margin_handicap_roi_pct": margin_roi,
        "candidate_handicap_roi_pct": cand_roi,
        "roi_gain_pct_point": cand_roi - margin_roi if h_races else float("nan"),
    }])


def main() -> None:
    p = argparse.ArgumentParser(description="OOS shadow tracker for V1_MARGIN + HANDICAP_REL")
    p.add_argument(
        "--input",
        type=Path,
        default=Path("data/master/race_levels_v1_margin_handicap_prod_candidate.xlsx"),
    )
    p.add_argument("--start-date", type=int, default=DEFAULT_START_DATE)
    p.add_argument(
        "--out",
        type=Path,
        default=Path("data/master/v1_margin_handicap_oos_shadow.xlsx"),
    )
    args = p.parse_args()

    src = args.input.resolve()
    out = args.out.resolve()
    if not src.exists():
        raise FileNotFoundError(src)
    if src == out:
        raise ValueError("--out must differ from --input")

    e = load_candidate(src)
    detail = build_race_detail(e, int(args.start_date))

    cumulative_rows = []
    for model in ["V1_MARGIN", "V1_MARGIN_HANDICAP"]:
        for scope in ["ALL", "HANDICAP"]:
            cumulative_rows.append(model_summary(detail, model, scope))
    cumulative = pd.DataFrame(cumulative_rows)
    daily = build_daily(detail)
    changed = build_changed(detail)
    progress = build_progress(detail, int(args.start_date))

    config = pd.DataFrame([
        {"item": "model", "value": "V1_MARGIN_HANDICAP_PROD_CANDIDATE"},
        {"item": "oos_start_date", "value": int(args.start_date)},
        {"item": "handicap_beta", "value": 4.0},
        {"item": "target_handicap_races", "value": TARGET_HANDICAP_RACES},
        {"item": "history_mode", "value": "REBUILD_EVERY_RUN_NO_APPEND"},
        {"item": "automatic_model_change", "value": "NO"},
        {"item": "roi_definition", "value": "top1 win bet 100 yen; return=odds*100"},
    ])

    out.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        progress.to_excel(writer, sheet_name="progress", index=False)
        cumulative.to_excel(writer, sheet_name="cumulative", index=False)
        daily.to_excel(writer, sheet_name="daily", index=False)
        changed.to_excel(writer, sheet_name="changed_top1", index=False)
        detail.to_excel(writer, sheet_name="race_detail", index=False)
        config.to_excel(writer, sheet_name="config", index=False)

    p0 = progress.iloc[0]
    print(f"[done] {out}")
    print(f"[status] {p0['status']}")
    print(
        "[oos] "
        f"start={int(args.start_date)} "
        f"all_races={int(p0['completed_all_races'])} "
        f"handicap_races={int(p0['completed_handicap_races'])}/{TARGET_HANDICAP_RACES} "
        f"remaining={int(p0['remaining_handicap_races'])}"
    )
    if int(p0["completed_handicap_races"]) > 0:
        print(
            "[handicap] "
            f"win_gain={float(p0['win_rate_gain']):+.4f} "
            f"place_gain={float(p0['place_rate_gain']):+.4f} "
            f"roi_gain={float(p0['roi_gain_pct_point']):+.2f}pt "
            f"changed_win_delta={int(p0['net_changed_win_delta']):+d}"
        )


if __name__ == "__main__":
    main()
