# -*- coding: utf-8 -*-
"""
00_Export_To_Excel_4.py をそのままv1として実行した後、同じ入力データから
馬レベル/レースレベル v2 を独立再計算し、同じExcelへ比較用シートを追加する。

v2改善点
1. 時計差を1000m換算して距離間比較を正規化
2. 斤量をレース平均斤量との差として補正
3. 着順より勝ち馬との着差を重視（着順35% / 着差65%）
4. 短距離/マイル/中距離/長距離の距離帯別ratingを追加

既存v1は変更しない。v2は検証用の並走版。
"""
from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

HERE = Path(__file__).resolve().parent
BASE_SCRIPT = HERE / "00_Export_To_Excel_4.py"

# v2初期パラメータ。後で実データの時系列バックテストで最適化する。
OVERALL_W = 0.40
SURFACE_W = 0.35
DISTANCE_W = 0.25
OVERALL_UPDATE = 0.60
SURFACE_UPDATE = 1.00
DISTANCE_UPDATE = 1.05
WEIGHT_SCORE_PER_KG = 0.010
WEIGHT_SCORE_CAP = 0.05


def load_base_module():
    spec = importlib.util.spec_from_file_location("race_level_v1", BASE_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"v1コードを読み込めません: {BASE_SCRIPT}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def fnum(v) -> Optional[float]:
    if v is None or pd.isna(v):
        return None
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return None


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def distance_band(distance: Optional[int]) -> Optional[str]:
    if distance is None:
        return None
    if distance <= 1400:
        return "短距離"
    if distance <= 1800:
        return "マイル"
    if distance <= 2200:
        return "中距離"
    return "長距離"


def per1000(diff_sec: Optional[float], distance: Optional[int]) -> Optional[float]:
    if diff_sec is None or distance is None or distance <= 0:
        return None
    return float(diff_sec) * 1000.0 / float(distance)


def time_diff_rate(time_sec: Optional[float], master_sec: Optional[float]) -> Optional[float]:
    if time_sec is None or master_sec is None or master_sec <= 0:
        return None
    return (float(time_sec) - float(master_sec)) / float(master_sec)


def mean(values: List[float]) -> Optional[float]:
    return float(sum(values) / len(values)) if values else None


def recent_weighted(rows: List[Dict], field: str = "post_rating_v2", limit: int = 3) -> Optional[float]:
    rows = rows[-limit:]
    if not rows:
        return None
    weights = [0.50, 0.30, 0.20][: len(rows)]
    total = used = 0.0
    for row, w in zip(reversed(rows), weights):
        v = row.get(field)
        if v is None or pd.isna(v):
            continue
        total += float(v) * w
        used += w
    return total / used if used else None


def calc_result_time_adjustment_v2(
    race_best_per1000: Optional[float],
    top5_mean_per1000: Optional[float],
    fast_runner_rate: Optional[float],
) -> Optional[float]:
    vals = [race_best_per1000, top5_mean_per1000, fast_runner_rate]
    if all(v is None or pd.isna(v) for v in vals):
        return None
    score = 0.0
    if race_best_per1000 is not None:
        score += clamp(-9.0 * race_best_per1000, -18.0, 12.0)
    if top5_mean_per1000 is not None:
        score += clamp(-6.0 * top5_mean_per1000, -16.0, 12.0)
    if fast_runner_rate is not None:
        score += clamp(fast_runner_rate, 0.0, 1.0) * 8.0
    return score


def performance_score_v2(
    base,
    entry: Dict,
    race: Dict,
    field_size: int,
    winner_margin_sec: Optional[float],
    best_last3f: Optional[float],
    median_last3f: Optional[float],
    field_mean_weight: Optional[float],
) -> Tuple[float, Dict[str, Optional[float]]]:
    rank = entry.get("rank")
    rank_score = base.rank_to_score(rank, field_size)
    distance = race.get("distance")
    gap_sec = fnum(entry.get("gap_from_winner_sec"))
    gap1000 = per1000(gap_sec, distance)

    if rank == 1:
        win1000 = per1000(winner_margin_sec, distance)
        margin_component = 0.95 + (clamp(0.12 * win1000, 0.0, 0.05) if win1000 is not None else 0.0)
    elif gap1000 is not None:
        margin_component = clamp(0.95 - (0.22 * gap1000), 0.0, 0.95)
    else:
        margin_component = rank_score

    score = (0.35 * rank_score) + (0.65 * margin_component)

    race_class = base.normalize_race_class_key(race.get("class"))
    if race_class == "Ｇ１" and rank == 1:
        score += 0.08
        if base.is_classic_generation_g1(race.get("race_name")):
            score += 0.03
    elif race_class in ("Ｇ２", "Ｇ３") and rank == 1:
        score += 0.03

    time_sec = fnum(entry.get("time_sec"))
    master = fnum(entry.get("condition_best_time"))
    time1000 = per1000(None if time_sec is None or master is None else time_sec - master, distance)
    t_rate = time_diff_rate(time_sec, master)
    if time1000 is not None:
        score += clamp(-0.08 * time1000, -0.18, 0.10)

    last3f = fnum(entry.get("last3f"))
    if last3f is not None:
        if best_last3f is not None:
            score += clamp(-0.035 * (last3f - best_last3f), -0.05, 0.04)
        if median_last3f is not None:
            score += clamp(0.012 * (median_last3f - last3f), -0.025, 0.025)

    positions = base.parse_passing_positions(entry.get("passing"))
    score += base.calc_style_bonus(rank, field_size, positions)

    w = fnum(entry.get("weight"))
    weight_diff = None if w is None or field_mean_weight is None else w - field_mean_weight
    weight_adj = 0.0 if weight_diff is None else clamp(weight_diff * WEIGHT_SCORE_PER_KG, -WEIGHT_SCORE_CAP, WEIGHT_SCORE_CAP)
    score += weight_adj

    features = {
        "gap_from_winner_per_1000m": gap1000,
        "time_vs_master_per_1000m": time1000,
        "time_diff_rate": t_rate,
        "weight_diff_from_field": weight_diff,
        "weight_score_adjustment_v2": weight_adj,
        "rank_score": rank_score,
        "margin_component_v2": margin_component,
    }
    return clamp(score, 0.0, 1.0), features


def derive_v2(base, store):
    overall: Dict[int, float] = {}
    surface: Dict[str, Dict[int, float]] = {"芝": {}, "ダ": {}, "障": {}}
    dist: Dict[str, Dict[int, float]] = {"短距離": {}, "マイル": {}, "中距離": {}, "長距離": {}}
    starts: Dict[int, int] = {}
    histories: List[Dict] = []
    entry_rows: List[Dict] = []
    race_rows: List[Dict] = []

    entries_by_race: Dict[str, List[Dict]] = {}
    for e in store.entries:
        entries_by_race.setdefault(str(e["race_id"]), []).append(dict(e))

    def race_sort_key(item):
        rid, r = item
        return (str(r.get("date") or ""), str(r.get("start_time") or ""), str(rid))

    for rid, race in sorted(store.races.items(), key=race_sort_key):
        race_entries = entries_by_race.get(str(rid), [])
        if not race_entries:
            continue
        distance = race.get("distance")
        ground = race.get("ground")
        band = distance_band(distance)
        field = len(race_entries)

        weights = [fnum(e.get("weight")) for e in race_entries]
        weights = [v for v in weights if v is not None]
        field_mean_weight = mean(weights)
        last3fs = [fnum(e.get("last3f")) for e in race_entries]
        last3fs = [v for v in last3fs if v is not None]
        best_last3f = min(last3fs) if last3fs else None
        median_last3f = float(pd.Series(last3fs).median()) if last3fs else None

        winner_margin = None
        ranked = sorted([e for e in race_entries if e.get("rank") is not None], key=lambda x: x.get("rank"))
        if len(ranked) >= 2:
            winner_margin = fnum(ranked[1].get("gap_from_winner_sec"))

        pending = []
        pre_values = []
        for e in race_entries:
            hid = int(e["horse_id"])
            initial = float(store.horse_rows[hid].get("initial_rating", 1500.0))
            if hid not in overall:
                market_adj = fnum(e.get("market_prior_adjustment")) or 0.0
                overall[hid] = initial + market_adj
                if ground in surface:
                    surface[ground][hid] = initial + market_adj
                if band in dist:
                    dist[band][hid] = initial + market_adj

            pre_overall = overall.get(hid, initial)
            pre_surface = surface.get(ground, {}).get(hid, pre_overall)
            pre_distance = dist.get(band, {}).get(hid, pre_overall)
            pre_effective = OVERALL_W * pre_overall + SURFACE_W * pre_surface + DISTANCE_W * pre_distance
            pre_values.append(pre_effective)
            pending.append({
                "entry": e,
                "hid": hid,
                "pre_overall": pre_overall,
                "pre_surface": pre_surface,
                "pre_distance": pre_distance,
                "pre_effective": pre_effective,
            })

        ser = pd.Series(pre_values, dtype="float")
        pre_mean = float(ser.mean())
        pre_p50 = float(ser.median())
        pre_top1 = float(ser.max())
        pre_bottom1 = float(ser.min())
        pre_top3 = float(ser.sort_values(ascending=False).head(3).mean())
        pre_top5 = float(ser.sort_values(ascending=False).head(5).mean())
        pre_top7 = float(ser.sort_values(ascending=False).head(7).mean())
        pre_bottom5 = float(ser.sort_values().head(5).mean())
        pre_std = float(ser.std(ddof=0)) if field >= 2 else 0.0
        pre_iqr = float(ser.quantile(0.75) - ser.quantile(0.25))

        for p in pending:
            hid = p["hid"]
            others = [x["pre_effective"] for x in pending if x["hid"] != hid]
            opp_avg = mean(others) if others else p["pre_effective"]
            top3 = sorted(others, reverse=True)[:3]
            opp_top3 = mean(top3) if top3 else opp_avg
            opp_strength = 0.55 * opp_avg + 0.45 * opp_top3 + base.class_rating_offset(race.get("class"))
            actual, feats = performance_score_v2(
                base, p["entry"], race, field, winner_margin,
                best_last3f, median_last3f, field_mean_weight,
            )
            expected = base.expected_score(p["pre_effective"], opp_strength)
            n = starts.get(hid, 0)
            k = base.k_factor(race.get("class"), n, field, winner_margin)
            p.update({
                "opp_avg": opp_avg,
                "opp_top3": opp_top3,
                "opp_strength": opp_strength,
                "actual": actual,
                "expected": expected,
                "k": k,
                "raw": k * (actual - expected),
                "features": feats,
            })

        race_mean_raw = float(sum(p["raw"] for p in pending) / len(pending))
        for p in pending:
            e = p["entry"]
            hid = p["hid"]
            adjusted = p["raw"] - race_mean_raw
            post_overall = p["pre_overall"] + adjusted * OVERALL_UPDATE
            post_surface = p["pre_surface"] + adjusted * SURFACE_UPDATE
            post_distance = p["pre_distance"] + adjusted * DISTANCE_UPDATE
            post_effective = OVERALL_W * post_overall + SURFACE_W * post_surface + DISTANCE_W * post_distance
            overall[hid] = post_overall
            if ground in surface:
                surface[ground][hid] = post_surface
            if band in dist:
                dist[band][hid] = post_distance
            starts[hid] = starts.get(hid, 0) + 1

            hrow = {
                "horse_id": hid,
                "race_id": str(rid),
                "date": race.get("date"),
                "pre_rating_v2": p["pre_effective"],
                "post_rating_v2": post_effective,
                "pre_overall_rating_v2": p["pre_overall"],
                "post_overall_rating_v2": post_overall,
                "pre_surface_rating_v2": p["pre_surface"],
                "post_surface_rating_v2": post_surface,
                "pre_distance_rating_v2": p["pre_distance"],
                "post_distance_rating_v2": post_distance,
                "surface": ground,
                "distance_band": band,
                "k_factor": p["k"],
                "actual_score_v2": p["actual"],
                "expected_score_v2": p["expected"],
                "raw_delta_v2": p["raw"],
                "race_mean_delta_v2": race_mean_raw,
                "adjusted_delta_v2": adjusted,
                "opp_avg_rating_v2": p["opp_avg"],
                "opp_top3_rating_v2": p["opp_top3"],
                "opp_strength_v2": p["opp_strength"],
                **p["features"],
            }
            histories.append(hrow)
            entry_rows.append({
                "race_id": str(rid),
                "horse_id": hid,
                "number": e.get("number"),
                "rank": e.get("rank"),
                "weight": e.get("weight"),
                "pre_rating_v2": p["pre_effective"],
                "pre_overall_rating_v2": p["pre_overall"],
                "pre_surface_rating_v2": p["pre_surface"],
                "pre_distance_rating_v2": p["pre_distance"],
                "distance_band": band,
                **p["features"],
            })

        pre_score = base.calc_pre_race_level_score(pre_top3, pre_top5, pre_mean, pre_p50)
        timed = [e for e in race_entries if fnum(e.get("time_sec")) is not None and fnum(e.get("condition_best_time")) is not None]
        normalized = []
        for e in timed:
            diff = fnum(e.get("time_sec")) - fnum(e.get("condition_best_time"))
            v = per1000(diff, distance)
            if v is not None:
                normalized.append((e.get("rank"), v))
        ranked_norm = [v for _, v in sorted(normalized, key=lambda x: (999999 if x[0] is None else x[0]))]
        best_norm = min((v for _, v in normalized), default=None)
        top5_norm_mean = mean(ranked_norm[:5])
        fast_rate = (sum(1 for _, v in normalized if v <= 0) / len(normalized)) if normalized else None
        time_adj = calc_result_time_adjustment_v2(best_norm, top5_norm_mean, fast_rate)
        final_score = pre_score if time_adj is None else pre_score + time_adj
        race_rows.append({
            "race_id": str(rid),
            "date": race.get("date"),
            "start_time": race.get("start_time"),
            "place": race.get("place"),
            "class": race.get("class"),
            "ground": ground,
            "distance": distance,
            "distance_band": band,
            "baba": race.get("baba"),
            "race_name": race.get("race_name"),
            "field_size": field,
            "pre_mean_v2": pre_mean,
            "pre_p50_v2": pre_p50,
            "pre_top1_v2": pre_top1,
            "pre_top3_mean_v2": pre_top3,
            "pre_top5_mean_v2": pre_top5,
            "pre_top7_mean_v2": pre_top7,
            "pre_bottom1_v2": pre_bottom1,
            "pre_bottom5_mean_v2": pre_bottom5,
            "pre_std_v2": pre_std,
            "pre_iqr_v2": pre_iqr,
            "gap_top1_p50_v2": pre_top1 - pre_p50,
            "gap_top3_p50_v2": pre_top3 - pre_p50,
            "gap_top5_p50_v2": pre_top5 - pre_p50,
            "pre_race_level_score_v2": pre_score,
            "race_best_vs_master_per_1000m": best_norm,
            "top5_time_vs_master_per_1000m_mean": top5_norm_mean,
            "fast_runner_rate": fast_rate,
            "result_time_level_adjustment_v2": time_adj,
            "final_race_level_score_v2": final_score,
        })

    hist_by_horse: Dict[int, List[Dict]] = {}
    for h in histories:
        hist_by_horse.setdefault(int(h["horse_id"]), []).append(h)

    rating_rows = []
    for hid in sorted(overall):
        rating_rows.append({
            "horse_id": hid,
            "rating_v2": overall.get(hid),
            "turf_rating_v2": surface["芝"].get(hid),
            "dirt_rating_v2": surface["ダ"].get(hid),
            "jump_rating_v2": surface["障"].get(hid),
            "sprint_rating_v2": dist["短距離"].get(hid),
            "mile_rating_v2": dist["マイル"].get(hid),
            "middle_rating_v2": dist["中距離"].get(hid),
            "long_rating_v2": dist["長距離"].get(hid),
            "start_count": starts.get(hid, 0),
            "recent_rating_v2": recent_weighted(hist_by_horse.get(hid, [])),
        })

    df_ratings = pd.DataFrame(rating_rows)
    df_hist = pd.DataFrame(histories)
    df_entries = pd.DataFrame(entry_rows)
    df_races = pd.DataFrame(race_rows)
    if not df_races.empty:
        df_races["pre_race_level_score_v2_rank"] = df_races["pre_race_level_score_v2"].rank(method="dense", ascending=False).astype("Int64")
        df_races["final_race_level_score_v2_rank"] = df_races["final_race_level_score_v2"].rank(method="dense", ascending=False).astype("Int64")

    if not df_hist.empty:
        sums = df_hist.groupby("race_id")["adjusted_delta_v2"].sum().abs()
        bad = int((sums >= 1e-6).sum())
        if bad:
            raise RuntimeError(f"v2ゼロサム検証NG: {bad}レース")
        print(f"[verify-v2] adjusted_delta_v2ゼロサム: OK ({len(sums)}レース)")

    return df_ratings, df_hist, df_entries, df_races


def write_v2_sheets(out_path: Path, dfs) -> None:
    df_ratings, df_hist, df_entries, df_races = dfs
    with pd.ExcelWriter(out_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        df_ratings.to_excel(writer, sheet_name="ratings_v2", index=False)
        df_hist.to_excel(writer, sheet_name="ratings_history_v2", index=False)
        df_entries.to_excel(writer, sheet_name="entries_v2", index=False)
        df_races.to_excel(writer, sheet_name="race_levels_v2", index=False)
    print("[done-v2] ratings_v2 / ratings_history_v2 / entries_v2 / race_levels_v2 を追加しました")


def main():
    base = load_base_module()
    parser = argparse.ArgumentParser(description="馬レベル・レースレベル v1/v2 並走生成")
    parser.add_argument("--excel", default=None, help="入力 racedata_results.xlsx")
    parser.add_argument("--out", default=None, help="出力 race_levels.xlsx")
    args = parser.parse_args()

    base_dir = base.BASE_DIR
    src = Path(args.excel) if args.excel else base_dir / "racedata_results.xlsx"
    dst = Path(args.out) if args.out else base_dir / "race_levels.xlsx"
    if not src.exists():
        raise SystemExit(f"入力Excelが見つかりません: {src}")

    print(f"[v1+v2] 入力: {src}")
    print(f"[v1+v2] 出力: {dst}")
    store = base.process_excel_to_memory(src)
    base.write_store_to_excel(store, dst, src)
    dfs = derive_v2(base, store)
    write_v2_sheets(dst, dfs)

    print("[v1+v2] 完了。v1既存シートとv2比較用4シートを同一Excelに保存しました。")


if __name__ == "__main__":
    main()
