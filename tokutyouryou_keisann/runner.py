# runner.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd

from .common import (
    _coalesce_merge_columns,
    _normalize_surface_name,
    build_rid_to_date_map,
    discover_files,
    load_results_all_sheets,
    parse_rid_meta,
)
from .config import (
    COURSE_STYLE_FEATURE_COLS,
    CONFIG,
    EXCEL_DIR,
    FEAT_COLS,
    FEATURE_WEIGHTS_SEED,
    OPTIMIZER_FIXED_ZERO_FEATURES,
    PROJECT_ROOT,
    PY_DIR,
    RACE_LEVEL_XLSX,
    calc_extra_penalty,
    calc_rest_dist_risk,
)
from .features import build_features_from_one_file
from .optimizer import optimize_placewise_weights
from .scoring import eval_success_and_roi
from .adoption import evaluate_test_gate, evaluate_valid_gate, split_train_valid_test
from .baseline import (
    BASELINE_ERROR_CODE,
    BaselineWeightError,
    BaselineWeightInfo,
    compare_weights_maps,
    load_effective_weights_file,
    load_baseline_weights,
    normalized_weights_sha256,
    select_baseline_weight_file,
    sha256_file,
)


def _build_eval_debug_summary(
    df_target: pd.DataFrame,
    df_res_entries: pd.DataFrame,
    df_res_payout: pd.DataFrame,
    weights_map,
) -> dict:
    if df_target is None or df_target.empty:
        return {
            "rid_total": 0,
            "rid_with_top3_names": 0,
            "rid_with_top3_nums": 0,
            "rid_with_top3_complete": 0,
            "rid_with_payout_type": 0,
            "rid_with_top3_and_payout": 0,
            "detail_count": 0,
            "missing_top3_or_num_count": 0,
            "payout_missing_count": 0,
            "gap_skip_count": 0,
            "bet_count": 0,
            "point_sum": 0.0,
            "top5_point_rate": 0.0,
            "top3_complete_rate": 0.0,
            "win_in_top5_rate": 0.0,
            "place_in_top5_rate": 0.0,
            "invest_yen": 0,
            "return_yen": 0,
            "roi": 0.0,
        }

    target_rids = set(df_target["rid_str"].dropna().astype(str).unique().tolist())

    top3_name_rids = set()
    top3_num_rids = set()

    if df_res_entries is not None and not df_res_entries.empty:
        df_top3 = df_res_entries[df_res_entries["着順_num"].isin([1, 2, 3])].copy()

        if not df_top3.empty:
            name_cnt = (
                df_top3.groupby("rid_str")["name_norm"]
                .apply(lambda s: int(s.notna().sum()))
                .to_dict()
            )
            num_cnt = (
                df_top3.groupby("rid_str")["馬番_int"]
                .apply(lambda s: int(s.notna().sum()))
                .to_dict()
            )

            top3_name_rids = {str(rid) for rid, cnt in name_cnt.items() if int(cnt) >= 3}
            top3_num_rids = {str(rid) for rid, cnt in num_cnt.items() if int(cnt) >= 3}

    top3_complete_rids = top3_name_rids & top3_num_rids

    payout_rids = set()
    if df_res_payout is not None and not df_res_payout.empty:
        bet_type = str(CONFIG.get("BET_TYPE", "") or "")
        df_pay = df_res_payout[
            df_res_payout["払戻種別"].astype(str).str.contains(bet_type, na=False)
        ].copy()
        if not df_pay.empty:
            payout_rids = set(df_pay["rid_str"].dropna().astype(str).unique().tolist())

    s, t, i, r, det, stab = eval_success_and_roi(
        weights_map, df_target, df_res_entries, df_res_payout
    )

    detail_rids = set(str(k) for k in det.keys())
    payout_missing_count = sum(int(v.get("payout_missing", 0)) for v in det.values())
    gap_skip_count = sum(int(v.get("skip_by_gap", 0)) for v in det.values())

    missing_top3_or_num_count = len(target_rids - detail_rids)

    return {
        "rid_total": len(target_rids),
        "rid_with_top3_names": len(target_rids & top3_name_rids),
        "rid_with_top3_nums": len(target_rids & top3_num_rids),
        "rid_with_top3_complete": len(target_rids & top3_complete_rids),
        "rid_with_payout_type": len(target_rids & payout_rids),
        "rid_with_top3_and_payout": len(target_rids & top3_complete_rids & payout_rids),
        "detail_count": len(detail_rids),
        "missing_top3_or_num_count": int(missing_top3_or_num_count),
        "payout_missing_count": int(payout_missing_count),
        "gap_skip_count": int(gap_skip_count),
        "bet_count": int(t),
        "point_sum": float(s),
        "top5_point_rate": float(stab.get("top5_point_rate", 0.0)),
        "top3_complete_rate": float(stab.get("top3_complete_rate", 0.0)),
        "win_in_top5_rate": float(stab.get("win_in_top5_rate", 0.0)),
        "place_in_top5_rate": float(stab.get("place_in_top5_rate", 0.0)),
        "invest_yen": int(i),
        "return_yen": int(r),
        "roi": float(stab.get("roi", 0.0)),
    }


def _print_eval_debug_summary(
    label: str,
    df_target: pd.DataFrame,
    df_res_entries: pd.DataFrame,
    df_res_payout: pd.DataFrame,
    weights_map,
) -> dict:
    summary = _build_eval_debug_summary(
        df_target=df_target,
        df_res_entries=df_res_entries,
        df_res_payout=df_res_payout,
        weights_map=weights_map,
    )

    print(f"\n=== [{label}] debug summary ===")
    print(f"rid_total={summary['rid_total']}")
    print(f"rid_with_top3_names={summary['rid_with_top3_names']}")
    print(f"rid_with_top3_nums={summary['rid_with_top3_nums']}")
    print(f"rid_with_top3_complete={summary['rid_with_top3_complete']}")
    print(f"rid_with_payout_type={summary['rid_with_payout_type']}")
    print(f"rid_with_top3_and_payout={summary['rid_with_top3_and_payout']}")
    print(f"detail_count={summary['detail_count']}")
    print(f"missing_top3_or_num_count={summary['missing_top3_or_num_count']}")
    print(f"payout_missing_count={summary['payout_missing_count']}")
    print(f"gap_skip_count={summary['gap_skip_count']}")
    print(f"bet_count={summary['bet_count']}")
    print(f"point_sum={summary['point_sum']:.3f}")
    print(f"top5_point_rate={summary['top5_point_rate']:.3f}")
    print(f"top3_complete_rate={summary['top3_complete_rate']:.3f}")
    print(f"win_in_top5_rate={summary['win_in_top5_rate']:.3f}")
    print(f"place_in_top5_rate={summary['place_in_top5_rate']:.3f}")

    return summary


def _build_clean_eval_rids(
    df_target: pd.DataFrame,
    df_res_entries: pd.DataFrame,
    min_rows_per_rid: int = 6,
) -> tuple[set[str], dict]:
    if df_target is None or df_target.empty or "rid_str" not in df_target.columns:
        return set(), {
            "source_rids": 0,
            "clean_rids": 0,
            "excluded_few_rows_rids": 0,
            "excluded_missing_top3_rids": 0,
            "excluded_empty_rid_rows": 0,
            "min_rows_per_rid": int(min_rows_per_rid),
        }

    rid_series = df_target["rid_str"].fillna("").astype(str).str.strip()
    source_rids = set(rid_series[rid_series != ""].unique().tolist())
    empty_rid_count = int((rid_series == "").sum())

    row_counts = df_target.assign(_rid_str_clean=rid_series).groupby("_rid_str_clean").size()
    rids_with_enough_rows = set(
        str(rid) for rid, cnt in row_counts.items()
        if str(rid) and int(cnt) >= int(min_rows_per_rid)
    )

    rids_with_top3 = set()
    if df_res_entries is not None and not df_res_entries.empty:
        df_top3 = df_res_entries[df_res_entries["着順_num"].isin([1, 2, 3])].copy()
        if not df_top3.empty:
            name_cnt = (
                df_top3.groupby("rid_str")["name_norm"]
                .apply(lambda s: int(s.notna().sum()))
                .to_dict()
            )
            num_cnt = (
                df_top3.groupby("rid_str")["馬番_int"]
                .apply(lambda s: int(s.notna().sum()))
                .to_dict()
            )
            rids_with_top3 = {
                str(rid)
                for rid in set(name_cnt.keys()) | set(num_cnt.keys())
                if int(name_cnt.get(rid, 0)) >= 3 and int(num_cnt.get(rid, 0)) >= 3
            }

    clean_rids = source_rids & rids_with_enough_rows & rids_with_top3
    summary = {
        "source_rids": int(len(source_rids)),
        "clean_rids": int(len(clean_rids)),
        "excluded_few_rows_rids": int(len(source_rids - rids_with_enough_rows)),
        "excluded_missing_top3_rids": int(len(source_rids - rids_with_top3)),
        "excluded_empty_rid_rows": int(empty_rid_count),
        "min_rows_per_rid": int(min_rows_per_rid),
    }
    return clean_rids, summary


def _filter_clean_eval_df(
    df_target: pd.DataFrame,
    df_res_entries: pd.DataFrame,
    min_rows_per_rid: int = 6,
) -> tuple[pd.DataFrame, dict]:
    clean_rids, summary = _build_clean_eval_rids(
        df_target=df_target,
        df_res_entries=df_res_entries,
        min_rows_per_rid=min_rows_per_rid,
    )
    if df_target is None or df_target.empty:
        return pd.DataFrame(columns=[]), summary

    out = df_target[df_target["rid_str"].fillna("").astype(str).isin(clean_rids)].copy()
    summary["clean_rows"] = int(len(out))
    return out, summary


def _print_clean_eval_summary(summary: dict, label: str) -> dict:
    row = {"label": label, **summary}
    print(f"\n=== [{label}] clean eval target ===")
    print(f"source_rids={row['source_rids']}")
    print(f"clean_rids={row['clean_rids']}")
    print(f"clean_rows={row.get('clean_rows', 0)}")
    print(f"min_rows_per_rid={row['min_rows_per_rid']}")
    print(f"excluded_few_rows_rids={row['excluded_few_rows_rids']}")
    print(f"excluded_missing_top3_rids={row['excluded_missing_top3_rids']}")
    print(f"excluded_empty_rid_rows={row['excluded_empty_rid_rows']}")
    return row


def _distance_bucket(distance_m: object) -> str:
    v = pd.to_numeric(pd.Series([distance_m]), errors="coerce").iloc[0]
    if pd.isna(v):
        return "unknown"
    v = int(v)
    if v < 1400:
        return "short_lt1400"
    if v < 1800:
        return "mile_1400_1799"
    if v < 2200:
        return "middle_1800_2199"
    if v < 2600:
        return "long_2200_2599"
    return "stayer_ge2600"


def _field_size_bucket(field_size: object) -> str:
    v = pd.to_numeric(pd.Series([field_size]), errors="coerce").iloc[0]
    if pd.isna(v):
        return "unknown"
    v = int(v)
    if v <= 8:
        return "small_le8"
    if v <= 12:
        return "medium_9_12"
    if v <= 16:
        return "large_13_16"
    return "full_ge17"


def _race_class_bucket(text: object) -> str:
    s = str(text or "").upper()
    if any(x in s for x in ["Ｇ３", "G3", "GIII", "ＪＰＮ３", "JPN3"]):
        return "g3"
    if any(x in s for x in ["Ｇ２", "G2", "GII", "ＪＰＮ２", "JPN2"]):
        return "g2"
    if any(x in s for x in ["Ｇ１", "G1", "GI", "ＪＰＮ１", "JPN1"]):
        return "g1"
    if any(x in s for x in ["オープン", "ｵｰﾌﾟﾝ", "OPEN", "L", "リステッド"]):
        return "open_l"
    if any(x in s for x in ["3勝", "３勝", "1600"]):
        return "3win"
    if any(x in s for x in ["2勝", "２勝", "1000"]):
        return "2win"
    if any(x in s for x in ["1勝", "１勝", "500"]):
        return "1win"
    if any(x in s for x in ["未勝利", "新馬"]):
        return "maiden_new"
    return "other"


def _build_clean_condition_analysis(
    df_target: pd.DataFrame,
    details: dict,
    label: str,
    min_races: int,
) -> pd.DataFrame:
    if df_target is None or df_target.empty or not details:
        return pd.DataFrame()

    meta = (
        df_target.copy()
        .assign(rid_str=lambda x: x["rid_str"].fillna("").astype(str))
        .groupby("rid_str")
        .agg(
            place_name=("place_name", "first") if "place_name" in df_target.columns else ("rid_str", "first"),
            surface_name=("surface_name", "first") if "surface_name" in df_target.columns else ("rid_str", "first"),
            distance_m=("distance_m", "first") if "distance_m" in df_target.columns else ("rid_str", "size"),
            field_size=("field_size", "first") if "field_size" in df_target.columns else ("rid_str", "size"),
            race_class=("race_class", "first") if "race_class" in df_target.columns else ("rid_str", "first"),
        )
        .reset_index()
    )
    meta["surface_name"] = meta["surface_name"].map(_normalize_surface_name)
    meta["place_surface"] = meta["place_name"].fillna("").astype(str) + "_" + meta["surface_name"].fillna("").astype(str)
    meta["distance_bucket"] = meta["distance_m"].map(_distance_bucket)
    meta["field_size_bucket"] = meta["field_size"].map(_field_size_bucket)
    meta["race_class_bucket"] = meta["race_class"].map(_race_class_bucket)

    detail_rows = []
    for rid, d in details.items():
        if int(d.get("evaluated", 0)) != 1:
            continue
        detail_rows.append(
            {
                "rid_str": str(rid),
                "top5_hit_points": float(d.get("top5_hit_points", 0.0)),
                "top3_complete": float(d.get("top3_complete", 0.0)),
                "win_in_top5": float(d.get("win_in_top5", 0.0)),
                "place_capture_rate": float(d.get("place_capture_rate", 0.0)),
                "rank1_place": float(d.get("rank1_place", 0.0)),
                "rank1_win": float(d.get("rank1_win", 0.0)),
            }
        )
    if not detail_rows:
        return pd.DataFrame()

    detail_df = pd.DataFrame(detail_rows)
    joined = detail_df.merge(meta, on="rid_str", how="left")
    max_points = (
        float(CONFIG["TOP5_HIT_W_FIRST"])
        + float(CONFIG["TOP5_HIT_W_SECOND"])
        + float(CONFIG["TOP5_HIT_W_THIRD"])
    )

    condition_cols = [
        ("place", "place_name"),
        ("surface", "surface_name"),
        ("place_surface", "place_surface"),
        ("distance_bucket", "distance_bucket"),
        ("field_size_bucket", "field_size_bucket"),
        ("race_class_bucket", "race_class_bucket"),
    ]
    rows = []
    for condition_type, col in condition_cols:
        if col not in joined.columns:
            continue
        grouped = joined.groupby(col, dropna=False)
        for condition_value, g in grouped:
            race_count = int(len(g))
            if race_count < int(min_races):
                continue
            rows.append(
                {
                    "mode": label,
                    "condition_type": condition_type,
                    "condition_value": str(condition_value or "unknown"),
                    "race_count": race_count,
                    "point_sum": float(g["top5_hit_points"].sum()),
                    "top5_point_rate": float(g["top5_hit_points"].mean() / max_points) if max_points > 0 else 0.0,
                    "top3_complete_rate": float(g["top3_complete"].mean()),
                    "win_in_top5_rate": float(g["win_in_top5"].mean()),
                    "place_in_top5_rate": float(g["place_capture_rate"].mean()),
                    "rank1_place_rate": float(g["rank1_place"].mean()),
                    "rank1_win_rate": float(g["rank1_win"].mean()),
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(
        ["mode", "top3_complete_rate", "top5_point_rate", "race_count"],
        ascending=[True, True, True, False],
    ).reset_index(drop=True)


def _print_weakness_preview(df_analysis: pd.DataFrame, label: str, limit: int = 10) -> None:
    if df_analysis is None or df_analysis.empty:
        print(f"\n=== [{label}] clean condition weakness ===")
        print("no rows")
        return
    print(f"\n=== [{label}] clean condition weakness worst {limit} ===")
    view = df_analysis[df_analysis["mode"].astype(str) == label].head(int(limit))
    for _, row in view.iterrows():
        print(
            f"{row['condition_type']}={row['condition_value']} "
            f"races={int(row['race_count'])} "
            f"top5={float(row['top5_point_rate']):.3f} "
            f"complete={float(row['top3_complete_rate']):.3f} "
            f"win_in5={float(row['win_in_top5_rate']):.3f}"
        )


def _build_rid_rows_summary(df: pd.DataFrame, label: str) -> dict:
    if df is None or df.empty or "rid_str" not in df.columns:
        return {
            "label": label,
            "rows": 0,
            "rids": 0,
            "rows_per_rid_mean": 0.0,
            "rows_per_rid_median": 0.0,
            "rows_per_rid_min": 0,
            "rows_per_rid_max": 0,
            "rids_eq_1row": 0,
            "rids_eq_2row": 0,
            "rids_eq_3to5row": 0,
            "rids_eq_6to9row": 0,
            "rids_ge_10row": 0,
        }

    counts = df.groupby("rid_str").size().astype(int)

    return {
        "label": label,
        "rows": int(len(df)),
        "rids": int(counts.shape[0]),
        "rows_per_rid_mean": float(counts.mean()),
        "rows_per_rid_median": float(counts.median()),
        "rows_per_rid_min": int(counts.min()),
        "rows_per_rid_max": int(counts.max()),
        "rids_eq_1row": int((counts == 1).sum()),
        "rids_eq_2row": int((counts == 2).sum()),
        "rids_eq_3to5row": int(((counts >= 3) & (counts <= 5)).sum()),
        "rids_eq_6to9row": int(((counts >= 6) & (counts <= 9)).sum()),
        "rids_ge_10row": int((counts >= 10).sum()),
    }


def _print_rid_rows_summary(df: pd.DataFrame, label: str) -> dict:
    summary = _build_rid_rows_summary(df, label)

    print(f"\n=== [{label}] rid row distribution ===")
    print(f"rows={summary['rows']}")
    print(f"rids={summary['rids']}")
    print(f"rows_per_rid_mean={summary['rows_per_rid_mean']:.3f}")
    print(f"rows_per_rid_median={summary['rows_per_rid_median']:.3f}")
    print(f"rows_per_rid_min={summary['rows_per_rid_min']}")
    print(f"rows_per_rid_max={summary['rows_per_rid_max']}")
    print(f"rids_eq_1row={summary['rids_eq_1row']}")
    print(f"rids_eq_2row={summary['rids_eq_2row']}")
    print(f"rids_eq_3to5row={summary['rids_eq_3to5row']}")
    print(f"rids_eq_6to9row={summary['rids_eq_6to9row']}")
    print(f"rids_ge_10row={summary['rids_ge_10row']}")

    return summary


def _build_feature_column_summary(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """
    指定特徴量が最適化データに入っているか、欠損や非0件数を確認する。
    """
    rows = []
    for col in columns:
        if col not in df.columns:
            rows.append(
                {
                    "feature": col,
                    "exists": 0,
                    "non_null": 0,
                    "non_zero": 0,
                    "seed_weight": float(FEATURE_WEIGHTS_SEED.get(col, 0.0)),
                    "fixed_zero": int(col in OPTIMIZER_FIXED_ZERO_FEATURES),
                }
            )
            continue

        values = pd.to_numeric(df[col], errors="coerce")
        rows.append(
            {
                "feature": col,
                "exists": 1,
                "non_null": int(values.notna().sum()),
                "non_zero": int((values.fillna(0.0) != 0.0).sum()),
                "seed_weight": float(FEATURE_WEIGHTS_SEED.get(col, 0.0)),
                "fixed_zero": int(col in OPTIMIZER_FIXED_ZERO_FEATURES),
            }
        )
    return pd.DataFrame(rows)


def _print_course_style_feature_summary(df: pd.DataFrame, label: str) -> pd.DataFrame:
    """今回コース脚質適性関連の特徴量状態をログに出す。"""
    summary_df = _build_feature_column_summary(df, COURSE_STYLE_FEATURE_COLS)
    print(f"\n=== [{label}] course style feature summary ===")
    if summary_df.empty:
        print("summary_empty=1")
    else:
        print(summary_df.to_string(index=False))
    return summary_df


def _build_file_debug_row(
    file_path: Path,
    df_feat: pd.DataFrame,
    train_start_date: str,
    train_end_date: str,
    test_start_date: str,
) -> dict:
    base = {
        "file_name": file_path.name,
        "rows_all": 0,
        "rids_all": 0,
        "rows_per_rid_mean_all": 0.0,
        "rows_per_rid_median_all": 0.0,
        "rows_train": 0,
        "rids_train": 0,
        "rows_per_rid_mean_train": 0.0,
        "rows_per_rid_median_train": 0.0,
        "rows_test": 0,
        "rids_test": 0,
        "rows_per_rid_mean_test": 0.0,
        "rows_per_rid_median_test": 0.0,
        "rids_eq_1row_all": 0,
        "rids_eq_1row_train": 0,
        "rids_eq_1row_test": 0,
    }

    if df_feat is None or df_feat.empty or "rid_str" not in df_feat.columns:
        return base

    dfx = df_feat.copy()

    if "date" not in dfx.columns:
        dfx["date"] = dfx["rid_str"].astype(str).map(lambda x: parse_rid_meta(x, {}).date)

    dfx["date"] = dfx["date"].fillna("").astype(str)

    all_counts = dfx.groupby("rid_str").size().astype(int)
    base["rows_all"] = int(len(dfx))
    base["rids_all"] = int(all_counts.shape[0])
    base["rows_per_rid_mean_all"] = float(all_counts.mean())
    base["rows_per_rid_median_all"] = float(all_counts.median())
    base["rids_eq_1row_all"] = int((all_counts == 1).sum())

    if train_start_date and train_end_date:
        dfx_train = dfx[
            (dfx["date"] >= str(train_start_date)) &
            (dfx["date"] <= str(train_end_date))
        ].copy()
    elif train_end_date:
        dfx_train = dfx[dfx["date"] <= str(train_end_date)].copy()
    else:
        dfx_train = dfx.copy()

    if not dfx_train.empty:
        train_counts = dfx_train.groupby("rid_str").size().astype(int)
        base["rows_train"] = int(len(dfx_train))
        base["rids_train"] = int(train_counts.shape[0])
        base["rows_per_rid_mean_train"] = float(train_counts.mean())
        base["rows_per_rid_median_train"] = float(train_counts.median())
        base["rids_eq_1row_train"] = int((train_counts == 1).sum())

    if test_start_date:
        dfx_test = dfx[dfx["date"] >= str(test_start_date)].copy()
    elif train_end_date:
        dfx_test = dfx[dfx["date"] > str(train_end_date)].copy()
    else:
        dfx_test = pd.DataFrame(columns=dfx.columns)

    if not dfx_test.empty:
        test_counts = dfx_test.groupby("rid_str").size().astype(int)
        base["rows_test"] = int(len(dfx_test))
        base["rids_test"] = int(test_counts.shape[0])
        base["rows_per_rid_mean_test"] = float(test_counts.mean())
        base["rows_per_rid_median_test"] = float(test_counts.median())
        base["rids_eq_1row_test"] = int((test_counts == 1).sum())

    return base


def _print_file_debug_rows(df_file_debug: pd.DataFrame) -> None:
    print("\n=== [FILE DEBUG] per file feature rows ===")
    if df_file_debug is None or df_file_debug.empty:
        print("no file debug rows")
        return

    for _, row in df_file_debug.iterrows():
        print(
            "[FILE] "
            f"{row['file_name']} | "
            f"all: rows={int(row['rows_all'])}, rids={int(row['rids_all'])}, "
            f"mean={float(row['rows_per_rid_mean_all']):.3f}, "
            f"med={float(row['rows_per_rid_median_all']):.3f}, "
            f"eq1={int(row['rids_eq_1row_all'])} | "
            f"train: rows={int(row['rows_train'])}, rids={int(row['rids_train'])}, "
            f"mean={float(row['rows_per_rid_mean_train']):.3f}, "
            f"med={float(row['rows_per_rid_median_train']):.3f}, "
            f"eq1={int(row['rids_eq_1row_train'])} | "
            f"test: rows={int(row['rows_test'])}, rids={int(row['rids_test'])}, "
            f"mean={float(row['rows_per_rid_mean_test']):.3f}, "
            f"med={float(row['rows_per_rid_median_test']):.3f}, "
            f"eq1={int(row['rids_eq_1row_test'])}"
        )


def _should_exclude_file_from_train(file_debug_row: dict) -> tuple[bool, str]:
    """
    train から除外すべき壊れファイルかを判定する。
    test にしか使われないファイルは除外しない。
    """
    rids_train = int(file_debug_row.get("rids_train", 0) or 0)
    rows_mean_train = float(file_debug_row.get("rows_per_rid_mean_train", 0.0) or 0.0)
    rows_median_train = float(file_debug_row.get("rows_per_rid_median_train", 0.0) or 0.0)
    rids_eq_1row_train = int(file_debug_row.get("rids_eq_1row_train", 0) or 0)

    if rids_train <= 0:
        return False, ""

    eq1_ratio = (rids_eq_1row_train / rids_train) if rids_train > 0 else 0.0

    if rows_median_train <= 1.0:
        return True, f"rows_per_rid_median_train<=1.0 ({rows_median_train:.3f})"

    if rows_mean_train < 5.0:
        return True, f"rows_per_rid_mean_train<5.0 ({rows_mean_train:.3f})"

    if eq1_ratio >= 0.80:
        return True, f"rids_eq_1row_train_ratio>=0.80 ({eq1_ratio:.3f})"

    return False, ""


def _split_train_test_with_file_exclusion(
    df_feat_all: pd.DataFrame,
    df_file_debug: pd.DataFrame,
    train_start_date: str,
    train_end_date: str,
    test_start_date: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    file_debug の品質判定を使って、train からだけ壊れたファイルを除外する。
    test は除外しない。
    """
    if df_feat_all is None or df_feat_all.empty:
        empty = pd.DataFrame(columns=[] if df_feat_all is None else df_feat_all.columns)
        return empty.copy(), empty.copy(), pd.DataFrame()

    dfx = df_feat_all.copy()

    if "source_file_name" not in dfx.columns:
        if train_start_date and train_end_date:
            df_train = dfx[
                (dfx["date"] >= train_start_date) &
                (dfx["date"] <= train_end_date)
            ].copy()
        elif train_end_date:
            df_train = dfx[dfx["date"] <= train_end_date].copy()
        else:
            df_train = dfx.copy()

        if test_start_date:
            df_test = dfx[dfx["date"] >= test_start_date].copy()
        elif train_end_date:
            df_test = dfx[dfx["date"] > train_end_date].copy()
        else:
            df_test = pd.DataFrame(columns=dfx.columns)

        return df_train, df_test, pd.DataFrame()

    exclude_rows = []
    exclude_file_names: set[str] = set()

    if df_file_debug is not None and not df_file_debug.empty:
        for _, row in df_file_debug.iterrows():
            row_dict = row.to_dict()
            file_name = str(row_dict.get("file_name", "") or "")
            should_exclude, reason = _should_exclude_file_from_train(row_dict)

            exclude_rows.append(
                {
                    "file_name": file_name,
                    "exclude_from_train": int(should_exclude),
                    "exclude_reason": reason,
                    "rows_train": int(row_dict.get("rows_train", 0) or 0),
                    "rids_train": int(row_dict.get("rids_train", 0) or 0),
                    "rows_per_rid_mean_train": float(row_dict.get("rows_per_rid_mean_train", 0.0) or 0.0),
                    "rows_per_rid_median_train": float(row_dict.get("rows_per_rid_median_train", 0.0) or 0.0),
                    "rids_eq_1row_train": int(row_dict.get("rids_eq_1row_train", 0) or 0),
                }
            )

            if should_exclude and file_name:
                exclude_file_names.add(file_name)

    if train_start_date and train_end_date:
        df_train = dfx[
            (dfx["date"] >= train_start_date) &
            (dfx["date"] <= train_end_date)
        ].copy()
    elif train_end_date:
        df_train = dfx[dfx["date"] <= train_end_date].copy()
    else:
        df_train = dfx.copy()

    if exclude_file_names:
        before_rows = len(df_train)
        before_rids = df_train["rid_str"].astype(str).nunique() if "rid_str" in df_train.columns else 0

        df_train = df_train[~df_train["source_file_name"].astype(str).isin(exclude_file_names)].copy()

        after_rows = len(df_train)
        after_rids = df_train["rid_str"].astype(str).nunique() if "rid_str" in df_train.columns else 0

        print("\n=== [TRAIN FILE EXCLUSION] ===")
        print(f"excluded_file_count={len(exclude_file_names)}")
        print(f"train_rows_before={before_rows}")
        print(f"train_rows_after={after_rows}")
        print(f"train_rids_before={before_rids}")
        print(f"train_rids_after={after_rids}")
        print("excluded_files:")
        for file_name in sorted(exclude_file_names):
            reason_row = next(
                (x for x in exclude_rows if str(x.get('file_name', '')) == file_name),
                None,
            )
            reason = "" if reason_row is None else str(reason_row.get("exclude_reason", ""))
            print(f"  - {file_name} | {reason}")

    if test_start_date:
        df_test = dfx[dfx["date"] >= test_start_date].copy()
    elif train_end_date:
        df_test = dfx[dfx["date"] > train_end_date].copy()
    else:
        df_test = pd.DataFrame(columns=dfx.columns)

    df_exclusion_summary = pd.DataFrame(exclude_rows)
    return df_train, df_test, df_exclusion_summary


def _write_weights_module(out_path: Path, weights_map) -> None:
    """候補・採用済みで共通のPython重みモジュールを書き出す。"""
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("# -*- coding: utf-8 -*-\n")
        f.write('"""自動生成：特徴量重み（時系列採用ゲート対応）"""\n\n')
        f.write("FEATURE_WEIGHTS = {\n")
        f.write('    "__default__": {\n')
        for key in FEAT_COLS:
            f.write(
                f'        "{key}": '
                f'{float(weights_map["__default__"].get(key, 0.0)):.10f},\n'
            )
        f.write("    },\n")
        place_keys = sorted(
            [
                place
                for place in weights_map
                if isinstance(place, str) and place != "__default__"
            ]
        )
        for place_name in place_keys:
            f.write(f"    {place_name!r}: {{\n")
            for key in FEAT_COLS:
                f.write(
                    f'        "{key}": '
                    f'{float(weights_map[place_name].get(key, 0.0)):.10f},\n'
                )
            f.write("    },\n")
        f.write("}\n\n")
        f.write("FEATURE_WEIGHTS_BY_PLACE_SURFACE = {\n")
        place_surface_keys = sorted(
            [
                key
                for key in weights_map
                if isinstance(key, tuple) and len(key) == 2
            ],
            key=lambda value: (value[0], value[1]),
        )
        for place_name, surface_name in place_surface_keys:
            f.write(f"    ({place_name!r}, {surface_name!r}): {{\n")
            for key in FEAT_COLS:
                f.write(
                    f'        "{key}": '
                    f'{float(weights_map[(place_name, surface_name)].get(key, 0.0)):.10f},\n'
                )
            f.write("    },\n")
        f.write("}\n")


def _publish_candidate_and_best(
    candidate_path: Path,
    best_path: Path,
    weights_map,
    decision: str,
    candidate_already_saved: bool = False,
) -> tuple[str, list[str], str, bool]:
    """candidateは保存し、不採用または既存bestありならbestを一切変更しない。"""
    if not candidate_already_saved:
        _write_weights_module(candidate_path, weights_map)
    reasons: list[str] = []
    adopted_path = ""
    best_updated = False
    final_decision = decision
    if decision == "adopted":
        if best_path.exists():
            final_decision = "rejected"
            reasons.append(
                f"同日bestファイルが既に存在するため上書きを拒否: {best_path.name}"
            )
        else:
            shutil.copy2(candidate_path, best_path)
            adopted_path = str(best_path)
            best_updated = True
    return final_decision, reasons, adopted_path, best_updated


def _prepare_candidate_roundtrip(
    memory_weights,
    candidate_path: Path,
    baseline_info: BaselineWeightInfo | None,
) -> dict:
    """candidateを保存・本番同等再読込し、採用評価前の安全判定を返す。"""
    _write_weights_module(candidate_path, memory_weights)
    candidate_file_sha256 = sha256_file(candidate_path)
    reloaded_weights = load_effective_weights_file(candidate_path)
    comparison = compare_weights_maps(memory_weights, reloaded_weights)

    reason_code = ""
    reason = ""
    if not comparison.match:
        reason_code = "candidate_roundtrip_mismatch"
        reason = "candidate保存前後の実効重みが一致しません"
    elif baseline_info is None:
        reason_code = BASELINE_ERROR_CODE
        reason = "baselineを本番と同じ方法で読み込めません"
    elif candidate_file_sha256 == baseline_info.sha256:
        reason_code = "candidate_identical_to_baseline"
        reason = "candidateファイルとbaselineファイルのSHA-256が同一です"
    elif compare_weights_maps(
        reloaded_weights,
        baseline_info.weights_map,
    ).match:
        reason_code = "candidate_effective_weights_identical_to_baseline"
        reason = "candidateとbaselineの再読込後実効WeightsMapが同一です"

    return {
        "candidate_file_sha256": candidate_file_sha256,
        "reloaded_weights": reloaded_weights,
        "memory_candidate_normalized_sha256": comparison.left_normalized_sha256,
        "reloaded_candidate_normalized_sha256": comparison.right_normalized_sha256,
        "baseline_normalized_sha256": (
            normalized_weights_sha256(baseline_info.weights_map)
            if baseline_info is not None
            else ""
        ),
        "candidate_roundtrip_match": comparison.match,
        "differing_group_count": comparison.differing_group_count,
        "differing_feature_count": comparison.differing_feature_count,
        "weight_differences_top100": comparison.differences,
        "max_absolute_difference": comparison.max_absolute_difference,
        "keys_missing_after_save": comparison.missing_from_right,
        "keys_added_on_reload": comparison.added_in_right,
        "reason_code": reason_code,
        "reason": reason,
    }


def _rejection_message(reason_code: str) -> str:
    """採用不可理由をターミナル向けに区別する。"""
    return {
        "valid_gate_failed": "VALIDゲート不合格のため既存best重みは変更しません",
        "test_gate_failed": "TESTゲート不合格のため既存best重みは変更しません",
        BASELINE_ERROR_CODE: "baseline読込失敗のため既存best重みは変更しません",
        "candidate_roundtrip_mismatch": (
            "candidateラウンドトリップ不一致のため既存best重みは変更しません"
        ),
        "candidate_identical_to_baseline": (
            "baselineとcandidateファイルが同一のため更新しません"
        ),
        "candidate_effective_weights_identical_to_baseline": (
            "baselineとcandidateの実効重みが同一のため更新しません"
        ),
        "same_day_best_exists": (
            "採用ゲートは合格しましたが、同日bestファイルが既に存在するため"
            "上書きを拒否しました。"
        ),
        "save_failed": "その他の保存失敗のため既存best重みは変更しません",
    }.get(reason_code, "その他の理由により既存best重みは変更しません")


def _empty_eval_result() -> tuple[float, int, int, int, dict, dict]:
    """未評価期間をeval_success_and_roiへ渡さず、互換形式だけ用意する。"""
    stability = {
        "n_bets": 0,
        "top5_point_rate": 0.0,
        "top3_complete_rate": 0.0,
        "win_in_top5_rate": 0.0,
        "place_in_top5_rate": 0.0,
        "rank1_win_rate": 0.0,
        "rank1_place_rate": 0.0,
        "roi": 0.0,
    }
    return 0.0, 0, 0, 0, {}, stability


def _evaluate_adoption_flow(
    baseline_weights,
    candidate_weights,
    df_train: pd.DataFrame,
    df_valid: pd.DataFrame,
    df_test: pd.DataFrame,
    df_res_entries: pd.DataFrame,
    df_res_payout: pd.DataFrame,
    baseline_ready: bool,
    evaluator=eval_success_and_roi,
) -> dict:
    """baseline/candidateを同一形式で評価し、VALID不合格ならTESTを呼ばない。"""
    train_result = evaluator(
        candidate_weights, df_train, df_res_entries, df_res_payout
    )
    valid_result = evaluator(
        candidate_weights, df_valid, df_res_entries, df_res_payout
    )
    if baseline_ready:
        baseline_train_result = evaluator(
            baseline_weights, df_train, df_res_entries, df_res_payout
        )
        baseline_valid_result = evaluator(
            baseline_weights, df_valid, df_res_entries, df_res_payout
        )
    else:
        baseline_train_result = _empty_eval_result()
        baseline_valid_result = _empty_eval_result()

    valid_passed, reasons, gates = evaluate_valid_gate(
        baseline_valid=baseline_valid_result[5],
        candidate_valid=valid_result[5],
        train_candidate=train_result[5],
        config=CONFIG,
        baseline_ready=baseline_ready,
        baseline_error_code=BASELINE_ERROR_CODE if not baseline_ready else "",
    )
    baseline_test_result = None
    candidate_test_result = None
    test_evaluated = False
    test_not_evaluated_reason = ""
    decision = "rejected"
    if valid_passed:
        baseline_test_result = evaluator(
            baseline_weights, df_test, df_res_entries, df_res_payout
        )
        candidate_test_result = evaluator(
            candidate_weights, df_test, df_res_entries, df_res_payout
        )
        test_evaluated = True
        test_passed, test_reasons, test_gates = evaluate_test_gate(
            baseline_test_result[5], candidate_test_result[5], CONFIG
        )
        reasons.extend(test_reasons)
        gates.update(test_gates)
        decision = "adopted" if test_passed else "rejected"
        reason_code = "" if test_passed else "test_gate_failed"
    else:
        test_not_evaluated_reason = (
            BASELINE_ERROR_CODE if not baseline_ready else "valid_gate_failed"
        )
        reason_code = test_not_evaluated_reason
        gates["test_gate_passed"] = False
        gates["test_evaluated"] = False
    return {
        "decision": decision,
        "reason_code": reason_code,
        "reasons": reasons,
        "gates": gates,
        "valid_passed": valid_passed,
        "test_evaluated": test_evaluated,
        "test_not_evaluated_reason": test_not_evaluated_reason,
        "candidate_train": train_result,
        "candidate_valid": valid_result,
        "candidate_test": candidate_test_result,
        "baseline_train": baseline_train_result,
        "baseline_valid": baseline_valid_result,
        "baseline_test": baseline_test_result,
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """baseline検証と短時間試験用のCLI引数を解釈する。"""
    parser = argparse.ArgumentParser(description="競馬特徴量重みの時系列最適化")
    parser.add_argument("--baseline-weight-file", default="")
    parser.add_argument(
        "--verify-baseline-only",
        "--dry-run",
        action="store_true",
        dest="verify_baseline_only",
    )
    parser.add_argument("--trial-iterations", type=int, default=0)
    parser.add_argument("--trial-max-files-per-period", type=int, default=0)
    parser.add_argument(
        "--validate-train-data-only",
        action="store_true",
        help="特徴量とTRAIN品質ゲートだけを確認し、最適化・candidate保存を行わない",
    )
    return parser.parse_args(argv)


def _verify_split_without_features(files: list[str]) -> dict:
    """dry-run用に結果実績から期間・重複を読み取り、入力ファイル数を補う。"""
    entries, _ = load_results_all_sheets(CONFIG["RESULTS_FILE"])
    rid_to_date = build_rid_to_date_map(CONFIG["RESULTS_FILE"])
    frame = entries.copy()
    frame["date"] = frame["rid_str"].astype(str).map(rid_to_date).fillna("")
    frame["source_file_name"] = ""
    split = split_train_valid_test(
        frame,
        str(CONFIG.get("TRAIN_START_DATE", "") or ""),
        str(CONFIG.get("TRAIN_END_DATE", "") or ""),
        str(CONFIG.get("VALID_START_DATE", "") or ""),
        str(CONFIG.get("VALID_END_DATE", "") or ""),
        str(CONFIG.get("TEST_START_DATE", "") or ""),
        str(CONFIG.get("TEST_END_DATE", "") or ""),
    )
    summary = split.summary
    for label, start_key, end_key in (
        ("train", "TRAIN_START_DATE", "TRAIN_END_DATE"),
        ("valid", "VALID_START_DATE", "VALID_END_DATE"),
        ("test", "TEST_START_DATE", "TEST_END_DATE"),
    ):
        start = str(CONFIG.get(start_key, "") or "")
        end = str(CONFIG.get(end_key, "") or "99999999")
        matching = []
        for raw_path in files:
            name = Path(raw_path).name
            digits = "".join(ch for ch in name if ch.isdigit())
            date = digits[-8:] if len(digits) >= 8 else ""
            if date and start <= date <= end:
                matching.append(name)
        summary[label]["files"] = len(set(matching))
    return summary


def _limit_trial_files(files: list[str], max_per_period: int) -> list[str]:
    """期間境界を変えず、短時間試験で各期間の末尾Nファイルだけを選ぶ。"""
    if max_per_period <= 0:
        return files
    periods = (
        (
            str(CONFIG.get("TRAIN_START_DATE", "") or ""),
            str(CONFIG.get("TRAIN_END_DATE", "") or "99999999"),
        ),
        (
            str(CONFIG.get("VALID_START_DATE", "") or ""),
            str(CONFIG.get("VALID_END_DATE", "") or "99999999"),
        ),
        (
            str(CONFIG.get("TEST_START_DATE", "") or ""),
            str(CONFIG.get("TEST_END_DATE", "") or "99999999"),
        ),
    )
    buckets: list[list[str]] = [[], [], []]
    for raw_path in sorted(files):
        name = Path(raw_path).name
        digits = "".join(ch for ch in name if ch.isdigit())
        date = digits[-8:] if len(digits) >= 8 else ""
        for index, (start, end) in enumerate(periods):
            if date and start <= date <= end:
                buckets[index].append(raw_path)
                break
    selected = [path for bucket in buckets for path in bucket[-max_per_period:]]
    return selected


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    print(f"[INFO] PROJECT_ROOT={PROJECT_ROOT}")
    print(f"[INFO] EXCEL_DIR={EXCEL_DIR}")
    print(f"[INFO] PY_DIR={PY_DIR}")
    print(f"[INFO] RESULTS_FILE={CONFIG['RESULTS_FILE']}")
    print(f"[INFO] RACE_LEVEL_XLSX={RACE_LEVEL_XLSX}")

    if calc_extra_penalty is None or calc_rest_dist_risk is None:
        print("[WARN] 1_keibayosou_penalties.py を import できませんでした。フォールバックで動きます。")
    else:
        print("[INFO] penalties.py を使用します。")

    baseline_info: BaselineWeightInfo | None = None
    baseline_selection = None
    baseline_error = ""
    try:
        baseline_selection = select_baseline_weight_file(args.baseline_weight_file or None)
        baseline_info = load_baseline_weights(baseline_selection, verify_production=True)
        print(f"[INFO] baseline_file={baseline_info.path}")
        print(f"[INFO] baseline_method={baseline_info.method}")
        print(f"[INFO] baseline_sha256={baseline_info.sha256}")
        print(f"[INFO] production_weight_match={baseline_info.production_weights_match}")
    except BaselineWeightError as exc:
        baseline_error = f"{exc.code}: {exc}"
        print(f"[ERROR] {baseline_error}")

    files = discover_files(CONFIG["DATA_GLOB"])
    if not files:
        raise FileNotFoundError(
            "入力の『馬の競走成績_YYYYMMDD.xlsx』が見つかりませんでした。\n"
            f"探したパターン: {CONFIG['DATA_GLOB']}\n"
            "対策:\n"
            "  - EXCEL_DIR（xlsx もしくは data/input）にファイルを置く\n"
            "  - もしくは環境変数 KEIBA_EXCEL_DIR を設定する"
        )
    if args.verify_baseline_only:
        split_summary = _verify_split_without_features(files)
        payload = {
            "mode": "verify-baseline-only",
            "baseline_ok": baseline_info is not None,
            "baseline_error": baseline_error,
            "baseline_file": (
                str(baseline_info.path)
                if baseline_info
                else str(baseline_selection.path) if baseline_selection else ""
            ),
            "baseline_sha256": baseline_info.sha256 if baseline_info else "",
            "baseline_method": baseline_info.method if baseline_info else "",
            "production_file": str(baseline_info.production_path) if baseline_info else "",
            "production_sha256": baseline_info.production_sha256 if baseline_info else "",
            "production_weights_match": (
                baseline_info.production_weights_match if baseline_info else False
            ),
            "common_weight_count": (
                baseline_info.common_weight_count if baseline_info else 0
            ),
            "place_surface_group_count": (
                baseline_info.place_surface_group_count if baseline_info else 0
            ),
            "split_diagnostics": split_summary,
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        if baseline_info is None:
            raise SystemExit(2)
        return

    if args.trial_iterations > 0:
        trial_count = max(1, int(args.trial_iterations))
        CONFIG["N_ITER_DEFAULT"] = trial_count
        CONFIG["N_ITER_PLACE"] = trial_count
        CONFIG["N_ITER_PLACE_SURFACE"] = trial_count
        CONFIG["OPTIMIZER_SEEDS"] = [int(CONFIG.get("RANDOM_SEED", 13))]
        print(f"[INFO] trial_iterations={trial_count}")
    if args.trial_max_files_per_period > 0:
        files = _limit_trial_files(files, int(args.trial_max_files_per_period))
        print(
            "[INFO] trial_file_limit="
            f"{args.trial_max_files_per_period} selected_files={len(files)}"
        )

    train_start_date = str(CONFIG.get("TRAIN_START_DATE", "") or "")
    train_end_date = str(CONFIG.get("TRAIN_END_DATE", "") or "")
    valid_start_date = str(CONFIG.get("VALID_START_DATE", "") or "")
    valid_end_date = str(CONFIG.get("VALID_END_DATE", "") or "")
    test_start_date = str(CONFIG.get("TEST_START_DATE", "") or "")
    test_end_date = str(CONFIG.get("TEST_END_DATE", "") or "")

    df_res_entries, df_res_payout = load_results_all_sheets(CONFIG["RESULTS_FILE"])
    rid_to_date = build_rid_to_date_map(CONFIG["RESULTS_FILE"])

    feat_all = []
    metas = []
    file_debug_rows = []

    for p in files:
        df_feat = build_features_from_one_file(p)

        if df_feat is None or df_feat.empty:
            file_debug_rows.append(
                _build_file_debug_row(
                    Path(p),
                    pd.DataFrame(),
                    train_start_date,
                    train_end_date,
                    test_start_date,
                )
            )
            continue

        df_feat = df_feat.copy()
        df_feat["source_file_name"] = Path(p).name

        file_debug_rows.append(
            _build_file_debug_row(
                Path(p),
                df_feat,
                train_start_date,
                train_end_date,
                test_start_date,
            )
        )
        feat_all.append(df_feat)

        for rid in df_feat["rid_str"].astype(str).unique().tolist():
            meta = parse_rid_meta(rid, rid_to_date)
            metas.append(
                {
                    "rid_str": meta.rid_str,
                    "date": meta.date,
                    "place_code": meta.place_code,
                    "place_name": meta.place_name,
                }
            )

    if not feat_all:
        raise RuntimeError("特徴量が作成できませんでした。入力Excelの列構成を確認してください。")

    df_file_debug = pd.DataFrame(file_debug_rows)
    _print_file_debug_rows(df_file_debug)

    df_feat_all = pd.concat(feat_all, ignore_index=True)
    df_meta = pd.DataFrame(metas).drop_duplicates(subset=["rid_str"])

    print("\n=== [MERGE DEBUG] before meta merge ===")
    print(f"df_feat_all_rows_before_merge={len(df_feat_all)}")
    print(f"df_feat_all_rids_before_merge={df_feat_all['rid_str'].nunique()}")

    df_feat_all = df_feat_all.merge(df_meta, on="rid_str", how="left")

    print("\n=== [MERGE DEBUG] after meta merge ===")
    print(f"df_feat_all_rows_after_merge={len(df_feat_all)}")
    print(f"df_feat_all_rids_after_merge={df_feat_all['rid_str'].nunique()}")

    df_feat_all = _coalesce_merge_columns(df_feat_all, ["date", "place_code", "place_name"])

    print("\n=== [MERGE DEBUG] after _coalesce_merge_columns ===")
    print(f"df_feat_all_rows_after_coalesce={len(df_feat_all)}")
    print(f"df_feat_all_rids_after_coalesce={df_feat_all['rid_str'].nunique()}")

    if "date" not in df_feat_all.columns:
        df_feat_all["date"] = ""
    if "place_code" not in df_feat_all.columns:
        df_feat_all["place_code"] = ""
    if "place_name" not in df_feat_all.columns:
        df_feat_all["place_name"] = ""
    if "surface_name" not in df_feat_all.columns:
        df_feat_all["surface_name"] = ""
    if "source_file_name" not in df_feat_all.columns:
        df_feat_all["source_file_name"] = ""

    df_feat_all["date"] = df_feat_all["date"].fillna("").astype(str)
    df_feat_all["place_code"] = df_feat_all["place_code"].fillna("").astype(str)
    df_feat_all["place_name"] = df_feat_all["place_name"].fillna("").astype(str).str.strip()
    df_feat_all["surface_name"] = df_feat_all["surface_name"].fillna("").map(_normalize_surface_name)
    df_feat_all["source_file_name"] = df_feat_all["source_file_name"].fillna("").astype(str)

    _, _, df_file_exclusion_summary = _split_train_test_with_file_exclusion(
        df_feat_all=df_feat_all,
        df_file_debug=df_file_debug,
        train_start_date=train_start_date,
        train_end_date=train_end_date,
        test_start_date=test_start_date,
    )
    excluded_train_files = set()
    if (
        df_file_exclusion_summary is not None
        and not df_file_exclusion_summary.empty
    ):
        excluded_train_files = set(
            df_file_exclusion_summary.loc[
                df_file_exclusion_summary["exclude_from_train"].eq(1),
                "file_name",
            ].astype(str)
        )
    period_split = split_train_valid_test(
        df_feat_all,
        train_start=train_start_date,
        train_end=train_end_date,
        valid_start=valid_start_date,
        valid_end=valid_end_date,
        test_start=test_start_date,
        test_end=test_end_date,
        excluded_train_files=excluded_train_files,
    )
    df_train = period_split.train
    df_valid = period_split.valid
    df_test = period_split.test

    print(f"[INFO] PAYOUT_CAP_YEN={int(CONFIG.get('PAYOUT_CAP_YEN', 0) or 0)}")
    print(f"[INFO] SKIP_IF_PAYOUT_MISSING={CONFIG['SKIP_IF_PAYOUT_MISSING']}")
    print("[INFO] ROI指標はTOP5評価から除外します")
    print(f"[INFO] SCORE_GAP_MIN={float(CONFIG.get('SCORE_GAP_MIN', 0.0) or 0.0)}")
    print(f"[INFO] WEIGHT_RANGE=({CONFIG['WEIGHT_MIN']}, {CONFIG['WEIGHT_MAX']})")
    print(f"[INFO] RACELEVEL_WEIGHT_RANGE=({CONFIG['RACELEVEL_WEIGHT_MIN']}, {CONFIG['RACELEVEL_WEIGHT_MAX']})")
    print(f"[INFO] OPTIMIZER_SEEDS={CONFIG.get('OPTIMIZER_SEEDS', [CONFIG.get('RANDOM_SEED')])}")
    print(
        "[INFO] OBJECTIVE_WEIGHTS="
        f"top5={CONFIG['OBJ_W_TOP5_POINT_RATE']} "
        f"top3_complete={CONFIG['OBJ_W_TOP3_COMPLETE_RATE']} "
        f"rank1_place={CONFIG['OBJ_W_RANK1_PLACE_RATE']}"
    )
    print(f"[INFO] 特徴量: {len(df_feat_all)} 行 / rid数={df_feat_all['rid_str'].nunique()}")
    print(
        f"[INFO] df_train: {len(df_train)} 行 / rid数={df_train['rid_str'].nunique()} "
        f"TRAIN_START_DATE={train_start_date} TRAIN_END_DATE={train_end_date}"
    )
    print(
        f"[INFO] df_valid: {len(df_valid)} 行 / rid数={df_valid['rid_str'].nunique()} "
        f"VALID_START_DATE={valid_start_date} VALID_END_DATE={valid_end_date}"
    )
    print(
        f"[INFO] df_test:  {len(df_test)} 行 / rid数={df_test['rid_str'].nunique()} "
        f"TEST_START_DATE={test_start_date} TEST_END_DATE={test_end_date or 'latest'}"
    )
    print(
        "[INFO] 期間分割診断="
        + json.dumps(period_split.summary, ensure_ascii=False, sort_keys=True)
    )
    print(f"[INFO] MIN_PLACE_RACES={CONFIG['MIN_PLACE_RACES']} / MIN_PLACE_BETS={CONFIG['MIN_PLACE_BETS']}")
    print(
        f"[INFO] MIN_PLACE_SURFACE_RACES={CONFIG['MIN_PLACE_SURFACE_RACES']} / "
        f"MIN_PLACE_SURFACE_BETS={CONFIG['MIN_PLACE_SURFACE_BETS']}"
    )
    print(f"[INFO] PLACE_BLEND_WITH_DEFAULT={CONFIG['PLACE_BLEND_WITH_DEFAULT']}")
    print(f"[INFO] PLACE_SURFACE_BLEND_WITH_PLACE={CONFIG['PLACE_SURFACE_BLEND_WITH_PLACE']}")
    print(f"[INFO] MIN_WEAKNESS_GROUP_RACES={CONFIG.get('MIN_WEAKNESS_GROUP_RACES', 20)}")

    if df_file_exclusion_summary is not None and not df_file_exclusion_summary.empty:
        excluded_count = int(df_file_exclusion_summary["exclude_from_train"].sum())
        print(f"[INFO] train除外ファイル数={excluded_count}")

    all_course_style_feature_summary = _print_course_style_feature_summary(df_feat_all, "ALL FEATURES")
    train_course_style_feature_summary = _print_course_style_feature_summary(df_train, "TRAIN FEATURES")
    test_course_style_feature_summary = _print_course_style_feature_summary(df_test, "TEST FEATURES")

    all_rid_summary = _print_rid_rows_summary(df_feat_all, "ALL FEATURES")
    train_rid_summary = _print_rid_rows_summary(df_train, "TRAIN FEATURES")
    test_rid_summary = _print_rid_rows_summary(df_test, "TEST FEATURES")

    min_eval_rows = int(CONFIG.get("MIN_EVAL_ROWS_PER_RID", 6) or 6)
    df_clean_all, clean_all_target_summary = _filter_clean_eval_df(
        df_feat_all, df_res_entries, min_rows_per_rid=min_eval_rows
    )
    df_clean_train, clean_train_target_summary = _filter_clean_eval_df(
        df_train, df_res_entries, min_rows_per_rid=min_eval_rows
    )
    df_clean_test, clean_test_target_summary = _filter_clean_eval_df(
        df_test, df_res_entries, min_rows_per_rid=min_eval_rows
    )

    clean_all_target_summary = _print_clean_eval_summary(clean_all_target_summary, "CLEAN ALL")
    clean_train_target_summary = _print_clean_eval_summary(clean_train_target_summary, "CLEAN TRAIN")
    clean_test_target_summary = _print_clean_eval_summary(clean_test_target_summary, "CLEAN TEST")

    clean_all_rid_summary = _print_rid_rows_summary(df_clean_all, "CLEAN ALL FEATURES")
    clean_train_rid_summary = _print_rid_rows_summary(df_clean_train, "CLEAN TRAIN FEATURES")
    clean_test_rid_summary = _print_rid_rows_summary(df_clean_test, "CLEAN TEST FEATURES")

    remaining_bad_train_files = 0
    if (
        df_file_exclusion_summary is not None
        and not df_file_exclusion_summary.empty
    ):
        remaining_bad_train_files = int(
            pd.to_numeric(
                df_file_exclusion_summary["exclude_from_train"],
                errors="coerce",
            ).fillna(0).sum()
        )
    if remaining_bad_train_files:
        raise RuntimeError(
            "TRAIN_DATA_QUALITY_BLOCKED: "
            f"品質不良ファイルが{remaining_bad_train_files}件残っているため、"
            "本格最適化と本番採用を禁止します"
        )
    if args.validate_train_data_only:
        print(
            "[OK] TRAINデータ品質ゲート合格: "
            "最適化・candidate保存・本番採用は実行していません"
        )
        return

    weights_map, place_summary_df = optimize_placewise_weights(
        df_train=df_train,
        df_res_entries=df_res_entries,
        df_res_payout=df_res_payout,
    )

    PY_DIR.mkdir(parents=True, exist_ok=True)
    now = datetime.now()
    today_str = now.strftime("%Y%m%d")
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    candidate_py = PY_DIR / f"candidate_feature_weights_{timestamp}.py"
    adopted_py = PY_DIR / f"best_feature_weights_{today_str}.py"
    roundtrip = _prepare_candidate_roundtrip(
        weights_map,
        candidate_py,
        baseline_info,
    )
    print(f"\n[OK] candidate weights saved: {candidate_py}")
    print(
        "[INFO] candidate_sha256="
        f"{roundtrip['candidate_file_sha256']}"
    )
    print(
        "[INFO] candidate_roundtrip_match="
        f"{roundtrip['candidate_roundtrip_match']}"
    )

    preflight_reason_code = str(roundtrip["reason_code"])
    if preflight_reason_code:
        decision_dir = PROJECT_ROOT / "data" / "output" / "weight_adoption"
        decision_dir.mkdir(parents=True, exist_ok=True)
        decision_path = decision_dir / f"decision_{timestamp}.json"
        best_before_hash = (
            sha256_file(adopted_py) if adopted_py.is_file() else ""
        )
        payload = {
            "decision": "rejected",
            "adopted": False,
            "best_weight_updated": False,
            "reason": [str(roundtrip["reason"])],
            "reason_code": preflight_reason_code,
            "reason_codes": [preflight_reason_code],
            "baseline_weight_file": (
                str(baseline_info.path) if baseline_info else ""
            ),
            "baseline_weight_sha256": (
                baseline_info.sha256 if baseline_info else ""
            ),
            "baseline_normalized_sha256": roundtrip[
                "baseline_normalized_sha256"
            ],
            "baseline_load_error": baseline_error,
            "candidate_weight_file": str(candidate_py.resolve()),
            "candidate_weight_sha256": roundtrip["candidate_file_sha256"],
            "memory_candidate_normalized_sha256": roundtrip[
                "memory_candidate_normalized_sha256"
            ],
            "reloaded_candidate_normalized_sha256": roundtrip[
                "reloaded_candidate_normalized_sha256"
            ],
            "candidate_roundtrip_match": roundtrip[
                "candidate_roundtrip_match"
            ],
            "differing_group_count": roundtrip["differing_group_count"],
            "differing_feature_count": roundtrip["differing_feature_count"],
            "weight_differences_top100": roundtrip[
                "weight_differences_top100"
            ],
            "max_absolute_difference": roundtrip[
                "max_absolute_difference"
            ],
            "keys_missing_after_save": roundtrip["keys_missing_after_save"],
            "keys_added_on_reload": roundtrip["keys_added_on_reload"],
            "valid_gate_passed": False,
            "test_evaluated": False,
            "test_not_evaluated_reason": preflight_reason_code,
            "best_weight_file": str(adopted_py),
            "best_weight_sha256_before": best_before_hash,
            "best_weight_sha256_after": (
                sha256_file(adopted_py) if adopted_py.is_file() else ""
            ),
            "split_diagnostics": period_split.summary,
            "created_at": now.isoformat(timespec="seconds"),
        }
        decision_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        print(f"[INFO] {_rejection_message(preflight_reason_code)}")
        print(f"[OK] adoption decision saved: {decision_path}")
        return

    # 採用ゲート以降は、保存前メモリ重みではなく本番同等再読込後の実効重みだけを使う。
    weights_map = roundtrip["reloaded_weights"]
    flow = _evaluate_adoption_flow(
        baseline_weights=baseline_info.weights_map if baseline_info else None,
        candidate_weights=weights_map,
        df_train=df_train,
        df_valid=df_valid,
        df_test=df_test,
        df_res_entries=df_res_entries,
        df_res_payout=df_res_payout,
        baseline_ready=baseline_info is not None,
    )
    train_s, train_t, train_i, train_r, train_det, train_stab = flow["candidate_train"]
    valid_s, valid_t, valid_i, valid_r, valid_det, valid_stab = flow["candidate_valid"]
    _, _, _, _, _, baseline_train_stab = flow["baseline_train"]
    _, _, _, _, _, baseline_valid_stab = flow["baseline_valid"]
    baseline_test_stab = (
        flow["baseline_test"][5] if flow["baseline_test"] is not None else None
    )
    if flow["candidate_test"] is not None:
        test_s, test_t, test_i, test_r, test_det, test_stab = flow["candidate_test"]
    else:
        test_s, test_t, test_i, test_r, test_det, test_stab = _empty_eval_result()
    valid_gate_passed = bool(flow["valid_passed"])
    test_evaluated = bool(flow["test_evaluated"])
    test_not_evaluated_reason = str(flow["test_not_evaluated_reason"])
    adoption_decision = str(flow["decision"])
    adoption_reasons = list(flow["reasons"])
    adoption_gates = dict(flow["gates"])
    candidate_all_frame = (
        df_feat_all
        if test_evaluated
        else pd.concat([df_train, df_valid], ignore_index=True)
    )
    candidate_clean_all_frame = df_clean_all
    if not test_evaluated:
        candidate_clean_all_frame, _ = _filter_clean_eval_df(
            candidate_all_frame, df_res_entries, min_rows_per_rid=min_eval_rows
        )
    all_s, all_t, all_i, all_r, all_det, all_stab = eval_success_and_roi(
        weights_map, candidate_all_frame, df_res_entries, df_res_payout
    )
    clean_train_s, clean_train_t, clean_train_i, clean_train_r, clean_train_det, clean_train_stab = eval_success_and_roi(
        weights_map, df_clean_train, df_res_entries, df_res_payout
    )
    if test_evaluated:
        clean_test_s, clean_test_t, clean_test_i, clean_test_r, clean_test_det, clean_test_stab = eval_success_and_roi(
            weights_map, df_clean_test, df_res_entries, df_res_payout
        )
    else:
        clean_test_s, clean_test_t, clean_test_i, clean_test_r, clean_test_det, clean_test_stab = _empty_eval_result()
    clean_all_s, clean_all_t, clean_all_i, clean_all_r, clean_all_det, clean_all_stab = eval_success_and_roi(
        weights_map, candidate_clean_all_frame, df_res_entries, df_res_payout
    )

    weakness_min_races = int(CONFIG.get("MIN_WEAKNESS_GROUP_RACES", 20) or 20)
    clean_train_condition_analysis = _build_clean_condition_analysis(
        df_clean_train, clean_train_det, "CLEAN_TRAIN", weakness_min_races
    )
    clean_test_condition_analysis = _build_clean_condition_analysis(
        df_clean_test, clean_test_det, "CLEAN_TEST", weakness_min_races
    )
    clean_all_condition_analysis = _build_clean_condition_analysis(
        df_clean_all, clean_all_det, "CLEAN_ALL", weakness_min_races
    )
    clean_condition_analysis_df = pd.concat(
        [
            clean_train_condition_analysis,
            clean_test_condition_analysis,
            clean_all_condition_analysis,
        ],
        ignore_index=True,
    )

    train_debug = _print_eval_debug_summary(
        label="TRAIN",
        df_target=df_train,
        df_res_entries=df_res_entries,
        df_res_payout=df_res_payout,
        weights_map=weights_map,
    )
    valid_debug = _print_eval_debug_summary(
        label="VALID",
        df_target=df_valid,
        df_res_entries=df_res_entries,
        df_res_payout=df_res_payout,
        weights_map=weights_map,
    )
    if test_evaluated:
        test_debug = _print_eval_debug_summary(
            label="TEST",
            df_target=df_test,
            df_res_entries=df_res_entries,
            df_res_payout=df_res_payout,
            weights_map=weights_map,
        )
    else:
        test_debug = _build_eval_debug_summary(
            df_test.iloc[0:0], df_res_entries, df_res_payout, weights_map
        )
    all_debug = _print_eval_debug_summary(
        label="ALL",
        df_target=df_feat_all,
        df_res_entries=df_res_entries,
        df_res_payout=df_res_payout,
        weights_map=weights_map,
    )
    clean_train_debug = _print_eval_debug_summary(
        label="CLEAN TRAIN",
        df_target=df_clean_train,
        df_res_entries=df_res_entries,
        df_res_payout=df_res_payout,
        weights_map=weights_map,
    )
    if test_evaluated:
        clean_test_debug = _print_eval_debug_summary(
            label="CLEAN TEST",
            df_target=df_clean_test,
            df_res_entries=df_res_entries,
            df_res_payout=df_res_payout,
            weights_map=weights_map,
        )
    else:
        clean_test_debug = _build_eval_debug_summary(
            df_clean_test.iloc[0:0], df_res_entries, df_res_payout, weights_map
        )
    clean_all_debug = _print_eval_debug_summary(
        label="CLEAN ALL",
        df_target=candidate_clean_all_frame,
        df_res_entries=df_res_entries,
        df_res_payout=df_res_payout,
        weights_map=weights_map,
    )

    print("\n=== [TRAIN] place/surface weights applied ===")
    print(f"point_sum={train_s:.3f} / races={train_t}")
    print(f"top5_point_rate={train_stab['top5_point_rate']:.3f}")
    print(f"top3_complete_rate={train_stab['top3_complete_rate']:.3f}")
    print(f"win_in_top5_rate={train_stab['win_in_top5_rate']:.3f}")
    print(f"place_in_top5_rate={train_stab['place_in_top5_rate']:.3f}")

    print("\n=== [TEST] place/surface weights applied ===")
    print(f"point_sum={test_s:.3f} / races={test_t}")
    print(f"top5_point_rate={test_stab['top5_point_rate']:.3f}")
    print(f"top3_complete_rate={test_stab['top3_complete_rate']:.3f}")
    print(f"win_in_top5_rate={test_stab['win_in_top5_rate']:.3f}")
    print(f"place_in_top5_rate={test_stab['place_in_top5_rate']:.3f}")

    print("\n=== [ALL] place/surface weights applied ===")
    print(f"point_sum={all_s:.3f} / races={all_t}")
    print(f"top5_point_rate={all_stab['top5_point_rate']:.3f}")
    print(f"top3_complete_rate={all_stab['top3_complete_rate']:.3f}")
    print(f"win_in_top5_rate={all_stab['win_in_top5_rate']:.3f}")
    print(f"place_in_top5_rate={all_stab['place_in_top5_rate']:.3f}")

    print("\n=== [CLEAN TRAIN] place/surface weights applied ===")
    print(f"point_sum={clean_train_s:.3f} / races={clean_train_t}")
    print(f"top5_point_rate={clean_train_stab['top5_point_rate']:.3f}")
    print(f"top3_complete_rate={clean_train_stab['top3_complete_rate']:.3f}")
    print(f"win_in_top5_rate={clean_train_stab['win_in_top5_rate']:.3f}")
    print(f"place_in_top5_rate={clean_train_stab['place_in_top5_rate']:.3f}")

    print("\n=== [CLEAN TEST] place/surface weights applied ===")
    print(f"point_sum={clean_test_s:.3f} / races={clean_test_t}")
    print(f"top5_point_rate={clean_test_stab['top5_point_rate']:.3f}")
    print(f"top3_complete_rate={clean_test_stab['top3_complete_rate']:.3f}")
    print(f"win_in_top5_rate={clean_test_stab['win_in_top5_rate']:.3f}")
    print(f"place_in_top5_rate={clean_test_stab['place_in_top5_rate']:.3f}")

    print("\n=== [CLEAN ALL] place/surface weights applied ===")
    print(f"point_sum={clean_all_s:.3f} / races={clean_all_t}")
    print(f"top5_point_rate={clean_all_stab['top5_point_rate']:.3f}")
    print(f"top3_complete_rate={clean_all_stab['top3_complete_rate']:.3f}")
    print(f"win_in_top5_rate={clean_all_stab['win_in_top5_rate']:.3f}")
    print(f"place_in_top5_rate={clean_all_stab['place_in_top5_rate']:.3f}")

    _print_weakness_preview(clean_condition_analysis_df, "CLEAN_TEST", limit=10)

    place_eval_rows = []
    evaluation_frame = candidate_all_frame
    all_places = sorted([p for p in evaluation_frame["place_name"].dropna().astype(str).unique().tolist() if p])
    for place_name in all_places:
        place_df_all = evaluation_frame[evaluation_frame["place_name"].astype(str) == place_name].copy()
        if place_df_all.empty:
            continue

        s, t, _, _, _, stab = eval_success_and_roi(
            weights_map, place_df_all, df_res_entries, df_res_payout
        )
        place_eval_rows.append(
            {
                "place_name": place_name,
                "rid_count_all": int(place_df_all["rid_str"].astype(str).nunique()),
                "point_sum": s,
                "total_races": t,
                "top5_point_rate": stab["top5_point_rate"],
                "top3_complete_rate": stab["top3_complete_rate"],
                "win_in_top5_rate": stab["win_in_top5_rate"],
                "place_in_top5_rate": stab["place_in_top5_rate"],
                "has_place_weight": int(place_name in weights_map),
            }
        )
    place_eval_df = pd.DataFrame(place_eval_rows)

    place_surface_eval_rows = []
    all_place_surfaces = sorted(
        [
            (str(place_name or "").strip(), _normalize_surface_name(surface_name))
            for place_name, surface_name in (
                evaluation_frame[["place_name", "surface_name"]]
                .drop_duplicates()
                .itertuples(index=False, name=None)
            )
            if str(place_name or "").strip() and _normalize_surface_name(surface_name)
        ],
        key=lambda x: (x[0], x[1]),
    )
    for place_name, surface_name in all_place_surfaces:
        place_surface_df_all = evaluation_frame[
            (evaluation_frame["place_name"].astype(str) == place_name)
            & (evaluation_frame["surface_name"].map(_normalize_surface_name) == surface_name)
        ].copy()
        if place_surface_df_all.empty:
            continue

        s, t, _, _, _, stab = eval_success_and_roi(
            weights_map, place_surface_df_all, df_res_entries, df_res_payout
        )
        place_surface_eval_rows.append(
            {
                "place_name": place_name,
                "surface_name": surface_name,
                "rid_count_all": int(place_surface_df_all["rid_str"].astype(str).nunique()),
                "point_sum": s,
                "total_races": t,
                "top5_point_rate": stab["top5_point_rate"],
                "top3_complete_rate": stab["top3_complete_rate"],
                "win_in_top5_rate": stab["win_in_top5_rate"],
                "place_in_top5_rate": stab["place_in_top5_rate"],
                "has_place_surface_weight": int((place_name, surface_name) in weights_map),
            }
        )
    place_surface_eval_df = pd.DataFrame(place_surface_eval_rows)

    best_weight_sha256_before = (
        sha256_file(adopted_py) if adopted_py.is_file() else ""
    )
    requested_decision = adoption_decision if baseline_info is not None else "rejected"
    adoption_decision, publish_reasons, adopted_weight_file, best_weight_updated = (
        _publish_candidate_and_best(
            candidate_py,
            adopted_py,
            weights_map,
            requested_decision,
            candidate_already_saved=True,
        )
    )
    adoption_reasons.extend(publish_reasons)
    candidate_sha256 = str(roundtrip["candidate_file_sha256"])

    if best_weight_updated:
        print(f"[OK] adopted weights saved: {adopted_py}")
    else:
        if requested_decision == "adopted" and adopted_py.exists():
            final_reason_code = "same_day_best_exists"
        else:
            final_reason_code = str(flow["reason_code"] or "save_failed")
        print(f"[INFO] {_rejection_message(final_reason_code)}")

    decision_dir = PROJECT_ROOT / "data" / "output" / "weight_adoption"
    decision_dir.mkdir(parents=True, exist_ok=True)
    decision_path = decision_dir / f"decision_{timestamp}.json"
    baseline_record_path = (
        baseline_info.path
        if baseline_info
        else baseline_selection.path if baseline_selection else None
    )
    baseline_record_hash = ""
    baseline_record_mtime = ""
    if baseline_record_path is not None and baseline_record_path.is_file():
        baseline_record_hash = sha256_file(baseline_record_path)
        baseline_record_mtime = datetime.fromtimestamp(
            baseline_record_path.stat().st_mtime
        ).astimezone().isoformat()
    decision_payload = {
        "decision": adoption_decision,
        "adopted": adoption_decision == "adopted",
        "best_weight_updated": best_weight_updated,
        "reason": adoption_reasons,
        "reason_code": (
            ""
            if best_weight_updated
            else final_reason_code
        ),
        "reason_codes": (
            []
            if best_weight_updated
            else [final_reason_code]
        ),
        "baseline_weight_file": str(baseline_record_path or ""),
        "baseline_weight_file_name": (
            baseline_record_path.name if baseline_record_path else ""
        ),
        "baseline_weight_modified_at": (
            baseline_info.modified_at if baseline_info else baseline_record_mtime
        ),
        "baseline_weight_sha256": (
            baseline_info.sha256 if baseline_info else baseline_record_hash
        ),
        "baseline_acquisition_method": (
            baseline_info.method
            if baseline_info
            else baseline_selection.method if baseline_selection else ""
        ),
        "baseline_common_weight_count": (
            baseline_info.common_weight_count if baseline_info else 0
        ),
        "baseline_common_group_count": (
            baseline_info.common_group_count if baseline_info else 0
        ),
        "baseline_place_surface_group_count": (
            baseline_info.place_surface_group_count if baseline_info else 0
        ),
        "baseline_production_file": (
            str(baseline_info.production_path) if baseline_info else ""
        ),
        "baseline_production_sha256": (
            baseline_info.production_sha256 if baseline_info else ""
        ),
        "baseline_production_weights_match": (
            baseline_info.production_weights_match if baseline_info else False
        ),
        "baseline_load_error": baseline_error,
        "candidate_weight_file": str(candidate_py.resolve()),
        "candidate_weight_sha256": candidate_sha256,
        "memory_candidate_normalized_sha256": roundtrip[
            "memory_candidate_normalized_sha256"
        ],
        "reloaded_candidate_normalized_sha256": roundtrip[
            "reloaded_candidate_normalized_sha256"
        ],
        "baseline_normalized_sha256": roundtrip[
            "baseline_normalized_sha256"
        ],
        "candidate_roundtrip_match": roundtrip[
            "candidate_roundtrip_match"
        ],
        "differing_group_count": roundtrip["differing_group_count"],
        "differing_feature_count": roundtrip["differing_feature_count"],
        "weight_differences_top100": roundtrip[
            "weight_differences_top100"
        ],
        "max_absolute_difference": roundtrip[
            "max_absolute_difference"
        ],
        "keys_missing_after_save": roundtrip["keys_missing_after_save"],
        "keys_added_on_reload": roundtrip["keys_added_on_reload"],
        "adopted_weight_file": adopted_weight_file,
        "best_weight_file": str(adopted_py),
        "best_weight_sha256_before": best_weight_sha256_before,
        "best_weight_sha256_after": (
            sha256_file(adopted_py) if adopted_py.is_file() else ""
        ),
        "train_period": {
            "start": train_start_date,
            "end": train_end_date,
            **period_split.summary["train"],
        },
        "valid_period": {
            "start": valid_start_date,
            "end": valid_end_date,
            **period_split.summary["valid"],
        },
        "test_period": {
            "start": test_start_date,
            "end": test_end_date,
            **period_split.summary["test"],
        },
        "baseline_metrics": {
            "train": baseline_train_stab,
            "valid": baseline_valid_stab,
            "test": baseline_test_stab,
        },
        "candidate_metrics": {
            "train": train_stab,
            "valid": valid_stab,
            "test": test_stab if valid_gate_passed else None,
        },
        "valid_gate_passed": valid_gate_passed,
        "test_evaluated": test_evaluated,
        "test_not_evaluated_reason": test_not_evaluated_reason,
        "gate_results": adoption_gates,
        "split_diagnostics": period_split.summary,
        "created_at": now.isoformat(timespec="seconds"),
    }
    decision_path.write_text(
        json.dumps(decision_payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    print(f"[OK] adoption decision saved: {decision_path}")

    EXCEL_DIR.mkdir(parents=True, exist_ok=True)
    if args.trial_iterations > 0 or args.trial_max_files_per_period > 0:
        out_xlsx = EXCEL_DIR / f"success_report_top5hit_trial_{timestamp}.xlsx"
    else:
        out_xlsx = EXCEL_DIR / f"success_report_top5hit_{today_str}.xlsx"

    meta_map = df_meta.set_index("rid_str").to_dict(orient="index")
    rows = []
    for rid, d in all_det.items():
        rid = str(rid)
        m = meta_map.get(rid, {})
        rows.append(
            {
                "rid_str": rid,
                "date": m.get("date", ""),
                "place": m.get("place_name", ""),
                "surface": d.get("surface_name", ""),
                "top5_hit_points": float(d.get("top5_hit_points", 0.0)),
                "top3_complete": int(d.get("top3_complete", 0)),
                "win_in_top5": int(d.get("win_in_top5", 0)),
                "place_capture_rate": float(d.get("place_capture_rate", 0.0)),
                "hit1": int(d.get("hit1", 0)),
                "hit2": int(d.get("hit2", 0)),
                "hit3": int(d.get("hit3", 0)),
                "pred_top5_names": " / ".join(d.get("pred_top5_names", [])),
                "actual_top3_names": " / ".join(d.get("actual_top3_names", [])),
                "actual_top3_nums": str(d.get("actual_top3_nums", "")),
            }
        )

    debug_summary_df = pd.DataFrame(
        [
            {"mode": "TRAIN", **train_debug},
            {"mode": "VALID", **valid_debug},
            {"mode": "TEST", **test_debug},
            {"mode": "ALL", **all_debug},
            {"mode": "CLEAN_TRAIN", **clean_train_debug},
            {"mode": "CLEAN_TEST", **clean_test_debug},
            {"mode": "CLEAN_ALL", **clean_all_debug},
        ]
    )

    rid_row_summary_df = pd.DataFrame(
        [
            all_rid_summary,
            train_rid_summary,
            test_rid_summary,
            clean_all_rid_summary,
            clean_train_rid_summary,
            clean_test_rid_summary,
        ]
    )

    course_style_feature_summary_df = pd.concat(
        [
            all_course_style_feature_summary.assign(mode="ALL"),
            train_course_style_feature_summary.assign(mode="TRAIN"),
            test_course_style_feature_summary.assign(mode="TEST"),
        ],
        ignore_index=True,
    )

    clean_target_summary_df = pd.DataFrame(
        [
            {"mode": "CLEAN_ALL", **clean_all_target_summary},
            {"mode": "CLEAN_TRAIN", **clean_train_target_summary},
            {"mode": "CLEAN_TEST", **clean_test_target_summary},
        ]
    )

    eval_summary_rows = [
        ("TRAIN", train_s, train_t, train_stab),
        ("VALID", valid_s, valid_t, valid_stab),
        ("TEST", test_s, test_t, test_stab),
        ("ALL", all_s, all_t, all_stab),
        ("CLEAN_TRAIN", clean_train_s, clean_train_t, clean_train_stab),
        ("CLEAN_TEST", clean_test_s, clean_test_t, clean_test_stab),
        ("CLEAN_ALL", clean_all_s, clean_all_t, clean_all_stab),
    ]
    eval_summary_df = pd.DataFrame(
        [
            {
                "mode": mode,
                "point_sum": point_sum,
                "total_races": total_races,
                "top5_point_rate": stab["top5_point_rate"],
                "top3_complete_rate": stab["top3_complete_rate"],
                "win_in_top5_rate": stab["win_in_top5_rate"],
                "place_in_top5_rate": stab["place_in_top5_rate"],
                "rank1_win_rate": stab.get("rank1_win_rate", 0.0),
                "rank1_place_rate": stab.get("rank1_place_rate", 0.0),
            }
            for mode, point_sum, total_races, stab in eval_summary_rows
        ]
    )
    roi_related_cols = [
        "roi",
        "invest",
        "return",
        "invest_yen",
        "return_yen",
        "best_roi",
        "best_invest",
        "best_return",
    ]
    debug_summary_df = debug_summary_df.drop(columns=roi_related_cols, errors="ignore")
    place_summary_export_df = place_summary_df.drop(columns=roi_related_cols, errors="ignore")

    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        pd.DataFrame(rows).to_excel(writer, sheet_name="report", index=False)

        pd.DataFrame(
            [
                {
                    "mode": "TRAIN",
                    "point_sum": train_s,
                    "total_races": train_t,
                    "top5_point_rate": train_stab["top5_point_rate"],
                    "top3_complete_rate": train_stab["top3_complete_rate"],
                    "win_in_top5_rate": train_stab["win_in_top5_rate"],
                    "place_in_top5_rate": train_stab["place_in_top5_rate"],
                }
            ]
        ).to_excel(writer, sheet_name="train_summary", index=False)

        pd.DataFrame(
            [
                {
                    "mode": "TEST",
                    "point_sum": test_s,
                    "total_races": test_t,
                    "top5_point_rate": test_stab["top5_point_rate"],
                    "top3_complete_rate": test_stab["top3_complete_rate"],
                    "win_in_top5_rate": test_stab["win_in_top5_rate"],
                    "place_in_top5_rate": test_stab["place_in_top5_rate"],
                }
            ]
        ).to_excel(writer, sheet_name="test_summary", index=False)

        pd.DataFrame(
            [
                {
                    "mode": "ALL",
                    "point_sum": all_s,
                    "total_races": all_t,
                    "top5_point_rate": all_stab["top5_point_rate"],
                    "top3_complete_rate": all_stab["top3_complete_rate"],
                    "win_in_top5_rate": all_stab["win_in_top5_rate"],
                    "place_in_top5_rate": all_stab["place_in_top5_rate"],
                }
            ]
        ).to_excel(writer, sheet_name="all_summary", index=False)

        eval_summary_df.to_excel(writer, sheet_name="eval_summary", index=False)
        clean_target_summary_df.to_excel(writer, sheet_name="clean_target_summary", index=False)
        clean_condition_analysis_df.to_excel(writer, sheet_name="clean_condition_analysis", index=False)
        debug_summary_df.to_excel(writer, sheet_name="debug_summary", index=False)
        rid_row_summary_df.to_excel(writer, sheet_name="rid_row_summary", index=False)
        course_style_feature_summary_df.to_excel(writer, sheet_name="course_style_features", index=False)
        df_file_debug.to_excel(writer, sheet_name="file_debug", index=False)

        if df_file_exclusion_summary is not None and not df_file_exclusion_summary.empty:
            df_file_exclusion_summary.to_excel(writer, sheet_name="file_exclusion_summary", index=False)
        else:
            pd.DataFrame(
                columns=[
                    "file_name",
                    "exclude_from_train",
                    "exclude_reason",
                    "rows_train",
                    "rids_train",
                    "rows_per_rid_mean_train",
                    "rows_per_rid_median_train",
                    "rids_eq_1row_train",
                ]
            ).to_excel(writer, sheet_name="file_exclusion_summary", index=False)

        place_summary_export_df.to_excel(writer, sheet_name="place_opt_summary", index=False)
        place_eval_df.to_excel(writer, sheet_name="place_eval_all", index=False)
        place_surface_eval_df.to_excel(writer, sheet_name="place_surface_eval_all", index=False)

    print(f"[OK] report saved: {out_xlsx}")


if __name__ == "__main__":
    main()
