# -*- coding: utf-8 -*-
"""旧bestと新bestを、同一特徴量・同一レース集合で比較する検証専用スクリプト。"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from tokutyouryou_keisann.adoption import split_train_valid_test
from tokutyouryou_keisann.baseline import load_effective_weights_file, sha256_file
from tokutyouryou_keisann.common import (
    _coalesce_merge_columns,
    _normalize_combo,
    _normalize_result_columns,
    _normalize_surface_name,
    _norm_name,
    build_rid_to_date_map,
    discover_files,
    find_col,
    load_results_all_sheets,
    parse_rid_meta,
    resolve_duplicate_feature_races,
)
from tokutyouryou_keisann.config import CONFIG, FEAT_COLS, PROJECT_ROOT, RACE_LEVEL_XLSX
from tokutyouryou_keisann.features import build_features_from_one_file
from tokutyouryou_keisann.runner import (
    _build_file_debug_row,
    _split_train_test_with_file_exclusion,
)
from tokutyouryou_keisann.scoring import (
    build_eval_context,
    compute_scores_with_optimizer_weights,
    eval_success_and_roi,
)


MODEL_FILES = {
    "旧best_20260730": PROJECT_ROOT / "yosou_py" / "best_feature_weights_20260730.py",
    "新best_20260817": PROJECT_ROOT / "yosou_py" / "best_feature_weights_20260817.py",
}


def _json_default(value: Any) -> Any:
    """NumPy・pandas型をJSONへ安全に変換する。"""
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if pd.isna(value):
        return None
    raise TypeError(f"JSONへ変換できない型です: {type(value)!r}")


def _write_json(path: Path, payload: Any) -> None:
    """検証結果JSONをUTF-8で保存する。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )


def _hash_frame_columns(df: pd.DataFrame) -> str:
    """DataFrameの列順を監査用SHAへ変換する。"""
    joined = "\n".join(str(column) for column in df.columns)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def _build_or_load_feature_bundle(cache_path: Path, reuse_cache: bool) -> dict[str, Any]:
    """本番最適化runnerと同じ入口から特徴量を一度だけ構築する。"""
    if reuse_cache and cache_path.exists():
        print(f"[INFO] 特徴量キャッシュを再利用します: {cache_path}")
        return pd.read_pickle(cache_path)

    files = discover_files(CONFIG["DATA_GLOB"])
    if not files:
        raise FileNotFoundError(f"入力ファイルがありません: {CONFIG['DATA_GLOB']}")

    df_res_entries, df_res_payout = load_results_all_sheets(CONFIG["RESULTS_FILE"])
    rid_to_date = build_rid_to_date_map(CONFIG["RESULTS_FILE"])
    train_start = str(CONFIG.get("TRAIN_START_DATE", "") or "")
    train_end = str(CONFIG.get("TRAIN_END_DATE", "") or "")
    test_start = str(CONFIG.get("TEST_START_DATE", "") or "")

    feature_frames: list[pd.DataFrame] = []
    metas: list[dict[str, str]] = []
    file_debug_rows: list[dict[str, Any]] = []

    for index, source in enumerate(files, start=1):
        print(f"[INFO] 特徴量作成 {index}/{len(files)}: {Path(source).name}", flush=True)
        feature_df = build_features_from_one_file(source)
        if feature_df is None or feature_df.empty:
            file_debug_rows.append(
                _build_file_debug_row(
                    Path(source), pd.DataFrame(), train_start, train_end, test_start
                )
            )
            continue

        feature_df = feature_df.copy()
        feature_df["source_file_name"] = Path(source).name
        file_debug_rows.append(
            _build_file_debug_row(
                Path(source), feature_df, train_start, train_end, test_start
            )
        )
        feature_frames.append(feature_df)

        for rid in feature_df["rid_str"].astype(str).unique().tolist():
            meta = parse_rid_meta(rid, rid_to_date)
            metas.append(
                {
                    "rid_str": meta.rid_str,
                    "date": meta.date,
                    "place_code": meta.place_code,
                    "place_name": meta.place_name,
                }
            )

    if not feature_frames:
        raise RuntimeError("特徴量を1件も作成できませんでした")

    feature_all = pd.concat(feature_frames, ignore_index=True)
    meta_df = pd.DataFrame(metas).drop_duplicates(subset=["rid_str"])
    feature_all = feature_all.merge(meta_df, on="rid_str", how="left")
    feature_all = _coalesce_merge_columns(
        feature_all, ["date", "place_code", "place_name"]
    )

    for column, default in {
        "date": "",
        "place_code": "",
        "place_name": "",
        "surface_name": "",
        "source_file_name": "",
    }.items():
        if column not in feature_all.columns:
            feature_all[column] = default

    feature_all["date"] = feature_all["date"].fillna("").astype(str)
    feature_all["place_code"] = feature_all["place_code"].fillna("").astype(str)
    feature_all["place_name"] = (
        feature_all["place_name"].fillna("").astype(str).str.strip()
    )
    feature_all["surface_name"] = (
        feature_all["surface_name"].fillna("").map(_normalize_surface_name)
    )
    feature_all["source_file_name"] = (
        feature_all["source_file_name"].fillna("").astype(str)
    )

    feature_all, duplicate_summary = resolve_duplicate_feature_races(feature_all)
    file_debug = pd.DataFrame(file_debug_rows)
    _, _, file_exclusion = _split_train_test_with_file_exclusion(
        df_feat_all=feature_all,
        df_file_debug=file_debug,
        train_start_date=train_start,
        train_end_date=train_end,
        test_start_date=test_start,
    )
    excluded_train_files: set[str] = set()
    if file_exclusion is not None and not file_exclusion.empty:
        excluded_train_files = set(
            file_exclusion.loc[
                file_exclusion["exclude_from_train"].eq(1), "file_name"
            ].astype(str)
        )

    period_split = split_train_valid_test(
        feature_all,
        train_start=str(CONFIG.get("TRAIN_START_DATE", "") or ""),
        train_end=str(CONFIG.get("TRAIN_END_DATE", "") or ""),
        valid_start=str(CONFIG.get("VALID_START_DATE", "") or ""),
        valid_end=str(CONFIG.get("VALID_END_DATE", "") or ""),
        test_start=str(CONFIG.get("TEST_START_DATE", "") or ""),
        test_end=str(CONFIG.get("TEST_END_DATE", "") or ""),
        excluded_train_files=excluded_train_files,
    )

    bundle = {
        "feature_all": feature_all,
        "results_entries": df_res_entries,
        "results_payout": df_res_payout,
        "files": files,
        "file_debug": file_debug,
        "file_exclusion": file_exclusion,
        "duplicate_summary": duplicate_summary,
        "split_summary": period_split.summary,
    }
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    pd.to_pickle(bundle, cache_path)
    print(f"[OK] 特徴量キャッシュ保存: {cache_path}")
    return bundle


def _load_official_popularity(results_path: Path) -> pd.DataFrame:
    """結果Excelの人気を、事後セグメント分析専用に抽出する。"""
    rows: list[pd.DataFrame] = []
    with pd.ExcelFile(results_path, engine="openpyxl") as excel:
        for sheet_name in excel.sheet_names:
            raw = pd.read_excel(excel, sheet_name=sheet_name, header=0)
            if raw is None or raw.empty:
                continue
            worksheet = excel.book[sheet_name]
            raw_header = next(
                worksheet.iter_rows(
                    min_row=1,
                    max_row=1,
                    min_col=1,
                    max_col=len(raw.columns),
                    values_only=True,
                ),
                tuple(raw.columns),
            )
            normalized = raw.copy()
            normalized.columns = _normalize_result_columns(
                list(raw_header), xlsx_path=results_path, sheet_name=sheet_name
            )
            columns = list(normalized.columns)
            rid_col = find_col(columns, ["レースID", "race_id"])
            uma_col = find_col(columns, ["馬番", "馬 番"])
            pop_col = _select_official_popularity_column(columns)
            if not rid_col or not uma_col or not pop_col:
                continue
            use = normalized[[rid_col, uma_col, pop_col]].copy()
            use["rid_str"] = use[rid_col].astype(str)
            use["馬番_int"] = pd.to_numeric(use[uma_col], errors="coerce").astype("Int64")
            use["official_popularity"] = _official_popularity_to_numeric(
                use[pop_col]
            )
            rows.append(use[["rid_str", "馬番_int", "official_popularity"]])

    if not rows:
        return pd.DataFrame(columns=["rid_str", "馬番_int", "official_popularity"])
    output = pd.concat(rows, ignore_index=True)
    output = output.dropna(subset=["馬番_int"]).drop_duplicates(
        subset=["rid_str", "馬番_int"], keep="first"
    )
    return output


def _select_official_popularity_column(columns: list[str]) -> str | None:
    """主結果側の数値人気を、払戻側の文字列人気より優先して選ぶ。"""
    return find_col(columns, ["人 気", "単勝人気", "人気"])


def _official_popularity_to_numeric(series: pd.Series) -> pd.Series:
    """数値または「1人気」形式から、確定人気の整数部分を抽出する。"""
    popularity_text = series.astype("string").str.extract(r"(\d+)", expand=False)
    return pd.to_numeric(popularity_text, errors="coerce")


def _period_label(date_value: Any) -> str:
    """設定済みTRAIN/VALID/TEST境界から期間名を返す。"""
    date_text = re.sub(r"\D", "", str(date_value))[:8]
    if not date_text:
        return "OUTSIDE"
    train_start = str(CONFIG.get("TRAIN_START_DATE", "") or "")
    train_end = str(CONFIG.get("TRAIN_END_DATE", "") or "")
    valid_start = str(CONFIG.get("VALID_START_DATE", "") or "")
    valid_end = str(CONFIG.get("VALID_END_DATE", "") or "")
    test_start = str(CONFIG.get("TEST_START_DATE", "") or "")
    test_end = str(CONFIG.get("TEST_END_DATE", "") or "")
    if train_start <= date_text <= train_end:
        return "TRAIN"
    if valid_start <= date_text <= valid_end:
        return "VALID"
    if date_text >= test_start and (not test_end or date_text <= test_end):
        return "TEST"
    return "OUTSIDE"


def _register_pipeline_aliases() -> Any:
    """数字始まりの本番モジュールを既存alias名で読み込めるようにする。"""
    import importlib
    import sys

    aliases = [
        ("keibayosou_config", "1_keibayosou_config"),
        ("keibayosou_utils", "1_keibayosou_utils"),
        ("keibayosou_course_style", "1_keibayosou_course_style"),
        ("keibayosou_loaders", "1_keibayosou_loaders"),
        ("keibayosou_features", "1_keibayosou_features"),
        ("keibayosou_penalties", "1_keibayosou_penalties"),
        ("keibayosou_ranking", "1_keibayosou_ranking"),
    ]
    for alias, actual in aliases:
        if alias not in sys.modules:
            sys.modules[alias] = importlib.import_module(actual)
    return importlib.import_module("1_keibayosou_pipeline")


def _race_prediction_records(
    model_name: str,
    scored: pd.DataFrame,
    results_entries: pd.DataFrame,
    official_popularity: pd.DataFrame,
    judge_rank: Any,
) -> pd.DataFrame:
    """一意順位と実績を1レース1行へ照合する。"""
    actual = results_entries.copy()
    actual["rid_str"] = actual["rid_str"].astype(str)
    actual["name_norm"] = actual["name_norm"].astype(str)
    pop_map = {
        (str(row.rid_str), int(row.馬番_int)): float(row.official_popularity)
        for row in official_popularity.itertuples(index=False)
        if pd.notna(row.馬番_int) and pd.notna(row.official_popularity)
    }

    rows: list[dict[str, Any]] = []
    for rid, race in scored.groupby("rid_str", sort=True):
        race_sorted = race.sort_values("rank", kind="mergesort")
        if len(race_sorted) < 5:
            continue
        actual_race = actual[actual["rid_str"].eq(str(rid))].sort_values(
            "着順_num", kind="mergesort"
        )
        actual_top3 = actual_race[actual_race["着順_num"].isin([1.0, 2.0, 3.0])]
        if len(actual_top3) < 3 or actual_top3["馬番_int"].notna().sum() < 3:
            continue

        top5 = race_sorted.head(5)
        top5_names = top5["name_norm"].fillna("").astype(str).tolist()
        actual_names = actual_top3["name_norm"].fillna("").astype(str).tolist()[:3]
        hit_flags = [int(name in set(top5_names)) for name in actual_names]
        points = int(hit_flags[0] * 3 + hit_flags[1] * 2 + hit_flags[2])
        rank1 = race_sorted.iloc[0]
        rank1_name = str(rank1.get("name_norm", "") or "")
        rank1_actual = actual_race[actual_race["name_norm"].eq(rank1_name)]
        finish = (
            float(rank1_actual["着順_num"].iloc[0])
            if not rank1_actual.empty and pd.notna(rank1_actual["着順_num"].iloc[0])
            else np.nan
        )
        uma1 = int(rank1["馬番"]) if pd.notna(rank1.get("馬番")) else None
        top_scores = pd.to_numeric(race_sorted["score"], errors="coerce").tolist()
        score1 = float(top_scores[0]) if len(top_scores) >= 1 and pd.notna(top_scores[0]) else np.nan
        score2 = float(top_scores[1]) if len(top_scores) >= 2 and pd.notna(top_scores[1]) else score1
        gap12 = score1 - score2 if np.isfinite(score1) and np.isfinite(score2) else np.nan
        score_rank2 = top_scores[1] if len(top_scores) >= 2 else np.nan
        score_rank5 = top_scores[4] if len(top_scores) >= 5 else np.nan
        dango = (
            float(score_rank2 - score_rank5)
            if pd.notna(score_rank2) and pd.notna(score_rank5)
            else np.nan
        )
        rank3 = race_sorted.iloc[2]
        rank_label, judgment, reason = judge_rank(
            score1=score1,
            gap12=gap12,
            dango_2_5=dango,
            rank1_master_rating_field_percentile=rank1.get(
                "master_rating_field_percentile"
            ),
            rank1_recent3_time_idx=rank1.get("recent3_time_idx"),
            rank1_extra_penalty=rank1.get("extra_penalty"),
            rank3_popularity=None,
            rank3_score=rank3.get("score"),
            rank3_extra_penalty=rank3.get("extra_penalty"),
        )
        date_text = re.sub(r"\D", "", str(rank1.get("date", "")))[:8]
        month = date_text[:6] if len(date_text) == 8 else ""
        rows.append(
            {
                "model": model_name,
                "rid_str": str(rid),
                "date": date_text,
                "month": month,
                "period": _period_label(date_text),
                "place_name": str(rank1.get("place_name", "") or ""),
                "surface_name": str(rank1.get("surface_name", "") or ""),
                "field_size": float(rank1.get("field_size")) if pd.notna(rank1.get("field_size")) else float(len(race_sorted)),
                "horse_count": int(len(race_sorted)),
                "top5_points": points,
                "top5_point_rate_race": points / 6.0,
                "top3_complete": int(all(hit_flags)),
                "win_in_top5": hit_flags[0],
                "place_capture_rate": sum(hit_flags) / 3.0,
                "rank1_win": int(rank1_name == actual_names[0]),
                "rank1_place": int(rank1_name in set(actual_names)),
                "rank1_finish": finish,
                "rank1_name": rank1_name,
                "rank1_umaban": uma1,
                "rank1_official_popularity": pop_map.get((str(rid), int(uma1))) if uma1 is not None else np.nan,
                "rank1_score": score1,
                "rank2_score": score2,
                "gap12": gap12,
                "dango_2_5": dango,
                "rank1_favorite_risk": float(rank1.get("favorite_risk", 0.0) or 0.0),
                "rank1_rest_dist_risk": float(rank1.get("rest_dist_risk", 0.0) or 0.0),
                "rank1_extra_penalty": float(rank1.get("extra_penalty", 0.0) or 0.0),
                "rank3_umaban": int(rank3["馬番"]) if pd.notna(rank3.get("馬番")) else None,
                "rank3_name": str(rank3.get("name_norm", "") or ""),
                "rank3_score": float(rank3.get("score")) if pd.notna(rank3.get("score")) else np.nan,
                "rank3_extra_penalty": float(rank3.get("extra_penalty", 0.0) or 0.0),
                "bet_rank": rank_label,
                "bet_judgment": judgment,
                "bet_rank_reason": reason,
                "rank2_umaban": int(race_sorted.iloc[1]["馬番"]),
                "rank4_umaban": int(race_sorted.iloc[3]["馬番"]),
                "rank5_umaban": int(race_sorted.iloc[4]["馬番"]),
            }
        )
    return pd.DataFrame(rows)


def _summarize_prediction(group: pd.DataFrame) -> dict[str, Any]:
    """レース単位の予測指標を絶対件数と率へ集計する。"""
    races = len(group)
    horse_count = int(group["horse_count"].sum()) if races else 0
    return {
        "race_count": races,
        "horse_count": horse_count,
        "top5_weighted_points": int(group["top5_points"].sum()) if races else 0,
        "top5_max_points": races * 6,
        "top5_point_rate": float(group["top5_points"].sum() / (races * 6)) if races else np.nan,
        "top3_complete_count": int(group["top3_complete"].sum()) if races else 0,
        "top3_complete_rate": float(group["top3_complete"].mean()) if races else np.nan,
        "rank1_win_count": int(group["rank1_win"].sum()) if races else 0,
        "rank1_win_rate": float(group["rank1_win"].mean()) if races else np.nan,
        "rank1_place_count": int(group["rank1_place"].sum()) if races else 0,
        "rank1_place_rate": float(group["rank1_place"].mean()) if races else np.nan,
        "rank1_avg_finish": float(group["rank1_finish"].mean()) if races else np.nan,
        "rank1_finish_count": int(group["rank1_finish"].notna().sum()) if races else 0,
        "win_in_top5_rate": float(group["win_in_top5"].mean()) if races else np.nan,
        "place_in_top5_rate": float(group["place_capture_rate"].mean()) if races else np.nan,
    }


def _prediction_summaries(records: pd.DataFrame) -> pd.DataFrame:
    """TRAIN/VALID/TEST/ALLとTEST月別の予測表を作る。"""
    rows: list[dict[str, Any]] = []
    for model, model_df in records.groupby("model", sort=False):
        for period in ["TRAIN", "VALID", "TEST", "ALL"]:
            use = model_df if period == "ALL" else model_df[model_df["period"].eq(period)]
            rows.append({"model": model, "period": period, **_summarize_prediction(use)})
        test_df = model_df[model_df["period"].eq("TEST")]
        for month, month_df in test_df.groupby("month", sort=True):
            rows.append(
                {"model": model, "period": f"TEST_{month}", **_summarize_prediction(month_df)}
            )
    return pd.DataFrame(rows)


def _paired_bootstrap_ci(values: np.ndarray, seed: int, iterations: int = 5000) -> tuple[float, float]:
    """レース対応を保った平均差の95%ブートストラップ区間を返す。"""
    clean = values[np.isfinite(values)]
    if len(clean) < 2:
        return np.nan, np.nan
    rng = np.random.default_rng(seed)
    means = np.empty(iterations, dtype=float)
    chunk = 250
    for start in range(0, iterations, chunk):
        count = min(chunk, iterations - start)
        picks = rng.integers(0, len(clean), size=(count, len(clean)))
        means[start : start + count] = clean[picks].mean(axis=1)
    low, high = np.quantile(means, [0.025, 0.975])
    return float(low), float(high)


def _prediction_differences(records: pd.DataFrame) -> pd.DataFrame:
    """新best－旧bestの対応レース差と信頼区間を作る。"""
    old_name, new_name = list(MODEL_FILES.keys())
    old = records[records["model"].eq(old_name)].set_index("rid_str")
    new = records[records["model"].eq(new_name)].set_index("rid_str")
    common = old.index.intersection(new.index)
    if len(common) != len(old) or len(common) != len(new):
        raise RuntimeError(
            f"モデル間の評価レース集合が不一致です: old={len(old)} new={len(new)} common={len(common)}"
        )
    rows: list[dict[str, Any]] = []
    metric_map = {
        "top5_point_rate": "top5_point_rate_race",
        "top3_complete_rate": "top3_complete",
        "rank1_win_rate": "rank1_win",
        "rank1_place_rate": "rank1_place",
        "rank1_avg_finish": "rank1_finish",
    }
    for period in ["TRAIN", "VALID", "TEST", "ALL"]:
        period_rids = common if period == "ALL" else common[old.loc[common, "period"].eq(period)]
        for metric, column in metric_map.items():
            differences = (
                pd.to_numeric(new.loc[period_rids, column], errors="coerce").to_numpy(dtype=float)
                - pd.to_numeric(old.loc[period_rids, column], errors="coerce").to_numpy(dtype=float)
            )
            low, high = _paired_bootstrap_ci(
                differences, seed=20260817 + len(rows), iterations=5000
            )
            rows.append(
                {
                    "period": period,
                    "metric": metric,
                    "race_count": int(np.isfinite(differences).sum()),
                    "new_minus_old": float(np.nanmean(differences)),
                    "ci95_low": low,
                    "ci95_high": high,
                }
            )
    return pd.DataFrame(rows)


def _paired_average_finish(records: pd.DataFrame) -> pd.DataFrame:
    """両モデルとも着順がある同一レースだけで、平均着順の絶対値を比較する。"""
    old_name, new_name = list(MODEL_FILES.keys())
    old = records[records["model"].eq(old_name)].set_index("rid_str")
    new = records[records["model"].eq(new_name)].set_index("rid_str")
    common = old.index.intersection(new.index)
    if len(common) != len(old) or len(common) != len(new):
        raise RuntimeError(
            f"平均着順の評価レース集合が不一致です: old={len(old)} new={len(new)} common={len(common)}"
        )

    rows: list[dict[str, Any]] = []
    for period in ["TRAIN", "VALID", "TEST", "ALL"]:
        period_rids = common if period == "ALL" else common[old.loc[common, "period"].eq(period)]
        old_finish = pd.to_numeric(
            old.loc[period_rids, "rank1_finish"], errors="coerce"
        ).to_numpy(dtype=float)
        new_finish = pd.to_numeric(
            new.loc[period_rids, "rank1_finish"], errors="coerce"
        ).to_numpy(dtype=float)
        paired_mask = np.isfinite(old_finish) & np.isfinite(new_finish)
        rows.append(
            {
                "period": period,
                "common_race_count": int(paired_mask.sum()),
                "old_avg_finish": float(old_finish[paired_mask].mean()),
                "new_avg_finish": float(new_finish[paired_mask].mean()),
                "new_minus_old": float(
                    (new_finish[paired_mask] - old_finish[paired_mask]).mean()
                ),
            }
        )
    return pd.DataFrame(rows)


def _official_popularity_segment(value: Any) -> str:
    """確定人気を事後成績用の区分へ変換する。"""
    if pd.isna(value):
        return "不明"
    number = int(value)
    if number == 1:
        return "1番人気"
    if number <= 3:
        return "2-3番人気"
    if number <= 5:
        return "4-5番人気"
    if number <= 9:
        return "6-9番人気"
    return "10番人気以下"


def _segment_prediction(records: pd.DataFrame) -> pd.DataFrame:
    """予想1位の確定人気別成績を作る。"""
    work = records.copy()
    work["segment"] = work["rank1_official_popularity"].map(_official_popularity_segment)
    rows: list[dict[str, Any]] = []
    for (model, period, segment), group in work.groupby(
        ["model", "period", "segment"], dropna=False, sort=False
    ):
        rows.append(
            {
                "model": model,
                "period": period,
                "segment": segment,
                **_summarize_prediction(group),
            }
        )
    return pd.DataFrame(rows)


def _payout_lookup(payout: pd.DataFrame) -> dict[tuple[str, str, str], int]:
    """結果払戻をレース・券種・組番の辞書へ変換する。"""
    lookup: dict[tuple[str, str, str], int] = {}
    for row in payout.itertuples(index=False):
        bet_type = str(row.払戻種別)
        if bet_type in {"nan", "None", ""}:
            continue
        key = (str(row.rid_str), bet_type, str(row.組番_norm))
        value = int(row.払戻金_int)
        if value > 0:
            lookup[key] = max(value, lookup.get(key, 0))
    return lookup


def _load_market_snapshots() -> tuple[pd.DataFrame, pd.DataFrame]:
    """保存済み事前OZZU CSVを読み、人気順位と取得時刻監査を作る。"""
    import importlib

    loaders = importlib.import_module("1_keibayosou_loaders")
    odds_dir = PROJECT_ROOT / "data" / "ozzu_csv"
    snapshot_rows: list[pd.DataFrame] = []
    audit_rows: list[dict[str, Any]] = []
    for source in sorted(odds_dir.glob("OZZU_*.csv")):
        match = re.search(r"(\d{8})", source.name)
        if not match:
            continue
        date_text = match.group(1)
        odds = loaders.load_odds_csv(str(odds_dir), raceday=date_text)
        if odds.empty:
            continue
        odds = odds.copy()
        odds["date"] = odds["date"].astype(str)
        odds["race_no"] = odds["race_no"].astype(str).str.zfill(2)
        odds["tansho"] = pd.to_numeric(odds["tansho"], errors="coerce")
        odds["snapshot_popularity"] = odds.groupby(
            ["date", "place", "race_no"], sort=False
        )["tansho"].rank(method="min", ascending=True)
        snapshot_rows.append(odds)
        audit_rows.append(
            {
                "file": str(source.resolve()),
                "date": date_text,
                "modified_at": datetime.fromtimestamp(source.stat().st_mtime).isoformat(),
                "rows": len(odds),
                "race_count": int(odds.groupby(["date", "place", "race_no"]).ngroups),
            }
        )
    snapshots = (
        pd.concat(snapshot_rows, ignore_index=True)
        if snapshot_rows
        else pd.DataFrame(
            columns=[
                "date",
                "place",
                "race_no",
                "umaban",
                "name_norm",
                "tansho",
                "fukusho",
                "snapshot_popularity",
            ]
        )
    )
    return snapshots, pd.DataFrame(audit_rows)


def _market_snapshot_popularity_map(snapshots: pd.DataFrame) -> dict[tuple[str, str, str, int], float]:
    """日付・場所・R番号・馬番から事前人気を引ける辞書を作る。"""
    output: dict[tuple[str, str, str, int], float] = {}
    for row in snapshots.itertuples(index=False):
        if pd.isna(row.umaban) or pd.isna(row.snapshot_popularity):
            continue
        key = (str(row.date), str(row.place), str(row.race_no).zfill(2), int(row.umaban))
        output[key] = float(row.snapshot_popularity)
    return output


def _trifecta_combos(row: pd.Series) -> list[str]:
    """現行回収率重視シートと同じ3点を返す。"""
    rank1 = int(row["rank1_umaban"])
    rank2 = int(row["rank2_umaban"])
    rank3 = int(row["rank3_umaban"])
    rank4 = int(row["rank4_umaban"])
    rank5 = int(row["rank5_umaban"])
    return [
        _normalize_combo(f"{rank1}-{rank3}-{rank2}"),
        _normalize_combo(f"{rank1}-{rank3}-{rank4}"),
        _normalize_combo(f"{rank1}-{rank3}-{rank5}"),
    ]


def _is_roi_focus_eligible(
    row: pd.Series,
    snapshot_popularity_map: dict[tuple[str, str, str, int], float],
    rule: dict[str, Any],
) -> tuple[bool, float | None]:
    """本番回収率重視シートと同じ購入条件を判定する。"""
    race_no = str(row["rid_str"])[-2:]
    key = (
        str(row["date"]),
        str(row["place_name"]),
        race_no,
        int(row["rank3_umaban"]),
    )
    popularity = snapshot_popularity_map.get(key)
    if popularity is None:
        return False, None
    eligible = (
        float(row["rank1_score"]) >= float(rule["score1_min"])
        and float(row["dango_2_5"]) >= float(rule["dango_2_5_min"])
        and float(row["gap12"]) >= float(rule["gap12_min"])
        and str(row["surface_name"]) in set(rule["allowed_surfaces"])
        and float(popularity) <= float(rule["rank3_popularity_max"])
        and float(row["rank3_extra_penalty"])
        < float(rule["rank3_extra_penalty_max_exclusive"])
        and float(row["rank3_score"]) >= float(rule["rank3_score_min"])
    )
    return bool(eligible), float(popularity)


def _make_bet_rows(
    records: pd.DataFrame,
    payout_lookup: dict[tuple[str, str, str], int],
    snapshots: pd.DataFrame,
    roi_rule: dict[str, Any],
) -> pd.DataFrame:
    """単勝・複勝・S/A/B別3連複・現行3連複の購入明細を作る。"""
    market_map = _market_snapshot_popularity_map(snapshots)
    rows: list[dict[str, Any]] = []
    for _, record in records.iterrows():
        common = {
            "model": record["model"],
            "rid_str": record["rid_str"],
            "date": record["date"],
            "period": record["period"],
            "month": record["month"],
            "bet_rank": record["bet_rank"],
            "gap12": record["gap12"],
            "rank1_official_popularity": record["rank1_official_popularity"],
            "field_size": record["field_size"],
            "rank1_favorite_risk": record["rank1_favorite_risk"],
            "rank1_rest_dist_risk": record["rank1_rest_dist_risk"],
        }
        uma1_combo = _normalize_combo(str(int(record["rank1_umaban"])))
        for strategy, bet_type in [("予想1位単勝", "単勝"), ("予想1位複勝", "複勝")]:
            payout = payout_lookup.get((str(record["rid_str"]), bet_type, uma1_combo), 0)
            rows.append(
                {
                    **common,
                    "strategy": strategy,
                    "ticket_no": 1,
                    "combo": uma1_combo,
                    "stake": 100,
                    "payout": payout,
                    "hit": int(payout > 0),
                    "market_snapshot_available": False,
                    "rank3_snapshot_popularity": np.nan,
                }
            )

        combos = _trifecta_combos(record)
        if record["bet_rank"] in {"S", "A", "B"}:
            strategy = f"{record['bet_rank']}ランク_3連複3点"
            for ticket_no, combo in enumerate(combos, start=1):
                payout = payout_lookup.get((str(record["rid_str"]), "3連複", combo), 0)
                rows.append(
                    {
                        **common,
                        "strategy": strategy,
                        "ticket_no": ticket_no,
                        "combo": combo,
                        "stake": 100,
                        "payout": payout,
                        "hit": int(payout > 0),
                        "market_snapshot_available": False,
                        "rank3_snapshot_popularity": np.nan,
                    }
                )

        roi_eligible, rank3_pop = _is_roi_focus_eligible(record, market_map, roi_rule)
        if roi_eligible:
            for ticket_no, combo in enumerate(combos, start=1):
                payout = payout_lookup.get((str(record["rid_str"]), "3連複", combo), 0)
                rows.append(
                    {
                        **common,
                        "strategy": "現行回収率重視_3連複3点",
                        "ticket_no": ticket_no,
                        "combo": combo,
                        "stake": 100,
                        "payout": payout,
                        "hit": int(payout > 0),
                        "market_snapshot_available": True,
                        "rank3_snapshot_popularity": rank3_pop,
                    }
                )
    output = pd.DataFrame(rows)
    output["profit"] = output["payout"] - output["stake"]
    return output


def _max_losing_streak(race_profit: pd.Series) -> int:
    """購入レース単位の最大連敗数を返す。"""
    maximum = 0
    current = 0
    for value in race_profit.tolist():
        if float(value) <= -1.0:
            current += 1
            maximum = max(maximum, current)
        else:
            current = 0
    return maximum


def _max_drawdown(race_profit: pd.Series) -> float:
    """購入レース単位の累積収支から最大ドローダウンを返す。"""
    cumulative = race_profit.cumsum().to_numpy(dtype=float)
    if len(cumulative) == 0:
        return 0.0
    path = np.concatenate([[0.0], cumulative])
    peaks = np.maximum.accumulate(path)
    return float(np.max(peaks - path))


def _summarize_bets(group: pd.DataFrame) -> dict[str, Any]:
    """購入明細を収支・連敗・ドローダウン・黒字日へ集計する。"""
    if group.empty:
        return {
            "purchase_count": 0,
            "bet_race_count": 0,
            "hit_count": 0,
            "hit_rate": np.nan,
            "total_stake": 0,
            "total_payout": 0,
            "profit": 0,
            "roi": np.nan,
            "avg_profit_per_race": np.nan,
            "max_losing_streak_races": 0,
            "max_drawdown": 0,
            "bet_days": 0,
            "profitable_days": 0,
            "profitable_day_rate": np.nan,
        }
    ordered = group.sort_values(["date", "rid_str", "ticket_no"], kind="mergesort")
    race_profit = ordered.groupby(["date", "rid_str"], sort=True)["profit"].sum()
    day_profit = ordered.groupby("date", sort=True)["profit"].sum()
    stake = int(ordered["stake"].sum())
    payout = int(ordered["payout"].sum())
    purchase_count = int(len(ordered))
    hit_count = int(ordered["hit"].sum())
    return {
        "purchase_count": purchase_count,
        "bet_race_count": int(len(race_profit)),
        "hit_count": hit_count,
        "hit_rate": hit_count / purchase_count if purchase_count else np.nan,
        "total_stake": stake,
        "total_payout": payout,
        "profit": payout - stake,
        "roi": payout / stake if stake else np.nan,
        "avg_profit_per_race": float((payout - stake) / len(race_profit)) if len(race_profit) else np.nan,
        "max_losing_streak_races": _max_losing_streak(race_profit),
        "max_drawdown": _max_drawdown(race_profit),
        "bet_days": int(len(day_profit)),
        "profitable_days": int((day_profit > 0).sum()),
        "profitable_day_rate": float((day_profit > 0).mean()) if len(day_profit) else np.nan,
    }


def _bet_summaries(bets: pd.DataFrame) -> pd.DataFrame:
    """券種・期間別の収支比較表を作る。"""
    rows: list[dict[str, Any]] = []
    for (model, strategy), model_strategy in bets.groupby(["model", "strategy"], sort=False):
        for period in ["TRAIN", "VALID", "TEST", "ALL"]:
            use = (
                model_strategy
                if period == "ALL"
                else model_strategy[model_strategy["period"].eq(period)]
            )
            rows.append(
                {
                    "model": model,
                    "strategy": strategy,
                    "period": period,
                    **_summarize_bets(use),
                }
            )
    return pd.DataFrame(rows)


def _condition_label(condition: str, value: Any) -> str:
    """条件別分析用の固定binを返す。"""
    if pd.isna(value):
        return "不明"
    number = float(value)
    if condition == "gap12":
        if number < 1:
            return "<1"
        if number < 2:
            return "1-<2"
        if number < 3:
            return "2-<3"
        if number < 5:
            return "3-<5"
        return ">=5"
    if condition == "予想1位人気":
        return _official_popularity_segment(number)
    if condition == "頭数":
        if number <= 10:
            return "<=10"
        if number <= 13:
            return "11-13"
        if number <= 16:
            return "14-16"
        return ">=17"
    if condition in {"favorite_risk", "rest_dist_risk"}:
        if number == 0:
            return "0"
        if number < 1:
            return "0-<1"
        if number < 2:
            return "1-<2"
        return ">=2"
    return str(value)


def _condition_profit(bets: pd.DataFrame) -> pd.DataFrame:
    """指定された条件別に券種収支を分解する。"""
    mappings = {
        "gap12": "gap12",
        "予想1位人気": "rank1_official_popularity",
        "頭数": "field_size",
        "favorite_risk": "rank1_favorite_risk",
        "rest_dist_risk": "rank1_rest_dist_risk",
    }
    rows: list[dict[str, Any]] = []
    for condition, column in mappings.items():
        work = bets.copy()
        work["condition_value"] = work[column].map(
            lambda value: _condition_label(condition, value)
        )
        for (model, strategy, period, value), group in work.groupby(
            ["model", "strategy", "period", "condition_value"], sort=False
        ):
            rows.append(
                {
                    "model": model,
                    "strategy": strategy,
                    "period": period,
                    "condition": condition,
                    "condition_value": value,
                    **_summarize_bets(group),
                }
            )
    return pd.DataFrame(rows)


def _daily_walk_forward(records: pd.DataFrame, bets: pd.DataFrame) -> pd.DataFrame:
    """日付順の固定モデル診断を作る。再最適化は一切行わない。"""
    prediction_daily = (
        records.groupby(["model", "date", "period"], sort=True)
        .agg(
            race_count=("rid_str", "nunique"),
            horse_count=("horse_count", "sum"),
            top5_points=("top5_points", "sum"),
            top3_complete_count=("top3_complete", "sum"),
            rank1_win_count=("rank1_win", "sum"),
            rank1_place_count=("rank1_place", "sum"),
            rank1_avg_finish=("rank1_finish", "mean"),
        )
        .reset_index()
    )
    prediction_daily["top5_point_rate"] = prediction_daily["top5_points"] / (
        prediction_daily["race_count"] * 6
    )
    prediction_daily["top3_complete_rate"] = (
        prediction_daily["top3_complete_count"] / prediction_daily["race_count"]
    )
    prediction_daily["rank1_win_rate"] = (
        prediction_daily["rank1_win_count"] / prediction_daily["race_count"]
    )
    prediction_daily["rank1_place_rate"] = (
        prediction_daily["rank1_place_count"] / prediction_daily["race_count"]
    )

    profit_daily = (
        bets.groupby(["model", "date", "strategy"], sort=True)
        .agg(total_stake=("stake", "sum"), total_payout=("payout", "sum"))
        .reset_index()
    )
    profit_daily["profit"] = profit_daily["total_payout"] - profit_daily["total_stake"]
    profit_pivot = profit_daily.pivot_table(
        index=["model", "date"],
        columns="strategy",
        values="profit",
        aggfunc="sum",
    ).reset_index()
    profit_pivot.columns = [
        str(column) if not isinstance(column, tuple) else "_".join(map(str, column))
        for column in profit_pivot.columns
    ]
    return prediction_daily.merge(profit_pivot, on=["model", "date"], how="left")


def _audit_test_reuse() -> tuple[pd.DataFrame, dict[str, Any]]:
    """採用判断JSONからTEST明示評価回数と補助的利用を数える。"""
    decision_dir = PROJECT_ROOT / "data" / "output" / "weight_adoption"
    rows: list[dict[str, Any]] = []
    for path in sorted(decision_dir.glob("decision_*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        gate_results = payload.get("gate_results") or {}
        explicit = bool(payload.get("test_evaluated") is True or gate_results.get("test_evaluated") is True)
        aggregate_pre_gate = bool(
            path.name == "decision_20260728_221000.json"
            and payload.get("test_period")
            and payload.get("candidate_metrics")
        )
        test_metrics_present = bool(
            isinstance(payload.get("baseline_metrics"), dict)
            and isinstance(payload["baseline_metrics"].get("test"), dict)
            and isinstance(payload.get("candidate_metrics"), dict)
            and isinstance(payload["candidate_metrics"].get("test"), dict)
        )
        test_period = payload.get("test_period") or {}
        rows.append(
            {
                "decision_file": path.name,
                "created_at": payload.get("created_at", ""),
                "decision": payload.get("decision", ""),
                "adopted": bool(payload.get("adopted") is True),
                "test_start": test_period.get("start", ""),
                "test_end": test_period.get("end", ""),
                "test_races_source": test_period.get("races", ""),
                "test_evaluated_explicit": explicit,
                "test_metrics_present": test_metrics_present,
                "aggregate_pre_gate_possible_test_use": aggregate_pre_gate,
            }
        )
    frame = pd.DataFrame(rows)
    summary = {
        "explicit_test_evaluation_count": int(frame["test_evaluated_explicit"].sum()),
        "explicit_or_test_metrics_count": int(
            (frame["test_evaluated_explicit"] | frame["test_metrics_present"]).sum()
        ),
        "additional_aggregate_pre_gate_count": int(
            frame["aggregate_pre_gate_possible_test_use"].sum()
        ),
        "test_reused_for_model_selection": bool(frame["test_evaluated_explicit"].sum() > 1),
        "virgin_holdout_exists_after_latest_adoption": False,
    }
    return frame, summary


def _selector_audit() -> dict[str, Any]:
    """本番とbaselineの選択基準を別々に記録する。"""
    import importlib

    production_config = importlib.import_module("1_keibayosou_config")
    latest = production_config._find_latest_weights_module(
        str(PROJECT_ROOT / "yosou_py")
    )
    if latest is None:
        raise RuntimeError("本番bestを選択できません")
    from tokutyouryou_keisann.baseline import select_baseline_weight_file

    baseline_selection = select_baseline_weight_file(None)
    return {
        "production_selected_file": str(Path(latest[1]).resolve()),
        "production_selected_date": latest[0],
        "production_selection_rule": "best_feature_weights_YYYYMMDD.pyのファイル名日付最大",
        "production_uses_adoption_json": False,
        "optimizer_baseline_selected_file": str(baseline_selection.path.resolve()),
        "optimizer_baseline_selection_method": baseline_selection.method,
        "optimizer_baseline_uses_adoption_json_first": True,
    }


def _weight_audit(weights_by_model: dict[str, dict[Any, dict[str, float]]]) -> pd.DataFrame:
    """実効重みのグループ数・要素数・非0数を監査する。"""
    rows: list[dict[str, Any]] = []
    for model, weights_map in weights_by_model.items():
        values = [float(value) for group in weights_map.values() for value in group.values()]
        rows.append(
            {
                "model": model,
                "file": str(MODEL_FILES[model].resolve()),
                "file_sha256": sha256_file(MODEL_FILES[model]),
                "group_count": len(weights_map),
                "element_count": len(values),
                "nonzero_count": int(sum(abs(value) > 0 for value in values)),
                "feature_count_config": len(FEAT_COLS),
                "dl_rank_score_nonzero_groups": int(
                    sum(abs(float(group.get("dl_rank_score", 0.0))) > 0 for group in weights_map.values())
                ),
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    """比較処理を実行し、Excel作成前の監査用CSV/JSONを出力する。"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "outputs" / "best_weights_profit_comparison_20260817"),
    )
    parser.add_argument("--reuse-cache", action="store_true")
    args = parser.parse_args()

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    cache_path = output_dir / "feature_bundle.pkl"
    bundle = _build_or_load_feature_bundle(cache_path, args.reuse_cache)
    feature_all: pd.DataFrame = bundle["feature_all"]
    results_entries: pd.DataFrame = bundle["results_entries"]
    results_payout: pd.DataFrame = bundle["results_payout"]

    # OUTSIDEを除外し、両モデルへ完全に同じDataFrameを渡す。
    comparison_features = feature_all[
        feature_all["date"].map(_period_label).isin(["TRAIN", "VALID", "TEST"])
    ].copy()
    official_popularity = _load_official_popularity(Path(CONFIG["RESULTS_FILE"]))
    pipeline = _register_pipeline_aliases()
    judge_rank = pipeline._judge_bet_rank
    roi_rule = dict(pipeline.FIVE_BLOCK_BET_RULE)

    weights_by_model = {
        model: load_effective_weights_file(path) for model, path in MODEL_FILES.items()
    }
    context = build_eval_context(comparison_features, results_entries, results_payout)
    model_records: list[pd.DataFrame] = []
    adoption_reproduction_rows: list[dict[str, Any]] = []

    for model, weights in weights_by_model.items():
        print(f"[INFO] 同一EvalContextでスコア計算: {model}")
        scored = compute_scores_with_optimizer_weights(
            comparison_features,
            weights,
            eval_context=context,
        )
        model_records.append(
            _race_prediction_records(
                model, scored, results_entries, official_popularity, judge_rank
            )
        )

        for period in ["TRAIN", "VALID", "TEST"]:
            period_df = comparison_features[
                comparison_features["date"].map(_period_label).eq(period)
            ].copy()
            period_context = build_eval_context(period_df, results_entries, results_payout)
            _, _, _, _, _, stability = eval_success_and_roi(
                weights,
                period_df,
                results_entries,
                results_payout,
                eval_context=period_context,
            )
            adoption_reproduction_rows.append(
                {"model": model, "period": period, **stability}
            )

    records = pd.concat(model_records, ignore_index=True)
    prediction_summary = _prediction_summaries(records)
    prediction_difference = _prediction_differences(records)
    paired_average_finish = _paired_average_finish(records)
    popularity_segment = _segment_prediction(records)

    snapshots, snapshot_audit = _load_market_snapshots()
    bets = _make_bet_rows(
        records,
        _payout_lookup(results_payout),
        snapshots,
        roi_rule,
    )
    bet_summary = _bet_summaries(bets)
    condition_profit = _condition_profit(bets)
    walk_forward = _daily_walk_forward(records, bets)
    test_reuse, test_reuse_summary = _audit_test_reuse()
    selector = _selector_audit()
    weight_audit = _weight_audit(weights_by_model)

    old_name, new_name = list(MODEL_FILES.keys())
    test_old = prediction_summary[
        prediction_summary["model"].eq(old_name)
        & prediction_summary["period"].eq("TEST")
    ].iloc[0]
    test_new = prediction_summary[
        prediction_summary["model"].eq(new_name)
        & prediction_summary["period"].eq("TEST")
    ].iloc[0]
    test_ratio_explanation = {
        "rank1_place": {
            "old_count": int(test_old["rank1_place_count"]),
            "new_count": int(test_new["rank1_place_count"]),
            "race_count": int(test_old["race_count"]),
            "old_rate": float(test_old["rank1_place_rate"]),
            "new_rate": float(test_new["rank1_place_rate"]),
            "ratio": float(test_new["rank1_place_rate"] / test_old["rank1_place_rate"]),
        },
        "top5_point": {
            "old_points": int(test_old["top5_weighted_points"]),
            "new_points": int(test_new["top5_weighted_points"]),
            "max_points": int(test_old["top5_max_points"]),
            "old_rate": float(test_old["top5_point_rate"]),
            "new_rate": float(test_new["top5_point_rate"]),
            "ratio": float(test_new["top5_point_rate"] / test_old["top5_point_rate"]),
        },
        "top3_complete": {
            "old_count": int(test_old["top3_complete_count"]),
            "new_count": int(test_new["top3_complete_count"]),
            "race_count": int(test_old["race_count"]),
            "old_rate": float(test_old["top3_complete_rate"]),
            "new_rate": float(test_new["top3_complete_rate"]),
            "ratio": float(test_new["top3_complete_rate"] / test_old["top3_complete_rate"]),
        },
    }

    audit = {
        "selector": selector,
        "comparison_models": {
            model: {
                "path": str(path.resolve()),
                "sha256": sha256_file(path),
            }
            for model, path in MODEL_FILES.items()
        },
        "feature_input": {
            "source_file_count": len(bundle["files"]),
            "rows": len(comparison_features),
            "races": int(comparison_features["rid_str"].nunique()),
            "columns": list(comparison_features.columns),
            "column_count": len(comparison_features.columns),
            "columns_sha256": _hash_frame_columns(comparison_features),
            "scoring_feature_columns": list(FEAT_COLS),
            "same_dataframe_object_used_for_both_models": True,
        },
        "results_data_flow": {
            "result_entry_columns": list(results_entries.columns),
            "payout_columns": list(results_payout.columns),
            "prediction_score_uses_results_or_payout": False,
            "results_used_after_ranking_for_evaluation": True,
            "official_popularity_used_only_for_posthoc_segment": True,
        },
        "leakage_audit": {
            "future_or_same_date_past_races_excluded": True,
            "future_or_same_date_ratings_excluded": True,
            "current_input_has_final_odds_columns": False,
            "final_odds_used_for_score_or_rank": False,
            "result_or_payout_used_for_score_or_rank": False,
            "dl_model_reads_result_workbook": True,
            "dl_score_bonus": 0.0,
            "dl_rank_score_disabled_and_zero_in_both_models": bool(
                weight_audit["dl_rank_score_nonzero_groups"].eq(0).all()
            ),
            "dl_result_training_has_ranking_effect": False,
            "market_snapshot_dates_for_current_trifecta": sorted(
                snapshots["date"].astype(str).unique().tolist()
            ),
            "market_snapshot_used_only_for_bet_filter_not_prediction_rank": True,
            "no_market_snapshot_dates_excluded_symmetrically_from_roi_focus_bet": True,
        },
        "split_summary": bundle["split_summary"],
        "duplicate_race_resolution_count": len(bundle["duplicate_summary"]),
        "test_reuse_summary": test_reuse_summary,
        "test_ratio_explanation": test_ratio_explanation,
        "paired_average_finish": paired_average_finish.to_dict(orient="records"),
        "roi_focus_rule": roi_rule,
        "holdout_assessment": {
            "unused_final_holdout_exists": False,
            "reason": "20260501以降のTESTは複数の採用判断で利用され、直近データも20260817採用判断に含まれるため",
            "forward_proposal": "20260822以降を固定HOLDOUTとし、最低300レースまたは8開催日が蓄積するまで重み・閾値・買い目条件を変更しない",
        },
    }

    outputs = {
        "prediction_race_records.csv": records,
        "prediction_summary.csv": prediction_summary,
        "prediction_difference_ci.csv": prediction_difference,
        "paired_average_finish.csv": paired_average_finish,
        "rank1_popularity_segment.csv": popularity_segment,
        "bet_ticket_records.csv": bets,
        "bet_summary.csv": bet_summary,
        "condition_profit.csv": condition_profit,
        "walk_forward_daily.csv": walk_forward,
        "test_reuse_audit.csv": test_reuse,
        "market_snapshot_audit.csv": snapshot_audit,
        "weight_audit.csv": weight_audit,
        "adoption_metric_reproduction.csv": pd.DataFrame(adoption_reproduction_rows),
    }
    for name, frame in outputs.items():
        frame.to_csv(output_dir / name, index=False, encoding="utf-8-sig")
    _write_json(output_dir / "comparison_audit.json", audit)
    _write_json(
        output_dir / "comparison_manifest.json",
        {
            "created_at": datetime.now().isoformat(),
            "output_dir": str(output_dir),
            "files": sorted([name for name in outputs] + ["comparison_audit.json"]),
        },
    )
    print(f"[OK] 比較中間成果物を出力しました: {output_dir}")


if __name__ == "__main__":
    main()
