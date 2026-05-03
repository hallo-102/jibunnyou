# runner.py
# -*- coding: utf-8 -*-
from __future__ import annotations

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
    CONFIG,
    EXCEL_DIR,
    FEAT_COLS,
    PROJECT_ROOT,
    PY_DIR,
    RACE_LEVEL_XLSX,
    calc_extra_penalty,
    calc_rest_dist_risk,
)
from .features import build_features_from_one_file
from .optimizer import optimize_placewise_weights
from .scoring import eval_success_and_roi


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


def main() -> None:
    print(f"[INFO] PROJECT_ROOT={PROJECT_ROOT}")
    print(f"[INFO] EXCEL_DIR={EXCEL_DIR}")
    print(f"[INFO] PY_DIR={PY_DIR}")
    print(f"[INFO] RESULTS_FILE={CONFIG['RESULTS_FILE']}")
    print(f"[INFO] RACE_LEVEL_XLSX={RACE_LEVEL_XLSX}")

    if calc_extra_penalty is None or calc_rest_dist_risk is None:
        print("[WARN] keibayosou_penalties.py を import できませんでした。フォールバックで動きます。")
    else:
        print("[INFO] penalties.py を使用します。")

    files = discover_files(CONFIG["DATA_GLOB"])
    if not files:
        raise FileNotFoundError(
            "入力の『馬の競走成績_YYYYMMDD.xlsx』が見つかりませんでした。\n"
            f"探したパターン: {CONFIG['DATA_GLOB']}\n"
            "対策:\n"
            "  - EXCEL_DIR（xlsx もしくは data/input）にファイルを置く\n"
            "  - もしくは環境変数 KEIBA_EXCEL_DIR を設定する"
        )

    train_start_date = str(CONFIG.get("TRAIN_START_DATE", "") or "")
    train_end_date = str(CONFIG.get("TRAIN_END_DATE", "") or "")
    test_start_date = str(CONFIG.get("TEST_START_DATE", "") or "")

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

    df_train, df_test, df_file_exclusion_summary = _split_train_test_with_file_exclusion(
        df_feat_all=df_feat_all,
        df_file_debug=df_file_debug,
        train_start_date=train_start_date,
        train_end_date=train_end_date,
        test_start_date=test_start_date,
    )

    print(f"[INFO] PAYOUT_CAP_YEN={int(CONFIG.get('PAYOUT_CAP_YEN', 0) or 0)}")
    print(f"[INFO] SKIP_IF_PAYOUT_MISSING={CONFIG['SKIP_IF_PAYOUT_MISSING']}")
    print("[INFO] ROI指標はTOP5評価から除外します")
    print(f"[INFO] SCORE_GAP_MIN={float(CONFIG.get('SCORE_GAP_MIN', 0.0) or 0.0)}")
    print(f"[INFO] WEIGHT_RANGE=({CONFIG['WEIGHT_MIN']}, {CONFIG['WEIGHT_MAX']})")
    print(f"[INFO] RACELEVEL_WEIGHT_RANGE=({CONFIG['RACELEVEL_WEIGHT_MIN']}, {CONFIG['RACELEVEL_WEIGHT_MAX']})")
    print(f"[INFO] 特徴量: {len(df_feat_all)} 行 / rid数={df_feat_all['rid_str'].nunique()}")
    print(
        f"[INFO] df_train: {len(df_train)} 行 / rid数={df_train['rid_str'].nunique()} "
        f"TRAIN_START_DATE={train_start_date} TRAIN_END_DATE={train_end_date}"
    )
    print(
        f"[INFO] df_test:  {len(df_test)} 行 / rid数={df_test['rid_str'].nunique()} "
        f"TEST_START_DATE={test_start_date}"
    )
    print(f"[INFO] MIN_PLACE_RACES={CONFIG['MIN_PLACE_RACES']} / MIN_PLACE_BETS={CONFIG['MIN_PLACE_BETS']}")
    print(
        f"[INFO] MIN_PLACE_SURFACE_RACES={CONFIG['MIN_PLACE_SURFACE_RACES']} / "
        f"MIN_PLACE_SURFACE_BETS={CONFIG['MIN_PLACE_SURFACE_BETS']}"
    )
    print(f"[INFO] PLACE_BLEND_WITH_DEFAULT={CONFIG['PLACE_BLEND_WITH_DEFAULT']}")
    print(f"[INFO] PLACE_SURFACE_BLEND_WITH_PLACE={CONFIG['PLACE_SURFACE_BLEND_WITH_PLACE']}")

    if df_file_exclusion_summary is not None and not df_file_exclusion_summary.empty:
        excluded_count = int(df_file_exclusion_summary["exclude_from_train"].sum())
        print(f"[INFO] train除外ファイル数={excluded_count}")

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

    weights_map, place_summary_df = optimize_placewise_weights(
        df_train=df_train,
        df_res_entries=df_res_entries,
        df_res_payout=df_res_payout,
    )

    train_s, train_t, train_i, train_r, train_det, train_stab = eval_success_and_roi(
        weights_map, df_train, df_res_entries, df_res_payout
    )
    test_s, test_t, test_i, test_r, test_det, test_stab = eval_success_and_roi(
        weights_map, df_test, df_res_entries, df_res_payout
    )
    all_s, all_t, all_i, all_r, all_det, all_stab = eval_success_and_roi(
        weights_map, df_feat_all, df_res_entries, df_res_payout
    )
    clean_train_s, clean_train_t, clean_train_i, clean_train_r, clean_train_det, clean_train_stab = eval_success_and_roi(
        weights_map, df_clean_train, df_res_entries, df_res_payout
    )
    clean_test_s, clean_test_t, clean_test_i, clean_test_r, clean_test_det, clean_test_stab = eval_success_and_roi(
        weights_map, df_clean_test, df_res_entries, df_res_payout
    )
    clean_all_s, clean_all_t, clean_all_i, clean_all_r, clean_all_det, clean_all_stab = eval_success_and_roi(
        weights_map, df_clean_all, df_res_entries, df_res_payout
    )

    train_debug = _print_eval_debug_summary(
        label="TRAIN",
        df_target=df_train,
        df_res_entries=df_res_entries,
        df_res_payout=df_res_payout,
        weights_map=weights_map,
    )
    test_debug = _print_eval_debug_summary(
        label="TEST",
        df_target=df_test,
        df_res_entries=df_res_entries,
        df_res_payout=df_res_payout,
        weights_map=weights_map,
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
    clean_test_debug = _print_eval_debug_summary(
        label="CLEAN TEST",
        df_target=df_clean_test,
        df_res_entries=df_res_entries,
        df_res_payout=df_res_payout,
        weights_map=weights_map,
    )
    clean_all_debug = _print_eval_debug_summary(
        label="CLEAN ALL",
        df_target=df_clean_all,
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

    place_eval_rows = []
    all_places = sorted([p for p in df_feat_all["place_name"].dropna().astype(str).unique().tolist() if p])
    for place_name in all_places:
        place_df_all = df_feat_all[df_feat_all["place_name"].astype(str) == place_name].copy()
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
                df_feat_all[["place_name", "surface_name"]]
                .drop_duplicates()
                .itertuples(index=False, name=None)
            )
            if str(place_name or "").strip() and _normalize_surface_name(surface_name)
        ],
        key=lambda x: (x[0], x[1]),
    )
    for place_name, surface_name in all_place_surfaces:
        place_surface_df_all = df_feat_all[
            (df_feat_all["place_name"].astype(str) == place_name)
            & (df_feat_all["surface_name"].map(_normalize_surface_name) == surface_name)
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

    PY_DIR.mkdir(parents=True, exist_ok=True)

    today_str = datetime.now().strftime("%Y%m%d")
    out_py = PY_DIR / f"best_feature_weights_{today_str}.py"

    with open(out_py, "w", encoding="utf-8") as f:
        f.write("# -*- coding: utf-8 -*-\n")
        f.write('"""自動生成：特徴量重み（TOP5命中率 + 場所別 + 場所×芝ダ 最適化）"""\n\n')
        f.write("FEATURE_WEIGHTS = {\n")
        f.write('    "__default__": {\n')
        for k in FEAT_COLS:
            f.write(f'        "{k}": {float(weights_map["__default__"].get(k, 0.0)):.10f},\n')
        f.write("    },\n")

        place_keys = sorted([p for p in weights_map.keys() if isinstance(p, str) and p != "__default__"])
        for place_name in place_keys:
            f.write(f'    "{place_name}": {{\n')
            for k in FEAT_COLS:
                f.write(f'        "{k}": {float(weights_map[place_name].get(k, 0.0)):.10f},\n')
            f.write("    },\n")

        f.write("}\n\n")
        f.write("FEATURE_WEIGHTS_BY_PLACE_SURFACE = {\n")
        place_surface_keys = sorted(
            [key for key in weights_map.keys() if isinstance(key, tuple) and len(key) == 2],
            key=lambda x: (x[0], x[1]),
        )
        for place_name, surface_name in place_surface_keys:
            f.write(f"    ({place_name!r}, {surface_name!r}): {{\n")
            for k in FEAT_COLS:
                f.write(f'        "{k}": {float(weights_map[(place_name, surface_name)].get(k, 0.0)):.10f},\n')
            f.write("    },\n")
        f.write("}\n")

    print(f"\n[OK] best weights saved: {out_py}")

    EXCEL_DIR.mkdir(parents=True, exist_ok=True)
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

    clean_target_summary_df = pd.DataFrame(
        [
            {"mode": "CLEAN_ALL", **clean_all_target_summary},
            {"mode": "CLEAN_TRAIN", **clean_train_target_summary},
            {"mode": "CLEAN_TEST", **clean_test_target_summary},
        ]
    )

    eval_summary_rows = [
        ("TRAIN", train_s, train_t, train_stab),
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
        debug_summary_df.to_excel(writer, sheet_name="debug_summary", index=False)
        rid_row_summary_df.to_excel(writer, sheet_name="rid_row_summary", index=False)
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
