# -*- coding: utf-8 -*-
"""本番・評価で共有するレース内順位の一意化と検証処理。"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd


def _require_columns(df: pd.DataFrame, columns: list[str], context: str) -> None:
    """処理に必須の列が無ければ、曖昧な代替をせず停止する。"""
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"{context}に必要な列がありません: {missing}")


def create_unique_rank_series(
    df: pd.DataFrame,
    race_id_col: str,
    raw_score_col: str,
    risk_score_col: Optional[str] = None,
    extra_penalty_col: Optional[str] = None,
    data_confidence_col: Optional[str] = None,
    horse_number_col: str = "馬番",
) -> pd.Series:
    """丸め前scoreと決定的な補助キーから、レース内の一意な連番順位を返す。"""
    _require_columns(
        df,
        [race_id_col, raw_score_col, horse_number_col],
        context="順位計算",
    )
    if df.empty:
        return pd.Series(index=df.index, dtype="int64", name="rank")

    work = pd.DataFrame(index=df.index)
    # 元indexが重複していても行位置へ正しく順位を戻せるよう、位置番号を保持する。
    work["_original_position"] = np.arange(len(df), dtype=int)
    work["_race_id"] = df[race_id_col].astype("string")
    work["_raw_score"] = pd.to_numeric(df[raw_score_col], errors="coerce")
    work["_horse_number"] = pd.to_numeric(df[horse_number_col], errors="coerce")

    invalid_required = work[["_race_id", "_raw_score", "_horse_number"]].isna().any(axis=1)
    if invalid_required.any():
        bad_positions = work.loc[invalid_required, "_original_position"].tolist()
        raise RuntimeError(
            "順位計算の必須値に欠損または非数値があります。"
            f" positions={bad_positions[:20]}"
        )

    duplicate_horse = work.duplicated(["_race_id", "_horse_number"], keep=False)
    if duplicate_horse.any():
        detail = work.loc[duplicate_horse, ["_race_id", "_horse_number"]].to_dict("records")
        raise RuntimeError(f"同一レースID・馬番が重複しています: {detail[:20]}")

    sort_columns = ["_race_id", "_raw_score"]
    ascending = [True, False]
    optional_specs = [
        # 指定された優先順どおり、リスク降順・減点昇順・信頼度降順で比較する。
        (risk_score_col, "_risk_score", False),
        (extra_penalty_col, "_extra_penalty", True),
        (data_confidence_col, "_data_confidence", False),
    ]
    for source_col, internal_col, is_ascending in optional_specs:
        if source_col and source_col in df.columns:
            work[internal_col] = pd.to_numeric(df[source_col], errors="coerce")
            sort_columns.append(internal_col)
            ascending.append(is_ascending)

    # 人気・オッズは使わず、最後は必ず馬番昇順で決定的に一意化する。
    sort_columns.append("_horse_number")
    ascending.append(True)
    ranked = work.sort_values(
        sort_columns,
        ascending=ascending,
        kind="mergesort",
        na_position="last",
    ).copy()
    ranked["_rank"] = (
        ranked.groupby("_race_id", sort=False).cumcount().add(1).astype("int64")
    )

    rank_by_position = np.empty(len(df), dtype=np.int64)
    rank_by_position[ranked["_original_position"].to_numpy(dtype=int)] = ranked["_rank"].to_numpy(dtype=np.int64)
    return pd.Series(rank_by_position, index=df.index, name="rank", dtype="int64")


def _diagnostic_records(
    df: pd.DataFrame,
    race_ids: list[str],
    race_id_col: str,
    rank_col: str,
    horse_number_col: str,
    horse_name_col: str,
    score_col: str,
    raw_score_col: Optional[str],
) -> list[dict]:
    """停止ログへ出す最小限の馬単位情報を組み立てる。"""
    columns = [race_id_col, horse_number_col]
    for column in [horse_name_col, raw_score_col, score_col, rank_col]:
        if column and column in df.columns and column not in columns:
            columns.append(column)
    mask = df[race_id_col].astype(str).isin(race_ids)
    return df.loc[mask, columns].head(100).to_dict("records")


def validate_prediction_ranks(
    df: pd.DataFrame,
    race_id_col: str = "rid_str",
    rank_col: str = "rank",
    horse_number_col: str = "馬番",
    horse_name_col: str = "馬名",
    score_col: str = "score",
    raw_score_col: Optional[str] = None,
    model_name: str = "予想",
) -> None:
    """順位重複、1位数、連番、TOP5数、馬の重複を保存前に一括検証する。"""
    _require_columns(
        df,
        [race_id_col, rank_col, horse_number_col],
        context=f"{model_name}順位検証",
    )
    if df.empty:
        return

    work = df[[race_id_col, rank_col, horse_number_col]].copy()
    work[race_id_col] = work[race_id_col].astype("string")
    work[rank_col] = pd.to_numeric(work[rank_col], errors="coerce")
    work[horse_number_col] = pd.to_numeric(work[horse_number_col], errors="coerce")

    bad_races: set[str] = set()
    reasons: list[str] = []
    missing_mask = work[[race_id_col, rank_col, horse_number_col]].isna().any(axis=1)
    if missing_mask.any():
        bad_races.update(work.loc[missing_mask, race_id_col].dropna().astype(str))
        reasons.append("必須値の欠損")

    duplicate_horse = work.duplicated([race_id_col, horse_number_col], keep=False)
    if duplicate_horse.any():
        bad_races.update(work.loc[duplicate_horse, race_id_col].astype(str))
        reasons.append("race_id・馬番の重複")

    duplicate_rank = work.duplicated([race_id_col, rank_col], keep=False)
    if duplicate_rank.any():
        bad_races.update(work.loc[duplicate_rank, race_id_col].astype(str))
        reasons.append("同一レース内の順位重複")

    for race_id, race in work.groupby(race_id_col, sort=False, dropna=False):
        race_id_text = str(race_id)
        ranks = pd.to_numeric(race[rank_col], errors="coerce")
        expected = list(range(1, len(race) + 1))
        if ranks.isna().any() or sorted(ranks.astype(int).tolist()) != expected:
            bad_races.add(race_id_text)
            reasons.append("順位が1から出走頭数までの連番ではない")
        if int(ranks.eq(1).sum()) != 1:
            bad_races.add(race_id_text)
            reasons.append("予想1位が1頭ではない")
        if int(ranks.le(5).sum()) != min(5, len(race)):
            bad_races.add(race_id_text)
            reasons.append("TOP5件数が出走頭数に対する期待数と異なる")

    if bad_races:
        unique_reasons = list(dict.fromkeys(reasons))
        detail = _diagnostic_records(
            df=df,
            race_ids=sorted(bad_races),
            race_id_col=race_id_col,
            rank_col=rank_col,
            horse_number_col=horse_number_col,
            horse_name_col=horse_name_col,
            score_col=score_col,
            raw_score_col=raw_score_col,
        )
        raise RuntimeError(
            f"{model_name}順位検証に失敗しました。"
            f" reasons={unique_reasons} race_ids={sorted(bad_races)} detail={detail}"
        )


def select_rank1_predictions(
    df: pd.DataFrame,
    raw_score_col: Optional[str] = None,
    race_id_col: str = "rid_str",
    rank_col: str = "rank",
) -> pd.DataFrame:
    """検証済み順位から、各レース1頭だけの予想1位を返す。"""
    validate_prediction_ranks(
        df,
        race_id_col=race_id_col,
        rank_col=rank_col,
        raw_score_col=raw_score_col,
    )
    return df.loc[pd.to_numeric(df[rank_col], errors="coerce").eq(1)].copy()


def select_top5_predictions(
    df: pd.DataFrame,
    raw_score_col: Optional[str] = None,
    race_id_col: str = "rid_str",
    rank_col: str = "rank",
) -> pd.DataFrame:
    """検証済み順位から、1レース最大5頭を順位順に返す。"""
    validate_prediction_ranks(
        df,
        race_id_col=race_id_col,
        rank_col=rank_col,
        raw_score_col=raw_score_col,
    )
    ordered = df.sort_values(
        [race_id_col, rank_col],
        ascending=[True, True],
        kind="mergesort",
    )
    return (
        ordered.groupby(race_id_col, sort=False, group_keys=False)
        .head(5)
        .copy()
    )
