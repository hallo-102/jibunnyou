# -*- coding: utf-8 -*-
"""2026年8月16日の実出力を使う順位一意化回帰テスト。"""

from __future__ import annotations

import importlib
from pathlib import Path

import pandas as pd
import pandas.testing as pdt
import pytest


ranking = importlib.import_module("1_keibayosou_ranking")
WORKBOOK = Path("data/output/馬の競走成績_with_feat_20260816_with_result.xlsx")


@pytest.fixture(scope="module")
def corrected_target() -> pd.DataFrame:
    """既存Excelを変更せず、保存済み丸め前scoreから修正版順位を再現する。"""
    if not WORKBOOK.exists():
        pytest.skip(f"実ファイルがありません: {WORKBOOK}")

    df = pd.read_excel(WORKBOOK, sheet_name="TARGET", engine="openpyxl")
    df["rid_str"] = df["rid_str"].astype(str).str.replace(r"\.0$", "", regex=True)
    df["rank_before"] = pd.to_numeric(df["rank"], errors="coerce")
    df["rank"] = ranking.create_unique_rank_series(
        df=df,
        race_id_col="rid_str",
        # totalはペナルティとDL補正を反映済みの新best最終・丸め前値である。
        raw_score_col="total",
        risk_score_col="リスクスコア",
        extra_penalty_col="extra_penalty",
        data_confidence_col="データ信頼度",
        horse_number_col="馬番",
    )
    df["five_block_rank_fixed"] = ranking.create_unique_rank_series(
        df=df,
        race_id_col="rid_str",
        raw_score_col="5ブロック最終生スコア",
        risk_score_col="リスクスコア",
        extra_penalty_col="extra_penalty",
        data_confidence_col="データ信頼度",
        horse_number_col="馬番",
    )
    return df


def test_202607020801_rank1_and_top5_are_unique(corrected_target: pd.DataFrame) -> None:
    """問題レースの予想1位を1頭、TOP5を5頭へ修正できる。"""
    race = corrected_target.loc[corrected_target["rid_str"].eq("202607020801")]

    assert int(race["rank_before"].eq(1).sum()) == 2
    assert int(race["rank_before"].le(5).sum()) == 6
    assert int(race["rank"].eq(1).sum()) == 1
    assert int(race["rank"].le(5).sum()) == 5
    assert race.loc[race["rank"].eq(1), "馬名"].tolist() == ["アームズラムレイ"]
    assert sorted(race["rank"].tolist()) == list(range(1, len(race) + 1))


def test_202604020812_top5_is_exactly_five(corrected_target: pd.DataFrame) -> None:
    """5位表示score同点レースでも修正後TOP5を5頭に限定する。"""
    race = corrected_target.loc[corrected_target["rid_str"].eq("202604020812")]

    assert int(race["rank_before"].le(5).sum()) == 6
    assert int(race["rank"].le(5).sum()) == 5
    assert sorted(race["rank"].tolist()) == list(range(1, len(race) + 1))


def test_real_file_best_and_five_block_have_no_duplicate_rank(corrected_target: pd.DataFrame) -> None:
    """実ファイル全28レースで新best・5ブロックとも順位重複を残さない。"""
    assert not corrected_target.duplicated(["rid_str", "rank"]).any()
    assert not corrected_target.duplicated(["rid_str", "five_block_rank_fixed"]).any()


def test_real_file_ranking_is_reproducible(corrected_target: pd.DataFrame) -> None:
    """同じ実ファイルを再順位付けしても順位とTOP5順が完全一致する。"""
    second_rank = ranking.create_unique_rank_series(
        df=corrected_target,
        race_id_col="rid_str",
        raw_score_col="total",
        risk_score_col="リスクスコア",
        extra_penalty_col="extra_penalty",
        data_confidence_col="データ信頼度",
        horse_number_col="馬番",
    )
    second = corrected_target.assign(rank=second_rank)
    first_top5 = ranking.select_top5_predictions(corrected_target, raw_score_col="total")
    second_top5 = ranking.select_top5_predictions(second, raw_score_col="total")

    pdt.assert_series_equal(corrected_target["rank"], second_rank)
    pdt.assert_frame_equal(
        first_top5[["rid_str", "馬番", "rank"]].reset_index(drop=True),
        second_top5[["rid_str", "馬番", "rank"]].reset_index(drop=True),
    )
