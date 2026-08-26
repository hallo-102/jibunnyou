# -*- coding: utf-8 -*-
"""延期・再開催で複数入力日に存在する特徴量レースの解決テスト。"""

from __future__ import annotations

import pandas as pd
import pytest

from tokutyouryou_keisann.common import resolve_duplicate_feature_races


def test_resolve_duplicate_feature_races_keeps_result_date_source() -> None:
    """結果確定日と一致する入力版だけを残し、古い予定日版を除外する。"""
    frame = pd.DataFrame(
        [
            {
                "rid_str": "202605010401",
                "馬番": 1,
                "date": "20260210",
                "source_file_name": "馬の競走成績_20260208.xlsx",
                "track": "良",
            },
            {
                "rid_str": "202605010401",
                "馬番": 1,
                "date": "20260210",
                "source_file_name": "馬の競走成績_20260210.xlsx",
                "track": "稍",
            },
            {
                "rid_str": "202605010401",
                "馬番": 2,
                "date": "20260210",
                "source_file_name": "馬の競走成績_20260210.xlsx",
                "track": "稍",
            },
            {
                "rid_str": "202608150101",
                "馬番": 3,
                "date": "20260815",
                "source_file_name": "馬の競走成績_20260815.xlsx",
                "track": "良",
            },
        ]
    )

    resolved, summary = resolve_duplicate_feature_races(frame)

    assert resolved[["rid_str", "馬番"]].to_dict("records") == [
        {"rid_str": "202605010401", "馬番": 1},
        {"rid_str": "202605010401", "馬番": 2},
        {"rid_str": "202608150101", "馬番": 3},
    ]
    assert resolved.loc[resolved["rid_str"].eq("202605010401"), "track"].tolist() == [
        "稍",
        "稍",
    ]
    assert summary.loc[0, "selected_source_file"] == "馬の競走成績_20260210.xlsx"
    assert summary.loc[0, "excluded_rows"] == 1


def test_resolve_duplicate_feature_races_rejects_missing_result_date_source() -> None:
    """確定日と一致する入力版がなければ、推測でいずれかを選ばず停止する。"""
    frame = pd.DataFrame(
        [
            {
                "rid_str": "202605010401",
                "馬番": 1,
                "date": "20260210",
                "source_file_name": "馬の競走成績_20260208.xlsx",
            },
            {
                "rid_str": "202605010401",
                "馬番": 1,
                "date": "20260210",
                "source_file_name": "馬の競走成績_20260209.xlsx",
            },
        ]
    )

    with pytest.raises(RuntimeError, match="結果確定日と一致する入力版"):
        resolve_duplicate_feature_races(frame)


def test_resolve_duplicate_feature_races_rejects_duplicate_within_selected_source() -> None:
    """確定日版そのものの馬番重複は隠さず異常として扱う。"""
    frame = pd.DataFrame(
        [
            {
                "rid_str": "202605010401",
                "馬番": 1,
                "date": "20260210",
                "source_file_name": "馬の競走成績_20260208.xlsx",
            },
            {
                "rid_str": "202605010401",
                "馬番": 1,
                "date": "20260210",
                "source_file_name": "馬の競走成績_20260210.xlsx",
            },
            {
                "rid_str": "202605010401",
                "馬番": 1,
                "date": "20260210",
                "source_file_name": "馬の競走成績_20260210.xlsx",
            },
        ]
    )

    with pytest.raises(RuntimeError, match="入力版内でレースID・馬番が重複"):
        resolve_duplicate_feature_races(frame)
