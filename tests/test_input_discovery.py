# -*- coding: utf-8 -*-
"""特徴量入力Excelの探索に関する回帰テスト。"""

from __future__ import annotations

from pathlib import Path

import pytest

from tokutyouryou_keisann import common


def _touch(path: Path) -> None:
    """探索テスト用の空ファイルを作成する。"""
    path.touch()


def test_discover_files_prefers_canonical_file_for_duplicate_date(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """同日バックアップがあっても正規名だけを特徴量入力へ採用する。"""
    canonical = tmp_path / "馬の競走成績_20260509.xlsx"
    backup = tmp_path / "_馬の競走成績_20260509.xlsx"
    backup_only = tmp_path / "_馬の競走成績_20250608.xlsx"
    for path in (canonical, backup, backup_only):
        _touch(path)

    monkeypatch.setitem(common.CONFIG, "EXCLUDE_KEYWORDS", ["_with_topN"])

    selected = common.discover_files(str(tmp_path / "*馬の競走成績_*.xlsx"))

    assert selected == [str(backup_only), str(canonical)]
    assert str(backup) not in selected


def test_discover_files_rejects_ambiguous_noncanonical_duplicates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """正規名がない同日複数候補を推測で選ばず安全に停止する。"""
    first = tmp_path / "_馬の競走成績_20260509.xlsx"
    second = tmp_path / "backup_馬の競走成績_20260509.xlsx"
    for path in (first, second):
        _touch(path)

    monkeypatch.setitem(common.CONFIG, "EXCLUDE_KEYWORDS", ["_with_topN"])

    with pytest.raises(RuntimeError, match="正規ファイルを一意に選べません"):
        common.discover_files(str(tmp_path / "*馬の競走成績_*.xlsx"))
