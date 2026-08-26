# -*- coding: utf-8 -*-
"""結果Excel読込とbaseline重み検証の回帰テスト。"""

from __future__ import annotations

import importlib
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from tokutyouryou_keisann import baseline, common


production_config = importlib.import_module("1_keibayosou_config")


@pytest.mark.parametrize(
    ("raw_column", "expected"),
    [
        (" 通常列 ", "通常列"),
        (("上段", "下段"), "上段 / 下段"),
        (["上段", None, "Unnamed: 2_level_1", "下段"], "上段 / 下段"),
        (0, "0"),
        (1.5, "1.5"),
        (None, "Unnamed: 0"),
        (np.nan, "Unnamed: 0"),
        ("Unnamed: 3", "Unnamed: 0"),
        ("", "Unnamed: 0"),
    ],
)
def test_result_column_normalization_supports_all_required_types(
    raw_column: object,
    expected: str,
) -> None:
    """文字列・複合列・数値・欠損列を決定論的な文字列へ正規化する。"""
    normalized = common._normalize_result_columns(
        [raw_column],
        xlsx_path="dummy.xlsx",
        sheet_name="20260815",
    )

    assert normalized == [expected]


def test_result_column_normalization_rejects_duplicates() -> None:
    """空白除去や複合列連結の後に衝突する列名を黙って採用しない。"""
    with pytest.raises(common.ResultsWorkbookSchemaError, match="正規化後の列名が重複"):
        common._normalize_result_columns(
            ["馬名", " 馬名 "],
            xlsx_path="dummy.xlsx",
            sheet_name="duplicate_sheet",
        )


def _write_results_workbook(path: Path, columns: list[object]) -> None:
    """一時ディレクトリに最小の結果・払戻兼用シートを作る。"""
    rows = [[
        "202608150101",
        "テストホース",
        1,
        3,
        "3連複",
        "1-2-3",
        "1,230円",
        "馬場指数",
        None,
        4.9,
    ]]
    df = pd.DataFrame(rows, columns=columns)

    # テスト用一時ファイルだけを作成し、実データのExcelは変更しない。
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="20260815", index=False)


def test_load_results_discards_auxiliary_numeric_and_odds_columns(tmp_path: Path) -> None:
    """実シート由来の数値・馬場・追加オッズ列を後続DataFrameへ残さない。"""
    workbook = tmp_path / "numeric_headers.xlsx"
    _write_results_workbook(
        workbook,
        [
            "レースID",
            "馬名",
            "着順",
            "馬番",
            "払戻種別",
            "組番",
            "払戻金",
            0,
            1,
            "('オッズ 更新', 'オッズ 更新')",
        ],
    )

    entries, payouts = common.load_results_all_sheets(str(workbook))

    assert entries.columns.tolist() == [
        "rid_str",
        "name_norm",
        "着順_num",
        "馬番_int",
    ]
    assert payouts.columns.tolist() == [
        "rid_str",
        "払戻種別",
        "組番_norm",
        "払戻金_int",
    ]
    assert len(entries) == 1
    assert len(payouts) == 1
    assert entries.loc[0, "馬番_int"] == 3
    assert payouts.loc[0, "払戻金_int"] == 1230


def test_load_results_reports_missing_required_columns(tmp_path: Path) -> None:
    """結果らしいシートで必須列が欠落した場合は診断情報付きで停止する。"""
    workbook = tmp_path / "missing_columns.xlsx"
    df = pd.DataFrame({"レースID": ["202608150101"], "馬名": ["テストホース"]})
    with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="20260815", index=False)

    with pytest.raises(common.ResultsWorkbookSchemaError) as exc_info:
        common.load_results_all_sheets(str(workbook))

    message = str(exc_info.value)
    assert str(workbook.resolve()) in message
    assert "20260815" in message
    assert "期待した必須列" in message
    assert "header=0" in message
    assert "skiprows=None" in message


def test_load_results_detects_duplicate_raw_headers(tmp_path: Path) -> None:
    """pandasが重複列を自動改名する前のExcel生ヘッダーで衝突を検出する。"""
    workbook = tmp_path / "duplicate_headers.xlsx"
    df = pd.DataFrame(
        [["202608150101", "テストホース", "別名", 1, 3, "3連複", "1-2-3", "1,230円"]],
        columns=["レースID", "馬名", "馬名", "着順", "馬番", "払戻種別", "組番", "払戻金"],
    )
    with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="20260815", index=False)

    with pytest.raises(common.ResultsWorkbookSchemaError, match="正規化後の列名が重複"):
        common.load_results_all_sheets(str(workbook))


def _stub_baseline_environment(
    monkeypatch: pytest.MonkeyPatch,
    baseline_path: Path,
    production_path: Path,
    baseline_weights: dict,
    production_weights: dict,
) -> None:
    """パスと実効重みの一致判定だけを分離して検証できるようにする。"""
    def fake_effective_map(path: Path):
        selected = baseline_weights if Path(path).resolve() == baseline_path.resolve() else production_weights
        element_count = len(selected.get("__default__", {}))
        return selected, element_count, len(selected), 0

    def fake_effective_map_with_stats(path: Path):
        selected, element_count, group_count, surface_count = fake_effective_map(path)
        stats = baseline.WeightTransformStats(0, 0, 0, 0, 0)
        return selected, element_count, group_count, surface_count, stats

    monkeypatch.setattr(baseline, "_effective_map_from_file", fake_effective_map)
    monkeypatch.setattr(
        baseline,
        "_effective_map_from_file_with_stats",
        fake_effective_map_with_stats,
    )
    monkeypatch.setattr(baseline, "production_best_path", lambda: production_path.resolve())
    monkeypatch.setattr(baseline, "active_production_weights", lambda: production_weights)


def test_baseline_accepts_same_path_and_effective_weights(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """本番とbaselineの絶対パス・実効重みが完全一致する場合だけ通過する。"""
    weight_file = tmp_path / "best_feature_weights_20260730.py"
    weight_file.write_text("# test\n", encoding="utf-8")
    weights = {"__default__": {"feature_a": 1.0}}
    _stub_baseline_environment(monkeypatch, weight_file, weight_file, weights, weights)

    info = baseline.load_baseline_weights(
        baseline.BaselineSelection(weight_file.resolve(), "test"),
        verify_production=True,
    )

    assert info.production_weights_match is True
    assert info.path == info.production_path


def test_baseline_rejects_same_path_with_different_effective_weights(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ファイルパスだけ同じでも実効重みが違えば安全に停止する。"""
    weight_file = tmp_path / "best_feature_weights_20260730.py"
    weight_file.write_text("# test\n", encoding="utf-8")
    _stub_baseline_environment(
        monkeypatch,
        weight_file,
        weight_file,
        {"__default__": {"feature_a": 1.0}},
        {"__default__": {"feature_a": 2.0}},
    )

    with pytest.raises(baseline.BaselineWeightError, match="feature_a"):
        baseline.load_baseline_weights(
            baseline.BaselineSelection(weight_file.resolve(), "test"),
            verify_production=True,
        )


def test_baseline_rejects_same_filename_in_different_directories(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """同名・同じ重みでも絶対パスが異なれば一致扱いにしない。"""
    old_file = tmp_path / "old" / "best_feature_weights_20260730.py"
    current_file = tmp_path / "current" / "best_feature_weights_20260730.py"
    old_file.parent.mkdir()
    current_file.parent.mkdir()
    old_file.write_text("# old\n", encoding="utf-8")
    current_file.write_text("# current\n", encoding="utf-8")
    weights = {"__default__": {"feature_a": 1.0}}
    _stub_baseline_environment(monkeypatch, old_file, current_file, weights, weights)

    with pytest.raises(baseline.BaselineWeightError, match="baseline_resolved_path"):
        baseline.load_baseline_weights(
            baseline.BaselineSelection(old_file.resolve(), "test"),
            verify_production=True,
        )


def test_default_baseline_ignores_moved_adoption_path_and_uses_production(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """過去の採用JSONに関係なく明示本番パスだけを使う。"""
    old_file = tmp_path / "old" / "best_feature_weights_20260730.py"
    current_file = tmp_path / "current" / "best_feature_weights_20260730.py"
    old_file.parent.mkdir()
    current_file.parent.mkdir()
    old_file.write_text("# old project\n", encoding="utf-8")
    current_file.write_text("# current project\n", encoding="utf-8")
    monkeypatch.setattr(baseline, "production_best_path", lambda: current_file.resolve())

    selection = baseline.select_baseline_weight_file()

    assert selection.path == current_file.resolve()
    assert selection.method == "production_model_config"


def test_effective_weights_apply_completion_sign_guard_and_disabled_zero(tmp_path: Path) -> None:
    """本番共通関数による不足補完・符号補正・無効特徴量0固定を同時に確認する。"""
    weight_file = tmp_path / "best_feature_weights_20990101.py"
    weight_file.write_text(
        "FEATURE_WEIGHTS = {\n"
        "    '__default__': {\n"
        "        'avg_finish': 1.25,\n"
        "        'avg_last3f': 2.0,\n"
        "        'style_pressure_fit': 9.0,\n"
        "    },\n"
        "}\n"
        "FEATURE_WEIGHTS_BY_PLACE_SURFACE = {}\n",
        encoding="utf-8",
    )

    effective = baseline.load_effective_weights_file(weight_file)

    # 外部ファイルにない場所group・特徴量は本番組み込み重みから補完される。
    assert "札幌" in effective
    assert set(production_config.FEAT_COLS).issubset(effective["__default__"])
    # 実績符号ガードと無効特徴量0固定も本番configと同じ順番で適用される。
    assert effective["__default__"]["avg_last3f"] == -2.0
    assert effective["__default__"]["style_pressure_fit"] == 0.0
