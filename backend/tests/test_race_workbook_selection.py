from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from app.api.v1.endpoints import races as races_endpoint
from app.db.migrations import upgrade_database
from app.db.models import Horse, Race, RawFileRecord
from app.legacy_bridge import excel_importer
from app.schemas.api import RaceWorkbookSelectRequest


def _session(path: Path) -> Session:
    engine = create_engine(f"sqlite:///{path}")
    upgrade_database(db_engine=engine)
    # 本番と同じautoflush=Falseで、未flush行の重複を検出できるようにする。
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)()


def _write_workbook(path: Path) -> None:
    rows = pd.DataFrame(
        [
            {
                "レースID": "202607110101",
                "馬番": 1,
                "馬名": "選択テストホース1",
                "場所": "東京",
                "レース名": "Excel選択テスト",
                "発走時刻": "10:00",
                "距離": "芝1600",
                "頭数": 2,
            },
            {
                "レースID": "202607110101",
                "馬番": 2,
                "馬名": "選択テストホース2",
                "場所": "東京",
                "レース名": "Excel選択テスト",
                "発走時刻": "10:00",
                "距離": "芝1600",
                "頭数": 2,
            },
        ]
    )
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        rows.to_excel(writer, sheet_name="今走レース情報", index=False)


def test_lists_only_strict_feature_workbooks_and_imports_selected_file(monkeypatch, tmp_path):
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    selected_file = output_dir / "馬の競走成績_with_feat_20260711.xlsx"
    _write_workbook(selected_file)
    _write_workbook(output_dir / "馬の競走成績_with_feat_20260711_with_result.xlsx")
    _write_workbook(output_dir / "馬の競走成績_with_feat_20260711_training.xlsx")
    monkeypatch.setattr(
        excel_importer,
        "get_settings",
        lambda: SimpleNamespace(
            legacy_output_dir=output_dir,
            staging_dir=tmp_path / "staging",
        ),
    )

    with _session(tmp_path / "selection.db") as db:
        before = races_endpoint.list_race_workbook_files(db)
        assert [item.file_name for item in before] == [selected_file.name]
        assert before[0].race_date.isoformat() == "2026-07-11"
        assert before[0].is_imported is False

        selected = races_endpoint.select_race_workbook(
            RaceWorkbookSelectRequest(file_name=selected_file.name),
            db,
        )
        assert selected.workbook.file_name == selected_file.name
        assert selected.workbook.is_imported is True
        assert selected.import_summary.entries == 2
        assert selected.quality_summary.checked_races == 1
        assert db.get(Race, "202607110101") is not None
        assert db.scalar(select(RawFileRecord).where(RawFileRecord.file_name == selected_file.name))

        after = races_endpoint.list_race_workbook_files(db)
        assert after[0].is_imported is True


def test_rejects_path_traversal_for_workbook_selection(monkeypatch, tmp_path):
    monkeypatch.setattr(
        excel_importer,
        "get_settings",
        lambda: SimpleNamespace(
            legacy_output_dir=tmp_path,
            staging_dir=tmp_path / "staging",
        ),
    )
    with _session(tmp_path / "traversal.db") as db:
        with pytest.raises(HTTPException) as exc_info:
            races_endpoint.select_race_workbook(
                RaceWorkbookSelectRequest(file_name="../馬の競走成績_with_feat_20260711.xlsx"),
                db,
            )
        assert exc_info.value.status_code == 422


def test_imports_multiple_entries_when_horses_already_exist_with_autoflush_disabled(monkeypatch, tmp_path):
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    selected_file = output_dir / "馬の競走成績_with_feat_20260711.xlsx"
    _write_workbook(selected_file)
    monkeypatch.setattr(
        excel_importer,
        "get_settings",
        lambda: SimpleNamespace(
            legacy_output_dir=output_dir,
            staging_dir=tmp_path / "staging",
        ),
    )

    with _session(tmp_path / "existing-horses.db") as db:
        # 実運用DBのように馬マスタが先に存在する状態を再現する。
        db.add_all(
            [
                Horse(name="選択テストホース1", normalized_name="選択テストホース1"),
                Horse(name="選択テストホース2", normalized_name="選択テストホース2"),
            ]
        )
        db.commit()

        selected = races_endpoint.select_race_workbook(
            RaceWorkbookSelectRequest(file_name=selected_file.name),
            db,
        )

        assert selected.import_summary.races == 1
        assert selected.import_summary.entries == 2
        assert db.get(Race, "202607110101") is not None
