from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from app.api.v1.endpoints import jobs as jobs_endpoint
from app.core.config import Settings
from app.db.base import Base
from app.db.models import (
    ConfigVersion,
    FeatureWeightVersion,
    PredictionResult,
    PredictionRun,
    Race,
    RaceEntry,
)
from app.legacy_bridge.prediction_runner import (
    PredictionRunnerError,
    import_prediction_workbook,
)
from app.schemas.api import JobCreate
from app.services.prediction_golden_master import compare_prediction_workbooks
from app.services.prediction_workspace import (
    PREDICTION_CODE_FILES,
    PREDICTION_MASTER_FILES,
    PredictionWorkspaceError,
    prepare_prediction_workspace,
    register_prediction_versions,
)


def _session(path: Path) -> Session:
    engine = create_engine(f"sqlite:///{path}")
    Base.metadata.create_all(engine)
    return Session(engine)


def _write_prediction_workbook(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        pd.DataFrame(rows).to_excel(writer, sheet_name="TARGET", index=False)


def _prediction_rows(*, second_name: str = "テスト馬B") -> list[dict]:
    return [
        {
            "レースID": "202699010101",
            "馬番": 1,
            "馬名": "テスト馬A",
            "予想順位": 1,
            "score": 72.5,
            "dl_rank": 1,
            "dl_prob": 0.72,
            "dl_score": 7.2,
            "favorite_risk": 0.0,
            "extra_penalty": 0.0,
            "推定馬券内率_オッズ補正後": 0.62,
            "期待値": 1.18,
        },
        {
            "レースID": "202699010101",
            "馬番": 2,
            "馬名": second_name,
            "予想順位": 2,
            "score": 64.0,
            "dl_rank": 2,
            "dl_prob": 0.51,
            "dl_score": 5.1,
            "favorite_risk": 0.3,
            "extra_penalty": 0.0,
            "推定馬券内率_オッズ補正後": 0.48,
            "期待値": 0.96,
        },
    ]


def _add_race_entries(db: Session) -> None:
    race = Race(
        race_id="202699010101",
        race_date=date(2026, 7, 10),
        race_number=1,
        venue="東京",
        headcount=2,
    )
    db.add(race)
    db.add_all(
        [
            RaceEntry(race_id=race.race_id, horse_no=1, horse_name="テスト馬A"),
            RaceEntry(race_id=race.race_id, horse_no=2, horse_name="テスト馬B"),
        ]
    )
    db.commit()


def test_prediction_workspace_isolated_history_and_versions(tmp_path: Path) -> None:
    legacy_root = tmp_path / "legacy"
    data_root = tmp_path / "data"
    input_dir = data_root / "input"
    odds_dir = data_root / "ozzu_csv"
    output_dir = data_root / "output"
    master_dir = data_root / "master"
    weights_dir = legacy_root / "yosou_py"
    for directory in (legacy_root, input_dir, odds_dir, output_dir, master_dir, weights_dir):
        directory.mkdir(parents=True, exist_ok=True)

    for file_name in PREDICTION_CODE_FILES:
        (legacy_root / file_name).write_text(f"# {file_name}\n", encoding="utf-8")
    for file_name in PREDICTION_MASTER_FILES:
        (master_dir / file_name).write_bytes(f"master:{file_name}".encode("utf-8"))
    (odds_dir / "OZZU_20260710.csv").write_text("horse_no,odds\n1,2.5\n", encoding="utf-8")
    source_workbook = input_dir / "馬の競走成績_20260710.xlsx"
    source_workbook.write_bytes(b"source-workbook")
    (weights_dir / "best_feature_weights_20260701.py").write_text(
        "WEIGHTS = {'speed': 1.0}\n",
        encoding="utf-8",
    )
    (weights_dir / "best_feature_weights_20260711.py").write_text(
        "WEIGHTS = {'speed': 9.0}\n",
        encoding="utf-8",
    )
    for yyyymmdd in ("20260701", "20260710", "20260711"):
        (output_dir / f"馬の競走成績_with_feat_{yyyymmdd}.xlsx").write_bytes(
            f"history:{yyyymmdd}".encode("utf-8")
        )

    settings = Settings(
        environment="test",
        data_root=data_root,
        excel_input_dir=input_dir,
        odds_input_dir=odds_dir,
        legacy_output_dir=output_dir,
        exports_dir=data_root / "exports",
        staging_dir=data_root / "staging",
        logs_dir=data_root / "logs",
        legacy_root=legacy_root,
    )
    workspace = prepare_prediction_workspace(
        settings,
        run_id="prediction-workspace-001",
        race_date=date(2026, 7, 10),
        source_workbook=source_workbook,
    )

    assert workspace.script_path.is_file()
    assert workspace.source_input_path.read_bytes() == b"source-workbook"
    assert workspace.manifest["history_cutoff_exclusive"] == "20260710"
    assert [item["name"] for item in workspace.manifest["history_files"]] == [
        "馬の競走成績_with_feat_20260701.xlsx"
    ]
    assert len(workspace.manifest["input_snapshot_sha256"]) == 64
    assert len(workspace.manifest["code_bundle_sha256"]) == 64
    assert workspace.manifest["weight"]["name"] == "best_feature_weights_20260701.py"
    assert workspace.manifest["weight_cutoff_exclusive"] == "20260710"

    with _session(tmp_path / "versions.db") as db:
        first_config, first_weight = register_prediction_versions(
            db,
            manifest=workspace.manifest,
            environment="test",
        )
        db.commit()
        second_config, second_weight = register_prediction_versions(
            db,
            manifest=workspace.manifest,
            environment="test",
        )
        assert second_config.id == first_config.id
        assert second_weight.id == first_weight.id
        assert db.scalar(select(func.count(ConfigVersion.id))) == 1
        assert db.scalar(select(func.count(FeatureWeightVersion.id))) == 1

    with pytest.raises(PredictionWorkspaceError, match="already exists"):
        prepare_prediction_workspace(
            settings,
            run_id="prediction-workspace-001",
            race_date=date(2026, 7, 10),
            source_workbook=source_workbook,
        )


def test_prediction_job_is_dispatched_to_dedicated_queue(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'prediction-queue.db'}")
    Base.metadata.create_all(engine)
    dispatched: list[tuple[str, list[str], str]] = []
    monkeypatch.setattr(
        jobs_endpoint,
        "get_settings",
        lambda: SimpleNamespace(job_execution_mode="queue"),
    )
    monkeypatch.setattr(
        jobs_endpoint.celery_app,
        "send_task",
        lambda name, args, queue: dispatched.append((name, args, queue)),
    )

    with Session(engine) as db:
        job = jobs_endpoint.create_job(
            JobCreate(
                job_type="prediction.run",
                race_date=date(2026, 7, 10),
                race_id="202699010101",
            ),
            db,
        )

        assert job.status == "queued"
        assert job.started_at is None
        assert dispatched == [
            ("keiba_ai_studio.prediction.run", [job.id], "prediction")
        ]


def test_prediction_import_matches_identity_and_is_immutable(tmp_path: Path) -> None:
    workbook = tmp_path / "prediction_output.xlsx"
    _write_prediction_workbook(workbook, _prediction_rows(second_name=" テスト馬B "))

    with _session(tmp_path / "prediction-import.db") as db:
        _add_race_entries(db)
        summary = import_prediction_workbook(
            db,
            prediction_run_id="prediction-import-001",
            workbook_path=workbook,
            race_date=date(2026, 7, 10),
            race_id="202699010101",
            input_checksum="a" * 64,
        )
        run = db.get(PredictionRun, "prediction-import-001")
        first_entry = db.scalar(
            select(RaceEntry).where(
                RaceEntry.race_id == "202699010101",
                RaceEntry.horse_no == 1,
            )
        )

        assert summary.results == 2
        assert summary.matched == 2
        assert summary.mismatches == 0
        assert run.status == "completed"
        assert run.input_checksum == "a" * 64
        assert first_entry.prediction_rank == 1
        assert first_entry.prediction_score == 72.5

        with pytest.raises(PredictionRunnerError, match="immutable"):
            import_prediction_workbook(
                db,
                prediction_run_id="prediction-import-001",
                workbook_path=workbook,
            )
        assert db.scalar(select(func.count(PredictionResult.id))) == 2


def test_prediction_import_rejects_horse_identity_projection(tmp_path: Path) -> None:
    workbook = tmp_path / "prediction_mismatch.xlsx"
    _write_prediction_workbook(workbook, _prediction_rows(second_name="別の馬"))

    with _session(tmp_path / "prediction-mismatch.db") as db:
        _add_race_entries(db)
        summary = import_prediction_workbook(
            db,
            prediction_run_id="prediction-mismatch-001",
            workbook_path=workbook,
            race_date=date(2026, 7, 10),
            race_id="202699010101",
        )
        run = db.get(PredictionRun, "prediction-mismatch-001")
        second_entry = db.scalar(
            select(RaceEntry).where(
                RaceEntry.race_id == "202699010101",
                RaceEntry.horse_no == 2,
            )
        )

        assert summary.mismatches == 1
        assert run.status == "failed"
        assert second_entry.horse_name == "テスト馬B"
        assert second_entry.prediction_rank is None
        assert second_entry.prediction_score is None


def test_prediction_import_rejects_invalid_rank_sequence(tmp_path: Path) -> None:
    workbook = tmp_path / "prediction_bad_rank.xlsx"
    rows = _prediction_rows()
    rows[1]["予想順位"] = 1
    _write_prediction_workbook(workbook, rows)

    with _session(tmp_path / "prediction-bad-rank.db") as db:
        _add_race_entries(db)
        with pytest.raises(PredictionRunnerError, match="unique and contiguous"):
            import_prediction_workbook(
                db,
                prediction_run_id="prediction-bad-rank-001",
                workbook_path=workbook,
            )
        assert db.scalar(select(func.count(PredictionResult.id))) == 0


def test_golden_master_compares_critical_prediction_values(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.xlsx"
    identical = tmp_path / "identical.xlsx"
    changed = tmp_path / "changed.xlsx"
    rows = _prediction_rows()
    _write_prediction_workbook(baseline, rows)
    shutil.copy2(baseline, identical)
    changed_rows = _prediction_rows()
    changed_rows[0]["score"] = 72.4
    _write_prediction_workbook(changed, changed_rows)

    identical_result = compare_prediction_workbooks(baseline, identical)
    changed_result = compare_prediction_workbooks(baseline, changed)

    assert identical_result.passed is True
    assert identical_result.mismatch_count == 0
    assert changed_result.passed is False
    assert changed_result.mismatch_count == 1
    assert "field=prediction_score" in changed_result.diagnostics[0]
