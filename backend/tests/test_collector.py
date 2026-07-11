from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.base import Base
from app.api.v1.endpoints import jobs as jobs_endpoint
from app.db.models import ArtifactFile, CollectionCacheEntry, CollectionRun, JobLog, JobRun, RawFileRecord
from app.schemas.api import JobCreate
from app.services.collector import execute_with_finite_retry, run_collection_pipeline
from app.services.collector import CollectorBlockedError
from app.services import collector as collector_service
from app.services.data_quality import has_blocking_quality_status


def test_finite_retry_retries_only_transient_failures() -> None:
    calls = 0
    delays: list[float] = []

    def transient_then_success() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise TimeoutError("temporary timeout")
        return "ok"

    result, attempts, retries = execute_with_finite_retry(
        transient_then_success,
        max_retries=3,
        retry_delays_seconds=[10, 60, 300],
        sleeper=delays.append,
    )

    assert result == "ok"
    assert attempts == 3
    assert retries == 2
    assert delays == [10, 60]


def test_finite_retry_does_not_retry_schema_or_validation_failure() -> None:
    calls = 0
    delays: list[float] = []

    def permanent_failure() -> None:
        nonlocal calls
        calls += 1
        raise ValueError("schema changed")

    with pytest.raises(ValueError, match="schema changed"):
        execute_with_finite_retry(
            permanent_failure,
            max_retries=3,
            retry_delays_seconds=[10, 60, 300],
            sleeper=delays.append,
        )

    assert calls == 1
    assert delays == []


def test_collection_pipeline_persists_three_layers_and_reuses_valid_cache(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    workbook = input_dir / "馬の競走成績_20260713.xlsx"
    with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
        pd.DataFrame(
            [
                {
                    "レースID": "202699010101",
                    "馬番": 1,
                    "馬名": "三層保存テスト馬",
                    "場所": "東京",
                    "頭数": 1,
                }
            ]
        ).to_excel(writer, sheet_name="今走レース情報", index=False)

    monkeypatch.setenv("KEIBA_EXCEL_INPUT_DIR", str(input_dir))
    monkeypatch.setenv("KEIBA_ODDS_INPUT_DIR", str(tmp_path / "odds"))
    monkeypatch.setenv("KEIBA_LEGACY_OUTPUT_DIR", str(tmp_path / "legacy_output"))
    monkeypatch.setenv("KEIBA_DATA_ROOT", str(tmp_path / "data"))
    monkeypatch.setenv("KEIBA_RAW_SNAPSHOTS_DIR", str(tmp_path / "raw_snapshots"))
    monkeypatch.setenv("KEIBA_NORMALIZED_DIR", str(tmp_path / "normalized"))
    monkeypatch.setenv("KEIBA_SNAPSHOTS_DIR", str(tmp_path / "snapshots"))
    monkeypatch.setenv("KEIBA_STAGING_DIR", str(tmp_path / "staging"))
    monkeypatch.setenv("KEIBA_EXPORTS_DIR", str(tmp_path / "exports"))
    monkeypatch.setenv("KEIBA_LOGS_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("KEIBA_LEGACY_ROOT", str(tmp_path))
    monkeypatch.setenv("KEIBA_LEGACY_RUNNER_MODE", "dry_run")
    get_settings.cache_clear()

    engine = create_engine(f"sqlite:///{tmp_path / 'collector.db'}")
    Base.metadata.create_all(engine)
    try:
        with Session(engine) as db:
            first_job = JobRun(
                job_type="collection.race_info",
                status="running",
                race_date=date(2026, 7, 13),
                force=False,
                params={"reason": "contract-test"},
            )
            db.add(first_job)
            db.commit()

            first_payload = run_collection_pipeline(db, first_job)
            first_run = db.scalar(
                select(CollectionRun).where(CollectionRun.job_run_id == first_job.id)
            )
            raw_record = db.get(RawFileRecord, first_run.raw_file_record_id)
            cache = db.scalar(select(CollectionCacheEntry))
            artifacts = list(
                db.scalars(select(ArtifactFile).where(ArtifactFile.job_run_id == first_job.id))
            )

            assert first_payload["collection"]["status"] == "succeeded"
            assert first_payload["collection"]["quality_status"] == "GREEN"
            assert first_payload["import"]["entries"] == 1
            assert raw_record.is_immutable is True
            assert raw_record.source_code == "SRC_NETKEIBA_001"
            assert Path(raw_record.file_path).is_relative_to(tmp_path / "raw_snapshots")
            assert Path(raw_record.file_path).read_bytes() == workbook.read_bytes()
            assert Path(first_payload["collection"]["summary"]["layers"]["normalized"]).is_file()
            assert cache.status == "active"
            assert len(artifacts) == 7

            second_job = JobRun(
                job_type="collection.race_info",
                status="running",
                race_date=date(2026, 7, 13),
                force=False,
                params={"reason": "contract-test"},
            )
            db.add(second_job)
            db.commit()

            second_payload = run_collection_pipeline(db, second_job)
            second_run = db.scalar(
                select(CollectionRun).where(CollectionRun.job_run_id == second_job.id)
            )
            db.refresh(cache)

            assert second_payload["collection"]["status"] == "cached"
            assert second_payload["collection"]["cache_hit"] is True
            assert second_run.raw_file_record_id == first_run.raw_file_record_id
            assert second_run.raw_artifact_id == first_run.raw_artifact_id
            assert cache.hit_count == 1
    finally:
        get_settings.cache_clear()


def test_collection_job_is_dispatched_to_collector_queue_in_queue_mode(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'queue-dispatch.db'}")
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
                job_type="collection.race_info",
                race_date=date(2026, 7, 13),
            ),
            db,
        )
        log = db.scalar(select(JobLog).where(JobLog.job_run_id == job.id))

        assert job.status == "queued"
        assert job.started_at is None
        assert log.event_code == "JOB_QUEUED"
        assert dispatched == [
            ("keiba_ai_studio.collector.run", [job.id], "collector")
        ]


def test_latest_failed_collection_or_active_collection_blocks_downstream_job(tmp_path: Path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'collection-gate.db'}")
    Base.metadata.create_all(engine)
    target_date = date(2026, 7, 13)

    with Session(engine) as db:
        failed_job = JobRun(
            job_type="collection.odds",
            status="completed",
            race_date=target_date,
        )
        db.add(failed_job)
        db.flush()
        db.add(
            CollectionRun(
                job_run_id=failed_job.id,
                source_code="SRC_JRA_003",
                data_kind="odds",
                status="partial",
                mode="dry_run",
                race_date=target_date,
                force=False,
                cache_key="a" * 64,
                quality_status="RED",
                created_at=datetime(2026, 7, 13, 0, 0, tzinfo=timezone.utc),
            )
        )
        db.commit()

        assert has_blocking_quality_status(db, race_date=target_date) is True

        recovered_job = JobRun(
            job_type="collection.odds",
            status="completed",
            race_date=target_date,
            force=True,
        )
        db.add(recovered_job)
        db.flush()
        db.add(
            CollectionRun(
                job_run_id=recovered_job.id,
                source_code="SRC_JRA_003",
                data_kind="odds",
                status="succeeded",
                mode="dry_run",
                race_date=target_date,
                force=True,
                cache_key="b" * 64,
                quality_status="GREEN",
                created_at=datetime(2026, 7, 13, 0, 1, tzinfo=timezone.utc),
            )
        )
        db.commit()

        assert has_blocking_quality_status(db, race_date=target_date) is False

        active_job = JobRun(
            job_type="collection.race_info",
            status="queued",
            race_date=target_date,
        )
        db.add(active_job)
        db.commit()

        assert has_blocking_quality_status(db, race_date=target_date) is True


def test_execute_mode_requires_explicit_source_approval(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'source-approval.db'}")
    Base.metadata.create_all(engine)
    monkeypatch.setattr(
        collector_service,
        "get_settings",
        lambda: SimpleNamespace(
            legacy_runner_mode="execute",
            collector_approved_sources=[],
        ),
    )

    with Session(engine) as db:
        job = JobRun(
            job_type="collection.odds",
            status="running",
            race_date=date(2026, 7, 13),
        )
        db.add(job)
        db.commit()

        with pytest.raises(CollectorBlockedError, match="source execution is not approved"):
            run_collection_pipeline(db, job)

        collection_run = db.scalar(
            select(CollectionRun).where(CollectionRun.job_run_id == job.id)
        )
        assert collection_run.status == "blocked"
