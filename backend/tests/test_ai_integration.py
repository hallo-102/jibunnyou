from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from app.api.v1.endpoints import jobs as jobs_endpoint
from app.api.v1.deps import get_db
from app.core.config import Settings
from app.db.migrations import upgrade_database
from app.db.models import (
    AiAnalysis,
    AiAnalysisOutput,
    ArtifactFile,
    Horse,
    HorsePastPerformance,
    PredictionResult,
    PredictionRun,
    Race,
    RaceEntry,
    RaceQualityStatus,
)
from app.schemas.ai_integration import IntegrationInput
from app.schemas.api import JobCreate
from app.services.ai_independent import payload_sha256, run_independent_analysis
from app.services.ai_integration import (
    AiIntegrationError,
    _validate_integration,
    build_comparison_input,
    run_comparison_integration,
)
from app.services.ai_provider import (
    AiComparisonProviderResult,
    DeterministicMockAiProvider,
)
from app.services import ai_integration as ai_integration_service
from app.main import app


RACE_ID = "202607101101"
PREDICTION_RUN_ID = "phase6-python-run-001"


def _session(path: Path) -> Session:
    engine = create_engine(f"sqlite:///{path}")
    upgrade_database(db_engine=engine)
    return Session(engine)


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        environment="test",
        database_url=f"sqlite:///{tmp_path / 'settings.db'}",
        exports_dir=tmp_path / "exports",
        staging_dir=tmp_path / "staging",
        logs_dir=tmp_path / "logs",
        raw_snapshots_dir=tmp_path / "raw",
        normalized_dir=tmp_path / "normalized",
        snapshots_dir=tmp_path / "snapshots",
        ai_provider="mock",
        ai_max_retries=2,
        ai_retry_delays_seconds=[0, 0],
    )


def _seed_race_and_python(db: Session) -> None:
    race = Race(
        race_id=RACE_ID,
        race_date=date(2026, 7, 10),
        race_number=1,
        venue="東京",
        name="比較統合テスト",
        course="芝1600m",
        track_condition="良",
        headcount=6,
    )
    horses = [
        Horse(name=f"統合ホース{index}", normalized_name=f"統合ホース{index}")
        for index in range(1, 7)
    ]
    db.add_all([race, *horses])
    db.flush()
    for index, horse in enumerate(horses, start=1):
        db.add(
            RaceEntry(
                race_id=RACE_ID,
                horse_id=horse.id,
                horse_no=index,
                horse_name=horse.name,
                popularity=index,
                win_odds=float(index * 2),
            )
        )
        db.add(
            HorsePastPerformance(
                source_file="phase6.xlsx",
                source_sheet=RACE_ID,
                target_race_id=RACE_ID,
                past_race_id=f"2026060101{index:02d}",
                horse_name=horse.name,
                race_date=date(2026, 6, index),
                race_name="過去走",
                horse_no=index,
                finish_position=index,
                popularity=index,
                odds=float(index * 2),
                distance="1600m",
                raw={"馬場": "良"},
            )
        )

    prediction_run = PredictionRun(
        id=PREDICTION_RUN_ID,
        status="completed",
        race_date=race.race_date,
        race_id=RACE_ID,
        prediction_version="python-v6-test",
        feature_version="feature-v6-test",
        weight_version="weight-v6-test",
        model_version="model-v6-test",
        result_count=6,
        matched_count=6,
        mismatch_count=0,
        finished_at=datetime.utcnow(),
    )
    db.add(prediction_run)
    for index, horse in enumerate(horses, start=1):
        # Python順位を独立AIの決定論的順位と逆順にし、重大不一致を検証する。
        python_rank = 7 - index
        db.add(
            PredictionResult(
                prediction_run_id=PREDICTION_RUN_ID,
                race_id=RACE_ID,
                horse_no=index,
                horse_name=horse.name,
                prediction_rank=python_rank,
                prediction_score=float(100 - python_rank),
                estimated_in3_rate=round(0.1 * index, 2),
                expected_value=round(0.8 + 0.1 * index, 2),
                risk_flag=index == 6,
                risk_score=float(index),
                risk_reason="順位変動リスク" if index == 6 else None,
                evaluation_reason=f"Python固定ロジックで{python_rank}位",
            )
        )
    db.add(
        RaceQualityStatus(
            race_id=RACE_ID,
            status="GREEN",
            summary="整合性確認済み",
            issue_count=0,
            red_count=0,
            yellow_count=0,
        )
    )
    db.commit()


def _create_independent(db: Session, tmp_path: Path) -> dict:
    return run_independent_analysis(
        db,
        race_id=RACE_ID,
        provider=DeterministicMockAiProvider(),
        settings=_settings(tmp_path),
        sleeper=lambda _seconds: None,
    )


class _InvalidComparisonProvider(DeterministicMockAiProvider):
    def compare(self, input_snapshot):
        result = super().compare(input_snapshot)
        invalid_output = result.output.model_copy(deep=True)
        # 入力と異なる順位差を返すProvider応答は永続化前に拒否される。
        invalid_output.horses[0].rank_gap = 0
        return AiComparisonProviderResult(output=invalid_output)


def test_comparison_input_requires_locked_independent_result(tmp_path):
    with _session(tmp_path / "missing.db") as db:
        _seed_race_and_python(db)
        with pytest.raises(AiIntegrationError, match="固定済み独立AI結果がありません"):
            build_comparison_input(db, race_id=RACE_ID)


def test_comparison_and_integration_are_separate_locked_stages(tmp_path):
    with _session(tmp_path / "success.db") as db:
        _seed_race_and_python(db)
        independent_summary = _create_independent(db, tmp_path)
        independent_output = db.scalar(
            select(AiAnalysisOutput).where(
                AiAnalysisOutput.analysis_id == independent_summary["analysis_id"],
                AiAnalysisOutput.stage == "independent",
            )
        )
        original_json = dict(independent_output.output_json)
        original_hash = independent_output.output_hash

        comparison_input = build_comparison_input(db, race_id=RACE_ID)
        assert comparison_input.input_visibility == "python_result_visible_after_independent_lock"
        assert comparison_input.independent_output_hash == original_hash
        assert comparison_input.python_prediction_run_id == PREDICTION_RUN_ID

        summary = run_comparison_integration(
            db,
            race_id=RACE_ID,
            provider=DeterministicMockAiProvider(),
            settings=_settings(tmp_path),
            sleeper=lambda _seconds: None,
        )

        analysis = db.get(AiAnalysis, summary["analysis_id"])
        outputs = list(
            db.scalars(
                select(AiAnalysisOutput)
                .where(AiAnalysisOutput.analysis_id == analysis.id)
                .order_by(AiAnalysisOutput.stage)
            )
        )
        assert analysis.parent_analysis_id == independent_summary["analysis_id"]
        assert analysis.prediction_run_id == PREDICTION_RUN_ID
        assert analysis.status == "succeeded"
        assert {output.stage for output in outputs} == {"comparison", "integration"}
        assert all(output.is_locked and output.python_result_visible for output in outputs)
        assert summary["runner_count"] == 6
        assert summary["overall_alignment"] == "low"
        integration_output = next(output for output in outputs if output.stage == "integration")
        integrated_scores = [
            horse["integrated_score"]
            for horse in sorted(
                integration_output.output_json["horses"],
                key=lambda horse: horse["integrated_rank"],
            )
        ]
        assert all(score is not None for score in integrated_scores)
        assert all(
            left > right for left, right in zip(integrated_scores, integrated_scores[1:])
        )
        assert db.scalar(select(func.count(ArtifactFile.id))) == 7

        db.refresh(independent_output)
        assert independent_output.output_json == original_json
        assert independent_output.output_hash == original_hash
        assert independent_output.python_result_visible is False
        assert independent_output.is_locked is True


def test_tampered_independent_result_is_rejected_by_hash(tmp_path):
    with _session(tmp_path / "tamper.db") as db:
        _seed_race_and_python(db)
        independent_summary = _create_independent(db, tmp_path)
        output = db.scalar(
            select(AiAnalysisOutput).where(
                AiAnalysisOutput.analysis_id == independent_summary["analysis_id"]
            )
        )
        tampered = dict(output.output_json)
        tampered["final_comment"] = "hashを更新していない改変"
        output.output_json = tampered
        db.commit()

        with pytest.raises(AiIntegrationError, match="hashが一致しません"):
            build_comparison_input(db, race_id=RACE_ID)


def test_invalid_comparison_facts_are_not_persisted(tmp_path):
    with _session(tmp_path / "invalid.db") as db:
        _seed_race_and_python(db)
        _create_independent(db, tmp_path)
        with pytest.raises(AiIntegrationError, match="順位事実が入力と一致しません"):
            run_comparison_integration(
                db,
                race_id=RACE_ID,
                provider=_InvalidComparisonProvider(),
                settings=_settings(tmp_path),
                sleeper=lambda _seconds: None,
            )

        failed = db.scalar(
            select(AiAnalysis)
            .where(AiAnalysis.parent_analysis_id.is_not(None))
            .order_by(AiAnalysis.analysis_sequence.desc())
        )
        assert failed.status == "failed"
        assert db.scalar(
            select(func.count(AiAnalysisOutput.id)).where(
                AiAnalysisOutput.analysis_id == failed.id
            )
        ) == 0


def test_integration_rejects_rank_shift_over_exceptional_limit(tmp_path):
    with _session(tmp_path / "limit.db") as db:
        _seed_race_and_python(db)
        _create_independent(db, tmp_path)
        comparison_input = build_comparison_input(db, race_id=RACE_ID)
        provider = DeterministicMockAiProvider()
        comparison = provider.compare(comparison_input).output
        integration_input = IntegrationInput(
            comparison_input=comparison_input,
            comparison_output_hash=payload_sha256(
                comparison.model_dump(mode="json", exclude_none=False)
            ),
            comparison_result=comparison,
        )
        integration = provider.integrate(integration_input).output.model_copy(deep=True)
        by_python_rank = {horse.python_rank: horse for horse in integration.horses}
        # 1位と6位を入れ替えると5順位変更となり、例外上限4を超える。
        by_python_rank[1].integrated_rank = 6
        by_python_rank[6].integrated_rank = 1

        with pytest.raises(AiIntegrationError, match="変更上限を超えています"):
            _validate_integration(integration_input, integration)


def test_comparison_integration_job_uses_ai_queue(tmp_path, monkeypatch):
    calls: list[tuple[str, list[str], str]] = []
    monkeypatch.setattr(
        jobs_endpoint,
        "get_settings",
        lambda: SimpleNamespace(job_execution_mode="queue"),
    )
    monkeypatch.setattr(
        jobs_endpoint.celery_app,
        "send_task",
        lambda task_name, args, queue: calls.append((task_name, args, queue)),
    )

    with _session(tmp_path / "queue.db") as db:
        _seed_race_and_python(db)
        job = jobs_endpoint.create_job(
            JobCreate(
                job_type="ai.compare_integrate",
                race_date=date(2026, 7, 10),
                race_id=RACE_ID,
                params={"prediction_run_id": PREDICTION_RUN_ID},
            ),
            db,
        )

        assert job.status == "queued"
        assert calls == [("keiba_ai_studio.ai.compare_integrate", [job.id], "ai")]


def test_comparison_integration_api_preserves_independent_latest(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'api.db'}")
    upgrade_database(db_engine=engine)
    test_settings = _settings(tmp_path)
    with Session(engine) as db:
        _seed_race_and_python(db)
        independent_summary = _create_independent(db, tmp_path)

    def override_db():
        with Session(engine) as db:
            yield db

    monkeypatch.setattr(
        jobs_endpoint,
        "get_settings",
        lambda: SimpleNamespace(job_execution_mode="inline"),
    )
    monkeypatch.setattr(ai_integration_service, "get_settings", lambda: test_settings)
    monkeypatch.setattr(
        ai_integration_service,
        "create_independent_ai_provider",
        lambda _settings: DeterministicMockAiProvider(),
    )
    app.dependency_overrides[get_db] = override_db
    try:
        with TestClient(app) as client:
            created = client.post(
                "/api/v1/ai/comparison-integration",
                headers={"Idempotency-Key": "ai-comparison-api-001"},
                json={
                    "race_id": RACE_ID,
                    "race_date": "2026-07-10",
                    "independent_analysis_id": independent_summary["analysis_id"],
                    "prediction_run_id": PREDICTION_RUN_ID,
                },
            )
            latest_integration = client.get(
                f"/api/v1/races/{RACE_ID}/ai-integration-analysis"
            )
            latest_independent = client.get(
                f"/api/v1/races/{RACE_ID}/ai-independent-analysis"
            )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert created.status_code == 202
    assert created.json()["status"] == "completed"
    assert latest_integration.status_code == 200
    integration_payload = latest_integration.json()
    assert integration_payload["status"] == "succeeded"
    assert integration_payload["comparison_locked"] is True
    assert integration_payload["integration_locked"] is True
    assert integration_payload["comparison"]["opposition"]["has_material_opposition"] is True
    assert latest_independent.status_code == 200
    assert latest_independent.json()["id"] == independent_summary["analysis_id"]
