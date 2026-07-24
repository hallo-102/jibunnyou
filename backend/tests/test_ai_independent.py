from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.db.migrations import upgrade_database
from app.db.models import (
    AiAnalysis,
    AiAnalysisOutput,
    ArtifactFile,
    Horse,
    HorsePastPerformance,
    Race,
    RaceEntry,
    RaceQualityStatus,
)
from app.api.v1.endpoints import jobs as jobs_endpoint
from app.schemas.api import JobCreate
from app.schemas.ai_independent import (
    AiRaceAssessment,
    IndependentAnalysisResponse,
    IndependentHorseEvaluation,
)
from app.services.ai_independent import (
    AiIndependentError,
    assert_python_hidden,
    build_independent_input,
    run_independent_analysis,
)
from app.services.ai_provider import (
    AiProviderResult,
    AiProviderUnavailable,
    DeterministicMockAiProvider,
    create_independent_ai_provider,
)
from app.services import ai_independent as ai_independent_service
from app.db.session import SessionLocal
from app.main import app


def _session(path: Path) -> Session:
    engine = create_engine(f"sqlite:///{path}")
    upgrade_database(db_engine=engine)
    return Session(engine)


def _settings(tmp_path: Path, **overrides) -> Settings:
    values = {
        "environment": "test",
        "database_url": f"sqlite:///{tmp_path / 'settings.db'}",
        "exports_dir": tmp_path / "exports",
        "staging_dir": tmp_path / "staging",
        "logs_dir": tmp_path / "logs",
        "raw_snapshots_dir": tmp_path / "raw",
        "normalized_dir": tmp_path / "normalized",
        "snapshots_dir": tmp_path / "snapshots",
        "ai_provider": "mock",
        "ai_max_retries": 2,
        "ai_retry_delays_seconds": [0, 0],
    }
    values.update(overrides)
    # テストは利用者の実.envを読まず、fixtureで指定した設定だけを使用する。
    return Settings(_env_file=None, **values)


def _seed_race(db: Session) -> None:
    race = Race(
        race_id="202607100901",
        race_date=date(2026, 7, 10),
        race_number=1,
        venue="東京",
        name="独立AIテスト",
        course="芝1600m",
        track_condition="良",
        headcount=3,
        # rawにはPython由来列を故意に入れ、allowlistで除外されることを確認する。
        raw={"天候": "晴", "距離": "1600m", "prediction_score": 9876.5},
    )
    horses = [
        Horse(name=f"テストホース{index}", normalized_name=f"テストホース{index}")
        for index in range(1, 4)
    ]
    db.add_all([race, *horses])
    db.flush()
    for index, horse in enumerate(horses, start=1):
        db.add(
            RaceEntry(
                race_id=race.race_id,
                horse_id=horse.id,
                horse_no=index,
                frame_no=index,
                horse_name=horse.name,
                age=4,
                carried_weight=56,
                jockey=f"騎手{index}",
                trainer=f"厩舎{index}",
                popularity=index,
                win_odds=float(index * 2),
                place_odds=float(index),
                # 独立入力では以下4列を絶対に参照しない。
                prediction_rank=4 - index,
                prediction_score=9000 + index,
                estimated_in3_rate=8000 + index,
                expected_value=7000 + index,
                raw={
                    "脚質": "差し",
                    "調教要約": "終いの反応を確認",
                    "expected_value": 6000 + index,
                    "danger_judgement": "禁止値",
                },
            )
        )
        db.add(
            HorsePastPerformance(
                source_file="test.xlsx",
                source_sheet=race.race_id,
                target_race_id=race.race_id,
                past_race_id=f"20260601010{index}",
                horse_name=horse.name,
                race_date=date(2026, 6, index),
                race_name="過去走",
                horse_no=index,
                finish_position=index,
                popularity=index + 1,
                odds=float(index * 3),
                distance="1600m",
                jockey=f"騎手{index}",
                raw={"馬場": "良", "score": 5000 + index},
            )
        )
    db.add(
        RaceQualityStatus(
            race_id=race.race_id,
            status="GREEN",
            summary="整合性確認済み",
            issue_count=0,
            red_count=0,
            yellow_count=0,
        )
    )
    db.commit()


class _CapturingProvider(DeterministicMockAiProvider):
    def __init__(self) -> None:
        self.captured = None

    def analyze(self, input_snapshot):
        self.captured = input_snapshot.model_dump(mode="json", exclude_none=False)
        return super().analyze(input_snapshot)


class _InvalidHorseProvider(DeterministicMockAiProvider):
    def analyze(self, input_snapshot):
        output = IndependentAnalysisResponse(
            race_id=input_snapshot.race.race_id,
            decision_status="completed",
            data_confidence="medium",
            manual_review_required=False,
            race_assessment=AiRaceAssessment(
                pace_assessment="uncertain",
                track_bias_assessment="判断材料が不足しています",
                summary="入力情報だけで評価しました",
            ),
            runners=[
                IndependentHorseEvaluation(
                    horse_no=40 if index == 1 else runner.horse_no,
                    horse_name=runner.horse_name,
                    ai_rank=index,
                    rank_range_low=index,
                    rank_range_high=index,
                    confidence=0.5,
                    risk_level="medium",
                    rationale="入力情報だけを使用した評価です",
                )
                for index, runner in enumerate(input_snapshot.runners, start=1)
            ],
            final_comment="検証用の不正な馬番を含む応答です",
        )
        return AiProviderResult(output=output)


class _TransientProvider(DeterministicMockAiProvider):
    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.calls = 0

    def analyze(self, input_snapshot):
        self.calls += 1
        if self.calls <= self.failures:
            raise TimeoutError("temporary timeout")
        return super().analyze(input_snapshot)


class _QuotaError(RuntimeError):
    status_code = 429


class _QuotaProvider(DeterministicMockAiProvider):
    def __init__(self) -> None:
        self.calls = 0

    def analyze(self, input_snapshot):
        self.calls += 1
        raise _QuotaError("insufficient_quota: exceeded your current quota")


def _all_keys(value) -> set[str]:
    keys: set[str] = set()
    if isinstance(value, dict):
        for key, nested in value.items():
            keys.add(key)
            keys.update(_all_keys(nested))
    elif isinstance(value, list):
        for nested in value:
            keys.update(_all_keys(nested))
    return keys


def test_independent_input_uses_allowlist_and_hides_python_results(tmp_path):
    with _session(tmp_path / "input.db") as db:
        _seed_race(db)
        snapshot = build_independent_input(db, "202607100901")
        payload = snapshot.model_dump(mode="json", exclude_none=False)

        assert_python_hidden(payload)
        keys = _all_keys(payload)
        assert not {
            "prediction_rank",
            "prediction_score",
            "estimated_in3_rate",
            "expected_value",
            "score",
            "danger_judgement",
        } & keys
        assert "9876.5" not in snapshot.model_dump_json()
        assert "9001" not in snapshot.model_dump_json()
        assert snapshot.input_visibility == "python_result_hidden"
        assert all(item.race_date < snapshot.race.race_date for runner in snapshot.runners for item in runner.past_performances)


def test_independent_analysis_is_validated_hashed_and_locked(tmp_path):
    with _session(tmp_path / "success.db") as db:
        _seed_race(db)
        provider = _CapturingProvider()
        summary = run_independent_analysis(
            db,
            race_id="202607100901",
            provider=provider,
            settings=_settings(tmp_path),
            sleeper=lambda _seconds: None,
        )

        assert summary["status"] == "succeeded"
        assert summary["runner_count"] == 3
        assert provider.captured is not None
        assert_python_hidden(provider.captured)

        analysis = db.get(AiAnalysis, summary["analysis_id"])
        output = db.scalar(
            select(AiAnalysisOutput).where(AiAnalysisOutput.analysis_id == analysis.id)
        )
        assert analysis is not None
        assert analysis.input_snapshot_hash == summary["input_snapshot_hash"]
        assert output is not None
        assert output.python_result_visible is False
        assert output.is_locked is True
        assert output.output_hash == summary["output_hash"]
        assert db.scalar(select(func.count(ArtifactFile.id))) == 2

        second = run_independent_analysis(
            db,
            race_id="202607100901",
            provider=DeterministicMockAiProvider(),
            settings=_settings(tmp_path),
            sleeper=lambda _seconds: None,
        )
        assert second["analysis_id"] != summary["analysis_id"]
        assert db.get(AiAnalysis, second["analysis_id"]).analysis_sequence == 2
        assert db.scalar(select(func.count(AiAnalysisOutput.id))) == 2


def test_invalid_horse_is_rejected_without_output(tmp_path):
    with _session(tmp_path / "invalid.db") as db:
        _seed_race(db)
        with pytest.raises(AiIndependentError, match="存在しない馬番"):
            run_independent_analysis(
                db,
                race_id="202607100901",
                provider=_InvalidHorseProvider(),
                settings=_settings(tmp_path),
                sleeper=lambda _seconds: None,
            )

        analysis = db.scalar(select(AiAnalysis))
        assert analysis.status == "failed"
        assert db.scalar(select(func.count(AiAnalysisOutput.id))) == 0


def test_transient_error_retries_only_within_limit(tmp_path):
    with _session(tmp_path / "retry.db") as db:
        _seed_race(db)
        provider = _TransientProvider(failures=2)
        summary = run_independent_analysis(
            db,
            race_id="202607100901",
            provider=provider,
            settings=_settings(tmp_path),
            sleeper=lambda _seconds: None,
        )
        assert summary["attempts"] == 3
        assert summary["retries"] == 2
        assert provider.calls == 3


def test_insufficient_quota_stops_without_retry_and_returns_japanese_guidance(tmp_path):
    with _session(tmp_path / "quota.db") as db:
        _seed_race(db)
        provider = _QuotaProvider()

        with pytest.raises(AiIndependentError, match="API Platform側のBilling"):
            run_independent_analysis(
                db,
                race_id="202607100901",
                provider=provider,
                settings=_settings(tmp_path),
                sleeper=lambda _seconds: None,
            )

        assert provider.calls == 1
        analysis = db.scalar(select(AiAnalysis))
        assert analysis.status == "failed"


def test_retired_openai_provider_is_disabled_without_api_key(tmp_path):
    settings = _settings(tmp_path, ai_provider="openai")
    with pytest.raises(AiProviderUnavailable, match="廃止されました"):
        create_independent_ai_provider(settings)


def test_independent_api_job_type_is_no_longer_supported(tmp_path, monkeypatch):
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
        _seed_race(db)
        with pytest.raises(Exception) as exc_info:
            jobs_endpoint.create_job(
                JobCreate(
                    job_type="ai.independent",
                    race_date=date(2026, 7, 10),
                    race_id="202607100901",
                ),
                db,
            )

        assert getattr(exc_info.value, "status_code", None) == 422
        assert calls == []


def test_independent_analysis_api_is_gone_and_history_remains_readable(monkeypatch):
    monkeypatch.setattr(
        ai_independent_service,
        "create_independent_ai_provider",
        lambda _settings: DeterministicMockAiProvider(),
    )
    race_id = "202607101001"
    with TestClient(app) as client:
        with SessionLocal() as db:
            if db.get(Race, race_id) is None:
                race = Race(
                    race_id=race_id,
                    race_date=date(2026, 7, 10),
                    race_number=1,
                    venue="東京",
                    name="独立AI APIテスト",
                    headcount=2,
                )
                horses = [
                    Horse(name="APIホース1", normalized_name="APIホース1"),
                    Horse(name="APIホース2", normalized_name="APIホース2"),
                ]
                db.add_all([race, *horses])
                db.flush()
                db.add_all(
                    [
                        RaceEntry(
                            race_id=race_id,
                            horse_id=horses[0].id,
                            horse_no=1,
                            horse_name=horses[0].name,
                            popularity=1,
                            win_odds=2.5,
                        ),
                        RaceEntry(
                            race_id=race_id,
                            horse_id=horses[1].id,
                            horse_no=2,
                            horse_name=horses[1].name,
                            popularity=2,
                            win_odds=4.5,
                        ),
                    ]
                )
                db.commit()

        created = client.post(
            "/api/v1/ai/independent-analysis",
            headers={"Idempotency-Key": "ai-independent-api-001"},
            json={"race_id": race_id, "race_date": "2026-07-10"},
        )
        latest = client.get(f"/api/v1/races/{race_id}/ai-independent-analysis")

    assert created.status_code == 410
    assert "廃止" in created.text
    assert latest.status_code == 200
    assert latest.json() is None
