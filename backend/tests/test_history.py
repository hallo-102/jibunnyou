from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db.migrations import upgrade_database
from app.db.models import (
    AiAnalysis,
    AiAnalysisOutput,
    ArtifactFile,
    AuditLog,
    BetCandidate,
    BetStatusHistory,
    IdempotencyRecord,
    PredictionRun,
)
from app.schemas.api import BetStatusUpdate
from app.services.betting import update_bet_status
from app.services.history import ImmutableArtifactError, register_artifact


def _session(path: Path) -> Session:
    """Return a session backed by a fully-migrated isolated database."""

    db_engine = create_engine(f"sqlite:///{path}")
    upgrade_database(db_engine=db_engine)
    return Session(db_engine)


def _prediction_run(run_id: str) -> PredictionRun:
    """Create the minimum valid immutable prediction run for history tests."""

    return PredictionRun(id=run_id, status="completed")


def test_bet_transition_appends_history_and_audit(tmp_path):
    with _session(tmp_path / "bet-history.db") as db:
        run = _prediction_run("run-history-001")
        candidate = BetCandidate(
            prediction_run_id=run.id,
            race_id="202607100101",
            rank="A",
            status="candidate",
            bet_type="3連複",
            strategy="テスト戦略",
            points=1,
            stake_per_point=500,
            total_amount=500,
            max_race_amount=3000,
            max_day_amount=12000,
        )
        db.add_all([run, candidate])
        db.commit()

        update_bet_status(db, candidate.id, BetStatusUpdate(status="planned", reason="確認済み"))
        update_bet_status(db, candidate.id, BetStatusUpdate(status="purchased", reason="手動購入"))
        update_bet_status(db, candidate.id, BetStatusUpdate(status="settled", reason="結果確定"))

        with pytest.raises(ValueError, match="invalid bet status transition"):
            update_bet_status(db, candidate.id, BetStatusUpdate(status="planned"))

        history = list(
            db.scalars(
                select(BetStatusHistory)
                .where(BetStatusHistory.bet_candidate_id == candidate.id)
                .order_by(BetStatusHistory.created_at)
            )
        )
        assert [(item.old_status, item.new_status) for item in history] == [
            ("candidate", "planned"),
            ("planned", "purchased"),
            ("purchased", "settled"),
        ]
        assert db.scalar(select(func.count(AuditLog.id))) == 3


def test_registered_artifact_cannot_change_in_place(tmp_path):
    artifact_path = tmp_path / "run-output.json"
    artifact_path.write_text('{"rank":1}', encoding="utf-8")

    with _session(tmp_path / "artifact-history.db") as db:
        run = _prediction_run("run-artifact-001")
        db.add(run)
        db.commit()

        artifact = register_artifact(
            db,
            path=artifact_path,
            artifact_kind="prediction_json",
            logical_name="engine_prediction_v1.json",
            prediction_run_id=run.id,
            content_type="application/json",
        )
        db.commit()
        original_hash = artifact.sha256

        same_artifact = register_artifact(
            db,
            path=artifact_path,
            artifact_kind="prediction_json",
            logical_name="engine_prediction_v1.json",
            prediction_run_id=run.id,
        )
        assert same_artifact.id == artifact.id
        assert db.scalar(select(func.count(ArtifactFile.id))) == 1

        artifact_path.write_text('{"rank":2}', encoding="utf-8")
        with pytest.raises(ImmutableArtifactError, match="content changed"):
            register_artifact(
                db,
                path=artifact_path,
                artifact_kind="prediction_json",
                logical_name="engine_prediction_v1.json",
                prediction_run_id=run.id,
            )
        assert artifact.sha256 == original_hash


def test_independent_ai_output_rejects_python_visibility(tmp_path):
    with _session(tmp_path / "ai-output.db") as db:
        analysis = AiAnalysis(
            race_id="202607100102",
            analysis_sequence=1,
            status="running",
            model_name="test-model",
            prompt_version="independent-v1",
        )
        db.add(analysis)
        db.commit()

        db.add(
            AiAnalysisOutput(
                analysis_id=analysis.id,
                stage="independent",
                output_schema_version="ai-independent-v1",
                output_json={"ranking": []},
                output_hash="a" * 64,
                python_result_visible=True,
            )
        )
        with pytest.raises(IntegrityError):
            db.commit()
        db.rollback()


def test_idempotency_key_is_unique_within_scope(tmp_path):
    with _session(tmp_path / "idempotency.db") as db:
        expires_at = datetime(2026, 7, 11, tzinfo=timezone.utc)
        first = IdempotencyRecord(
            scope="POST:/api/v1/jobs",
            idempotency_key="request-001",
            request_hash="b" * 64,
            expires_at=expires_at,
        )
        db.add(first)
        db.commit()

        db.add(
            IdempotencyRecord(
                scope="POST:/api/v1/jobs",
                idempotency_key="request-001",
                request_hash="b" * 64,
                expires_at=expires_at,
            )
        )
        with pytest.raises(IntegrityError):
            db.commit()
        db.rollback()

