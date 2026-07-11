from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.api.v1.deps import get_db
from app.db.migrations import upgrade_database
from app.db.models import AiAnalysis, AiAnalysisOutput, BetCandidate, PredictionResult, PredictionRun
from app.main import app
from app.schemas.ai_integration import IntegratedHorsePrediction, IntegrationResponse
from app.schemas.api import BetStatusUpdate
from app.services.ai_independent import payload_sha256
from app.services.betting import generate_bet_candidates, update_bet_status


RACE_ID = "202607110101"
RUN_ID = "phase7-python-run-001"


def _session(path: Path) -> Session:
    engine = create_engine(f"sqlite:///{path}")
    upgrade_database(db_engine=engine)
    return Session(engine)


def _seed_python(db: Session) -> None:
    db.add(
        PredictionRun(
            id=RUN_ID,
            status="completed",
            race_date=date(2026, 7, 11),
            race_id=RACE_ID,
            prediction_version="phase7-python-v1",
            feature_version="phase7-feature-v1",
            weight_version="phase7-weight-v1",
            model_version="phase7-model-v1",
            result_count=6,
            matched_count=6,
            mismatch_count=0,
            finished_at=datetime.utcnow(),
        )
    )
    for rank in range(1, 7):
        db.add(
            PredictionResult(
                prediction_run_id=RUN_ID,
                race_id=RACE_ID,
                horse_no=rank,
                horse_name=f"買い目ホース{rank}",
                prediction_rank=rank,
                prediction_score=float(72 - rank * 3),
                estimated_in3_rate=round(0.7 - rank * 0.05, 2),
                expected_value=round(1.5 - rank * 0.1, 2),
                risk_flag=False,
                evaluation_reason=f"Python順位{rank}位",
            )
        )
    db.commit()


def _seed_integration(db: Session, *, manual_review_required: bool) -> AiAnalysis:
    analysis = AiAnalysis(
        race_id=RACE_ID,
        race_date=date(2026, 7, 11),
        prediction_run_id=RUN_ID,
        analysis_sequence=1,
        status="succeeded",
        model_name="phase7-mock",
        prompt_version="ai-comparison-v1.0.0+ai-integration-v1.1.0",
        input_data_version="ai_comparison_input_v1",
        input_snapshot_hash="1" * 64,
        started_at=datetime.now(timezone.utc),
        finished_at=datetime.now(timezone.utc),
    )
    db.add(analysis)
    db.flush()
    integration = IntegrationResponse(
        race_id=RACE_ID,
        independent_analysis_id="independent-phase7",
        python_prediction_run_id=RUN_ID,
        integration_strategy="balanced",
        data_confidence="medium",
        manual_review_required=manual_review_required,
        horses=[
            IntegratedHorsePrediction(
                horse_no=rank,
                horse_name=f"買い目ホース{rank}",
                python_rank=rank,
                ai_rank=rank,
                integrated_rank=rank,
                integrated_score=float(101 - rank * 10),
                decision_basis="balanced",
                confidence=0.7,
                uncertainty_level="medium" if manual_review_required else "low",
                reasons=["Pythonと独立AIの比較結果"],
                risk_summary="順位差を確認",
            )
            for rank in range(1, 7)
        ],
        key_disagreements=["手動確認対象"] if manual_review_required else [],
        uncertainties=["重大不一致"] if manual_review_required else [],
        final_comment="買い目作成前の統合結果です",
    )
    payload = integration.model_dump(mode="json", exclude_none=False)
    db.add(
        AiAnalysisOutput(
            analysis_id=analysis.id,
            stage="integration",
            output_schema_version=integration.schema_version,
            output_json=payload,
            output_hash=payload_sha256(payload),
            confidence=0.66,
            python_result_visible=True,
            is_locked=True,
            locked_at=datetime.now(timezone.utc),
        )
    )
    db.commit()
    db.refresh(analysis)
    return analysis


def test_python_plan_supports_bet_types_and_strategy_modes(tmp_path):
    with _session(tmp_path / "multi.db") as db:
        _seed_python(db)
        summary = generate_bet_candidates(
            db,
            race_id=RACE_ID,
            source_modes=["python"],
            bet_types=["3連複", "ワイド"],
            strategy_modes=["formation", "box", "wheel"],
            stake_per_point=100,
            max_race_amount=5000,
            max_day_amount=10000,
            max_points=20,
        )
        candidates = list(db.scalars(select(BetCandidate).order_by(BetCandidate.id)))

        assert summary.generated == 6
        assert summary.candidates == 6
        assert len(candidates) == 6
        assert {candidate.bet_type for candidate in candidates} == {"3連複", "ワイド"}
        assert {candidate.strategy_mode for candidate in candidates} == {
            "formation",
            "box",
            "wheel",
        }
        for candidate in candidates:
            expected_horses = 3 if candidate.bet_type == "3連複" else 2
            assert candidate.points == len(candidate.combinations)
            assert candidate.total_amount == candidate.points * candidate.stake_per_point
            assert candidate.total_amount <= candidate.max_race_amount
            assert len({tuple(combo) for combo in candidate.combinations}) == candidate.points
            assert all(len(combo) == expected_horses for combo in candidate.combinations)
            assert candidate.source_type == "python"
            assert candidate.source_snapshot_hash and len(candidate.source_snapshot_hash) == 64
            assert candidate.requires_confirmation is True
            assert candidate.purchase_execution_enabled is False
            assert "AUTOMATIC_PURCHASE_DISABLED" in candidate.warning_codes


def test_point_race_and_day_limits_block_candidates(tmp_path):
    with _session(tmp_path / "limits.db") as db:
        _seed_python(db)
        point_summary = generate_bet_candidates(
            db,
            race_id=RACE_ID,
            strategy_modes=["box"],
            stake_per_point=100,
            max_race_amount=5000,
            max_day_amount=10000,
            max_points=5,
        )
        point_candidate = db.scalar(select(BetCandidate))
        assert point_summary.blocked == 1
        assert point_candidate.status == "blocked"
        assert "POINT_LIMIT_EXCEEDED" in point_candidate.warning_codes

    with _session(tmp_path / "race-limit.db") as db:
        _seed_python(db)
        generate_bet_candidates(
            db,
            race_id=RACE_ID,
            stake_per_point=500,
            max_race_amount=1500,
            max_day_amount=5000,
        )
        candidate = db.scalar(select(BetCandidate))
        assert candidate.status == "blocked"
        assert "RACE_BUDGET_EXCEEDED" in candidate.warning_codes

    with _session(tmp_path / "day-limit.db") as db:
        _seed_python(db)
        summary = generate_bet_candidates(
            db,
            race_id=RACE_ID,
            strategy_modes=["formation", "box"],
            stake_per_point=500,
            max_race_amount=5000,
            max_day_amount=5000,
            max_points=20,
        )
        candidates = list(db.scalars(select(BetCandidate).order_by(BetCandidate.id)))
        assert summary.candidates == 1
        assert summary.blocked == 1
        assert candidates[0].status == "candidate"
        assert candidates[1].status == "blocked"
        assert "DAY_BUDGET_EXCEEDED" in candidates[1].warning_codes


def test_ai_integrated_plan_requires_manual_review_on_major_disagreement(tmp_path):
    with _session(tmp_path / "ai.db") as db:
        _seed_python(db)
        analysis = _seed_integration(db, manual_review_required=True)
        summary = generate_bet_candidates(
            db,
            race_id=RACE_ID,
            source_modes=["ai_integrated"],
            ai_analysis_id=analysis.id,
            stake_per_point=100,
            max_race_amount=5000,
            max_day_amount=10000,
        )
        candidate = db.scalar(select(BetCandidate))

        assert summary.review_required == 1
        assert candidate.status == "review_required"
        assert candidate.source_type == "ai_integrated"
        assert candidate.ai_analysis_id == analysis.id
        assert candidate.source_snapshot_hash and len(candidate.source_snapshot_hash) == 64
        assert "AI_MANUAL_REVIEW_REQUIRED" in candidate.warning_codes
        assert "統合score=" in candidate.reason

        allowed = generate_bet_candidates(
            db,
            race_id=RACE_ID,
            source_modes=["ai_integrated"],
            ai_analysis_id=analysis.id,
            strategy_modes=["wheel"],
            stake_per_point=100,
            max_race_amount=5000,
            max_day_amount=10000,
            allow_manual_review=True,
        )
        allowed_candidate = db.scalar(
            select(BetCandidate).where(BetCandidate.strategy_mode == "wheel")
        )
        assert allowed.candidates == 1
        assert allowed_candidate.status == "candidate"
        assert "AI_MANUAL_REVIEW_REQUIRED" in allowed_candidate.warning_codes


def test_missing_integration_is_warning_not_fabricated_plan(tmp_path):
    with _session(tmp_path / "missing-ai.db") as db:
        _seed_python(db)
        summary = generate_bet_candidates(
            db,
            race_id=RACE_ID,
            source_modes=["ai_integrated"],
        )
        assert summary.generated == 0
        assert summary.warnings
        assert "固定済みAI統合結果がない" in summary.warnings[0]
        assert db.scalar(select(func.count(BetCandidate.id))) == 0


def test_same_plan_request_reuses_existing_candidate(tmp_path):
    with _session(tmp_path / "reuse.db") as db:
        _seed_python(db)
        first = generate_bet_candidates(db, race_id=RACE_ID)
        second = generate_bet_candidates(db, race_id=RACE_ID)
        assert first.generated == 1
        assert second.generated == 1
        assert db.scalar(select(func.count(BetCandidate.id))) == 1


def test_purchase_status_is_manual_record_and_execution_stays_disabled(tmp_path):
    with _session(tmp_path / "purchase.db") as db:
        _seed_python(db)
        generate_bet_candidates(db, race_id=RACE_ID)
        candidate = db.scalar(select(BetCandidate))
        update_bet_status(db, candidate.id, BetStatusUpdate(status="planned", reason="手動確認"))
        updated = update_bet_status(
            db,
            candidate.id,
            BetStatusUpdate(status="purchased", reason="外部で購入した記録"),
        )
        assert updated.status == "purchased"
        assert updated.purchase_execution_enabled is False

        updated.purchase_execution_enabled = True
        db.add(updated)
        with pytest.raises(IntegrityError):
            db.commit()
        db.rollback()


def test_bet_generate_api_validates_money_units_and_limits(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'api.db'}")
    upgrade_database(db_engine=engine)
    with Session(engine) as db:
        _seed_python(db)

    def override_db():
        with Session(engine) as db:
            yield db

    app.dependency_overrides[get_db] = override_db
    try:
        with TestClient(app) as client:
            invalid_unit = client.post(
                "/api/v1/bets/generate",
                json={"race_id": RACE_ID, "stake_per_point": 150},
            )
            invalid_limits = client.post(
                "/api/v1/bets/generate",
                json={"race_id": RACE_ID, "max_race_amount": 5000, "max_day_amount": 3000},
            )
            valid = client.post(
                "/api/v1/bets/generate",
                json={
                    "race_id": RACE_ID,
                    "source_modes": ["python"],
                    "bet_types": ["ワイド"],
                    "strategy_modes": ["wheel"],
                    "stake_per_point": 100,
                    "max_race_amount": 3000,
                    "max_day_amount": 5000,
                    "max_points": 10,
                },
            )
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert invalid_unit.status_code == 422
    assert invalid_limits.status_code == 422
    assert valid.status_code == 201
    assert valid.json()["generated"] == 1
