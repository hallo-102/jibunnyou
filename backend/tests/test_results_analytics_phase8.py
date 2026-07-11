from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.db.migrations import upgrade_database
from app.db.models import AiAnalysis, BetCandidate, PredictionResult, PredictionRun, Race
from app.schemas.api import PayoutItem, RaceResultCreate
from app.services.betting import (
    analytics_summary,
    generate_bet_candidates,
    settle_bets_for_race,
    upsert_race_result,
)


RACE_ID = "202607110201"
RUN_ID = "phase8-python-run-001"


def _session(path: Path) -> Session:
    engine = create_engine(f"sqlite:///{path}")
    upgrade_database(db_engine=engine)
    return Session(engine)


def _seed(db: Session) -> None:
    db.add(
        Race(
            race_id=RACE_ID,
            race_date=date(2026, 7, 11),
            race_number=2,
            venue="函館",
            name="Phase8分析テスト",
            course="芝1200m",
            race_class="1勝クラス",
            headcount=6,
        )
    )
    db.add(
        PredictionRun(
            id=RUN_ID,
            status="completed",
            race_date=date(2026, 7, 11),
            race_id=RACE_ID,
            prediction_version="phase8-v1",
            feature_version="feature-v1",
            weight_version="weight-v1",
            model_version="python-model-phase8",
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
                horse_name=f"結果ホース{rank}",
                prediction_rank=rank,
                prediction_score=float(70 - rank),
                expected_value=1.2,
                risk_flag=False,
            )
        )
    db.commit()


def _generate_two_types(db: Session) -> list[BetCandidate]:
    generate_bet_candidates(
        db,
        race_id=RACE_ID,
        bet_types=["3連複", "ワイド"],
        strategy_modes=["formation"],
        stake_per_point=100,
        max_race_amount=5000,
        max_day_amount=10000,
        max_points=20,
    )
    return list(db.scalars(select(BetCandidate).order_by(BetCandidate.bet_type)))


def test_structured_payout_settles_trio_and_wide(tmp_path):
    with _session(tmp_path / "settle.db") as db:
        _seed(db)
        candidates = _generate_two_types(db)
        by_type = {candidate.bet_type: candidate for candidate in candidates}
        result = upsert_race_result(
            db,
            RaceResultCreate(
                race_id=RACE_ID,
                race_date=date(2026, 7, 11),
                result_status="confirmed",
                finish_order=[1, 2, 3, 4, 5, 6],
                payouts=[
                    PayoutItem(
                        bet_type="3連複",
                        combination=by_type["3連複"].combinations[0],
                        payout_per_100=2400,
                    ),
                    PayoutItem(
                        bet_type="ワイド",
                        combination=by_type["ワイド"].combinations[0],
                        payout_per_100=520,
                    ),
                ],
            ),
        )
        settlements = settle_bets_for_race(db, RACE_ID)

        assert result.result_status == "confirmed"
        assert len(settlements) == 2
        assert all(item.is_hit and item.hit_count == 1 for item in settlements)
        assert {item.bet_type for item in settlements} == {"3連複", "ワイド"}
        assert sum(item.payout_amount for item in settlements) == 2920
        assert all(item.winning_combinations and item.payout_details_json for item in settlements)


def test_provisional_is_blocked_and_cancelled_refunds_all_tickets(tmp_path):
    with _session(tmp_path / "status.db") as db:
        _seed(db)
        candidate = _generate_two_types(db)[0]
        upsert_race_result(
            db,
            RaceResultCreate(
                race_id=RACE_ID,
                result_status="provisional",
                finish_order=[1, 2, 3],
            ),
        )
        with pytest.raises(ValueError, match="暫定結果"):
            settle_bets_for_race(db, RACE_ID)

        refund_items = [
            PayoutItem(
                bet_type=candidate.bet_type,
                combination=combo,
                payout_per_100=100,
                status="refund",
            )
            for combo in candidate.combinations
        ]
        upsert_race_result(
            db,
            RaceResultCreate(
                race_id=RACE_ID,
                result_status="cancelled",
                payouts=refund_items,
            ),
        )
        settlements = settle_bets_for_race(db, RACE_ID)
        target = next(item for item in settlements if item.bet_candidate_id == candidate.id)
        assert target.hit_count == candidate.points
        assert target.payout_amount == candidate.total_amount
        assert target.profit_loss == 0
        assert all(item["status"] == "refund" for item in target.payout_details_json)


def test_analytics_filters_and_breakdowns_include_models(tmp_path):
    with _session(tmp_path / "analytics.db") as db:
        _seed(db)
        candidates = _generate_two_types(db)
        analysis = AiAnalysis(
            race_id=RACE_ID,
            race_date=date(2026, 7, 11),
            prediction_run_id=RUN_ID,
            analysis_sequence=1,
            status="succeeded",
            model_name="ai-model-phase8",
            prompt_version="phase8",
        )
        db.add(analysis)
        db.flush()
        wide = next(item for item in candidates if item.bet_type == "ワイド")
        wide.source_type = "ai_integrated"
        wide.ai_analysis_id = analysis.id
        db.commit()
        trio = next(item for item in candidates if item.bet_type == "3連複")
        upsert_race_result(
            db,
            RaceResultCreate(
                race_id=RACE_ID,
                finish_order=[1, 2, 3],
                payouts=[
                    PayoutItem(
                        bet_type="3連複",
                        combination=trio.combinations[0],
                        payout_per_100=2000,
                    )
                ],
            ),
        )
        settle_bets_for_race(db, RACE_ID)

        summary = analytics_summary(
            db,
            date_from=date(2026, 7, 1),
            date_to=date(2026, 7, 31),
            venue="函館",
            prediction_model="python-model-phase8",
            group_by=["source_type", "bet_type", "venue", "course", "race_class", "ai_model"],
        )
        ai_only = analytics_summary(db, source_type="ai_integrated", ai_model="ai-model-phase8")

        assert summary.bets == 2
        assert summary.settled_bets == 2
        assert summary.hits == 1
        assert summary.hit_rate == 50.0
        assert summary.max_consecutive_losses == 1
        assert summary.max_drawdown >= 0
        assert {item.dimension for item in summary.breakdown} == {
            "source_type", "bet_type", "venue", "course", "race_class", "ai_model"
        }
        assert ai_only.bets == 1
        assert ai_only.hits == 0


def test_result_schema_rejects_invalid_combinations_and_duplicates():
    with pytest.raises(ValidationError):
        RaceResultCreate(race_id=RACE_ID, result_status="confirmed", finish_order=[])
    with pytest.raises(ValidationError):
        PayoutItem(bet_type="ワイド", combination=[1, 2, 3], payout_per_100=500)
    with pytest.raises(ValidationError):
        RaceResultCreate(
            race_id=RACE_ID,
            finish_order=[1, 2, 3],
            payouts=[
                PayoutItem(bet_type="ワイド", combination=[1, 2], payout_per_100=500),
                PayoutItem(bet_type="ワイド", combination=[2, 1], payout_per_100=500),
            ],
        )


def test_analytics_rejects_unknown_group(tmp_path):
    with _session(tmp_path / "group.db") as db:
        _seed(db)
        with pytest.raises(ValueError, match="unsupported analytics group"):
            analytics_summary(db, group_by=["unknown"])
