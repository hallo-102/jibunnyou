from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.v1.deps import get_db
from app.db.models import BetCandidate, BetSettlement, RaceResult, ReviewNote
from app.schemas.api import (
    AnalyticsSummaryRead,
    BetCandidateRead,
    BetGenerateRequest,
    BetGenerationSummary,
    BetSettlementRead,
    BetStatusUpdate,
    RaceResultCreate,
    RaceResultRead,
    ReviewNoteCreate,
    ReviewNoteRead,
)
from app.services.betting import (
    analytics_summary,
    create_review_note,
    generate_bet_candidates,
    settle_bets_for_race,
    update_bet_status,
    upsert_race_result,
)
from app.services.idempotency import (
    IdempotencyConflict,
    IdempotencyInProgress,
    IdempotencyPreviouslyFailed,
    begin_idempotent_request,
    complete_idempotent_request,
    fail_idempotent_request,
)

router = APIRouter()


@router.get("/bets", response_model=list[BetCandidateRead])
def list_bets(
    race_date: date | None = None,
    race_id: str | None = None,
    status_filter: str | None = Query(default=None, alias="status"),
    db: Session = Depends(get_db),
    limit: int = Query(default=200, ge=1, le=500),
) -> list[BetCandidate]:
    """List generated bet candidates."""

    stmt = select(BetCandidate).order_by(BetCandidate.created_at.desc()).limit(limit)
    if race_date is not None:
        stmt = stmt.where(BetCandidate.race_date == race_date)
    if race_id is not None:
        stmt = stmt.where(BetCandidate.race_id == race_id)
    if status_filter is not None:
        stmt = stmt.where(BetCandidate.status == status_filter)
    return list(db.scalars(stmt))


@router.get("/races/{race_id}/bets", response_model=list[BetCandidateRead])
def list_race_bets(race_id: str, db: Session = Depends(get_db)) -> list[BetCandidate]:
    """List bet candidates for one race."""

    stmt = (
        select(BetCandidate)
        .where(BetCandidate.race_id == race_id)
        .order_by(BetCandidate.created_at.desc())
    )
    return list(db.scalars(stmt))


@router.post("/bets/generate", response_model=BetGenerationSummary, status_code=status.HTTP_201_CREATED)
def generate_bets(payload: BetGenerateRequest, db: Session = Depends(get_db)) -> BetGenerationSummary:
    """Generate bounded previews without executing any purchase."""

    try:
        return generate_bet_candidates(
            db,
            race_date=payload.race_date,
            race_id=payload.race_id,
            prediction_run_id=payload.prediction_run_id,
            source_modes=payload.source_modes,
            bet_types=payload.bet_types,
            strategy_modes=payload.strategy_modes,
            ai_analysis_id=payload.ai_analysis_id,
            stake_per_point=payload.stake_per_point,
            max_race_amount=payload.max_race_amount,
            max_day_amount=payload.max_day_amount,
            max_points=payload.max_points,
            allow_manual_review=payload.allow_manual_review,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.patch("/bets/{bet_id}/status", response_model=BetCandidateRead)
def patch_bet_status(
    bet_id: int,
    payload: BetStatusUpdate,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=8, max_length=200)],
    db: Session = Depends(get_db),
) -> BetCandidate:
    """Update a bet candidate status."""

    if db.get(BetCandidate, bet_id) is None:
        raise HTTPException(status_code=404, detail="bet candidate not found")
    idempotency_record = None
    try:
        idempotency_record, replay = begin_idempotent_request(
            db,
            scope=f"PATCH:/api/v1/bets/{bet_id}/status",
            idempotency_key=idempotency_key,
            payload=payload,
        )
        if replay:
            replayed_candidate = db.get(BetCandidate, idempotency_record.resource_id)
            if replayed_candidate is None:
                raise HTTPException(status_code=409, detail="idempotent bet response is unavailable")
            return replayed_candidate

        candidate = update_bet_status(db, bet_id, payload)
        complete_idempotent_request(
            db,
            idempotency_record,
            response_status=status.HTTP_200_OK,
            response_body=BetCandidateRead.model_validate(candidate).model_dump(mode="json"),
            resource_type="bet_candidates",
            resource_id=str(candidate.id),
        )
        return candidate
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (IdempotencyConflict, IdempotencyInProgress, IdempotencyPreviouslyFailed) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        if idempotency_record is not None:
            fail_idempotent_request(
                db,
                idempotency_record,
                response_status=status.HTTP_422_UNPROCESSABLE_ENTITY,
                response_body={"detail": str(exc)},
            )
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/results", response_model=RaceResultRead, status_code=status.HTTP_201_CREATED)
def save_result(payload: RaceResultCreate, db: Session = Depends(get_db)) -> RaceResult:
    """Save race result and payout information."""

    return upsert_race_result(db, payload)


@router.get("/races/{race_id}/result", response_model=RaceResultRead | None)
def get_race_result(race_id: str, db: Session = Depends(get_db)) -> RaceResult | None:
    """Get saved result for one race."""

    return db.scalar(select(RaceResult).where(RaceResult.race_id == race_id))


@router.post("/races/{race_id}/settle", response_model=list[BetSettlementRead])
def settle_race_bets(race_id: str, db: Session = Depends(get_db)) -> list[BetSettlement]:
    """Settle bet candidates for one race."""

    try:
        return settle_bets_for_race(db, race_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/reviews", response_model=list[ReviewNoteRead])
def list_reviews(
    race_date: date | None = None,
    race_id: str | None = None,
    db: Session = Depends(get_db),
    limit: int = Query(default=100, ge=1, le=500),
) -> list[ReviewNote]:
    """List review notes."""

    stmt = select(ReviewNote).order_by(ReviewNote.created_at.desc()).limit(limit)
    if race_date is not None:
        stmt = stmt.where(ReviewNote.race_date == race_date)
    if race_id is not None:
        stmt = stmt.where(ReviewNote.race_id == race_id)
    return list(db.scalars(stmt))


@router.post("/reviews", response_model=ReviewNoteRead, status_code=status.HTTP_201_CREATED)
def save_review(payload: ReviewNoteCreate, db: Session = Depends(get_db)) -> ReviewNote:
    """Create a review note."""

    return create_review_note(db, payload)


@router.get("/analytics", response_model=AnalyticsSummaryRead)
def get_analytics(
    race_date: date | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    source_type: str | None = None,
    bet_type: str | None = None,
    venue: str | None = None,
    course: str | None = None,
    race_class: str | None = None,
    prediction_model: str | None = None,
    ai_model: str | None = None,
    group_by: str | None = None,
    db: Session = Depends(get_db),
) -> AnalyticsSummaryRead:
    """Get filtered betting performance and condition breakdowns."""

    try:
        return analytics_summary(
            db,
            race_date=race_date,
            date_from=date_from,
            date_to=date_to,
            source_type=source_type,
            bet_type=bet_type,
            venue=venue,
            course=course,
            race_class=race_class,
            prediction_model=prediction_model,
            ai_model=ai_model,
            group_by=[value.strip() for value in group_by.split(",") if value.strip()] if group_by else None,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
