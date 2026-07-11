from datetime import date

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.v1.deps import get_db
from app.db.models import DataQualityIssue, Race, RaceQualityStatus
from app.schemas.api import (
    DataQualityCheckRequest,
    DataQualityIssueRead,
    DataQualityRunSummary,
    RaceQualityStatusRead,
)
from app.services.data_quality import run_data_quality_checks

router = APIRouter()


@router.get("/data-quality/issues", response_model=list[DataQualityIssueRead])
def list_data_quality_issues(
    severity: str | None = None,
    race_id: str | None = None,
    db: Session = Depends(get_db),
    limit: int = Query(default=200, ge=1, le=1000),
) -> list[DataQualityIssue]:
    """List data quality issues recorded by import jobs."""

    stmt = select(DataQualityIssue).order_by(DataQualityIssue.created_at.desc()).limit(limit)
    if severity:
        stmt = stmt.where(DataQualityIssue.severity == severity)
    if race_id:
        stmt = stmt.where(DataQualityIssue.race_id == race_id)
    return list(db.scalars(stmt))


@router.get("/data-quality/statuses", response_model=list[RaceQualityStatusRead])
def list_data_quality_statuses(
    race_date: date | None = None,
    db: Session = Depends(get_db),
) -> list[RaceQualityStatus]:
    """List current race-level quality statuses."""

    stmt = select(RaceQualityStatus).order_by(RaceQualityStatus.checked_at.desc())
    if race_date:
        stmt = stmt.join(Race).where(Race.race_date == race_date)
    return list(db.scalars(stmt))


@router.post("/data-quality/checks", response_model=DataQualityRunSummary)
def run_data_quality_check(
    payload: DataQualityCheckRequest,
    db: Session = Depends(get_db),
) -> DataQualityRunSummary:
    """Run current data quality checks for a race date or one race."""

    summary = run_data_quality_checks(db, race_date=payload.race_date, race_id=payload.race_id)
    db.commit()
    return summary
