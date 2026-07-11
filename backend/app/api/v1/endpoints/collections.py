from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.v1.deps import get_db
from app.core.config import get_settings
from app.db.models import CollectionRun
from app.schemas.api import CollectionRunRead, CollectionSourceRead
from app.services.collector import SOURCE_POLICIES


router = APIRouter()


@router.get("/collections", response_model=list[CollectionRunRead])
def list_collection_runs(
    race_date: date | None = None,
    status_filter: str | None = Query(default=None, alias="status"),
    data_kind: str | None = None,
    db: Session = Depends(get_db),
    limit: int = Query(default=100, ge=1, le=500),
) -> list[CollectionRun]:
    """List collection status, failures, cache hits, and layer artifacts."""

    stmt = select(CollectionRun).order_by(CollectionRun.created_at.desc()).limit(limit)
    if race_date is not None:
        stmt = stmt.where(CollectionRun.race_date == race_date)
    if status_filter is not None:
        stmt = stmt.where(CollectionRun.status == status_filter)
    if data_kind is not None:
        stmt = stmt.where(CollectionRun.data_kind == data_kind)
    return list(db.scalars(stmt))


@router.get("/collections/{collection_run_id}", response_model=CollectionRunRead)
def get_collection_run(
    collection_run_id: str,
    db: Session = Depends(get_db),
) -> CollectionRun:
    """Get one collection run with safe failure and three-layer details."""

    collection_run = db.get(CollectionRun, collection_run_id)
    if collection_run is None:
        raise HTTPException(status_code=404, detail="collection run not found")
    return collection_run


@router.get("/collection-sources", response_model=list[CollectionSourceRead])
def list_collection_sources() -> list[CollectionSourceRead]:
    """Expose the approved source, cache, interval, and retry policy catalog."""

    approved_sources = set(get_settings().collector_approved_sources)
    return [
        CollectionSourceRead(
            job_type=policy.job_type,
            source_code=policy.source_code,
            data_kind=policy.data_kind,
            reliability_grade=policy.reliability_grade,
            adapter_configured=policy.output_kind is not None,
            execution_approved=policy.source_code in approved_sources,
            cache_ttl_seconds=policy.cache_ttl_seconds,
            min_interval_seconds=policy.min_interval_seconds,
            max_retries=policy.max_retries,
        )
        for policy in SOURCE_POLICIES.values()
    ]
