from datetime import date, datetime, timezone
import re
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.api.v1.deps import get_db
from app.db.models import Horse, Race, RaceDay, RaceEntry, RawFileRecord
from app.legacy_bridge.excel_importer import (
    import_race_workbook,
    list_feature_race_workbooks,
    resolve_feature_race_workbook,
)
from app.legacy_bridge.normalization import normalize_horse_name
from app.services.data_quality import run_data_quality_checks
from app.schemas.api import (
    RaceCreate,
    RaceDayRead,
    RaceDetail,
    RaceEntryCreate,
    RaceEntryRead,
    RaceRead,
    RaceWorkbookFileRead,
    RaceWorkbookSelectRequest,
    RaceWorkbookSelectionRead,
)

router = APIRouter()


@router.get("/race-days", response_model=list[RaceDayRead])
def list_race_days(
    db: Session = Depends(get_db),
    limit: int = Query(default=50, ge=1, le=200),
) -> list[RaceDay]:
    """List imported race days."""

    stmt = select(RaceDay).order_by(RaceDay.race_date.desc()).limit(limit)
    return list(db.scalars(stmt))


@router.get("/race-workbooks", response_model=list[RaceWorkbookFileRead])
def list_race_workbook_files(db: Session = Depends(get_db)) -> list[RaceWorkbookFileRead]:
    """List strict feature workbooks available in the configured output folder."""

    imported_names = set(db.scalars(select(RawFileRecord.file_name)))
    return [_workbook_read(path, path.name in imported_names) for path in list_feature_race_workbooks()]


@router.post("/race-workbooks/select", response_model=RaceWorkbookSelectionRead)
def select_race_workbook(
    payload: RaceWorkbookSelectRequest,
    db: Session = Depends(get_db),
) -> RaceWorkbookSelectionRead:
    """Import an explicitly selected output workbook and refresh its quality status."""

    try:
        path = resolve_feature_race_workbook(payload.file_name)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="選択したExcelファイルが見つかりません") from exc

    import_summary = import_race_workbook(db, path)
    quality_summary = run_data_quality_checks(db, race_date=import_summary.race_date)
    db.commit()
    return RaceWorkbookSelectionRead(
        workbook=_workbook_read(path, True),
        import_summary=import_summary,
        quality_summary=quality_summary,
    )


def _workbook_read(path: Path, is_imported: bool) -> RaceWorkbookFileRead:
    match = re.fullmatch(r"馬の競走成績_with_feat_(\d{8})\.xlsx", path.name)
    if match is None:
        raise ValueError(f"invalid feature workbook: {path.name}")
    stat = path.stat()
    return RaceWorkbookFileRead(
        file_name=path.name,
        race_date=datetime.strptime(match.group(1), "%Y%m%d").date(),
        size_bytes=stat.st_size,
        modified_at=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
        is_imported=is_imported,
    )


@router.get("/races", response_model=list[RaceRead])
def list_races(
    race_date: date | None = None,
    db: Session = Depends(get_db),
    limit: int = Query(default=200, ge=1, le=500),
) -> list[Race]:
    """List races, optionally filtered by race date."""

    stmt = select(Race).order_by(Race.race_date.desc(), Race.venue, Race.race_number).limit(limit)
    if race_date is not None:
        stmt = stmt.where(Race.race_date == race_date)
    return list(db.scalars(stmt))


@router.post("/races", response_model=RaceRead, status_code=status.HTTP_201_CREATED)
def create_race(payload: RaceCreate, db: Session = Depends(get_db)) -> Race:
    """Create or update a race record."""

    race = db.get(Race, payload.race_id)
    if race is None:
        race = Race(race_id=payload.race_id)
        db.add(race)

    for field, value in payload.model_dump(exclude={"race_id"}).items():
        setattr(race, field, value)

    if payload.race_date is not None:
        race_day = db.scalar(select(RaceDay).where(RaceDay.race_date == payload.race_date))
        if race_day is None:
            db.add(RaceDay(race_date=payload.race_date, source="api"))

    db.commit()
    db.refresh(race)
    return race


@router.get("/races/{race_id}", response_model=RaceDetail)
def get_race(race_id: str, db: Session = Depends(get_db)) -> Race:
    """Get one race with entries."""

    stmt = (
        select(Race)
        .where(Race.race_id == race_id)
        .options(selectinload(Race.entries).selectinload(RaceEntry.horse))
    )
    race = db.scalar(stmt)
    if race is None:
        raise HTTPException(status_code=404, detail="race not found")
    race.entries.sort(key=lambda entry: entry.horse_no)
    return race


@router.get("/races/{race_id}/entries", response_model=list[RaceEntryRead])
def list_entries(race_id: str, db: Session = Depends(get_db)) -> list[RaceEntry]:
    """List entries for one race."""

    if db.get(Race, race_id) is None:
        raise HTTPException(status_code=404, detail="race not found")
    stmt = select(RaceEntry).where(RaceEntry.race_id == race_id).order_by(RaceEntry.horse_no)
    return list(db.scalars(stmt))


@router.post(
    "/races/{race_id}/entries",
    response_model=RaceEntryRead,
    status_code=status.HTTP_201_CREATED,
)
def upsert_entry(
    race_id: str,
    payload: RaceEntryCreate,
    db: Session = Depends(get_db),
) -> RaceEntry:
    """Create or update a race entry."""

    race = db.get(Race, race_id)
    if race is None:
        raise HTTPException(status_code=404, detail="race not found")

    existing = db.scalar(
        select(RaceEntry).where(
            RaceEntry.race_id == race_id,
            RaceEntry.horse_no == payload.horse_no,
        )
    )

    horse_name = payload.horse_name.strip()
    if not horse_name:
        raise HTTPException(status_code=422, detail="horse_name is required")

    horse = db.scalar(select(Horse).where(Horse.name == horse_name))
    if horse is None:
        horse = Horse(name=horse_name, normalized_name=normalize_horse_name(horse_name))
        db.add(horse)
        db.flush()

    entry = existing or RaceEntry(
        race_id=race_id,
        horse_no=payload.horse_no,
        horse_name=horse_name,
    )
    if existing is None:
        db.add(entry)

    entry.horse_id = horse.id
    entry.horse_name = horse_name
    for field, value in payload.model_dump(exclude={"horse_name"}).items():
        setattr(entry, field, value)

    if race.headcount is not None and payload.horse_no > race.headcount:
        raise HTTPException(
            status_code=422,
            detail="horse_no must not exceed race headcount",
        )

    db.commit()
    db.refresh(entry)
    return entry
