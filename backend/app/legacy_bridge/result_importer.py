from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.models import RaceResult, RawFileRecord
from app.legacy_bridge.normalization import (
    file_sha256,
    normalize_race_id,
    parse_date,
    parse_race_date_from_filename,
    safe_int,
)
from app.schemas.api import ImportSummaryRead


def import_latest_result_artifact(
    db: Session,
    race_date: date | None = None,
) -> ImportSummaryRead:
    """Record the latest result workbook as a Phase 4 imported raw artifact."""

    path = find_result_workbook(race_date=race_date)
    if path is None:
        date_text = race_date.isoformat() if race_date else "latest"
        raise FileNotFoundError(f"result workbook not found: {date_text}")
    return import_result_artifact(db, path)


def find_result_workbook(race_date: date | None = None) -> Path | None:
    """Find a result workbook from legacy output or master folders."""

    settings = get_settings()
    if race_date is not None:
        ymd = race_date.strftime("%Y%m%d")
        exact_candidates = [
            settings.legacy_output_dir / f"馬の競走成績_with_feat_{ymd}_with_result.xlsx",
            settings.data_root / "master" / "racedata_results.xlsx",
        ]
        for candidate in exact_candidates:
            if candidate.exists():
                return candidate

    candidates: list[Path] = []
    if settings.legacy_output_dir.exists():
        candidates.extend(
            path
            for path in settings.legacy_output_dir.glob("*_with_result.xlsx")
            if not path.name.startswith("~$")
        )
    master_candidate = settings.data_root / "master" / "racedata_results.xlsx"
    if master_candidate.exists():
        candidates.append(master_candidate)
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def import_result_artifact(db: Session, path: Path) -> ImportSummaryRead:
    """Store result artifact metadata and row count in raw_file_records."""

    race_date = parse_race_date_from_filename(path)
    row_count = _count_result_rows(path)
    _record_raw_file(db, path, "result_workbook", race_date, row_count)
    result_count = _upsert_race_results_from_workbook(db, path, race_date)
    db.commit()
    return ImportSummaryRead(
        source_file=path.name,
        race_date=race_date,
        results=result_count or row_count,
    )


def _count_result_rows(path: Path) -> int:
    try:
        sheets = pd.read_excel(path, sheet_name=None)
    except Exception:
        return 0
    return sum(len(df.index) for df in sheets.values())


def _record_raw_file(
    db: Session,
    path: Path,
    file_type: str,
    race_date: date | None,
    row_count: int,
) -> None:
    checksum = file_sha256(path)
    file_path = str(path.resolve())
    record = db.scalar(select(RawFileRecord).where(RawFileRecord.file_path == file_path))
    if record is None:
        record = RawFileRecord(
            file_path=file_path,
            file_name=path.name,
            file_type=file_type,
            checksum=checksum,
        )
        db.add(record)

    record.race_date = race_date
    record.checksum = checksum
    record.row_count = row_count
    record.imported_at = datetime.utcnow()


def _upsert_race_results_from_workbook(
    db: Session,
    path: Path,
    default_race_date: date | None,
) -> int:
    try:
        sheets = pd.read_excel(path, sheet_name=None)
    except Exception:
        return 0

    imported = 0
    for sheet_name, df in sheets.items():
        race_id = normalize_race_id(sheet_name)
        if race_id is None or len(race_id) != 12 or df.empty:
            continue

        finish_rows: list[tuple[int, int]] = []
        for _, row in df.iterrows():
            horse_no = safe_int(_row_value(row, 8))
            finish_position = safe_int(_row_value(row, 12))
            if horse_no is not None and finish_position is not None and finish_position > 0:
                finish_rows.append((finish_position, horse_no))

        finish_order = [
            horse_no
            for _, horse_no in sorted(
                finish_rows,
                key=lambda item: (item[0], item[1]),
            )
        ]
        if len(finish_order) < 3:
            continue

        first_row = df.iloc[0]
        race_date = parse_date(_row_value(first_row, 1)) or default_race_date
        result = db.scalar(select(RaceResult).where(RaceResult.race_id == race_id))
        if result is None:
            result = RaceResult(
                race_id=race_id,
                payout_amount=0,
                payout_type="3連複",
            )
            db.add(result)

        result.race_date = race_date
        result.finish_order = finish_order
        result.source_file = str(path.resolve())
        result.raw = {"sheet": sheet_name, "row_count": int(len(df.index))}
        result.imported_at = datetime.utcnow()
        imported += 1

    return imported


def _row_value(row: pd.Series, position: int):
    if len(row.index) <= position:
        return None
    return row.iloc[position]
