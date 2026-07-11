from __future__ import annotations

import json
import re
import unicodedata
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.models import DataQualityIssue, OddsSnapshot, Race, RaceEntry, RawFileRecord
from app.legacy_bridge.normalization import (
    file_sha256,
    normalize_horse_name,
    parse_date,
    parse_race_date_from_filename,
    parse_race_no,
    pick_value,
    row_to_jsonable,
    safe_int,
    safe_str,
)
from app.schemas.api import ImportSummaryRead


def import_latest_odds_csv(db: Session, race_date: date | None = None) -> ImportSummaryRead:
    """Import the latest matching OZZU CSV."""

    path = find_odds_csv(race_date=race_date)
    if path is None:
        date_text = race_date.isoformat() if race_date else "latest"
        raise FileNotFoundError(f"OZZU CSV not found: {date_text}")
    return import_odds_csv(db, path)


def find_odds_csv(race_date: date | None = None) -> Path | None:
    """Find an OZZU CSV from the configured odds directory."""

    settings = get_settings()
    if race_date is not None:
        candidate = settings.odds_input_dir / f"OZZU_{race_date.strftime('%Y%m%d')}.csv"
        if candidate.exists():
            return candidate

    if not settings.odds_input_dir.exists():
        return None
    candidates = [
        path for path in settings.odds_input_dir.glob("OZZU_*.csv") if not path.name.startswith("~$")
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def import_odds_csv(db: Session, path: Path) -> ImportSummaryRead:
    """Import one OZZU CSV and apply odds to matching race entries."""

    settings = get_settings()
    settings.staging_dir.mkdir(parents=True, exist_ok=True)
    source_file = path.name
    race_date_from_name = parse_race_date_from_filename(path)

    db.execute(delete(DataQualityIssue).where(DataQualityIssue.source_file == source_file))
    db.execute(delete(OddsSnapshot).where(OddsSnapshot.source_file == source_file))
    db.flush()

    df = _read_csv(path)
    imported = 0
    applied = 0
    seen_business_keys: set[tuple] = set()
    fetched_at = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)

    for index, record in enumerate(df.to_dict(orient="records"), start=2):
        row = row_to_jsonable(record)
        row_race_date = parse_date(pick_value(row, ["date"]))
        if (
            row_race_date is not None
            and race_date_from_name is not None
            and row_race_date != race_date_from_name
        ):
            _add_issue(
                db,
                "error",
                "odds_date_mismatch",
                (
                    "OZZU CSVの行日付とファイル名日付が一致しません: "
                    f"row_date={row_race_date}, file_date={race_date_from_name}"
                ),
                source_file,
                row_number=index,
            )
            continue
        race_date = row_race_date or race_date_from_name
        racecourse = safe_str(pick_value(row, ["racecourse", "場所"]))
        race_no = parse_race_no(pick_value(row, ["race", "R"]))
        bet_type = safe_str(pick_value(row, ["bet_type", "式別"]))
        combination = safe_str(pick_value(row, ["combination", "馬番"]))
        horse_no = safe_int(combination) if re.fullmatch(r"\d+", combination or "") else None
        horse_name = safe_str(pick_value(row, ["name", "馬名"]))
        raw_odds = safe_str(pick_value(row, ["odds", "オッズ"]))
        odds, odds_min, odds_max, odds_state = _parse_odds_value(raw_odds)

        if race_date is None or racecourse is None or race_no is None or bet_type is None:
            _add_issue(
                db,
                "error",
                "missing_odds_key",
                "OZZU CSVのdate/racecourse/race/bet_typeが不足しています",
                source_file,
                row_number=index,
            )
            continue

        normalized_bet_type = _normalize_bet_type(bet_type)
        business_key = (
            race_date,
            racecourse,
            race_no,
            normalized_bet_type,
            horse_no,
            combination,
        )
        if business_key in seen_business_keys:
            _add_issue(
                db,
                "error",
                "duplicate_odds_key",
                (
                    "OZZU CSV内でオッズ業務キーが重複しています: "
                    f"racecourse={racecourse}, race={race_no}, bet_type={normalized_bet_type}, "
                    f"combination={combination}"
                ),
                source_file,
                race_id=_find_race_id(db, race_date, racecourse, race_no),
                row_number=index,
            )
            continue
        seen_business_keys.add(business_key)

        if odds_state in {"unavailable", "invalid"}:
            _add_issue(
                db,
                "warning" if odds_state == "unavailable" else "error",
                "odds_unavailable" if odds_state == "unavailable" else "invalid_odds_value",
                f"オッズ値を数値化できません: value={raw_odds or '-'}",
                source_file,
                race_id=_find_race_id(db, race_date, racecourse, race_no),
                row_number=index,
            )

        snapshot = OddsSnapshot(
            source_file=source_file,
            race_date=race_date,
            racecourse=racecourse,
            race_no=race_no,
            horse_no=horse_no,
            horse_name=horse_name,
            bet_type=normalized_bet_type,
            combination=combination,
            raw_odds=raw_odds,
            odds=odds,
            odds_min=odds_min,
            odds_max=odds_max,
            fetched_at=fetched_at,
        )
        db.add(snapshot)
        imported += 1
        applied_count, mismatch_reason = _apply_odds_to_entry(db, snapshot)
        applied += applied_count
        if mismatch_reason is not None:
            _add_issue(
                db,
                "error",
                "odds_entry_mismatch",
                (
                    "オッズと出走馬を安全に照合できないため反映しません: "
                    f"reason={mismatch_reason}, racecourse={racecourse}, race={race_no}, "
                    f"horse_no={horse_no}, horse_name={horse_name or '-'}"
                ),
                source_file,
                race_id=_find_race_id(db, race_date, racecourse, race_no),
                row_number=index,
            )

    _record_raw_file(db, path, "ozzu_csv", race_date_from_name, imported)
    db.commit()

    _rank_popularity_from_win_odds(db, race_date_from_name)
    db.commit()

    issue_count = len(
        db.scalars(select(DataQualityIssue).where(DataQualityIssue.source_file == source_file)).all()
    )
    summary = ImportSummaryRead(
        source_file=source_file,
        race_date=race_date_from_name,
        odds=imported,
        entries=applied,
        issues=issue_count,
    )
    _write_staging_summary(settings.staging_dir, summary)
    return summary


def _read_csv(path: Path) -> pd.DataFrame:
    for encoding in ("utf-8-sig", "cp932", "shift_jis"):
        try:
            return pd.read_csv(path, encoding=encoding)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path)


def _apply_odds_to_entry(db: Session, snapshot: OddsSnapshot) -> tuple[int, str | None]:
    bet_type = _normalize_bet_type(snapshot.bet_type)
    if bet_type not in {"単勝", "複勝"} or snapshot.odds is None:
        return 0, None

    race = db.scalar(
        select(Race).where(
            Race.race_date == snapshot.race_date,
            Race.venue == snapshot.racecourse,
            Race.race_number == snapshot.race_no,
        )
    )
    if race is None:
        return 0, "race_not_found"

    stmt = select(RaceEntry).where(RaceEntry.race_id == race.race_id)
    if snapshot.horse_no is not None:
        stmt = stmt.where(RaceEntry.horse_no == snapshot.horse_no)
        entry = db.scalar(stmt)
        if entry is None:
            return 0, "horse_number_not_found"
        if snapshot.horse_name and normalize_horse_name(entry.horse_name) != normalize_horse_name(
            snapshot.horse_name
        ):
            return 0, "horse_number_name_mismatch"
    elif snapshot.horse_name:
        name_norm = normalize_horse_name(snapshot.horse_name)
        entries = db.scalars(select(RaceEntry).where(RaceEntry.race_id == race.race_id)).all()
        matches = [item for item in entries if normalize_horse_name(item.horse_name) == name_norm]
        if len(matches) != 1:
            return 0, "horse_name_not_unique" if matches else "horse_name_not_found"
        entry = matches[0]
    else:
        return 0, "horse_identity_missing"

    if bet_type == "単勝":
        entry.win_odds = snapshot.odds
    elif bet_type == "複勝":
        entry.place_odds = snapshot.odds
    return 1, None


def _normalize_bet_type(value: str) -> str:
    return unicodedata.normalize("NFKC", value).strip()


def _find_race_id(
    db: Session,
    race_date: date,
    racecourse: str,
    race_no: int,
) -> str | None:
    return db.scalar(
        select(Race.race_id).where(
            Race.race_date == race_date,
            Race.venue == racecourse,
            Race.race_number == race_no,
        )
    )


def _parse_odds_value(value: str | None) -> tuple[float | None, float | None, float | None, str]:
    """Parse a numeric or min-max odds value without inventing a midpoint."""

    if value is None:
        return None, None, None, "unavailable"
    normalized = unicodedata.normalize("NFKC", value).strip()
    if normalized in {"", "-", "--", "---", "取消", "発売前", "未発売", "不明"}:
        return None, None, None, "unavailable"

    match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*[-~〜～]\s*(\d+(?:\.\d+)?)", normalized)
    if match is not None:
        lower = float(match.group(1))
        upper = float(match.group(2))
        if lower <= 0 or upper <= 0 or lower > upper:
            return None, None, None, "invalid"
        return None, lower, upper, "range"

    if re.fullmatch(r"\d+(?:\.\d+)?", normalized):
        numeric = float(normalized)
        if numeric <= 0:
            return None, None, None, "invalid"
        return numeric, numeric, numeric, "numeric"
    return None, None, None, "invalid"


def _rank_popularity_from_win_odds(db: Session, race_date: date | None) -> None:
    if race_date is None:
        return
    races = db.scalars(select(Race).where(Race.race_date == race_date)).all()
    for race in races:
        entries = db.scalars(select(RaceEntry).where(RaceEntry.race_id == race.race_id)).all()
        sortable = [entry for entry in entries if entry.win_odds is not None]
        sortable.sort(key=lambda entry: (entry.win_odds, entry.horse_no))
        for rank, entry in enumerate(sortable, start=1):
            entry.popularity = rank


def _add_issue(
    db: Session,
    severity: str,
    code: str,
    message: str,
    source_file: str,
    race_id: str | None = None,
    row_number: int | None = None,
) -> None:
    db.add(
        DataQualityIssue(
            severity=severity,
            code=code,
            message=message,
            source_file=source_file,
            race_id=race_id,
            row_number=row_number,
        )
    )


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


def _write_staging_summary(staging_dir: Path, summary: ImportSummaryRead) -> None:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = staging_dir / f"odds_import_{timestamp}.json"
    path.write_text(
        json.dumps(summary.model_dump(mode="json"), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
