from __future__ import annotations

import json
import re
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.models import (
    DataQualityIssue,
    Horse,
    HorsePastPerformance,
    Race,
    RaceDay,
    RaceEntry,
    RawFileRecord,
)
from app.legacy_bridge.normalization import (
    file_sha256,
    normalize_horse_name,
    normalize_race_id,
    parse_date,
    parse_race_date_from_filename,
    pick_value,
    race_no_from_id,
    row_to_jsonable,
    safe_float,
    safe_int,
    safe_str,
)
from app.schemas.api import ImportSummaryRead


NOW_RACE_SHEET = "今走レース情報"
TARGET_SHEET = "TARGET"
FEATURE_RACE_WORKBOOK_PATTERN = re.compile(r"^馬の競走成績_with_feat_(\d{8})\.xlsx$")


def list_feature_race_workbooks() -> list[Path]:
    """List selectable feature workbooks using the strict production filename contract."""

    folder = get_settings().legacy_output_dir
    if not folder.is_dir():
        return []
    return sorted(
        (
            path
            for path in folder.iterdir()
            if path.is_file() and FEATURE_RACE_WORKBOOK_PATTERN.fullmatch(path.name)
        ),
        key=lambda path: path.name,
        reverse=True,
    )


def resolve_feature_race_workbook(file_name: str) -> Path:
    """Resolve one selectable workbook without allowing arbitrary paths."""

    if Path(file_name).name != file_name or not FEATURE_RACE_WORKBOOK_PATTERN.fullmatch(file_name):
        raise ValueError("選択可能なExcelファイル名ではありません")
    folder = get_settings().legacy_output_dir.resolve()
    candidate = (folder / file_name).resolve()
    if not candidate.is_relative_to(folder):
        raise ValueError("Excelファイルが許可フォルダ外を指しています")
    if not candidate.is_file():
        raise FileNotFoundError(file_name)
    return candidate


def import_latest_race_workbook(
    db: Session,
    race_date: date | None = None,
    prefer_feature_file: bool = False,
) -> ImportSummaryRead:
    """Import the latest matching race workbook."""

    path = find_race_workbook(race_date=race_date, prefer_feature_file=prefer_feature_file)
    if path is None:
        date_text = race_date.isoformat() if race_date else "latest"
        raise FileNotFoundError(f"race workbook not found: {date_text}")
    return import_race_workbook(db, path)


def find_race_workbook(
    race_date: date | None = None,
    prefer_feature_file: bool = False,
) -> Path | None:
    """Find an existing race workbook from configured input or output folders."""

    settings = get_settings()
    ymd = race_date.strftime("%Y%m%d") if race_date else None

    exact_candidates: list[Path] = []
    if ymd and prefer_feature_file:
        exact_candidates.append(settings.legacy_output_dir / f"馬の競走成績_with_feat_{ymd}.xlsx")
    if ymd:
        exact_candidates.append(settings.excel_input_dir / f"馬の競走成績_{ymd}.xlsx")
        exact_candidates.append(settings.legacy_output_dir / f"馬の競走成績_with_feat_{ymd}.xlsx")
    for candidate in exact_candidates:
        if candidate.exists():
            return candidate

    search_patterns = []
    if prefer_feature_file:
        search_patterns.append((settings.legacy_output_dir, "馬の競走成績_with_feat_*.xlsx"))
    search_patterns.append((settings.excel_input_dir, "馬の競走成績_*.xlsx"))
    search_patterns.append((settings.legacy_output_dir, "馬の競走成績_with_feat_*.xlsx"))

    candidates: list[Path] = []
    for folder, pattern in search_patterns:
        if not folder.exists():
            continue
        for path in folder.glob(pattern):
            if path.name.startswith("~$") or "_with_result_" in path.name:
                continue
            candidates.append(path)
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def import_race_workbook(db: Session, path: Path) -> ImportSummaryRead:
    """Import one Excel workbook into races, entries, and past performances."""

    settings = get_settings()
    settings.staging_dir.mkdir(parents=True, exist_ok=True)

    source_file = path.name
    imported_race_date = parse_race_date_from_filename(path)

    db.execute(delete(DataQualityIssue).where(DataQualityIssue.source_file == source_file))
    db.execute(delete(HorsePastPerformance).where(HorsePastPerformance.source_file == source_file))
    db.flush()

    excel = pd.ExcelFile(path)
    summary = ImportSummaryRead(source_file=source_file, race_date=imported_race_date)

    if imported_race_date is not None:
        _upsert_race_day(db, imported_race_date, source_file)

    if NOW_RACE_SHEET in excel.sheet_names:
        now_df = pd.read_excel(path, sheet_name=NOW_RACE_SHEET)
        race_count, entry_count = _import_now_race_info(db, now_df, source_file, imported_race_date)
        summary.races += race_count
        summary.entries += entry_count
    else:
        _add_issue(db, "error", "missing_now_race_sheet", "今走レース情報シートが見つかりません", source_file)

    if TARGET_SHEET in excel.sheet_names:
        target_df = pd.read_excel(path, sheet_name=TARGET_SHEET)
        race_count, entry_count = _import_target_sheet(db, target_df, source_file, imported_race_date)
        summary.races += race_count
        summary.entries += entry_count

    past_rows = 0
    for sheet_name in excel.sheet_names:
        if not re.fullmatch(r"\d{12}", sheet_name):
            continue
        sheet_df = pd.read_excel(path, sheet_name=sheet_name)
        past_rows += _import_past_performance_sheet(db, sheet_df, source_file, sheet_name)
    summary.past_performances = past_rows

    _record_raw_file(db, path, "race_workbook", imported_race_date, summary.entries + summary.past_performances)
    db.commit()

    issue_count = db.scalar(
        select(func.count()).select_from(DataQualityIssue).where(DataQualityIssue.source_file == source_file)
    )
    summary.issues = int(issue_count or 0)
    _write_staging_summary(settings.staging_dir, summary)
    return summary


def _import_now_race_info(
    db: Session,
    df: pd.DataFrame,
    source_file: str,
    race_date: date | None,
) -> tuple[int, int]:
    seen_races: set[str] = set()
    entries = 0
    horse_numbers_by_race: dict[str, list[int]] = {}

    for index, record in enumerate(df.to_dict(orient="records"), start=2):
        row = row_to_jsonable(record)
        race_id = normalize_race_id(pick_value(row, ["レースID", "rid_str", "race_id"]))
        if race_id is None:
            _add_issue(db, "error", "missing_race_id", "レースIDが空です", source_file, row_number=index)
            continue

        horse_no = safe_int(pick_value(row, ["馬番", "馬 番", "umaban"]))
        horse_name = safe_str(pick_value(row, ["馬名", "horse_name", "name"]))
        if horse_no is None or horse_name is None:
            _add_issue(
                db,
                "error",
                "missing_entry_key",
                "馬番または馬名が空です",
                source_file,
                race_id,
                index,
            )
            continue

        race = _upsert_race_from_row(db, row, race_id, race_date, source_file)
        seen_races.add(race.race_id)
        horse_numbers_by_race.setdefault(race_id, []).append(horse_no)

        _upsert_entry_from_row(db, race_id, horse_no, horse_name, row)
        # 本番セッションはautoflush=Falseのため、次行のupsert検索から今行を参照できるよう確定する。
        db.flush()
        entries += 1

    _record_duplicate_horse_numbers(db, horse_numbers_by_race, source_file)
    return len(seen_races), entries


def _import_target_sheet(
    db: Session,
    df: pd.DataFrame,
    source_file: str,
    race_date: date | None,
) -> tuple[int, int]:
    seen_races: set[str] = set()
    entries = 0

    for index, record in enumerate(df.to_dict(orient="records"), start=2):
        row = row_to_jsonable(record)
        race_id = normalize_race_id(pick_value(row, ["レースID", "rid_str", "race_id"]))
        horse_no = safe_int(pick_value(row, ["馬番", "馬 番", "umaban"]))
        horse_name = safe_str(pick_value(row, ["馬名", "horse_name", "name"]))
        if race_id is None or horse_no is None or horse_name is None:
            continue

        _upsert_race_from_row(db, row, race_id, race_date, source_file)
        _upsert_entry_from_row(db, race_id, horse_no, horse_name, row)
        # TARGETが今走レース情報と同じ馬を含んでも、既存行として更新できるよう確定する。
        db.flush()
        seen_races.add(race_id)
        entries += 1

    return len(seen_races), entries


def _import_past_performance_sheet(
    db: Session,
    df: pd.DataFrame,
    source_file: str,
    target_race_id: str,
) -> int:
    rows = 0
    for record in df.to_dict(orient="records"):
        row = row_to_jsonable(record)
        horse_name = safe_str(pick_value(row, ["馬名", "horse_name", "name"]))
        past_race_id = normalize_race_id(pick_value(row, ["race_id", "レースID", "rid_str"]))
        db.add(
            HorsePastPerformance(
                source_file=source_file,
                source_sheet=target_race_id,
                target_race_id=target_race_id,
                past_race_id=past_race_id,
                horse_name=horse_name,
                race_date=parse_date(pick_value(row, ["日付", "date"])),
                race_name=safe_str(pick_value(row, ["レース名"])),
                horse_no=safe_int(pick_value(row, ["馬番", "馬 番"])),
                finish_position=safe_int(pick_value(row, ["着順", "着 順"])),
                popularity=safe_int(pick_value(row, ["人気", "人 気"])),
                odds=safe_float(pick_value(row, ["オッズ", "オ ッ ズ", "単勝オッズ"])),
                distance=safe_str(pick_value(row, ["距離"])),
                jockey=safe_str(pick_value(row, ["騎手"])),
                raw=row,
            )
        )
        rows += 1
    return rows


def _upsert_race_from_row(
    db: Session,
    row: dict[str, Any],
    race_id: str,
    race_date: date | None,
    source_file: str,
) -> Race:
    race = db.get(Race, race_id)
    if race is None:
        race = Race(race_id=race_id)
        db.add(race)

    _set_if_present(race, "race_date", race_date)
    _set_if_present(race, "race_number", race_no_from_id(race_id))
    _set_if_present(race, "venue", safe_str(pick_value(row, ["場所", "racecourse", "venue"])))
    _set_if_present(race, "name", safe_str(pick_value(row, ["レース名", "race_name"])))
    _set_if_present(race, "start_time", safe_str(pick_value(row, ["発走時刻", "start_time"])))
    _set_if_present(race, "course", safe_str(pick_value(row, ["コース", "距離", "course"])))
    _set_if_present(
        race,
        "track_condition",
        safe_str(pick_value(row, ["馬場", "馬 場", "track_condition"])),
    )
    _set_if_present(race, "race_type", safe_str(pick_value(row, ["レース種別", "race_type"])))
    _set_if_present(race, "race_class", safe_str(pick_value(row, ["クラス", "class"])))
    _set_if_present(race, "headcount", safe_int(pick_value(row, ["頭数", "頭 数"])))
    race.raw = {**(race.raw or {}), **row}

    if race_date is not None:
        _upsert_race_day(db, race_date, source_file)

    return race


def _set_if_present(model: Any, field: str, value: Any) -> None:
    if value is not None:
        setattr(model, field, value)


def _upsert_entry_from_row(
    db: Session,
    race_id: str,
    horse_no: int,
    horse_name: str,
    row: dict[str, Any],
) -> RaceEntry:
    horse = db.scalar(select(Horse).where(Horse.name == horse_name))
    if horse is None:
        horse = Horse(name=horse_name, normalized_name=normalize_horse_name(horse_name))
        db.add(horse)
        db.flush()

    entry = db.scalar(
        select(RaceEntry).where(RaceEntry.race_id == race_id, RaceEntry.horse_no == horse_no)
    )
    if entry is None:
        entry = RaceEntry(race_id=race_id, horse_no=horse_no, horse_name=horse_name)
        db.add(entry)

    entry.horse_id = horse.id
    entry.horse_name = horse_name
    _set_if_present(entry, "frame_no", safe_int(pick_value(row, ["枠番", "枠 番"])))
    _set_if_present(entry, "age", safe_int(pick_value(row, ["年齢"])))
    _set_if_present(entry, "carried_weight", safe_float(pick_value(row, ["斤量", "斤 量"])))
    _set_if_present(entry, "jockey", safe_str(pick_value(row, ["騎手"])))
    _set_if_present(entry, "trainer", safe_str(pick_value(row, ["厩舎", "厩舎 コメント"])))
    _set_if_present(entry, "popularity", safe_int(pick_value(row, ["人気", "人 気"])))
    _set_if_present(
        entry,
        "win_odds",
        safe_float(pick_value(row, ["単勝オッズ", "オッズ", "オ ッ ズ", "tansho"])),
    )
    _set_if_present(entry, "place_odds", safe_float(pick_value(row, ["複勝オッズ", "fukusho"])))
    _set_if_present(entry, "prediction_rank", safe_int(pick_value(row, ["予想順位", "rank"])))
    _set_if_present(entry, "prediction_score", safe_float(pick_value(row, ["score", "total"])))
    _set_if_present(
        entry,
        "estimated_in3_rate",
        safe_float(pick_value(row, ["推定馬券内率_オッズ補正後", "推定馬券内率"])),
    )
    _set_if_present(entry, "expected_value", safe_float(pick_value(row, ["期待値"])))
    entry.raw = {**(entry.raw or {}), **row}
    return entry


def _upsert_race_day(db: Session, race_date: date, source_file: str) -> RaceDay:
    race_day = db.scalar(select(RaceDay).where(RaceDay.race_date == race_date))
    if race_day is None:
        race_day = RaceDay(race_date=race_date, source=source_file)
        db.add(race_day)
        db.flush()
    else:
        race_day.source = source_file
        race_day.status = "imported"
    return race_day


def _record_duplicate_horse_numbers(
    db: Session,
    horse_numbers_by_race: dict[str, list[int]],
    source_file: str,
) -> None:
    for race_id, horse_numbers in horse_numbers_by_race.items():
        duplicates = [number for number, count in Counter(horse_numbers).items() if count > 1]
        for horse_no in duplicates:
            _add_issue(
                db,
                "error",
                "duplicate_horse_no",
                f"同一レース内で馬番が重複しています: horse_no={horse_no}",
                source_file,
                race_id,
            )


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
    path = staging_dir / f"race_workbook_import_{timestamp}.json"
    path.write_text(
        json.dumps(summary.model_dump(mode="json"), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
