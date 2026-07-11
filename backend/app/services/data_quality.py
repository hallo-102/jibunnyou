from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.models import (
    AiHorseEvaluation,
    AiPredictionRun,
    CollectionRun,
    DataQualityIssue,
    OddsSnapshot,
    Race,
    RaceEntry,
    RaceQualityStatus,
    JobRun,
)
from app.schemas.api import DataQualityRunSummary


QUALITY_SOURCE = "data_quality_check"


@dataclass(frozen=True)
class QualityFinding:
    severity: str
    code: str
    message: str
    race_id: str
    row_number: int | None = None


def run_data_quality_checks(
    db: Session,
    race_date: date | None = None,
    race_id: str | None = None,
) -> DataQualityRunSummary:
    """Run Phase 3 race-level quality checks and persist current status."""

    races = _load_target_races(db, race_date=race_date, race_id=race_id)
    race_ids = [race.race_id for race in races]
    if race_ids:
        db.execute(
            delete(DataQualityIssue).where(
                DataQualityIssue.source_file == QUALITY_SOURCE,
                DataQualityIssue.race_id.in_(race_ids),
            )
        )
        db.flush()

    summary = DataQualityRunSummary(
        race_date=race_date,
        race_id=race_id,
        checked_races=len(races),
    )

    for race in races:
        findings, status, status_summary = _check_one_race(db, race)
        red_count = sum(1 for finding in findings if finding.severity == "error")
        yellow_count = sum(1 for finding in findings if finding.severity == "warning")

        for finding in findings:
            db.add(
                DataQualityIssue(
                    severity=finding.severity,
                    code=finding.code,
                    message=finding.message,
                    source_file=QUALITY_SOURCE,
                    race_id=finding.race_id,
                    row_number=finding.row_number,
                )
            )

        status_record = db.scalar(
            select(RaceQualityStatus).where(RaceQualityStatus.race_id == race.race_id)
        )
        if status_record is None:
            status_record = RaceQualityStatus(race_id=race.race_id)
            db.add(status_record)

        status_record.status = status
        status_record.summary = status_summary
        status_record.issue_count = len(findings)
        status_record.red_count = red_count
        status_record.yellow_count = yellow_count
        status_record.checked_at = datetime.utcnow()

        if status == "GREEN":
            summary.green += 1
        elif status == "YELLOW":
            summary.yellow += 1
        elif status == "RED":
            summary.red += 1
        else:
            summary.gray += 1
        summary.issues_written += len(findings)

    db.flush()
    return summary


def has_blocking_quality_status(
    db: Session,
    race_date: date | None = None,
    race_id: str | None = None,
) -> bool:
    """Return True when the requested scope has a current RED quality status."""

    effective_race_date = race_date
    if race_id and effective_race_date is None:
        effective_race_date = db.scalar(
            select(Race.race_date).where(Race.race_id == race_id)
        )

    stmt = select(RaceQualityStatus).where(RaceQualityStatus.status == "RED")
    if race_id:
        stmt = stmt.where(RaceQualityStatus.race_id == race_id)
    elif effective_race_date:
        stmt = stmt.join(Race).where(Race.race_date == effective_race_date)
    if db.scalar(stmt.limit(1)) is not None:
        return True

    # 最新の出馬表・オッズ取得が未完了または異常なら、古い業務データを最新と誤認しない。
    collection_stmt = select(CollectionRun).where(
        CollectionRun.data_kind.in_(("race_card", "odds"))
    )
    if race_id:
        collection_stmt = collection_stmt.where(
            (CollectionRun.race_id == race_id) | (CollectionRun.race_id.is_(None))
        )
    if effective_race_date:
        collection_stmt = collection_stmt.where(CollectionRun.race_date == effective_race_date)
    latest_by_kind: dict[str, CollectionRun] = {}
    for collection_run in db.scalars(
        collection_stmt.order_by(CollectionRun.created_at.desc())
    ):
        latest_by_kind.setdefault(collection_run.data_kind, collection_run)
    for run in latest_by_kind.values():
        if run.status in {"queued", "running", "failed", "blocked"}:
            return True
        if run.status == "partial" or run.quality_status == "RED":
            if race_id and run.race_id is None:
                quality = (run.summary_json or {}).get("quality") or {}
                # 日付全体の一部raceだけがREDでも、正常な対象raceまで一律停止しない。
                # raceへ紐付けられないsource errorだけは対象日全体を安全側で停止する。
                if int(quality.get("unscoped_source_errors") or 0) > 0:
                    return True
                continue
            return True

    active_job_stmt = select(JobRun.id).where(
        JobRun.job_type.in_(("collection.race_info", "collection.odds")),
        JobRun.status.in_(("queued", "running")),
    )
    if effective_race_date:
        active_job_stmt = active_job_stmt.where(JobRun.race_date == effective_race_date)
    if race_id:
        active_job_stmt = active_job_stmt.where(
            (JobRun.race_id == race_id) | (JobRun.race_id.is_(None))
        )
    return db.scalar(active_job_stmt.limit(1)) is not None


def _load_target_races(
    db: Session,
    race_date: date | None,
    race_id: str | None,
) -> list[Race]:
    stmt = select(Race).order_by(Race.race_date.desc(), Race.venue, Race.race_number)
    if race_id:
        stmt = stmt.where(Race.race_id == race_id)
    elif race_date:
        stmt = stmt.where(Race.race_date == race_date)
    return list(db.scalars(stmt))


def _check_one_race(db: Session, race: Race) -> tuple[list[QualityFinding], str, str]:
    findings: list[QualityFinding] = []
    entries = list(
        db.scalars(
            select(RaceEntry)
            .where(RaceEntry.race_id == race.race_id)
            .order_by(RaceEntry.horse_no)
        )
    )

    if not entries:
        return findings, "GRAY", "出走馬データが未取込です"

    findings.extend(_check_race_identity(race))
    findings.extend(_check_entries(race, entries))
    findings.extend(_check_import_issues(db, race))
    findings.extend(_check_odds_timestamp(db, race, entries))
    findings.extend(_check_prediction_features(race, entries))
    findings.extend(_check_ai_results(db, race, entries))

    red_count = sum(1 for finding in findings if finding.severity == "error")
    yellow_count = sum(1 for finding in findings if finding.severity == "warning")
    if red_count:
        status = "RED"
    elif yellow_count:
        status = "YELLOW"
    else:
        status = "GREEN"

    if findings:
        status_summary = f"RED {red_count} / YELLOW {yellow_count}"
    else:
        status_summary = "品質チェック正常"
    return findings, status, status_summary


def _check_race_identity(race: Race) -> list[QualityFinding]:
    findings: list[QualityFinding] = []
    if race.race_date is None:
        findings.append(
            QualityFinding(
                severity="warning",
                code="DQ-001",
                message="開催日が未設定です",
                race_id=race.race_id,
            )
        )
        return findings

    race_year = race.race_id[:4] if race.race_id and len(race.race_id) >= 4 else ""
    if race_year.isdigit() and int(race_year) != race.race_date.year:
        findings.append(
            QualityFinding(
                severity="error",
                code="DQ-001",
                message=f"race_idの年と開催日が一致しません: race_id={race.race_id}, race_date={race.race_date}",
                race_id=race.race_id,
            )
        )
    return findings


def _check_entries(race: Race, entries: list[RaceEntry]) -> list[QualityFinding]:
    findings: list[QualityFinding] = []
    horse_no_counts = Counter(entry.horse_no for entry in entries)
    for horse_no, count in horse_no_counts.items():
        if count > 1:
            findings.append(
                QualityFinding(
                    severity="error",
                    code="DQ-002",
                    message=f"同一レース内で馬番が重複しています: horse_no={horse_no}, count={count}",
                    race_id=race.race_id,
                    row_number=horse_no,
                )
            )

    for entry in entries:
        if entry.horse_no < 1:
            findings.append(
                QualityFinding(
                    severity="error",
                    code="DQ-003",
                    message=f"馬番が1未満です: horse_no={entry.horse_no}",
                    race_id=race.race_id,
                    row_number=entry.horse_no,
                )
            )
        if not entry.horse_name.strip():
            findings.append(
                QualityFinding(
                    severity="error",
                    code="DQ-005",
                    message=f"馬名が空欄です: horse_no={entry.horse_no}",
                    race_id=race.race_id,
                    row_number=entry.horse_no,
                )
            )
        if entry.popularity is not None and (entry.popularity < 1 or entry.popularity > len(entries)):
            findings.append(
                QualityFinding(
                    severity="warning",
                    code="DQ-006",
                    message=f"人気が不正範囲です: horse_no={entry.horse_no}, popularity={entry.popularity}",
                    race_id=race.race_id,
                    row_number=entry.horse_no,
                )
            )
        if entry.win_odds is not None and entry.win_odds <= 0:
            findings.append(
                QualityFinding(
                    severity="error",
                    code="DQ-007",
                    message=f"単勝オッズが0以下です: horse_no={entry.horse_no}, win_odds={entry.win_odds}",
                    race_id=race.race_id,
                    row_number=entry.horse_no,
                )
            )
        if _raw_contains_scratched(entry.raw):
            findings.append(
                QualityFinding(
                    severity="error",
                    code="DQ-009",
                    message=f"取消・除外馬が混入しています: horse_no={entry.horse_no}, horse_name={entry.horse_name}",
                    race_id=race.race_id,
                    row_number=entry.horse_no,
                )
            )

    if race.headcount is not None and race.headcount != len(entries):
        findings.append(
            QualityFinding(
                severity="error",
                code="DQ-004",
                message=f"頭数と出走馬件数が一致しません: headcount={race.headcount}, entries={len(entries)}",
                race_id=race.race_id,
            )
        )
    return findings


def _check_odds_timestamp(
    db: Session,
    race: Race,
    entries: list[RaceEntry],
) -> list[QualityFinding]:
    has_odds_value = any(entry.win_odds is not None or entry.place_odds is not None for entry in entries)
    if not has_odds_value or race.race_date is None or race.venue is None or race.race_number is None:
        return []

    snapshot_count, latest_fetched_at = db.execute(
        select(func.count(), func.max(OddsSnapshot.fetched_at))
        .where(
            OddsSnapshot.race_date == race.race_date,
            OddsSnapshot.racecourse == race.venue,
            OddsSnapshot.race_no == race.race_number,
        )
    ).one()
    if int(snapshot_count or 0) == 0 or latest_fetched_at is None:
        return [
            QualityFinding(
                severity="warning",
                code="DQ-008",
                message="オッズ値はありますが、オッズ取得時刻を持つスナップショットが見つかりません",
                race_id=race.race_id,
            )
        ]

    now_utc = datetime.now(timezone.utc)
    now_jst = now_utc.astimezone(ZoneInfo("Asia/Tokyo"))
    if race.race_date != now_jst.date() or _race_has_started(race.start_time, now_jst):
        return []
    if latest_fetched_at.tzinfo is None:
        latest_fetched_at = latest_fetched_at.replace(tzinfo=timezone.utc)
    age_minutes = max(0.0, (now_utc - latest_fetched_at.astimezone(timezone.utc)).total_seconds() / 60)
    settings = get_settings()
    if age_minutes > settings.odds_freshness_critical_minutes:
        return [
            QualityFinding(
                severity="error",
                code="DQ-012",
                message=f"オッズが危険な鮮度です: age_minutes={age_minutes:.1f}",
                race_id=race.race_id,
            )
        ]
    if age_minutes > settings.odds_freshness_warning_minutes:
        return [
            QualityFinding(
                severity="warning",
                code="DQ-012",
                message=f"オッズの鮮度が低下しています: age_minutes={age_minutes:.1f}",
                race_id=race.race_id,
            )
        ]
    return []


def _check_import_issues(db: Session, race: Race) -> list[QualityFinding]:
    """Promote source-level identity errors into the race quality gate."""

    issues = list(
        db.scalars(
            select(DataQualityIssue).where(
                DataQualityIssue.race_id == race.race_id,
                DataQualityIssue.source_file != QUALITY_SOURCE,
                DataQualityIssue.severity.in_(("error", "warning")),
            )
        )
    )
    return [
        QualityFinding(
            severity=issue.severity,
            code=f"IMPORT-{issue.code}",
            message=issue.message,
            race_id=race.race_id,
            row_number=issue.row_number,
        )
        for issue in issues
    ]


def _race_has_started(start_time: str | None, now_jst: datetime) -> bool:
    if not start_time:
        return False
    try:
        hour_text, minute_text = start_time.split(":", maxsplit=1)
        start_minutes = int(hour_text) * 60 + int(minute_text)
    except (TypeError, ValueError):
        return False
    return now_jst.hour * 60 + now_jst.minute > start_minutes


def _check_prediction_features(race: Race, entries: list[RaceEntry]) -> list[QualityFinding]:
    has_any_prediction = any(
        entry.prediction_rank is not None
        or entry.prediction_score is not None
        or entry.estimated_in3_rate is not None
        or entry.expected_value is not None
        for entry in entries
    )
    if not has_any_prediction:
        return []

    findings: list[QualityFinding] = []
    missing_feature_count = sum(
        1
        for entry in entries
        if entry.prediction_score is None
        or entry.estimated_in3_rate is None
        or entry.expected_value is None
    )
    if missing_feature_count:
        findings.append(
            QualityFinding(
                severity="warning",
                code="DQ-010",
                message=f"必須特徴量が不足している出走馬があります: missing={missing_feature_count}, entries={len(entries)}",
                race_id=race.race_id,
            )
        )

    missing_rank_count = sum(1 for entry in entries if entry.prediction_rank is None)
    ranks = [entry.prediction_rank for entry in entries if entry.prediction_rank is not None]
    if missing_rank_count or len(set(ranks)) != len(ranks):
        findings.append(
            QualityFinding(
                severity="warning",
                code="DQ-011",
                message=f"Python順位が全頭分そろっていません: missing={missing_rank_count}, ranked={len(ranks)}",
                race_id=race.race_id,
            )
        )
    return findings


def _check_ai_results(db: Session, race: Race, entries: list[RaceEntry]) -> list[QualityFinding]:
    has_python_prediction = any(entry.prediction_rank is not None for entry in entries)
    if not has_python_prediction:
        return []

    run = db.scalar(
        select(AiPredictionRun)
        .where(AiPredictionRun.race_id == race.race_id, AiPredictionRun.ai_mode == "ai_second_opinion")
        .order_by(AiPredictionRun.created_at.desc())
        .limit(1)
    )
    if run is None:
        return [
            QualityFinding(
                severity="warning",
                code="AI-001",
                message="Python予想済みですが、AI逆張りチェックが未実行です",
                race_id=race.race_id,
            )
        ]

    evaluations = list(
        db.scalars(
            select(AiHorseEvaluation)
            .where(AiHorseEvaluation.ai_run_id == run.id)
            .order_by(AiHorseEvaluation.ai_rank)
        )
    )
    findings: list[QualityFinding] = []
    entry_horse_nos = {entry.horse_no for entry in entries}
    ai_horse_nos = [evaluation.horse_no for evaluation in evaluations]

    if len(evaluations) != len(entries):
        findings.append(
            QualityFinding(
                severity="warning",
                code="AI-002",
                message=f"AI評価件数が出走馬数と一致しません: ai={len(evaluations)}, entries={len(entries)}",
                race_id=race.race_id,
            )
        )
    missing_horse_nos = sorted(set(ai_horse_nos) - entry_horse_nos)
    if missing_horse_nos:
        findings.append(
            QualityFinding(
                severity="warning",
                code="AI-003",
                message=f"AI出力に存在しない馬番があります: horse_no={missing_horse_nos}",
                race_id=race.race_id,
            )
        )
    ai_ranks = [evaluation.ai_rank for evaluation in evaluations if evaluation.ai_rank is not None]
    if len(ai_ranks) != len(set(ai_ranks)):
        findings.append(
            QualityFinding(
                severity="warning",
                code="AI-004",
                message="AI順位が重複しています",
                race_id=race.race_id,
            )
        )
    ranked = [evaluation for evaluation in evaluations if evaluation.python_rank is not None]
    if ranked and all(evaluation.ai_rank == evaluation.python_rank for evaluation in ranked):
        findings.append(
            QualityFinding(
                severity="warning",
                code="AI-005",
                message="AI順位がPython順位と完全一致しています",
                race_id=race.race_id,
            )
        )
    if not any(evaluation.ai_adjust_score > 0 for evaluation in evaluations):
        findings.append(
            QualityFinding(
                severity="warning",
                code="AI-006",
                message="AI評価を上げた馬がありません",
                race_id=race.race_id,
            )
        )
    if not any(evaluation.ai_adjust_score < 0 for evaluation in evaluations):
        findings.append(
            QualityFinding(
                severity="warning",
                code="AI-007",
                message="AI評価を下げた馬がありません",
                race_id=race.race_id,
            )
        )
    if any(not evaluation.ai_reason.strip() for evaluation in evaluations):
        findings.append(
            QualityFinding(
                severity="warning",
                code="AI-008",
                message="AI評価理由が空欄の馬があります",
                race_id=race.race_id,
            )
        )
    if any(not evaluation.ai_bet_role.strip() for evaluation in evaluations):
        findings.append(
            QualityFinding(
                severity="warning",
                code="AI-009",
                message="AI馬券役割が空欄の馬があります",
                race_id=race.race_id,
            )
        )
    return findings


def _raw_contains_scratched(raw: dict[str, Any] | None) -> bool:
    if not raw:
        return False
    text = " ".join(str(value) for value in raw.values() if value is not None)
    return "取消" in text or "除外" in text
