from __future__ import annotations

import errno
import json
import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable, TypeVar

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.logging import redact_text
from app.db.models import (
    ArtifactFile,
    CollectionCacheEntry,
    CollectionRun,
    DataQualityIssue,
    HorsePastPerformance,
    JobRun,
    OddsSnapshot,
    Race,
    RaceEntry,
    RaceResult,
    RawFileRecord,
)
from app.legacy_bridge.excel_importer import import_race_workbook
from app.legacy_bridge.legacy_runner import LEGACY_COLLECTION_SCRIPTS, run_legacy_collection_job
from app.legacy_bridge.odds_importer import import_odds_csv
from app.legacy_bridge.result_importer import import_result_artifact
from app.services.data_quality import run_data_quality_checks
from app.services.history import file_sha256, record_job_log, register_artifact


T = TypeVar("T")
SAFE_PATH_SEGMENT = re.compile(r"^[A-Za-z0-9._-]+$")
ALLOWED_RAW_SUFFIXES = {".csv", ".htm", ".html", ".json", ".txt", ".xls", ".xlsx"}


class CollectorError(RuntimeError):
    """Base error for a collection pipeline failure."""


class CollectorBlockedError(CollectorError):
    """Raised when execution requires configuration or manual confirmation."""


class CollectorPathError(CollectorError):
    """Raised when a source path escapes the configured input boundary."""


@dataclass(frozen=True)
class SourcePolicy:
    job_type: str
    source_code: str
    data_kind: str
    reliability_grade: str
    output_kind: str | None
    cache_ttl_seconds: int
    min_interval_seconds: int
    max_retries: int


SOURCE_POLICIES: dict[str, SourcePolicy] = {
    "collection.race_info": SourcePolicy(
        job_type="collection.race_info",
        source_code="SRC_NETKEIBA_001",
        data_kind="race_card",
        reliability_grade="B",
        output_kind="race_workbook",
        cache_ttl_seconds=6 * 60 * 60,
        min_interval_seconds=60,
        max_retries=3,
    ),
    "collection.past_performances": SourcePolicy(
        job_type="collection.past_performances",
        source_code="SRC_NETKEIBA_001",
        data_kind="past_performances",
        reliability_grade="B",
        output_kind="race_workbook",
        cache_ttl_seconds=30 * 24 * 60 * 60,
        min_interval_seconds=60,
        max_retries=3,
    ),
    "collection.odds": SourcePolicy(
        job_type="collection.odds",
        source_code="SRC_JRA_003",
        data_kind="odds",
        reliability_grade="A",
        output_kind="odds_csv",
        cache_ttl_seconds=15 * 60,
        min_interval_seconds=60,
        max_retries=3,
    ),
    "collection.training": SourcePolicy(
        job_type="collection.training",
        source_code="SRC_NETKEIBA_002",
        data_kind="training",
        reliability_grade="B",
        output_kind=None,
        cache_ttl_seconds=12 * 60 * 60,
        min_interval_seconds=60,
        max_retries=3,
    ),
    "collection.results": SourcePolicy(
        job_type="collection.results",
        source_code="SRC_JRA_005",
        data_kind="results",
        reliability_grade="A",
        output_kind="result_workbook",
        cache_ttl_seconds=365 * 24 * 60 * 60,
        min_interval_seconds=60,
        max_retries=3,
    ),
}


def collection_cache_key(
    policy: SourcePolicy,
    *,
    race_date: date | None,
    race_id: str | None,
    params: dict[str, Any] | None,
) -> str:
    """Return a deterministic cache key for one source and target."""

    payload = {
        "source_code": policy.source_code,
        "data_kind": policy.data_kind,
        "race_date": race_date.isoformat() if race_date else None,
        "race_id": race_id,
        "params": params or {},
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def execute_with_finite_retry(
    operation: Callable[[], T],
    *,
    max_retries: int,
    retry_delays_seconds: list[int],
    sleeper: Callable[[float], None] = time.sleep,
    on_retry: Callable[[int, int, BaseException], None] | None = None,
) -> tuple[T, int, int]:
    """Execute transient failures at most `max_retries` times with bounded delays."""

    attempts = 0
    retries = 0
    while True:
        attempts += 1
        try:
            return operation(), attempts, retries
        except Exception as exc:
            if retries >= max_retries or not is_transient_collection_error(exc):
                raise
            delay = retry_delays_seconds[min(retries, len(retry_delays_seconds) - 1)]
            retries += 1
            if on_retry is not None:
                on_retry(retries, delay, exc)
            sleeper(delay)


def is_transient_collection_error(exc: BaseException) -> bool:
    """Classify only clearly temporary network, timeout, or I/O errors as retryable."""

    if isinstance(exc, (ConnectionError, TimeoutError, subprocess.TimeoutExpired)):
        return True
    if isinstance(exc, OSError):
        return exc.errno in {
            errno.EAGAIN,
            errno.EBUSY,
            errno.ECONNABORTED,
            errno.ECONNREFUSED,
            errno.ECONNRESET,
            errno.EHOSTUNREACH,
            errno.ENETDOWN,
            errno.ENETUNREACH,
            errno.ETIMEDOUT,
        }
    # 構造変更、必須項目欠落、一般的なlegacy終了コードは自動再試行しない。
    return False


def run_collection_pipeline(db: Session, job: JobRun) -> dict[str, Any]:
    """Run cache→collector→Raw→Normalized→Business→quality as one tracked pipeline."""

    policy = SOURCE_POLICIES.get(job.job_type)
    if policy is None or job.job_type not in LEGACY_COLLECTION_SCRIPTS:
        raise CollectorBlockedError(f"collection policy is not configured: {job.job_type}")

    settings = get_settings()
    cache_key = collection_cache_key(
        policy,
        race_date=job.race_date,
        race_id=job.race_id,
        params=job.params,
    )
    collection_run = db.scalar(
        select(CollectionRun).where(CollectionRun.job_run_id == job.id)
    )
    if collection_run is None:
        collection_run = CollectionRun(
            job_run_id=job.id,
            source_code=policy.source_code,
            data_kind=policy.data_kind,
            status="running",
            mode=settings.legacy_runner_mode.lower(),
            race_date=job.race_date,
            race_id=job.race_id,
            force=job.force,
            cache_key=cache_key,
            started_at=_utc_now(),
        )
        db.add(collection_run)
    else:
        collection_run.status = "running"
        collection_run.started_at = _utc_now()
    db.commit()
    db.refresh(collection_run)

    try:
        cache_entry = None if job.force else _find_valid_cache(db, cache_key)
        if cache_entry is not None:
            collection_run.status = "cached"
            collection_run.cache_hit = True
            collection_run.raw_file_record_id = cache_entry.raw_file_record_id
            collection_run.raw_artifact_id = cache_entry.artifact_file_id
            collection_run.attempt_count = 0
            collection_run.retry_count = 0
            collection_run.request_count = 0
            collection_run.summary_json = {
                "cache": "hit",
                "content_sha256": cache_entry.content_sha256,
                "fetched_at": cache_entry.fetched_at.isoformat(),
                "expires_at": cache_entry.expires_at.isoformat(),
            }
            collection_run.finished_at = _utc_now()
            cache_entry.hit_count += 1
            cache_entry.last_used_at = _utc_now()
            db.commit()
            return _collection_run_payload(collection_run)

        if policy.output_kind is None:
            raise CollectorBlockedError(
                f"adapter is not configured for {job.job_type}; external access was not attempted"
            )
        if (
            settings.legacy_runner_mode == "execute"
            and policy.source_code not in settings.collector_approved_sources
        ):
            raise CollectorBlockedError(
                "source execution is not approved; review the current terms and explicitly allow "
                f"source_code={policy.source_code}"
            )

        retry_delays = settings.collector_retry_delays_seconds or [10, 60, 300]
        max_retries = min(policy.max_retries, settings.collector_max_retries)

        def on_retry(retry_number: int, delay: int, exc: BaseException) -> None:
            record_job_log(
                db,
                job_run_id=job.id,
                level="WARNING",
                event_code="COLLECTION_RETRY_SCHEDULED",
                message="temporary collection failure; bounded retry scheduled",
                context={
                    "retry_number": retry_number,
                    "delay_seconds": delay,
                    "error_type": exc.__class__.__name__,
                },
            )
            db.commit()

        legacy_summary, attempts, retries = execute_with_finite_retry(
            lambda: run_legacy_collection_job(
                job.job_type,
                job.id,
                race_date=job.race_date,
                race_id=job.race_id,
                params=job.params,
                force=job.force,
            ),
            max_retries=max_retries,
            retry_delays_seconds=retry_delays,
            on_retry=on_retry,
        )
        collection_run.attempt_count = attempts
        collection_run.retry_count = retries
        # Legacy subprocess内部のHTTP回数は観測できないため、試行回数を通信回数として捏造しない。
        collection_run.request_count = 0
        if settings.legacy_runner_mode == "execute":
            collection_run.warnings_json = ["legacy adapter request_count is not observable"]
        _register_runner_artifacts(db, job, legacy_summary)

        source_path = _output_path(legacy_summary, policy.output_kind)
        if source_path is None:
            raise CollectorBlockedError(
                f"required collector output is unavailable: output_kind={policy.output_kind}"
            )

        raw_record, raw_artifact, raw_path = _snapshot_raw_file(
            db,
            job=job,
            collection_run=collection_run,
            policy=policy,
            source_path=source_path,
        )
        collection_run.raw_file_record_id = raw_record.id
        collection_run.raw_artifact_id = raw_artifact.id
        db.commit()

        import_summary = _import_business_data(db, policy, raw_path)
        normalized_path = _write_normalized_snapshot(
            db,
            collection_run=collection_run,
            policy=policy,
            source_path=raw_path,
            import_summary=import_summary,
        )
        normalized_artifact = register_artifact(
            db,
            path=normalized_path,
            artifact_kind="normalized_snapshot",
            logical_name=f"normalized:{policy.data_kind}",
            job_run_id=job.id,
            content_type="application/json",
            metadata={
                "source_code": policy.source_code,
                "data_kind": policy.data_kind,
                "reliability_grade": policy.reliability_grade,
            },
        )
        collection_run.normalized_artifact_id = normalized_artifact.id

        quality_payload, quality_status = _run_collection_quality(
            db,
            policy=policy,
            race_date=job.race_date or import_summary.get("race_date"),
            race_id=job.race_id,
            source_file=raw_path.name,
        )
        collection_run.quality_status = quality_status
        collection_run.status = "partial" if quality_status == "RED" else "succeeded"
        collection_run.summary_json = {
            "legacy": legacy_summary,
            "import": import_summary,
            "quality": quality_payload,
            "layers": {
                "raw": str(raw_path),
                "normalized": str(normalized_path),
                "business": "postgresql",
            },
        }
        collection_run.finished_at = _utc_now()

        cache_entry = db.scalar(
            select(CollectionCacheEntry).where(CollectionCacheEntry.cache_key == cache_key)
        )
        fetched_at = raw_record.fetched_at or _utc_now()
        if fetched_at.tzinfo is None:
            fetched_at = fetched_at.replace(tzinfo=timezone.utc)
        minimum_interval = max(policy.min_interval_seconds, settings.collector_min_interval_seconds)
        effective_ttl = max(policy.cache_ttl_seconds, minimum_interval)
        expires_at = fetched_at + timedelta(seconds=effective_ttl)
        if cache_entry is None:
            cache_entry = CollectionCacheEntry(
                cache_key=cache_key,
                source_code=policy.source_code,
                data_kind=policy.data_kind,
                race_date=job.race_date,
                race_id=job.race_id,
                raw_file_record_id=raw_record.id,
                artifact_file_id=raw_artifact.id,
                content_sha256=raw_record.checksum,
                fetched_at=fetched_at,
                expires_at=expires_at,
            )
            db.add(cache_entry)
        else:
            cache_entry.raw_file_record_id = raw_record.id
            cache_entry.artifact_file_id = raw_artifact.id
            cache_entry.content_sha256 = raw_record.checksum
            cache_entry.fetched_at = fetched_at
            cache_entry.expires_at = expires_at
        cache_entry.status = "invalid" if quality_status == "RED" else "active"
        db.commit()
        return _collection_run_payload(collection_run)
    except Exception as exc:
        db.rollback()
        collection_run = db.scalar(
            select(CollectionRun).where(CollectionRun.job_run_id == job.id)
        )
        if collection_run is not None:
            collection_run.status = "blocked" if isinstance(exc, CollectorBlockedError) else "failed"
            collection_run.error_code = exc.__class__.__name__
            collection_run.error_message = redact_text(str(exc))
            collection_run.finished_at = _utc_now()
            db.commit()
        raise


def execute_queued_collection_job(job_id: str) -> dict[str, str]:
    """Load and execute one queued collector job in a worker-owned DB session."""

    # Worker起動後の設定済みEngineを使い、API requestのSessionを共有しない。
    from app.db.session import SessionLocal

    with SessionLocal() as db:
        job = db.get(JobRun, job_id)
        if job is None:
            raise LookupError(f"queued collection job was not found: {job_id}")
        if job.job_type not in SOURCE_POLICIES:
            raise CollectorBlockedError(f"job is not a collection job: {job.job_type}")
        if job.status not in {"queued", "running"}:
            return {"job_id": job.id, "status": job.status}

        job.status = "running"
        job.started_at = datetime.utcnow()
        record_job_log(
            db,
            job_run_id=job.id,
            level="INFO",
            event_code="JOB_STARTED",
            message="collector worker started job execution",
            context={"job_type": job.job_type, "queue": "collector"},
        )
        db.commit()

        try:
            job.message = json.dumps(
                run_collection_pipeline(db, job),
                ensure_ascii=False,
                default=_json_default,
            )
            job.status = "completed"
        except Exception as exc:
            db.rollback()
            job = db.get(JobRun, job_id)
            if job is None:
                raise
            job.status = "failed"
            job.message = redact_text(f"{exc.__class__.__name__}: {exc}")
        finally:
            job.finished_at = datetime.utcnow()
            record_job_log(
                db,
                job_run_id=job.id,
                level="INFO" if job.status == "completed" else "ERROR",
                event_code="JOB_COMPLETED" if job.status == "completed" else "JOB_FAILED",
                message=(
                    "collector job execution completed"
                    if job.status == "completed"
                    else "collector job execution failed"
                ),
                context={"status": job.status, "queue": "collector"},
            )
            db.add(job)
            db.commit()
        return {"job_id": job.id, "status": job.status}


def _find_valid_cache(db: Session, cache_key: str) -> CollectionCacheEntry | None:
    now = _utc_now()
    entry = db.scalar(
        select(CollectionCacheEntry).where(CollectionCacheEntry.cache_key == cache_key)
    )
    if entry is None or entry.status != "active":
        return None
    expires_at = entry.expires_at
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    if expires_at <= now:
        entry.status = "stale"
        db.commit()
        return None
    raw_record = db.get(RawFileRecord, entry.raw_file_record_id)
    artifact = db.get(ArtifactFile, entry.artifact_file_id)
    if raw_record is None or artifact is None:
        entry.status = "invalid"
        db.commit()
        return None
    raw_path = Path(raw_record.file_path)
    if not raw_path.is_file() or file_sha256(raw_path) != entry.content_sha256:
        entry.status = "invalid"
        db.commit()
        return None
    return entry


def _register_runner_artifacts(
    db: Session,
    job: JobRun,
    legacy_summary: dict[str, Any],
) -> None:
    settings = get_settings()
    candidates = (
        (settings.exports_dir / "runs" / job.id / "input_manifest_v1.json", "input_manifest"),
        (settings.exports_dir / "runs" / job.id / "output_manifest_v1.json", "output_manifest"),
        (settings.logs_dir / "runs" / job.id / "stdout.log", "stdout_log"),
        (settings.logs_dir / "runs" / job.id / "stderr.log", "stderr_log"),
    )
    for path, artifact_kind in candidates:
        if not path.is_file():
            continue
        register_artifact(
            db,
            path=path,
            artifact_kind=artifact_kind,
            logical_name=f"collector:{artifact_kind}",
            job_run_id=job.id,
            content_type="application/json" if path.suffix == ".json" else "text/plain",
            metadata={"job_type": job.job_type, "mode": legacy_summary.get("mode")},
        )


def _output_path(legacy_summary: dict[str, Any], output_kind: str) -> Path | None:
    for output in legacy_summary.get("outputs", []):
        if output.get("kind") != output_kind or not output.get("exists"):
            continue
        path_text = output.get("path")
        if path_text:
            return Path(path_text)
    return None


def _snapshot_raw_file(
    db: Session,
    *,
    job: JobRun,
    collection_run: CollectionRun,
    policy: SourcePolicy,
    source_path: Path,
) -> tuple[RawFileRecord, ArtifactFile, Path]:
    settings = get_settings()
    resolved_source = _validate_source_path(source_path)
    if resolved_source.suffix.lower() not in ALLOWED_RAW_SUFFIXES:
        raise CollectorPathError(f"unsupported raw file extension: {resolved_source.suffix}")
    _validate_segment(policy.source_code)
    _validate_segment(collection_run.id)

    date_segment = (job.race_date or date.today()).strftime("%Y%m%d")
    target_dir = settings.raw_snapshots_dir / policy.source_code / date_segment / collection_run.id
    target_dir.mkdir(parents=True, exist_ok=True)
    destination = target_dir / resolved_source.name
    source_digest = file_sha256(resolved_source)
    if destination.exists():
        if file_sha256(destination) != source_digest:
            raise CollectorError("immutable raw snapshot already exists with different bytes")
    else:
        temporary = destination.with_name(f".{destination.name}.tmp")
        shutil.copyfile(resolved_source, temporary)
        if file_sha256(temporary) != source_digest:
            temporary.unlink(missing_ok=True)
            raise CollectorError("raw snapshot hash changed during copy")
        os.replace(temporary, destination)

    fetched_at = _utc_now()
    raw_record = db.scalar(
        select(RawFileRecord).where(RawFileRecord.file_path == str(destination.resolve()))
    )
    if raw_record is None:
        raw_record = RawFileRecord(
            file_path=str(destination.resolve()),
            file_name=destination.name,
            file_type=policy.output_kind or policy.data_kind,
            race_date=job.race_date,
            checksum=source_digest,
            row_count=0,
            source_code=policy.source_code,
            source_uri=str(resolved_source),
            original_file_path=str(resolved_source),
            fetched_at=fetched_at,
            is_immutable=True,
        )
        db.add(raw_record)
        db.flush()
    elif raw_record.checksum != source_digest:
        raise CollectorError("registered raw snapshot content changed")

    raw_artifact = register_artifact(
        db,
        path=destination,
        artifact_kind="raw_snapshot",
        logical_name=f"raw:{policy.data_kind}",
        job_run_id=job.id,
        content_type=_content_type(destination),
        metadata={
            "source_code": policy.source_code,
            "data_kind": policy.data_kind,
            "reliability_grade": policy.reliability_grade,
            "fetched_at": fetched_at.isoformat(),
        },
    )
    manifest_path = target_dir / "manifest.json"
    _write_json_atomic(
        manifest_path,
        {
            "schema_version": "raw_manifest_v1",
            "collection_run_id": collection_run.id,
            "job_run_id": job.id,
            "source_code": policy.source_code,
            "data_kind": policy.data_kind,
            "reliability_grade": policy.reliability_grade,
            "source_path": str(resolved_source),
            "snapshot_path": str(destination.resolve()),
            "sha256": source_digest,
            "size_bytes": destination.stat().st_size,
            "fetched_at": fetched_at.isoformat(),
        },
    )
    register_artifact(
        db,
        path=manifest_path,
        artifact_kind="raw_manifest",
        logical_name="collector:raw_manifest",
        job_run_id=job.id,
        content_type="application/json",
        metadata={"source_code": policy.source_code, "data_kind": policy.data_kind},
    )
    return raw_record, raw_artifact, destination


def _validate_source_path(path: Path) -> Path:
    settings = get_settings()
    resolved = path.resolve(strict=True)
    if not resolved.is_file():
        raise CollectorPathError("collector output must be a regular file")
    allowed_roots = (
        settings.excel_input_dir,
        settings.odds_input_dir,
        settings.legacy_output_dir,
        settings.data_root / "master",
    )
    resolved_roots = [root.resolve() for root in allowed_roots if root.exists()]
    if not any(resolved.is_relative_to(root) for root in resolved_roots):
        raise CollectorPathError("collector output escaped configured input directories")
    return resolved


def _import_business_data(
    db: Session,
    policy: SourcePolicy,
    raw_path: Path,
) -> dict[str, Any]:
    if policy.data_kind in {"race_card", "past_performances"}:
        return import_race_workbook(db, raw_path).model_dump(mode="json")
    if policy.data_kind == "odds":
        return import_odds_csv(db, raw_path).model_dump(mode="json")
    if policy.data_kind == "results":
        return import_result_artifact(db, raw_path).model_dump(mode="json")
    raise CollectorBlockedError(f"business importer is not configured: {policy.data_kind}")


def _write_normalized_snapshot(
    db: Session,
    *,
    collection_run: CollectionRun,
    policy: SourcePolicy,
    source_path: Path,
    import_summary: dict[str, Any],
) -> Path:
    settings = get_settings()
    race_date_value = collection_run.race_date or _date_value(import_summary.get("race_date"))
    date_segment = (race_date_value or date.today()).strftime("%Y%m%d")
    target_dir = settings.normalized_dir / date_segment / collection_run.id
    target_dir.mkdir(parents=True, exist_ok=True)
    output_path = target_dir / f"normalized_{policy.data_kind}.json"

    payload = {
        "schema_version": "normalized_collection_v1",
        "collection_run_id": collection_run.id,
        "source_code": policy.source_code,
        "data_kind": policy.data_kind,
        "reliability_grade": policy.reliability_grade,
        "race_date": race_date_value.isoformat() if race_date_value else None,
        "race_id": collection_run.race_id,
        "source_file": source_path.name,
        "source_sha256": file_sha256(source_path),
        "normalized_at": _utc_now().isoformat(),
        "import_summary": import_summary,
        "records": _normalized_records(
            db,
            policy=policy,
            source_file=source_path.name,
            race_date=race_date_value,
            race_id=collection_run.race_id,
        ),
    }
    _write_json_atomic(output_path, payload)
    return output_path


def _normalized_records(
    db: Session,
    *,
    policy: SourcePolicy,
    source_file: str,
    race_date: date | None,
    race_id: str | None,
) -> dict[str, Any]:
    if policy.data_kind in {"race_card", "past_performances"}:
        race_stmt = select(Race).order_by(Race.race_id)
        if race_id:
            race_stmt = race_stmt.where(Race.race_id == race_id)
        elif race_date:
            race_stmt = race_stmt.where(Race.race_date == race_date)
        races = list(db.scalars(race_stmt))
        race_ids = [race.race_id for race in races]
        entries = (
            list(
                db.scalars(
                    select(RaceEntry)
                    .where(RaceEntry.race_id.in_(race_ids))
                    .order_by(RaceEntry.race_id, RaceEntry.horse_no)
                )
            )
            if race_ids
            else []
        )
        past_count = db.query(HorsePastPerformance).filter(
            HorsePastPerformance.source_file == source_file
        ).count()
        return {
            "races": [
                {
                    "race_id": race.race_id,
                    "race_date": race.race_date,
                    "race_number": race.race_number,
                    "venue": race.venue,
                    "name": race.name,
                    "start_time": race.start_time,
                    "course": race.course,
                    "track_condition": race.track_condition,
                    "headcount": race.headcount,
                }
                for race in races
            ],
            "entries": [
                {
                    "race_id": entry.race_id,
                    "horse_no": entry.horse_no,
                    "horse_name": entry.horse_name,
                    "frame_no": entry.frame_no,
                    "jockey": entry.jockey,
                    "trainer": entry.trainer,
                    "win_odds": entry.win_odds,
                    "place_odds": entry.place_odds,
                }
                for entry in entries
            ],
            "past_performance_count": past_count,
        }
    if policy.data_kind == "odds":
        rows = list(
            db.scalars(
                select(OddsSnapshot)
                .where(OddsSnapshot.source_file == source_file)
                .order_by(
                    OddsSnapshot.racecourse,
                    OddsSnapshot.race_no,
                    OddsSnapshot.bet_type,
                    OddsSnapshot.horse_no,
                )
            )
        )
        return {
            "odds": [
                {
                    "race_date": row.race_date,
                    "racecourse": row.racecourse,
                    "race_no": row.race_no,
                    "horse_no": row.horse_no,
                    "horse_name": row.horse_name,
                    "bet_type": row.bet_type,
                    "combination": row.combination,
                    "raw_odds": row.raw_odds,
                    "odds": row.odds,
                    "odds_min": row.odds_min,
                    "odds_max": row.odds_max,
                    "fetched_at": row.fetched_at,
                    "imported_at": row.imported_at,
                }
                for row in rows
            ]
        }
    if policy.data_kind == "results":
        result_stmt = select(RaceResult).order_by(RaceResult.race_id)
        if race_id:
            result_stmt = result_stmt.where(RaceResult.race_id == race_id)
        elif race_date:
            result_stmt = result_stmt.where(RaceResult.race_date == race_date)
        results = list(db.scalars(result_stmt))
        return {
            "results": [
                {
                    "race_id": result.race_id,
                    "race_date": result.race_date,
                    "finish_order": result.finish_order,
                    "payout_type": result.payout_type,
                    "payout_amount": result.payout_amount,
                    "imported_at": result.imported_at,
                }
                for result in results
            ]
        }
    return {}


def _run_collection_quality(
    db: Session,
    *,
    policy: SourcePolicy,
    race_date: date | str | None,
    race_id: str | None,
    source_file: str,
) -> tuple[dict[str, Any] | None, str | None]:
    source_issues = list(
        db.scalars(
            select(DataQualityIssue).where(DataQualityIssue.source_file == source_file)
        )
    )
    source_errors = sum(1 for issue in source_issues if issue.severity == "error")
    source_warnings = sum(1 for issue in source_issues if issue.severity == "warning")
    unscoped_source_errors = sum(
        1
        for issue in source_issues
        if issue.severity == "error" and issue.race_id is None
    )
    if policy.data_kind not in {"race_card", "past_performances", "odds"}:
        if source_errors:
            return {
                "source_errors": source_errors,
                "source_warnings": source_warnings,
                "unscoped_source_errors": unscoped_source_errors,
            }, "RED"
        if source_warnings:
            return {
                "source_errors": 0,
                "source_warnings": source_warnings,
                "unscoped_source_errors": 0,
            }, "YELLOW"
        return None, None
    parsed_date = race_date if isinstance(race_date, date) else _date_value(race_date)
    summary = run_data_quality_checks(db, race_date=parsed_date, race_id=race_id)
    payload = summary.model_dump(mode="json")
    payload["source_errors"] = source_errors
    payload["source_warnings"] = source_warnings
    payload["unscoped_source_errors"] = unscoped_source_errors
    if summary.red or source_errors:
        return payload, "RED"
    if summary.yellow or source_warnings:
        return payload, "YELLOW"
    if summary.green:
        return payload, "GREEN"
    return payload, "GRAY"


def _collection_run_payload(collection_run: CollectionRun) -> dict[str, Any]:
    collection = {
        "collection_run_id": collection_run.id,
        "job_run_id": collection_run.job_run_id,
        "source_code": collection_run.source_code,
        "data_kind": collection_run.data_kind,
        "status": collection_run.status,
        "mode": collection_run.mode,
        "cache_hit": collection_run.cache_hit,
        "attempt_count": collection_run.attempt_count,
        "retry_count": collection_run.retry_count,
        "request_count": collection_run.request_count,
        "quality_status": collection_run.quality_status,
        "summary": collection_run.summary_json,
        "warnings": collection_run.warnings_json,
        "error_code": collection_run.error_code,
        "error_message": collection_run.error_message,
    }
    return {**(collection_run.summary_json or {}), "collection": collection}


def _validate_segment(value: str) -> None:
    if not SAFE_PATH_SEGMENT.fullmatch(value):
        raise CollectorPathError("unsafe collection path segment")


def _content_type(path: Path) -> str:
    return {
        ".csv": "text/csv",
        ".html": "text/html",
        ".htm": "text/html",
        ".json": "application/json",
        ".txt": "text/plain",
        ".xls": "application/vnd.ms-excel",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }.get(path.suffix.lower(), "application/octet-stream")


def _date_value(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value:
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)
