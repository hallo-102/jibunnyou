from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.request_context import get_request_id
from app.db.models import ArtifactFile, AuditLog, BetStatusHistory, JobLog


class ImmutableArtifactError(ValueError):
    """Raised when a registered immutable path now contains different bytes."""


def file_sha256(path: Path) -> str:
    """Calculate a file hash without loading the entire artifact into memory."""

    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def register_artifact(
    db: Session,
    *,
    path: Path,
    artifact_kind: str,
    logical_name: str,
    prediction_run_id: str | None = None,
    job_run_id: str | None = None,
    content_type: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> ArtifactFile:
    """Register one immutable artifact and reject changed bytes at the same path."""

    resolved_path = path.resolve(strict=True)
    if not resolved_path.is_file():
        raise ValueError("artifact path must point to a regular file")
    digest = file_sha256(resolved_path)
    existing = db.scalar(
        select(ArtifactFile).where(ArtifactFile.storage_path == str(resolved_path))
    )
    if existing is not None:
        if existing.sha256 != digest:
            raise ImmutableArtifactError(
                "registered immutable artifact content changed; create a new run path"
            )
        return existing

    artifact = ArtifactFile(
        prediction_run_id=prediction_run_id,
        job_run_id=job_run_id,
        artifact_kind=artifact_kind,
        logical_name=logical_name,
        storage_path=str(resolved_path),
        content_type=content_type,
        size_bytes=resolved_path.stat().st_size,
        sha256=digest,
        is_immutable=True,
        metadata_json=metadata or {},
    )
    db.add(artifact)
    db.flush()
    return artifact


def record_audit(
    db: Session,
    *,
    action: str,
    entity_type: str,
    entity_id: str,
    before: dict[str, Any] | None = None,
    after: dict[str, Any] | None = None,
    prediction_run_id: str | None = None,
    actor_type: str = "system",
    actor_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> AuditLog:
    """Append one audit row in the caller's transaction."""

    audit = AuditLog(
        actor_type=actor_type,
        actor_id=actor_id,
        action=action,
        entity_type=entity_type,
        entity_id=entity_id,
        prediction_run_id=prediction_run_id,
        trace_id=get_request_id(),
        before_json=before or {},
        after_json=after or {},
        metadata_json=metadata or {},
    )
    db.add(audit)
    db.flush()
    return audit


def record_bet_status_change(
    db: Session,
    *,
    bet_candidate_id: int,
    old_status: str | None,
    new_status: str,
    reason: str | None,
    prediction_run_id: str,
    changed_by: str | None = None,
) -> BetStatusHistory:
    """Append bet state history and its matching audit event atomically."""

    history = BetStatusHistory(
        bet_candidate_id=bet_candidate_id,
        old_status=old_status,
        new_status=new_status,
        changed_by=changed_by,
        reason=reason,
    )
    db.add(history)
    record_audit(
        db,
        action="change_bet_status",
        entity_type="bet_candidates",
        entity_id=str(bet_candidate_id),
        prediction_run_id=prediction_run_id,
        before={"status": old_status},
        after={"status": new_status},
        actor_type="user" if changed_by else "system",
        actor_id=changed_by,
        metadata={"reason": reason} if reason else {},
    )
    db.flush()
    return history


def record_job_log(
    db: Session,
    *,
    job_run_id: str,
    level: str,
    event_code: str,
    message: str,
    context: dict[str, Any] | None = None,
) -> JobLog:
    """Append one structured job event in the caller's transaction."""

    job_log = JobLog(
        job_run_id=job_run_id,
        level=level,
        event_code=event_code,
        message=message,
        context_json=context or {},
        trace_id=get_request_id(),
    )
    db.add(job_log)
    db.flush()
    return job_log
