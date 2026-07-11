from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from typing import Any

from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db.models import IdempotencyRecord


IDEMPOTENCY_KEY_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{8,200}$")


class IdempotencyConflict(ValueError):
    """Raised when a key is reused for a different request payload."""


class IdempotencyInProgress(RuntimeError):
    """Raised when the first request for a key has not completed."""


class IdempotencyPreviouslyFailed(RuntimeError):
    """Raised when a previous request failed before producing a replayable response."""


def canonical_request_hash(payload: BaseModel | dict[str, Any]) -> str:
    """Hash a request deterministically across retries and JSON key ordering."""

    value = payload.model_dump(mode="json") if isinstance(payload, BaseModel) else payload
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def begin_idempotent_request(
    db: Session,
    *,
    scope: str,
    idempotency_key: str,
    payload: BaseModel | dict[str, Any],
    ttl_hours: int = 24,
) -> tuple[IdempotencyRecord, bool]:
    """Create a processing record or return a completed matching request."""

    if not IDEMPOTENCY_KEY_PATTERN.fullmatch(idempotency_key):
        raise ValueError("Idempotency-Key must be 8-200 safe ASCII characters")
    request_hash = canonical_request_hash(payload)
    now = datetime.now(timezone.utc)
    existing = db.scalar(
        select(IdempotencyRecord).where(
            IdempotencyRecord.scope == scope,
            IdempotencyRecord.idempotency_key == idempotency_key,
        )
    )
    if existing is not None:
        expires_at = existing.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if expires_at <= now:
            existing.request_hash = request_hash
            existing.status = "processing"
            existing.response_status = None
            existing.response_body = None
            existing.resource_type = None
            existing.resource_id = None
            existing.expires_at = now + timedelta(hours=ttl_hours)
            existing.updated_at = now
            db.commit()
            db.refresh(existing)
            return existing, False
        if existing.request_hash != request_hash:
            raise IdempotencyConflict("Idempotency-Key was already used with a different body")
        if existing.status == "completed":
            return existing, True
        if existing.status == "failed":
            raise IdempotencyPreviouslyFailed(
                "previous request with this Idempotency-Key failed; use a new key"
            )
        raise IdempotencyInProgress("request with this Idempotency-Key is still processing")

    record = IdempotencyRecord(
        scope=scope,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        status="processing",
        expires_at=now + timedelta(hours=ttl_hours),
    )
    db.add(record)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return begin_idempotent_request(
            db,
            scope=scope,
            idempotency_key=idempotency_key,
            payload=payload,
            ttl_hours=ttl_hours,
        )
    db.refresh(record)
    return record, False


def complete_idempotent_request(
    db: Session,
    record: IdempotencyRecord,
    *,
    response_status: int,
    response_body: dict[str, Any],
    resource_type: str,
    resource_id: str,
) -> None:
    """Persist the first response so later identical requests can replay it."""

    record.status = "completed"
    record.response_status = response_status
    record.response_body = response_body
    record.resource_type = resource_type
    record.resource_id = resource_id
    record.updated_at = datetime.now(timezone.utc)
    db.add(record)
    db.commit()


def fail_idempotent_request(
    db: Session,
    record: IdempotencyRecord,
    *,
    response_status: int,
    response_body: dict[str, Any],
) -> None:
    """Mark a request as failed so it is never mistaken for an active request."""

    record.status = "failed"
    record.response_status = response_status
    record.response_body = response_body
    record.updated_at = datetime.now(timezone.utc)
    db.add(record)
    db.commit()
