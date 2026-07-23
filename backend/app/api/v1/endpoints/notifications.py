from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from app.api.v1.deps import get_db
from app.db.models import Notification, utc_now
from app.schemas.api import NotificationRead, NotificationReadUpdate, NotificationSummaryRead
from app.services.notifications import sync_operational_notifications

router = APIRouter()


@router.get("/notifications", response_model=list[NotificationRead])
def list_notifications(
    unread_only: bool = False,
    db: Session = Depends(get_db),
    limit: int = Query(default=100, ge=1, le=500),
) -> list[Notification]:
    """List persistent operational notifications newest first."""

    sync_operational_notifications(db)
    db.commit()
    stmt = select(Notification).order_by(Notification.created_at.desc()).limit(limit)
    if unread_only:
        stmt = stmt.where(Notification.is_read.is_(False))
    return list(db.scalars(stmt))


@router.get("/notifications/summary", response_model=NotificationSummaryRead)
def notification_summary(db: Session = Depends(get_db)) -> NotificationSummaryRead:
    """Return total and unread counts for the notification badge."""

    sync_operational_notifications(db)
    db.commit()
    return _summary(db)


@router.patch("/notifications/{notification_id}/read", response_model=NotificationRead)
def update_notification_read_state(
    notification_id: str,
    payload: NotificationReadUpdate,
    db: Session = Depends(get_db),
) -> Notification:
    """Mark one notification as read or unread."""

    notification = db.get(Notification, notification_id)
    if notification is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="notification not found")
    notification.is_read = payload.is_read
    notification.read_at = utc_now() if payload.is_read else None
    notification.updated_at = utc_now()
    db.commit()
    db.refresh(notification)
    return notification


@router.post("/notifications/read-all", response_model=NotificationSummaryRead)
def mark_all_notifications_read(db: Session = Depends(get_db)) -> NotificationSummaryRead:
    """Mark every current notification as read and return the updated summary."""

    sync_operational_notifications(db)
    read_at = utc_now()
    db.execute(
        update(Notification)
        .where(Notification.is_read.is_(False))
        .values(is_read=True, read_at=read_at, updated_at=read_at)
    )
    db.commit()
    return _summary(db)


def _summary(db: Session) -> NotificationSummaryRead:
    total_count = db.scalar(select(func.count()).select_from(Notification)) or 0
    unread_count = db.scalar(
        select(func.count()).select_from(Notification).where(Notification.is_read.is_(False))
    ) or 0
    error_count = db.scalar(
        select(func.count())
        .select_from(Notification)
        .where(Notification.is_read.is_(False), Notification.severity == "error")
    ) or 0
    warning_count = db.scalar(
        select(func.count())
        .select_from(Notification)
        .where(Notification.is_read.is_(False), Notification.severity == "warning")
    ) or 0
    return NotificationSummaryRead(
        total_count=int(total_count),
        unread_count=int(unread_count),
        error_count=int(error_count),
        warning_count=int(warning_count),
    )
