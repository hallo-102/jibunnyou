from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.logging import redact_text
from app.db.models import DataQualityIssue, JobRun, Notification


def sync_operational_notifications(db: Session) -> int:
    """Persist notifications for failed jobs and actionable data-quality issues."""

    existing_keys = set(
        db.execute(
            select(Notification.category, Notification.source_type, Notification.source_id)
        ).all()
    )
    created = 0

    failed_jobs = db.scalars(
        select(JobRun).where(JobRun.status == "failed").order_by(JobRun.created_at.asc())
    )
    for job in failed_jobs:
        key = ("job", "job_run", job.id)
        if key in existing_keys:
            continue
        db.add(
            Notification(
                category="job",
                severity="error",
                title=f"ジョブ失敗: {job.job_type}",
                message=_safe_message(job.message, "ジョブログで失敗理由を確認してください"),
                source_type="job_run",
                source_id=job.id,
                race_id=job.race_id,
                race_date=job.race_date,
                action_anchor="#operations",
                created_at=job.finished_at or job.created_at,
                updated_at=job.finished_at or job.created_at,
            )
        )
        existing_keys.add(key)
        created += 1

    quality_issues = db.scalars(
        select(DataQualityIssue)
        .where(DataQualityIssue.severity.in_(["warning", "error"]))
        .order_by(DataQualityIssue.created_at.asc())
    )
    for issue in quality_issues:
        source_id = str(issue.id)
        key = ("data_quality", "data_quality_issue", source_id)
        if key in existing_keys:
            continue
        db.add(
            Notification(
                category="data_quality",
                severity="error" if issue.severity == "error" else "warning",
                title=f"データ品質: {issue.code}",
                message=_safe_message(issue.message, "データ品質の詳細を確認してください"),
                source_type="data_quality_issue",
                source_id=source_id,
                race_id=issue.race_id,
                action_anchor="#operations",
                created_at=issue.created_at,
                updated_at=issue.created_at,
            )
        )
        existing_keys.add(key)
        created += 1

    if created:
        db.flush()
    return created


def _safe_message(message: str | None, fallback: str) -> str:
    """Redact secrets and keep notification payloads bounded for UI delivery."""

    return redact_text(message or fallback)[:2000]
