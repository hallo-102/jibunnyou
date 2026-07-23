from datetime import date

from fastapi.testclient import TestClient

from app.db.models import DataQualityIssue, JobRun
from app.db.session import SessionLocal
from app.main import app


def test_notification_center_persists_and_marks_operational_alerts_read() -> None:
    job_id = "notification-job-001"
    with TestClient(app) as client:
        with SessionLocal() as db:
            db.add(
                JobRun(
                    id=job_id,
                    job_type="ai.independent",
                    status="failed",
                    race_date=date(2026, 7, 22),
                    race_id="202607220101",
                    force=True,
                    message="OpenAI API request failed",
                )
            )
            quality_issue = DataQualityIssue(
                severity="warning",
                code="stale_odds",
                message="オッズの取得時刻が古くなっています",
                race_id="202607220101",
            )
            db.add(quality_issue)
            db.commit()
            db.refresh(quality_issue)

        listed = client.get("/api/v1/notifications")
        summary = client.get("/api/v1/notifications/summary")

        assert listed.status_code == 200
        notifications = listed.json()
        source_keys = {(item["source_type"], item["source_id"]) for item in notifications}
        assert ("job_run", job_id) in source_keys
        assert ("data_quality_issue", str(quality_issue.id)) in source_keys
        assert summary.status_code == 200
        assert summary.json()["unread_count"] >= 2

        target = next(item for item in notifications if item["source_id"] == job_id)
        marked = client.patch(
            f"/api/v1/notifications/{target['id']}/read",
            json={"is_read": True},
        )
        assert marked.status_code == 200
        assert marked.json()["is_read"] is True
        assert marked.json()["read_at"] is not None

        marked_all = client.post("/api/v1/notifications/read-all")
        assert marked_all.status_code == 200
        assert marked_all.json()["unread_count"] == 0

        unread = client.get("/api/v1/notifications?unread_only=true")
        assert unread.status_code == 200
        assert unread.json() == []
