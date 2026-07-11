from fastapi.testclient import TestClient

from app.main import app


def test_job_api_requires_and_replays_idempotency_key() -> None:
    payload = {
        "job_type": "maintenance.backup",
        "race_date": "2026-07-10",
        "force": True,
        "params": {"mode": "test"},
    }
    headers = {"Idempotency-Key": "job-request-001"}

    with TestClient(app) as client:
        missing = client.post("/api/v1/jobs", json=payload)
        first = client.post("/api/v1/jobs", json=payload, headers=headers)
        replay = client.post("/api/v1/jobs", json=payload, headers=headers)
        conflict = client.post(
            "/api/v1/jobs",
            json={**payload, "race_date": "2026-07-11"},
            headers=headers,
        )

    assert missing.status_code == 422
    assert first.status_code == 202
    assert replay.status_code == 202
    assert replay.json()["id"] == first.json()["id"]
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "CONFLICT"
