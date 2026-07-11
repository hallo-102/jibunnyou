from fastapi.testclient import TestClient

from app.api.v1.endpoints import health as health_endpoint
from app.db.migrations import head_revision
from app.main import app


def test_health_returns_application_and_dependency_status() -> None:
    """Liveness endpoint must stay available even when optional Redis is absent."""

    with TestClient(app) as client:
        response = client.get("/api/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["database"] == "ok"
    assert payload["app"] == "Keiba AI Studio"


def test_readiness_returns_200_when_all_checks_pass(monkeypatch) -> None:
    """Readiness must report ready only when every required dependency passes."""

    monkeypatch.setattr(health_endpoint, "_database_status", lambda: "ok")
    monkeypatch.setattr(health_endpoint, "_redis_status", lambda _url: "ok")
    monkeypatch.setattr(health_endpoint, "_storage_status", lambda _dirs: "ok")

    with TestClient(app) as client:
        response = client.get("/api/ready")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ready",
        "checks": {
            "postgres": "ok",
            "redis": "ok",
            "artifact_storage": "ok",
            "config": "ok",
        },
    }


def test_readiness_returns_503_when_a_dependency_fails(monkeypatch) -> None:
    """Gateway health checks must reject an API that cannot reach Redis."""

    monkeypatch.setattr(health_endpoint, "_database_status", lambda: "ok")
    monkeypatch.setattr(
        health_endpoint,
        "_redis_status",
        lambda _url: "error:ConnectionError",
    )
    monkeypatch.setattr(health_endpoint, "_storage_status", lambda _dirs: "ok")

    with TestClient(app) as client:
        response = client.get("/api/ready")

    assert response.status_code == 503
    assert response.json()["status"] == "not_ready"
    assert response.json()["checks"]["redis"] == "error:ConnectionError"


def test_version_does_not_expose_secrets() -> None:
    """Version endpoint returns deployment identity without connection strings."""

    with TestClient(app) as client:
        response = client.get("/api/version")

    assert response.status_code == 200
    payload = response.json()
    assert payload["environment"] == "test"
    assert payload["version"] == "test"
    assert payload["database_revision"] == head_revision()
    assert "database_url" not in payload
    assert "redis_url" not in payload
