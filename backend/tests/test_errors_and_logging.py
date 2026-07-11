from fastapi.testclient import TestClient

from app.main import app


@app.get("/api/test-only/unexpected-error", include_in_schema=False)
def raise_unexpected_error() -> None:
    """Provide a deterministic unexpected error used only by this test module."""

    raise RuntimeError("test secret token=must_not_be_returned")


def test_response_contains_valid_request_id() -> None:
    """A caller-provided safe request ID is preserved in response metadata."""

    with TestClient(app) as client:
        response = client.get("/api/version", headers={"X-Request-ID": "test-request-001"})

    assert response.status_code == 200
    assert response.headers["X-Request-ID"] == "test-request-001"


def test_invalid_request_id_is_replaced() -> None:
    """Unsafe request IDs must not reach logs or response headers."""

    with TestClient(app) as client:
        response = client.get("/api/version", headers={"X-Request-ID": "bad\nvalue"})

    assert response.status_code == 200
    assert response.headers["X-Request-ID"] != "bad\nvalue"


def test_validation_error_uses_common_safe_shape() -> None:
    """Validation errors omit raw input while retaining actionable field locations."""

    with TestClient(app) as client:
        response = client.post("/api/v1/races", json={})

    assert response.status_code == 422
    payload = response.json()
    assert payload["error"]["code"] == "VALIDATION_ERROR"
    assert payload["error"]["details"][0]["location"] == ["body", "race_id"]
    assert "input" not in payload["error"]["details"][0]
    assert payload["meta"]["request_id"]


def test_not_found_uses_common_error_shape() -> None:
    """Business 404 errors include a recovery hint and request ID."""

    with TestClient(app) as client:
        response = client.get("/api/v1/races/not-found")

    assert response.status_code == 404
    payload = response.json()
    assert payload["error"]["code"] == "NOT_FOUND"
    assert payload["error"]["recommended_action"]
    assert payload["meta"]["request_id"]


def test_unexpected_error_does_not_leak_exception_detail(caplog) -> None:
    """Unexpected exceptions return a generic message without secret-bearing details."""

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/api/test-only/unexpected-error")

    assert response.status_code == 500
    text = response.text
    assert "must_not_be_returned" not in text
    assert response.json()["error"]["code"] == "INTERNAL_ERROR"
    assert response.json()["meta"]["request_id"] != "-"
    assert response.headers["X-Request-ID"] == response.json()["meta"]["request_id"]
    assert "must_not_be_returned" not in caplog.text
