from fastapi.testclient import TestClient

from app.main import app


def test_collection_source_policy_and_status_endpoints() -> None:
    with TestClient(app) as client:
        sources = client.get("/api/v1/collection-sources")
        runs = client.get("/api/v1/collections")

    assert sources.status_code == 200
    assert runs.status_code == 200
    assert isinstance(runs.json(), list)
    source_rows = sources.json()
    assert len(source_rows) == 5
    assert all(row["max_retries"] <= 3 for row in source_rows)
    assert all(row["min_interval_seconds"] >= 60 for row in source_rows)
    assert all(row["execution_approved"] is False for row in source_rows)
    assert next(row for row in source_rows if row["data_kind"] == "odds")[
        "source_code"
    ] == "SRC_JRA_003"
    assert next(row for row in source_rows if row["data_kind"] == "training")[
        "adapter_configured"
    ] is False
