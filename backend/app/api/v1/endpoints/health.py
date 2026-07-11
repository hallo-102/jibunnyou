from pathlib import Path

from fastapi import APIRouter, Response, status
from sqlalchemy import text

from app.core.config import get_settings
from app.db.migrations import current_revision
from app.db.session import SessionLocal
from app.schemas.api import HealthResponse, ReadinessResponse, VersionResponse

router = APIRouter()


@router.get("/api/health", response_model=HealthResponse)
def health_check() -> HealthResponse:
    """Return dependency health for API, database, and Redis."""

    settings = get_settings()
    database_status = "ok"
    redis_status = "not_checked"

    try:
        with SessionLocal() as db:
            db.execute(text("SELECT 1"))
    except Exception as exc:  # pragma: no cover - defensive health reporting
        database_status = f"error: {exc.__class__.__name__}"

    try:
        import redis

        client = redis.from_url(settings.redis_url, socket_connect_timeout=0.5)
        client.ping()
        redis_status = "ok"
    except Exception as exc:  # pragma: no cover - Redis may be absent in local smoke tests
        redis_status = f"unavailable: {exc.__class__.__name__}"

    status = "ok" if database_status == "ok" else "degraded"
    return HealthResponse(
        status=status,
        database=database_status,
        redis=redis_status,
        app=settings.app_name,
    )


@router.get("/api/ready", response_model=ReadinessResponse)
def readiness_check(response: Response) -> ReadinessResponse:
    """Return readiness for dependencies required to process user operations."""

    settings = get_settings()
    checks = {
        "postgres": _database_status(),
        "redis": _redis_status(settings.redis_url),
        "artifact_storage": _storage_status(settings.writable_runtime_dirs),
        "config": "ok",
    }
    is_ready = all(value == "ok" for value in checks.values())
    if not is_ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return ReadinessResponse(status="ready" if is_ready else "not_ready", checks=checks)


@router.get("/api/version", response_model=VersionResponse)
def version_check() -> VersionResponse:
    """Return application and deployment version information."""

    settings = get_settings()
    return VersionResponse(
        app=settings.app_name,
        version=settings.app_version,
        environment=settings.environment,
        git_commit=settings.git_commit,
        database_revision=_database_revision(),
    )


def _database_revision() -> str:
    """Return the migration revision without exposing database connection details."""

    try:
        return current_revision() or "unversioned"
    except Exception as exc:  # pragma: no cover - defensive version reporting
        return f"unavailable:{exc.__class__.__name__}"


def _database_status() -> str:
    """Check that the configured database accepts a trivial query."""

    try:
        with SessionLocal() as db:
            db.execute(text("SELECT 1"))
    except Exception as exc:  # pragma: no cover - result is intentionally summarized
        return f"error:{exc.__class__.__name__}"
    return "ok"


def _redis_status(redis_url: str) -> str:
    """Check that Redis accepts a ping without exposing connection details."""

    try:
        import redis

        client = redis.from_url(redis_url, socket_connect_timeout=0.5)
        client.ping()
    except Exception as exc:  # pragma: no cover - result is intentionally summarized
        return f"error:{exc.__class__.__name__}"
    return "ok"


def _storage_status(directories: tuple[Path, ...]) -> str:
    """Check that every runtime directory exists and is writable."""

    for directory in directories:
        if not directory.is_dir():
            return "error:missing_directory"
        try:
            probe = directory / ".keiba_write_probe"
            probe.touch(exist_ok=True)
            probe.unlink(missing_ok=True)
        except OSError:
            return "error:not_writable"
    return "ok"
