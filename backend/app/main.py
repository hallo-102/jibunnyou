from contextlib import asynccontextmanager
import logging
import re
from time import perf_counter
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.router import api_router
from app.core.config import get_settings
from app.core.errors import install_exception_handlers, unexpected_exception_handler
from app.core.logging import configure_logging
from app.core.request_context import reset_request_id, set_request_id
from app.db.init_db import init_db


REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{1,100}$")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database tables at application startup."""

    settings.ensure_runtime_directories()
    init_db()
    yield


settings = get_settings()
configure_logging()

app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)
install_exception_handlers(app)


@app.middleware("http")
async def request_context_middleware(request: Request, call_next):
    """Attach a safe request ID and emit one structured access log."""

    supplied_request_id = request.headers.get("X-Request-ID", "")
    request_id = (
        supplied_request_id
        if REQUEST_ID_PATTERN.fullmatch(supplied_request_id)
        else str(uuid4())
    )
    token = set_request_id(request_id)
    started = perf_counter()
    try:
        try:
            response = await call_next(request)
        except Exception as exc:
            # 500応答を生成する間もrequest_idを保持し、秘密値を含む例外文は返さない。
            response = await unexpected_exception_handler(request, exc)
        response.headers["X-Request-ID"] = request_id
        logger.info(
            "request completed",
            extra={
                "event_code": "HTTP_REQUEST_COMPLETED",
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "duration_ms": round((perf_counter() - started) * 1000, 2),
            },
        )
        return response
    finally:
        reset_request_id(token)
