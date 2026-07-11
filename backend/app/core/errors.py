from __future__ import annotations

import logging
import traceback
from typing import Any

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.core.request_context import get_request_id
from app.core.logging import redact_text


logger = logging.getLogger(__name__)


def install_exception_handlers(app: FastAPI) -> None:
    """Register consistent, secret-safe API error responses."""

    app.add_exception_handler(HTTPException, _http_exception_handler)
    app.add_exception_handler(RequestValidationError, _validation_exception_handler)
    app.add_exception_handler(Exception, unexpected_exception_handler)


async def _http_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    http_exc = exc if isinstance(exc, HTTPException) else HTTPException(status_code=500)
    message = _safe_detail(http_exc.detail)
    return _error_response(
        status_code=http_exc.status_code,
        code=_http_error_code(http_exc.status_code),
        message=message,
        recommended_action=_recommended_action(http_exc.status_code),
    )


async def _validation_exception_handler(
    request: Request,
    exc: Exception,
) -> JSONResponse:
    validation_exc = exc if isinstance(exc, RequestValidationError) else None
    details: list[dict[str, Any]] = []
    if validation_exc is not None:
        for error in validation_exc.errors():
            # 入力値そのものは、APIキー等を含む可能性があるため返さない。
            details.append(
                {
                    "location": [str(item) for item in error.get("loc", ())],
                    "message": str(error.get("msg", "invalid value")),
                    "type": str(error.get("type", "validation_error")),
                }
            )
    return _error_response(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        code="VALIDATION_ERROR",
        message="入力内容を確認してください。",
        recommended_action="必須項目、形式、値の範囲を確認して再実行してください。",
        details=details,
    )


async def unexpected_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Log a redacted traceback and return a generic error response."""

    redacted_traceback = redact_text(
        "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    )
    logger.error(
        "unhandled request exception",
        extra={
            "event_code": "UNHANDLED_EXCEPTION",
            "method": request.method,
            "path": request.url.path,
            "exception_type": exc.__class__.__name__,
            "stack_trace": redacted_traceback,
        },
    )
    return _error_response(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        code="INTERNAL_ERROR",
        message="処理中に予期しないエラーが発生しました。",
        recommended_action="時間をおいて再実行し、解消しない場合はrequest_idを添えてログを確認してください。",
    )


def _error_response(
    *,
    status_code: int,
    code: str,
    message: str,
    recommended_action: str,
    details: list[dict[str, Any]] | None = None,
) -> JSONResponse:
    payload: dict[str, Any] = {
        "error": {
            "code": code,
            "message": message,
            "recommended_action": recommended_action,
        },
        "meta": {"request_id": get_request_id()},
    }
    if details:
        payload["error"]["details"] = details
    return JSONResponse(status_code=status_code, content=payload)


def _safe_detail(detail: Any) -> str:
    if isinstance(detail, str) and detail.strip():
        return detail
    return "要求された処理を完了できませんでした。"


def _http_error_code(status_code: int) -> str:
    return {
        400: "BAD_REQUEST",
        401: "UNAUTHORIZED",
        403: "FORBIDDEN",
        404: "NOT_FOUND",
        409: "CONFLICT",
        422: "UNPROCESSABLE_ENTITY",
        429: "RATE_LIMITED",
        503: "SERVICE_UNAVAILABLE",
    }.get(status_code, f"HTTP_{status_code}")


def _recommended_action(status_code: int) -> str:
    if status_code == 404:
        return "対象日、race_id、またはURLを確認してください。"
    if status_code == 409:
        return "同じ処理が実行中でないか確認し、完了後に再実行してください。"
    if status_code == 422:
        return "入力内容とデータ品質の警告を確認してください。"
    if status_code == 503:
        return "システム状態画面でDB、Redis、保存領域を確認してください。"
    return "入力内容と現在の処理状態を確認してください。"
