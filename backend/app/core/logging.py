from __future__ import annotations

import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any

from app.core.request_context import get_request_id


_SENSITIVE_PATTERNS = (
    # URL内のユーザー名・パスワードをログへ残さない。
    (re.compile(r"(?i)(postgres(?:ql)?(?:\+\w+)?://)[^:@/\s]+:[^@/\s]+@"), r"\1***:***@"),
    # Bearer tokenや一般的なsecret表現を値ごと伏せる。
    (re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._~+\-/=]+"), r"\1***"),
    (re.compile(r"(?i)((?:password|secret|api[_-]?key|token)\s*[=:]\s*)[^\s,;]+"), r"\1***"),
)


def redact_text(value: str) -> str:
    """Redact common credential patterns before a value reaches stdout."""

    redacted = value
    for pattern, replacement in _SENSITIVE_PATTERNS:
        redacted = pattern.sub(replacement, redacted)
    return redacted


class JsonLogFormatter(logging.Formatter):
    """Format application logs as one JSON object per line."""

    _extra_fields = (
        "event_code",
        "service_name",
        "method",
        "path",
        "status_code",
        "duration_ms",
        "run_id",
        "race_date",
        "race_id",
        "job_id",
        "exception_type",
        "stack_trace",
    )

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "service_name": getattr(
                record,
                "service_name",
                os.getenv("KEIBA_SERVICE_NAME", "api"),
            ),
            "request_id": get_request_id(),
            "message": redact_text(record.getMessage()),
        }
        for field in self._extra_fields:
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = redact_text(str(value)) if isinstance(value, str) else value
        if record.exc_info:
            payload["exception"] = redact_text(self.formatException(record.exc_info))
        return json.dumps(payload, ensure_ascii=False, default=str)


def configure_logging(level: int = logging.INFO) -> None:
    """Install deterministic JSON logging for API and worker processes."""

    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        if isinstance(handler.formatter, JsonLogFormatter):
            return

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonLogFormatter())
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(level)
