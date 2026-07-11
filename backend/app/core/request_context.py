from contextvars import ContextVar, Token


request_id_context: ContextVar[str] = ContextVar("request_id", default="-")


def set_request_id(request_id: str) -> Token[str]:
    """Store a request ID for logs emitted during the current request."""

    return request_id_context.set(request_id)


def reset_request_id(token: Token[str]) -> None:
    """Restore the request context after the response has been created."""

    request_id_context.reset(token)


def get_request_id() -> str:
    """Return the request ID associated with the current execution context."""

    return request_id_context.get()
