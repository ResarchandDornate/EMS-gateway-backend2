"""Request-correlation + access-logging ASGI middleware (slice O-1).

Pure ASGI middleware mounted OUTERMOST (wrapping Starlette's ServerErrorMiddleware, via
`asgi_app` in src/main.py) so:
  * X-Request-ID is injected on EVERY response, including framework-generated 500s;
  * exactly one access event is emitted per HTTP request (even when the app raises);
  * unhandled exceptions are NOT swallowed - they propagate so Starlette/FastAPI's
    ServerErrorMiddleware and any registered exception handlers run normally and the
    server still logs/instruments the error.
The access event carries only request_id + method/path/status/duration; never headers,
cookies, bodies, or query strings.
"""
import re
import time
import uuid

import structlog
from starlette.datastructures import MutableHeaders

# Safe correlation id: printable, bounded, no CR/LF / control chars / spaces, so it cannot
# inject a header. Validated AS RECEIVED (no trimming); anything else -> generated uuid.
_VALID_ID = re.compile(r"^[A-Za-z0-9._-]{1,128}$")


def _resolve_request_id(scope) -> str:
    found = [v for (k, v) in scope.get("headers", []) if k == b"x-request-id"]
    if len(found) == 1:                       # exactly one header; 0 or ambiguous duplicates -> generate
        try:
            candidate = found[0].decode("latin-1")
        except Exception:
            candidate = ""
        if _VALID_ID.fullmatch(candidate):    # validated as received, not stripped
            return candidate
    return uuid.uuid4().hex


class RequestContextMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)      # forward lifespan/websocket untouched
            return

        request_id = _resolve_request_id(scope)
        # 500 until a response.start is observed (covers the app raising before responding;
        # the real framework 500 flows through send_wrapper and overwrites this).
        state = {"status": 500}

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                state["status"] = message["status"]
                message.setdefault("headers", [])
                MutableHeaders(raw=message["headers"])["X-Request-ID"] = request_id
            await send(message)

        start = time.perf_counter()
        # bound_contextvars resets request_id on exit (normal OR exception) -> no leak.
        with structlog.contextvars.bound_contextvars(request_id=request_id):
            try:
                # Do NOT catch: unhandled exceptions propagate so ServerErrorMiddleware and
                # any registered handlers run. Its 500 response.start passes through
                # send_wrapper above, so the header is injected before it re-raises.
                await self.app(scope, receive, send_wrapper)
            finally:
                duration_ms = round((time.perf_counter() - start) * 1000, 2)
                structlog.get_logger("access").info(
                    "access",
                    request_id=request_id,
                    method=scope.get("method"),
                    path=scope.get("path"),
                    status=state["status"],
                    duration_ms=duration_ms,
                )
