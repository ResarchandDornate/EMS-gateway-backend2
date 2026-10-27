"""Slice O-1 tests: outermost request-correlation + access logging, and /ready probe.

The request-correlation middleware is mounted OUTERMOST as src.main.asgi_app (wrapping
Starlette's ServerErrorMiddleware), so these tests drive `asgi_app` to exercise the real
served path. `app` stays the FastAPI instance (dependency_overrides unaffected).
"""
import httpx
import pytest
import structlog
from fastapi import FastAPI
from fastapi.responses import JSONResponse

import src.main as main
from src.main import app, asgi_app
from src.middleware import RequestContextMiddleware, _VALID_ID


async def _get(path="/health", headers=None, target=None, raise_app_exceptions=True):
    transport = httpx.ASGITransport(app=target or asgi_app,
                                    raise_app_exceptions=raise_app_exceptions)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.get(path, headers=headers or {})


# ---- wiring -----------------------------------------------------------------------

def test_asgi_app_wraps_the_fastapi_instance():
    assert isinstance(app, FastAPI)
    assert isinstance(asgi_app, RequestContextMiddleware)
    assert asgi_app.app is app                          # overrides set on `app` apply when serving asgi_app


# ---- inbound request id (validated as received; no stripping) --------------------

async def test_request_id_generated_when_absent():
    r = await _get("/health")
    rid = r.headers.get("x-request-id")
    assert rid and _VALID_ID.fullmatch(rid)


async def test_valid_inbound_request_id_echoed_unchanged():
    r = await _get("/health", headers={"X-Request-ID": "abc-123_OK.v2"})
    assert r.headers.get("x-request-id") == "abc-123_OK.v2"


@pytest.mark.parametrize("bad", [
    "bad\r\ninject", "x" * 200, "has space", " leadingspace", "trailingspace ",
    "ctrl\x00byte", "slash/not/allowed", "",
])
async def test_invalid_inbound_request_id_replaced_safely(bad):
    r = await _get("/health", headers={"X-Request-ID": bad})
    rid = r.headers.get("x-request-id")
    assert rid != bad and _VALID_ID.fullmatch(rid)
    assert "\r" not in rid and "\n" not in rid


async def test_duplicate_request_id_headers_rejected():
    r = await _get("/health", headers=[("x-request-id", "aaa"), ("x-request-id", "bbb")])
    rid = r.headers.get("x-request-id")
    assert rid not in ("aaa", "bbb") and _VALID_ID.fullmatch(rid)


# ---- access log: one event, correlated, safe fields only -------------------------

async def test_access_event_correlates_with_response_request_id():
    with structlog.testing.capture_logs() as logs:
        r = await _get("/health", headers={"X-Request-ID": "corr-123"})
    access = [e for e in logs if e.get("event") == "access"]
    assert len(access) == 1
    ev = access[0]
    assert ev["request_id"] == "corr-123" == r.headers["x-request-id"]
    assert ev["method"] == "GET" and ev["path"] == "/health"
    assert ev["status"] == 200 and "duration_ms" in ev


async def test_access_log_omits_sensitive_fields():
    with structlog.testing.capture_logs() as logs:
        await _get("/health?token=supersecret",
                   headers={"Authorization": "Bearer secret", "Cookie": "s=1"})
    ev = next(e for e in logs if e.get("event") == "access")
    blob = repr(ev).lower()
    assert "authorization" not in blob and "bearer" not in blob
    assert "cookie" not in blob and "supersecret" not in blob and "token=" not in blob
    assert ev["path"] == "/health"


# ---- framework exception handling PRESERVED, header still injected ---------------

async def test_unhandled_exception_framework_500_has_header_and_one_access():
    tapp = FastAPI()

    @tapp.get("/boom")
    async def boom():
        raise RuntimeError("kaboom")

    wrapped = RequestContextMiddleware(tapp)            # outermost, like asgi_app
    with structlog.testing.capture_logs() as logs:
        # raise_app_exceptions=False so we observe the FRAMEWORK-generated 500 response
        # (ServerErrorMiddleware renders it, then re-raises -> normal framework behavior).
        r = await _get("/boom", target=wrapped, raise_app_exceptions=False)
    assert r.status_code == 500
    assert r.text == "Internal Server Error"            # Starlette's own 500 body (not swallowed/replaced)
    rid = r.headers.get("x-request-id")
    assert rid and _VALID_ID.fullmatch(rid)
    access = [e for e in logs if e.get("event") == "access"]
    assert len(access) == 1 and access[0]["status"] == 500 and access[0]["request_id"] == rid
    assert "request_id" not in structlog.contextvars.get_contextvars()


async def test_registered_exception_handler_is_preserved():
    tapp = FastAPI()

    @tapp.exception_handler(ValueError)
    async def handle_ve(_request, _exc):
        return JSONResponse({"custom": True}, status_code=418)

    @tapp.get("/ve")
    async def ve():
        raise ValueError("boom")

    wrapped = RequestContextMiddleware(tapp)
    with structlog.testing.capture_logs() as logs:
        r = await _get("/ve", target=wrapped)
    assert r.status_code == 418 and r.json() == {"custom": True}   # handler ran
    assert r.headers.get("x-request-id")                            # header injected on its response
    access = [e for e in logs if e.get("event") == "access"]
    assert len(access) == 1 and access[0]["status"] == 418


# ---- bare-middleware contract: no swallow, single log, header injection ----------

async def test_bare_inner_raises_propagates_and_logs_once():
    async def boom(scope, receive, send):
        raise RuntimeError("boom")
    mw = RequestContextMiddleware(boom)
    scope = {"type": "http", "method": "GET", "path": "/x", "headers": []}

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(_m):
        return None

    with structlog.testing.capture_logs() as logs:
        with pytest.raises(RuntimeError):               # NOT swallowed -> propagates
            await mw(scope, receive, send)
    access = [e for e in logs if e.get("event") == "access"]
    assert len(access) == 1 and access[0]["status"] == 500
    assert "request_id" not in structlog.contextvars.get_contextvars()


async def test_bare_inner_500_response_gets_header_and_one_access():
    async def app500(scope, receive, send):
        await send({"type": "http.response.start", "status": 500, "headers": []})
        await send({"type": "http.response.body", "body": b""})
    mw = RequestContextMiddleware(app500)
    scope = {"type": "http", "method": "GET", "path": "/x", "headers": []}

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    sent = []

    async def send(m):
        sent.append(m)

    with structlog.testing.capture_logs() as logs:
        await mw(scope, receive, send)
    start = next(m for m in sent if m["type"] == "http.response.start")
    assert start["status"] == 500 and dict(start["headers"]).get(b"x-request-id")
    access = [e for e in logs if e.get("event") == "access"]
    assert len(access) == 1 and access[0]["status"] == 500


async def test_non_http_scope_passes_through():
    seen = {"called": False}

    async def inner(scope, receive, send):
        seen["called"] = True

    mw = RequestContextMiddleware(inner)
    await mw({"type": "lifespan"}, None, None)
    assert seen["called"] is True


# ---- /ready probe (mocked deps) + /health unchanged -----------------------------

class _FakeConn:
    async def __aenter__(self): return self
    async def __aexit__(self, *a): return False
    async def execute(self, *a, **k): return None


class _FakeConnFail:
    async def __aenter__(self): raise RuntimeError("postgres down")
    async def __aexit__(self, *a): return False


class _FakeEngine:
    def __init__(self, ok=True): self._ok = ok
    def connect(self): return _FakeConn() if self._ok else _FakeConnFail()


class _FakeRedis:
    def __init__(self, ok): self._ok = ok
    async def ping(self):
        if not self._ok:
            raise RuntimeError("redis down")
    async def aclose(self): return None


class _FakeRedisFactory:
    def __init__(self, ok): self._ok = ok
    def from_url(self, _url): return _FakeRedis(self._ok)


async def test_ready_success_shape(monkeypatch):
    monkeypatch.setattr(main, "engine", _FakeEngine(ok=True))
    monkeypatch.setattr(main, "Redis", _FakeRedisFactory(ok=True))
    r = await _get("/ready")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok" and body["postgres"] == "ok" and body["redis"] == "ok"


async def test_ready_returns_503_when_redis_unavailable(monkeypatch):
    monkeypatch.setattr(main, "engine", _FakeEngine(ok=True))
    monkeypatch.setattr(main, "Redis", _FakeRedisFactory(ok=False))
    r = await _get("/ready")
    assert r.status_code == 503
    body = r.json()
    assert body["status"] == "unavailable" and body["postgres"] == "ok" and body["redis"].startswith("error")


async def test_ready_returns_503_when_postgres_unavailable(monkeypatch):
    monkeypatch.setattr(main, "engine", _FakeEngine(ok=False))
    monkeypatch.setattr(main, "Redis", _FakeRedisFactory(ok=True))
    r = await _get("/ready")
    assert r.status_code == 503
    body = r.json()
    assert body["status"] == "unavailable" and body["postgres"].startswith("error") and body["redis"] == "ok"


async def test_health_unchanged():
    r = await _get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok", "service": "ems-backend"}
