"""Guards the served ASGI entrypoint: asgi_app must wrap the request-context middleware, and
the Dockerfile must serve src.main:asgi_app (never the bare src.main:app, which drops the
middleware and CORS)."""
import re
from pathlib import Path

from src.main import app, asgi_app
from src.middleware import RequestContextMiddleware

_ROOT = Path(__file__).resolve().parents[2]


def test_asgi_app_wraps_request_context_middleware():
    assert isinstance(asgi_app, RequestContextMiddleware)
    assert asgi_app.app is app                      # wraps the FastAPI app, not something else


def test_dockerfile_serves_asgi_app_not_bare_app():
    txt = (_ROOT / "Dockerfile").read_text()
    assert "src.main:asgi_app" in txt               # correct entrypoint present
    assert not re.search(r"src\.main:app(\s|[\"\'])", txt)   # bare :app must NOT be served
