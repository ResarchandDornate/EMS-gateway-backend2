"""CORS configuration tests (no DB). Builds throwaway apps with add_cors + crafted settings."""
import pytest
from fastapi import FastAPI
from pydantic import ValidationError
from starlette.testclient import TestClient

from src.config import Settings, get_settings, resolve_cors_origins
from src.main import CORS_METHODS, add_cors

ALLOWED = "https://app.unityess.example"
DEV = "http://localhost:3000"


def _settings(**over):
    base = get_settings().model_dump()
    base.update(over)
    return Settings(**base)                      # re-runs validators (incl. wildcard guard)


def _client(**over):
    app = FastAPI()

    @app.get("/ping")
    async def ping():
        return {"ok": True}

    add_cors(app, _settings(**over))
    return TestClient(app)


def test_allowed_origin_prod():
    c = _client(env="prod", cors_allowed_origins=ALLOWED)
    r = c.get("/ping", headers={"Origin": ALLOWED})
    assert r.status_code == 200
    assert r.headers.get("access-control-allow-origin") == ALLOWED


def test_disallowed_origin_prod():
    c = _client(env="prod", cors_allowed_origins=ALLOWED)
    r = c.get("/ping", headers={"Origin": "https://evil.example"})
    assert r.status_code == 200                               # request still served
    assert "access-control-allow-origin" not in r.headers     # but not CORS-approved


def test_authorization_header_preflight():
    c = _client(env="prod", cors_allowed_origins=ALLOWED)
    r = c.options("/ping", headers={"Origin": ALLOWED,
                                    "Access-Control-Request-Method": "GET",
                                    "Access-Control-Request-Headers": "authorization"})
    assert r.status_code in (200, 204)
    assert r.headers.get("access-control-allow-origin") == ALLOWED
    assert "authorization" in r.headers.get("access-control-allow-headers", "").lower()


def test_required_methods_preflight():
    c = _client(env="prod", cors_allowed_origins=ALLOWED)
    for m in ("GET", "POST", "PUT", "PATCH", "DELETE"):
        r = c.options("/ping", headers={"Origin": ALLOWED, "Access-Control-Request-Method": m})
        assert m in r.headers.get("access-control-allow-methods", "")
    assert set(CORS_METHODS) >= {"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"}


def test_dev_origins_allowed_in_dev_only():
    dev = _client(env="dev")                                  # no explicit allowlist
    r = dev.get("/ping", headers={"Origin": DEV})
    assert r.headers.get("access-control-allow-origin") == DEV
    prod = _client(env="prod", cors_allowed_origins=ALLOWED)  # dev origin NOT allowed in prod
    r2 = prod.get("/ping", headers={"Origin": DEV})
    assert "access-control-allow-origin" not in r2.headers


def test_wildcard_with_credentials_rejected():
    with pytest.raises(ValidationError):
        _settings(env="prod", cors_allowed_origins="*", cors_allow_credentials=True)


def test_wildcard_outside_dev_rejected():
    with pytest.raises(ValidationError):
        _settings(env="prod", cors_allowed_origins="*", cors_allow_credentials=False)


def test_wildcard_tolerated_in_dev_without_credentials():
    s = _settings(env="dev", cors_allowed_origins="*", cors_allow_credentials=False)
    assert "*" in resolve_cors_origins(s)
