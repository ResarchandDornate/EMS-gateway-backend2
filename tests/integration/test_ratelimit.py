"""Rate-limiter mechanism tests (shadow-first; enforcement OFF by default).

No real Redis server is required: the Redis path is faked and the deterministic logic
uses the per-process fallback bucket. No numeric value here is approved policy - all are
test fixtures. INGEST_ENABLED and both rate-limit flags stay OFF by default throughout.
An opt-in real-Redis integration test is included but skipped unless REDIS_TEST_URL is set.
"""
import asyncio
import os
import time
import types
import uuid

import pytest
from fastapi import HTTPException, Request
from pydantic import ValidationError

import src.ratelimit as rl
from src.auth.security import create_gateway_token, hash_password
from src.config import Settings, get_settings
from src.tenancy.gateways.models import GatewayModel
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel


def _settings(**over):
    return get_settings().model_copy(update=over)


def _request(*, headers=None, host="1.2.3.4", limiter=None):
    raw = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    app = types.SimpleNamespace(state=types.SimpleNamespace(rate_limiter=limiter))
    scope = {"type": "http", "method": "POST", "path": "/x", "headers": raw,
             "client": (host, 1111) if host else None, "app": app}
    return Request(scope)


async def _make_gateway(session, *, org_key, site_key, gw_key, client_id, enabled=True):
    org = OrganizationModel(org_key=org_key, name="Org"); session.add(org); await session.flush()
    site = SiteModel(organization_id=org.id, site_key=site_key, name="Site"); session.add(site); await session.flush()
    gw = GatewayModel(site_id=site.id, gateway_key=gw_key, client_id=client_id,
                      client_secret_hash=hash_password("x"), enabled=enabled)
    session.add(gw); await session.commit()
    return gw


def _wrap_eval(monkeypatch, label=None):
    """Record each _evaluate invocation (route, or a fixed label) in call order."""
    calls = []
    orig = rl._evaluate

    async def rec(request, *, route, key, key_kind):
        calls.append(label or route)
        return await orig(request, route=route, key=key, key_kind=key_kind)

    monkeypatch.setattr(rl, "_evaluate", rec)
    return calls


class _FakeRaises:
    async def consume(self, *a, **k):
        raise RuntimeError("redis down")
    async def aclose(self):
        pass


class _FakeSlow:
    async def consume(self, *a, **k):
        await asyncio.sleep(1.0)
    async def aclose(self):
        pass


class _FakeEval:
    """Realistic fake: returns a Lua-shaped result list [allowed, tokens, retry]."""
    def __init__(self, result):
        self._r = result
    async def eval(self, *a, **k):
        return self._r
    async def aclose(self):
        pass


class _FakeClient:
    def __init__(self):
        self.closed = False
    async def aclose(self):
        self.closed = True


# ========================= defaults OFF (flags) =========================
def test_default_flags_off():
    s = Settings()
    assert s.rate_limit_enabled is False
    assert s.rate_limit_enforce is False
    assert s.ingest_enabled is False


async def test_disabled_is_noop(monkeypatch):
    monkeypatch.setattr(rl, "get_settings", lambda: _settings(rate_limit_enabled=False))
    await rl.token_rate_limit(_request())


async def test_enabled_but_unconfigured_policy_is_noop(monkeypatch):
    monkeypatch.setattr(rl, "get_settings", lambda: _settings(
        rate_limit_enabled=True, token_rate_capacity=None, token_rate_refill_per_sec=None))
    await rl.token_rate_limit(_request())


# ========================= shadow never blocks =========================
async def test_shadow_never_blocks(monkeypatch):
    rl._local_fallback._state.clear()
    monkeypatch.setattr(rl, "get_settings", lambda: _settings(
        rate_limit_enabled=True, rate_limit_enforce=False,
        token_rate_capacity=1, token_rate_refill_per_sec=0.0))
    req = _request(host="9.9.9.9", limiter=None)
    await rl.token_rate_limit(req)     # consumes only token
    await rl.token_rate_limit(req)     # would-deny -> shadow, MUST NOT raise
    await rl.token_rate_limit(req)


# ========================= item 5: dormant enforce branch =========================
async def test_enforce_branch_returns_429_when_explicitly_invoked(monkeypatch):
    # enforce flipped ON only inside this unit test; default config stays OFF.
    rl._local_fallback._state.clear()
    monkeypatch.setattr(rl, "get_settings", lambda: _settings(
        rate_limit_enabled=True, rate_limit_enforce=True,
        token_rate_capacity=1, token_rate_refill_per_sec=0.0))
    req = _request(host="7.7.7.7", limiter=None)
    await rl.token_rate_limit(req)                 # allowed
    with pytest.raises(HTTPException) as e:
        await rl.token_rate_limit(req)             # denied -> 429
    assert e.value.status_code == 429
    assert "Retry-After" in e.value.headers


async def test_shadow_denying_bucket_never_429(monkeypatch):
    rl._local_fallback._state.clear()
    monkeypatch.setattr(rl, "get_settings", lambda: _settings(
        rate_limit_enabled=True, rate_limit_enforce=False,
        token_rate_capacity=1, token_rate_refill_per_sec=0.0))
    req = _request(host="6.6.6.6", limiter=None)
    await rl.token_rate_limit(req)
    await rl.token_rate_limit(req)     # denying decision under shadow -> no raise


# ========================= item 1: bounded local fallback =========================
async def test_local_bucket_denies_when_empty():
    b = rl.LocalTokenBucket()
    p = rl.Policy(capacity=2.0, refill_per_sec=0.0)
    now = time.time()
    assert (await b.consume("k", p, now)).allowed
    assert (await b.consume("k", p, now)).allowed
    assert not (await b.consume("k", p, now)).allowed


async def test_local_bucket_serializes_concurrent_access():
    # Proves the PER-PROCESS lock serializes access (NOT a proof of Redis atomicity).
    b = rl.LocalTokenBucket()
    p = rl.Policy(capacity=5.0, refill_per_sec=0.0)
    now = time.time()
    res = await asyncio.gather(*[b.consume("k", p, now) for _ in range(20)])
    assert sum(1 for r in res if r.allowed) == 5


async def test_local_fallback_evicts_by_max_keys():
    b = rl.LocalTokenBucket(max_keys=2, idle_seconds=0.0)   # idle disabled -> size-only
    p = rl.Policy(capacity=5.0, refill_per_sec=0.0)
    await b.consume("k1", p, 100.0)
    await b.consume("k2", p, 100.0)
    await b.consume("k3", p, 100.0)
    assert len(b._state) == 2
    assert "k1" not in b._state and "k3" in b._state


async def test_local_fallback_evicts_idle_keys():
    b = rl.LocalTokenBucket(max_keys=1000, idle_seconds=10.0)
    p = rl.Policy(capacity=5.0, refill_per_sec=0.0)
    await b.consume("old", p, 100.0)
    await b.consume("new", p, 200.0)   # 100s later -> 'old' idle (>10s) -> evicted
    assert "old" not in b._state and "new" in b._state


# ========================= item 2: observable Redis init failure (no secrets) =========================
async def test_redis_init_failure_is_observable_and_fail_open(monkeypatch):
    from structlog.testing import capture_logs
    monkeypatch.setattr(rl, "get_settings", lambda: _settings(rate_limit_enabled=True))
    import redis.asyncio as raio

    def _boom(*a, **k):
        raise RuntimeError("no redis")
    monkeypatch.setattr(raio.Redis, "from_url", classmethod(lambda cls, *a, **k: _boom()))
    app = types.SimpleNamespace(state=types.SimpleNamespace())
    with capture_logs() as logs:
        await rl.init_rate_limiter(app)        # must NOT raise (fail-open)
    assert app.state.rate_limiter is None
    evt = [l for l in logs if l.get("event") == "ratelimit_redis_init_failed"]
    assert evt and evt[0]["exc_type"] == "RuntimeError"
    for l in logs:                             # never leak url/secret/token
        assert "url" not in l and "redis_url" not in l and "token" not in l


# ========================= item 3: config validation =========================
def test_config_rejects_negative_capacity():
    with pytest.raises(ValidationError):
        Settings(token_rate_capacity=-1)


def test_config_rejects_negative_refill():
    with pytest.raises(ValidationError):
        Settings(ingest_rate_refill_per_sec=-0.5)


def test_config_rejects_nonpositive_timeout():
    with pytest.raises(ValidationError):
        Settings(rate_limit_redis_timeout_seconds=0)


def test_config_rejects_bad_fallback_bounds():
    with pytest.raises(ValidationError):
        Settings(rate_limit_local_fallback_max_keys=0)
    with pytest.raises(ValidationError):
        Settings(rate_limit_local_fallback_idle_seconds=-1)


def test_policy_zero_capacity_is_inert():
    s = types.SimpleNamespace(token_rate_capacity=0, token_rate_refill_per_sec=1.0,
                              ingest_rate_capacity=0, ingest_rate_refill_per_sec=1.0)
    assert rl._policy_for("token", s) is None
    assert rl._policy_for("ingest", s) is None


# ========================= item 6: Redis Lua result handling + error path =========================
async def test_redis_bucket_parses_eval_result():
    allow = rl.RedisTokenBucket(_FakeEval([1, "3.0", "0.0"]))
    d = await allow.consume("k", rl.Policy(5.0, 1.0), 0.0)
    assert d.allowed and d.remaining == 3.0 and d.retry_after == 0.0
    deny = rl.RedisTokenBucket(_FakeEval([0, "0.0", "2.5"]))
    d2 = await deny.consume("k", rl.Policy(5.0, 1.0), 0.0)
    assert not d2.allowed and d2.retry_after == 2.5


async def test_redis_error_degrades_fail_open(monkeypatch):
    rl._local_fallback._state.clear()
    monkeypatch.setattr(rl, "get_settings", lambda: _settings(
        rate_limit_enabled=True, rate_limit_enforce=False,
        token_rate_capacity=100, token_rate_refill_per_sec=1.0,
        rate_limit_redis_timeout_seconds=0.05))
    await rl.token_rate_limit(_request(limiter=_FakeRaises()))   # must not raise


async def test_redis_timeout_degrades_fail_open(monkeypatch):
    rl._local_fallback._state.clear()
    monkeypatch.setattr(rl, "get_settings", lambda: _settings(
        rate_limit_enabled=True, rate_limit_enforce=False,
        token_rate_capacity=100, token_rate_refill_per_sec=1.0,
        rate_limit_redis_timeout_seconds=0.02))
    await rl.token_rate_limit(_request(limiter=_FakeSlow()))     # wait_for -> fallback


@pytest.mark.skipif(not os.environ.get("REDIS_TEST_URL"),
                    reason="set REDIS_TEST_URL to run the real-Redis integration test")
async def test_real_redis_token_bucket_integration():
    import redis.asyncio as raio
    client = raio.Redis.from_url(os.environ["REDIS_TEST_URL"], decode_responses=True)
    try:
        b = rl.RedisTokenBucket(client)
        key = f"rl:test:{uuid.uuid4()}"
        p = rl.Policy(capacity=2.0, refill_per_sec=0.0)
        now = time.time()
        assert (await b.consume(key, p, now)).allowed
        assert (await b.consume(key, p, now)).allowed
        assert not (await b.consume(key, p, now)).allowed
    finally:
        await client.aclose()


# ========================= keying on the AUTHENTICATED gateway =========================
async def test_ingest_limiter_keys_on_authenticated_gateway(monkeypatch):
    rl._local_fallback._state.clear()
    monkeypatch.setattr(rl, "get_settings", lambda: _settings(
        rate_limit_enabled=True, rate_limit_enforce=False,
        ingest_rate_capacity=5, ingest_rate_refill_per_sec=0.0))
    gid = uuid.uuid4()
    # gw is normally injected by Depends(get_current_gateway); pass it directly here.
    await rl.ingest_rate_limit(_request(), gw=types.SimpleNamespace(id=gid))
    assert f"rl:ingest:gw:{gid}" in rl._local_fallback._state


# ========================= item 7: lifespan + entrypoint =========================
async def test_lifespan_disabled_sets_none(monkeypatch):
    monkeypatch.setattr(rl, "get_settings", lambda: _settings(rate_limit_enabled=False))
    app = types.SimpleNamespace(state=types.SimpleNamespace())
    await rl.init_rate_limiter(app)
    assert app.state.rate_limiter is None
    await rl.shutdown_rate_limiter(app)


async def test_lifespan_enabled_builds_and_cleans(monkeypatch):
    monkeypatch.setattr(rl, "get_settings", lambda: _settings(rate_limit_enabled=True))
    fake = _FakeClient()
    import redis.asyncio as raio
    monkeypatch.setattr(raio.Redis, "from_url", classmethod(lambda cls, *a, **k: fake))
    app = types.SimpleNamespace(state=types.SimpleNamespace())
    await rl.init_rate_limiter(app)
    assert isinstance(app.state.rate_limiter, rl.RedisTokenBucket)
    await rl.shutdown_rate_limiter(app)
    assert fake.closed is True


def test_asgi_entrypoint_and_middleware_intact():
    import src.main as m
    from src.middleware import RequestContextMiddleware
    assert isinstance(m.asgi_app, RequestContextMiddleware)
    assert m.asgi_app.app is m.app


# ========================= item 4: auth precedes limiter; limiter precedes gate =========================
async def test_ingest_missing_token_not_rate_limited_still_401(client, session, monkeypatch):
    calls = _wrap_eval(monkeypatch)
    monkeypatch.setattr(rl, "get_settings", lambda: _settings(
        rate_limit_enabled=True, rate_limit_enforce=False,
        ingest_rate_capacity=1, ingest_rate_refill_per_sec=0.0))
    c, _ = client
    r = await c.post("/api/v1/gateway/ingest", json={})
    assert r.status_code == 401
    assert calls == []                         # limiter never reached (auth first)


async def test_ingest_invalid_token_not_rate_limited_still_401(client, session, monkeypatch):
    calls = _wrap_eval(monkeypatch)
    monkeypatch.setattr(rl, "get_settings", lambda: _settings(
        rate_limit_enabled=True, rate_limit_enforce=False,
        ingest_rate_capacity=1, ingest_rate_refill_per_sec=0.0))
    c, _ = client
    r = await c.post("/api/v1/gateway/ingest", json={}, headers={"Authorization": "Bearer garbage"})
    assert r.status_code == 401
    assert calls == []


async def test_ingest_disabled_gateway_403_not_rate_limited(client, session, monkeypatch):
    calls = _wrap_eval(monkeypatch)
    monkeypatch.setattr(rl, "get_settings", lambda: _settings(
        rate_limit_enabled=True, rate_limit_enforce=False,
        ingest_rate_capacity=1, ingest_rate_refill_per_sec=0.0))
    c, _ = client
    gw = await _make_gateway(session, org_key="od", site_key="sd", gw_key="gd",
                             client_id="cd", enabled=False)
    tok = create_gateway_token(gw.id)
    r = await c.post("/api/v1/gateway/ingest", json={}, headers={"Authorization": f"Bearer {tok}"})
    assert r.status_code == 403
    assert calls == []                         # disabled gateway -> 403 before limiting


async def test_ingest_gate_off_valid_token_503_limiter_ran(client, session, monkeypatch):
    rl._local_fallback._state.clear()
    calls = _wrap_eval(monkeypatch)
    monkeypatch.setattr(rl, "get_settings", lambda: _settings(
        rate_limit_enabled=True, rate_limit_enforce=False,
        ingest_rate_capacity=1, ingest_rate_refill_per_sec=0.0))
    c, _ = client
    gw = await _make_gateway(session, org_key="og", site_key="sg", gw_key="gg", client_id="cg")
    tok = create_gateway_token(gw.id)
    h = {"Authorization": f"Bearer {tok}"}
    r1 = await c.post("/api/v1/gateway/ingest", json={}, headers=h)  # allowed
    r2 = await c.post("/api/v1/gateway/ingest", json={}, headers=h)  # denied (shadow)
    assert r1.status_code == 503 and r2.status_code == 503          # gate OFF
    assert r1.status_code != 429 and r2.status_code != 429          # shadow never 429
    assert calls == ["ingest", "ingest"]                            # limiter ran (after auth)


async def test_ratelimit_runs_after_auth_before_gate(client, session, monkeypatch):
    # Observe ACTUAL runtime order: the limiter (which depends on auth) records before the
    # gate resolves its settings. Auth necessarily precedes the limiter (its sub-dependency).
    calls = []
    orig = rl._evaluate

    async def rec_eval(request, *, route, key, key_kind):
        calls.append("ratelimit")
        return await orig(request, route=route, key=key, key_kind=key_kind)

    real = get_settings()

    def rec_settings():
        calls.append("gate")
        return real                            # ingest_enabled False -> gate 503

    monkeypatch.setattr(rl, "_evaluate", rec_eval)
    monkeypatch.setattr(rl, "get_settings", lambda: _settings(
        rate_limit_enabled=True, rate_limit_enforce=False,
        ingest_rate_capacity=1, ingest_rate_refill_per_sec=0.0))
    c, _ = client
    gw = await _make_gateway(session, org_key="oo", site_key="so", gw_key="go", client_id="co")
    tok = create_gateway_token(gw.id)
    from src.main import app
    app.dependency_overrides[get_settings] = rec_settings
    try:
        r = await c.post("/api/v1/gateway/ingest", json={}, headers={"Authorization": f"Bearer {tok}"})
    finally:
        app.dependency_overrides.pop(get_settings, None)
    assert r.status_code == 503 and r.status_code != 429
    assert "ratelimit" in calls and "gate" in calls
    assert calls.index("ratelimit") < calls.index("gate")
