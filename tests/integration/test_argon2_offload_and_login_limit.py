"""Regression tests for audit finding D-10 (Argon2 event-loop blocking + login limiting).

- Argon2id hash/verify now run via asyncio.to_thread, so password auth no longer blocks the
  event loop. We assert correctness under concurrency (many simultaneous logins) and that
  valid/invalid credentials still behave exactly as before.
- A per-IP login limiter exists but is INERT by default (shares the auth master switch); when
  explicitly enabled+enforced it returns 429 past capacity.
"""
import asyncio

import src.auth.rate_limit as rl_mod
import src.ratelimit as ratelimit_mod
from src.auth.models import UserModel
from src.auth.security import hash_password
from src.auth.service import authenticate
from src.config import get_settings

LOGIN = "/api/v1/auth/login"


async def test_concurrent_authenticate_all_succeed(session):
    """10 concurrent authenticate() calls (offloaded Argon2) all return the user, and a
    wrong password still fails — correctness holds under concurrency."""
    session.add(UserModel(email="race@x.com", password_hash=hash_password("pw")))
    await session.flush()
    results = await asyncio.gather(*[authenticate(session, "race@x.com", "pw") for _ in range(10)])
    assert all(u is not None for u in results)
    assert await authenticate(session, "race@x.com", "wrong") is None


async def test_login_endpoint_token_and_invalid(client, session):
    """The /login endpoint issues a token for good credentials and 401 for bad ones; the
    limiter is inert by default so repeated logins are not blocked."""
    c, _ = client
    session.add(UserModel(email="admin@x.com", password_hash=hash_password("secret")))
    await session.flush()
    for _ in range(5):                       # well past any default — limiter is off
        ok = await c.post(LOGIN, json={"identifier": "admin@x.com", "password": "secret"})
        assert ok.status_code == 200 and ok.json()["access_token"]
    bad = await c.post(LOGIN, json={"identifier": "admin@x.com", "password": "nope"})
    assert bad.status_code == 401


async def test_login_limiter_enforced_returns_429(client, session, monkeypatch):
    """With the auth master switch on, enforce on, and a capacity of 2, the 3rd login from
    the same IP is rejected 429 (mechanism works when deliberately configured)."""
    c, _ = client
    session.add(UserModel(email="t@x.com", password_hash=hash_password("secret")))
    await session.flush()
    tuned = get_settings().model_copy(update={
        "otp_rate_limit_enabled": True, "otp_rate_limit_enforce": True,
        "login_ip_capacity": 2, "login_ip_refill_per_sec": 0.0})
    monkeypatch.setattr(rl_mod, "get_settings", lambda: tuned)
    monkeypatch.setattr(ratelimit_mod, "get_settings", lambda: tuned)
    codes = [(await c.post(LOGIN, json={"identifier": "t@x.com", "password": "secret"})).status_code
             for _ in range(3)]
    assert codes[:2] == [200, 200]
    assert codes[2] == 429
