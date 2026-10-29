"""OTP hardening F1 (rate limiting) + F2 (verification-attempt cap) tests.

All new controls are inert by default; these tests enable them per-test by monkeypatching
the shared cached Settings instance (every module reads the same get_settings() object).

F1 is exercised through the per-process local fallback bucket (Redis is not required): at
startup the flags are OFF so app.state.rate_limiter is None, and _limit falls back to the
bounded in-process bucket, which is deterministic for these assertions.

Uses the shared conftest `client`/`session` fixtures. Requires the ems_test Postgres.
"""
import pytest
from sqlalchemy import select

from src.auth.models import OtpChallengeModel, UserModel
from src.config import get_settings
from src.rbac.models import RoleModel, UserRoleModel


def _enable(monkeypatch, **kw):
    s = get_settings()
    for k, v in kw.items():
        monkeypatch.setattr(s, k, v)


@pytest.fixture(autouse=True)
def _clear_local_bucket():
    # the per-process fallback bucket is module-global; clear it around each test
    from src.ratelimit import _local_fallback
    _local_fallback._state.clear()
    yield
    _local_fallback._state.clear()


async def _org_admin(session, phone):
    role = RoleModel(name="org_admin")
    user = UserModel(phone=phone)
    session.add_all([role, user])
    await session.flush()
    session.add(UserRoleModel(user_id=user.id, role_id=role.id))
    await session.flush()
    return user


# ============================ F1: rate limiting ============================

async def test_f1_request_user_enforce_429(client, session, monkeypatch):
    c, _ = client
    user = await _org_admin(session, "919000000101")
    _enable(monkeypatch, otp_rate_limit_enabled=True, otp_rate_limit_enforce=True,
            otp_request_user_capacity=2, otp_request_user_refill_per_sec=0.0)
    r1 = await c.post("/api/v1/auth/otp/request", json={"identifier": user.phone})
    r2 = await c.post("/api/v1/auth/otp/request", json={"identifier": user.phone})
    r3 = await c.post("/api/v1/auth/otp/request", json={"identifier": user.phone})
    assert r1.status_code == 200 and r2.status_code == 200
    assert r3.status_code == 429, r3.text


async def test_f1_shadow_never_blocks(client, session, monkeypatch):
    c, _ = client
    user = await _org_admin(session, "919000000102")
    _enable(monkeypatch, otp_rate_limit_enabled=True, otp_rate_limit_enforce=False,
            otp_request_user_capacity=1, otp_request_user_refill_per_sec=0.0)
    for _ in range(3):
        r = await c.post("/api/v1/auth/otp/request", json={"identifier": user.phone})
        assert r.status_code == 200, r.text


async def test_f1_inert_by_default(client, session):
    c, _ = client
    user = await _org_admin(session, "919000000103")
    for _ in range(4):
        r = await c.post("/api/v1/auth/otp/request", json={"identifier": user.phone})
        assert r.status_code == 200, r.text


# ============================ F2: verification cap ============================

async def test_f2_cap_blocks_valid_code_enforce(client, session, monkeypatch):
    """3 wrong verifies reach the cap; a 4th verify with the CORRECT code is rejected 401
    and the challenge is NOT consumed -> proves F2 blocked a valid code."""
    c, mock = client
    user = await _org_admin(session, "919000000201")
    uid, phone = user.id, user.phone        # capture before any op that may expire the ORM instance
    _enable(monkeypatch, otp_rate_limit_enabled=True, otp_rate_limit_enforce=True,
            otp_verify_cap=3, otp_verify_cap_window_seconds=900)
    await c.post("/api/v1/auth/otp/request", json={"identifier": phone})
    code = mock.sent[-1][2]
    wrong = ("1" if code[0] == "0" else "0") + code[1:]
    for _ in range(3):
        r = await c.post("/api/v1/auth/otp/verify", json={"identifier": phone, "otp": wrong})
        assert r.status_code == 401
    # 4th with the correct code -> blocked by F2, generic 401, not consumed
    r4 = await c.post("/api/v1/auth/otp/verify", json={"identifier": phone, "otp": code})
    assert r4.status_code == 401, r4.text
    consumed = (await session.execute(
        select(OtpChallengeModel.consumed_at)
        .where(OtpChallengeModel.user_id == uid)
        .order_by(OtpChallengeModel.created_at.desc()).limit(1)
    )).scalars().first()
    assert consumed is None, "F2-blocked attempt must not consume the challenge"


async def test_f2_new_challenge_does_not_reset(client, session, monkeypatch):
    """After reaching the cap, requesting a NEW challenge does not reset the cumulative
    count; the fresh correct code is still rejected."""
    c, mock = client
    user = await _org_admin(session, "919000000202")
    _enable(monkeypatch, otp_rate_limit_enabled=True, otp_rate_limit_enforce=True,
            otp_verify_cap=3, otp_verify_cap_window_seconds=900)
    await c.post("/api/v1/auth/otp/request", json={"identifier": user.phone})
    code1 = mock.sent[-1][2]
    wrong = ("1" if code1[0] == "0" else "0") + code1[1:]
    for _ in range(3):
        await c.post("/api/v1/auth/otp/verify", json={"identifier": user.phone, "otp": wrong})
    # new challenge (supersedes the old one); cumulative attempts persist
    await c.post("/api/v1/auth/otp/request", json={"identifier": user.phone})
    code2 = mock.sent[-1][2]
    r = await c.post("/api/v1/auth/otp/verify", json={"identifier": user.phone, "otp": code2})
    assert r.status_code == 401, "new challenge must not reset the cumulative cap"


async def test_f2_shadow_allows_over_cap(client, session, monkeypatch):
    c, mock = client
    user = await _org_admin(session, "919000000203")
    _enable(monkeypatch, otp_rate_limit_enabled=True, otp_rate_limit_enforce=False,
            otp_verify_cap=1, otp_verify_cap_window_seconds=900)
    await c.post("/api/v1/auth/otp/request", json={"identifier": user.phone})
    code = mock.sent[-1][2]
    wrong = ("1" if code[0] == "0" else "0") + code[1:]
    await c.post("/api/v1/auth/otp/verify", json={"identifier": user.phone, "otp": wrong})  # attempts=1 >= cap
    r = await c.post("/api/v1/auth/otp/verify", json={"identifier": user.phone, "otp": code})
    assert r.status_code == 200, "shadow mode must not block"


async def test_f2_inert_by_default(client, session):
    c, mock = client
    user = await _org_admin(session, "919000000204")
    await c.post("/api/v1/auth/otp/request", json={"identifier": user.phone})
    code = mock.sent[-1][2]
    r = await c.post("/api/v1/auth/otp/verify", json={"identifier": user.phone, "otp": code})
    assert r.status_code == 200, "cap must be ignored when disabled"


async def test_f2_supersede_invalidates_prior_code(client, session, monkeypatch):
    """With F2 active, a second /otp/request supersedes the first challenge: the old code
    no longer verifies, the new one does (cap set high so only supersede is exercised)."""
    c, mock = client
    user = await _org_admin(session, "919000000205")
    _enable(monkeypatch, otp_rate_limit_enabled=True, otp_rate_limit_enforce=True,
            otp_verify_cap=100, otp_verify_cap_window_seconds=900)
    await c.post("/api/v1/auth/otp/request", json={"identifier": user.phone})
    code1 = mock.sent[-1][2]
    await c.post("/api/v1/auth/otp/request", json={"identifier": user.phone})
    code2 = mock.sent[-1][2]
    old = await c.post("/api/v1/auth/otp/verify", json={"identifier": user.phone, "otp": code1})
    assert old.status_code == 401, "superseded code must not verify"
    new = await c.post("/api/v1/auth/otp/verify", json={"identifier": user.phone, "otp": code2})
    assert new.status_code == 200, new.text
