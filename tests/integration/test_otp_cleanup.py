"""OTP challenge cleanup (F5) tests.

On-write, per-user purge of fully-expired challenges, inert unless
otp_challenge_cleanup_enabled. Retention is enforced >= the F2 window so rows still counted by
recent_attempt_total are never deleted. Uses the shared conftest client/session fixtures;
requires the ems_test Postgres.
"""
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select

from src.auth import otp as otp_module
from src.auth.models import OtpChallengeModel, UserModel
from src.config import get_settings
from src.rbac.models import RoleModel, UserRoleModel


def _enable(monkeypatch, **kw):
    s = get_settings()
    for k, v in kw.items():
        monkeypatch.setattr(s, k, v)


async def _org_admin(session, phone):
    role = RoleModel(name="org_admin")
    user = UserModel(phone=phone)
    session.add_all([role, user])
    await session.flush()
    session.add(UserRoleModel(user_id=user.id, role_id=role.id))
    await session.flush()
    return user


async def _add_challenge(session, uid, phone, *, expires_at):
    ch = OtpChallengeModel(user_id=uid, channel="sms", destination=phone,
                           otp_hash="x", expires_at=expires_at)
    session.add(ch)
    await session.flush()
    cid = ch.id
    await session.commit()
    return cid


async def _ids(session, uid):
    return set((await session.execute(
        select(OtpChallengeModel.id).where(OtpChallengeModel.user_id == uid)
    )).scalars().all())


async def test_cleanup_removes_old_expired(client, session, monkeypatch):
    c, _ = client
    user = await _org_admin(session, "919000000301")
    uid, phone = user.id, user.phone
    now = datetime.now(timezone.utc)
    stale = await _add_challenge(session, uid, phone, expires_at=now - timedelta(hours=2))
    _enable(monkeypatch, otp_rate_limit_enabled=False,
            otp_challenge_cleanup_enabled=True, otp_challenge_retention_seconds=3600)
    r = await c.post("/api/v1/auth/otp/request", json={"identifier": phone})
    assert r.status_code == 200, r.text
    assert stale not in await _ids(session, uid), "old expired challenge should be purged"


async def test_cleanup_keeps_live(client, session, monkeypatch):
    c, _ = client
    user = await _org_admin(session, "919000000302")
    uid, phone = user.id, user.phone
    now = datetime.now(timezone.utc)
    live = await _add_challenge(session, uid, phone, expires_at=now + timedelta(minutes=5))
    _enable(monkeypatch, otp_rate_limit_enabled=False,
            otp_challenge_cleanup_enabled=True, otp_challenge_retention_seconds=3600)
    r = await c.post("/api/v1/auth/otp/request", json={"identifier": phone})
    assert r.status_code == 200, r.text
    assert live in await _ids(session, uid), "a live challenge must never be purged"


async def test_cleanup_keeps_within_retention(client, session, monkeypatch):
    """Expired but newer than the retention horizon (i.e. still inside the F2 window) -> kept,
    so F2's cumulative count is never reset by cleanup."""
    c, _ = client
    user = await _org_admin(session, "919000000303")
    uid, phone = user.id, user.phone
    now = datetime.now(timezone.utc)
    recent = await _add_challenge(session, uid, phone, expires_at=now - timedelta(minutes=10))
    _enable(monkeypatch, otp_rate_limit_enabled=False,
            otp_challenge_cleanup_enabled=True, otp_challenge_retention_seconds=3600)
    r = await c.post("/api/v1/auth/otp/request", json={"identifier": phone})
    assert r.status_code == 200, r.text
    assert recent in await _ids(session, uid), "rows within retention must be kept (F2 safety)"


async def test_cleanup_failure_does_not_break_request(client, session, monkeypatch):
    c, mock = client
    user = await _org_admin(session, "919000000304")
    uid, phone = user.id, user.phone
    _enable(monkeypatch, otp_rate_limit_enabled=False,
            otp_challenge_cleanup_enabled=True, otp_challenge_retention_seconds=3600)

    async def _boom(*a, **k):
        raise RuntimeError("cleanup failed")
    monkeypatch.setattr(otp_module, "purge_stale_challenges", _boom)

    r = await c.post("/api/v1/auth/otp/request", json={"identifier": phone})
    assert r.status_code == 200, r.text
    # a new challenge was still created and the code sent
    assert mock.sent and mock.sent[-1][1] == phone
    assert len(await _ids(session, uid)) >= 1


async def test_cleanup_inert_by_default(client, session):
    c, _ = client
    user = await _org_admin(session, "919000000305")
    uid, phone = user.id, user.phone
    now = datetime.now(timezone.utc)
    stale = await _add_challenge(session, uid, phone, expires_at=now - timedelta(days=3))
    r = await c.post("/api/v1/auth/otp/request", json={"identifier": phone})
    assert r.status_code == 200, r.text
    assert stale in await _ids(session, uid), "cleanup disabled -> stale row must remain"
