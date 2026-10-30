"""OTP persistence regression tests.

These tests deliberately do NOT use the shared-session `client` fixture from
conftest.py. That fixture yields ONE session to every request, which hides the
very defect these tests guard against: `get_session` does not commit, so OTP
write paths that only `flush()` are rolled back when each real HTTP request gets
its own session.

Harness here gives every HTTP request an INDEPENDENT session (mirroring the
production `get_session`: autobegin, rollback-on-close unless the endpoint
commits). Assertions then open a SEPARATE fresh session to read only what was
actually committed to the database.

Requires the `ems_test` Postgres (same DB as the integration suite).
"""
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.config import get_settings
from src.models import Base
# import every model so create_all builds all tables (mirrors conftest)
from src.tenancy.organizations import models as _o  # noqa: F401
from src.tenancy.sites import models as _s  # noqa: F401
from src.tenancy.gateways import models as _g  # noqa: F401
from src.tenancy.bess_units import models as _b  # noqa: F401
from src.auth import models as _a  # noqa: F401
from src.rbac import models as _r  # noqa: F401
from src.signals import models as _sig  # noqa: F401
from src.ingestion import models as _ing  # noqa: F401

from src.auth.models import OtpChallengeModel, UserModel
from src.auth.otp_provider import OtpDeliveryProvider, MockOtpProvider, get_otp_provider
from src.database import get_session
from src.main import app
from src.rbac.models import RoleModel, UserRoleModel

TEST_DB_URL = make_url(get_settings().database_url).set(database="ems_test")
assert TEST_DB_URL.database == "ems_test", f"refusing non-test DB: {TEST_DB_URL.database}"

PHONE = "919000000001"


async def _seed_org_admin(maker, phone: str = PHONE):
    """Create an active org_admin user with a phone, committed via its own session."""
    async with maker() as s:
        role = RoleModel(name="org_admin")
        user = UserModel(phone=phone)
        s.add_all([role, user])
        await s.flush()
        s.add(UserRoleModel(user_id=user.id, role_id=role.id))
        await s.commit()
        return user.id


async def _challenges_for(maker, user_id):
    """Read OTP challenges for a user from a SEPARATE session (committed rows only)."""
    async with maker() as s:
        rows = (await s.execute(
            select(OtpChallengeModel).where(OtpChallengeModel.user_id == user_id)
        )).scalars().all()
        return rows


@pytest_asyncio.fixture
async def otp_env():
    """Fresh engine + schema; every request gets its own session (like production)."""
    engine = create_async_engine(TEST_DB_URL)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)

    async def _fresh_session():
        # mirrors src.database.get_session: no commit here; the endpoint commits.
        async with maker() as s:
            yield s

    app.dependency_overrides[get_session] = _fresh_session
    try:
        yield maker
    finally:
        app.dependency_overrides.clear()
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
        await engine.dispose()


def _wrong(code: str) -> str:
    """A 6-digit code guaranteed different from `code`."""
    first = "1" if code[0] == "0" else "0"
    return first + code[1:]


@pytest.mark.asyncio
async def test_challenge_persists_across_requests(otp_env):
    """T1: a challenge created in /otp/request is committed and visible to a
    separate session, so a later /otp/verify (its own session) can consume it."""
    maker = otp_env
    user_id = await _seed_org_admin(maker)
    mock = MockOtpProvider()
    app.dependency_overrides[get_otp_provider] = lambda: mock

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        r1 = await c.post("/api/v1/auth/otp/request", json={"identifier": PHONE})
        assert r1.status_code == 200, r1.text

        rows = await _challenges_for(maker, user_id)
        assert len(rows) == 1, "challenge was not committed to the DB"

        code = mock.sent[-1][2]
        r2 = await c.post("/api/v1/auth/otp/verify", json={"identifier": PHONE, "otp": code})
        assert r2.status_code == 200, r2.text
        assert "access_token" in r2.json()


@pytest.mark.asyncio
async def test_failed_attempt_persists_across_requests(otp_env):
    """T2: a wrong-code /otp/verify commits the incremented attempt counter,
    visible to a separate session."""
    maker = otp_env
    user_id = await _seed_org_admin(maker)
    mock = MockOtpProvider()
    app.dependency_overrides[get_otp_provider] = lambda: mock

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        r1 = await c.post("/api/v1/auth/otp/request", json={"identifier": PHONE})
        assert r1.status_code == 200, r1.text
        code = mock.sent[-1][2]

        r2 = await c.post("/api/v1/auth/otp/verify",
                          json={"identifier": PHONE, "otp": _wrong(code)})
        assert r2.status_code == 401, r2.text

    rows = await _challenges_for(maker, user_id)
    assert len(rows) == 1
    assert rows[0].attempts == 1, "failed attempt was not persisted"
    assert rows[0].consumed_at is None


@pytest.mark.asyncio
async def test_single_use_across_requests(otp_env):
    """T3: a successful /otp/verify commits consumption; a second verify with the
    same code (its own session) is rejected."""
    maker = otp_env
    user_id = await _seed_org_admin(maker)
    mock = MockOtpProvider()
    app.dependency_overrides[get_otp_provider] = lambda: mock

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        await c.post("/api/v1/auth/otp/request", json={"identifier": PHONE})
        code = mock.sent[-1][2]

        first = await c.post("/api/v1/auth/otp/verify", json={"identifier": PHONE, "otp": code})
        assert first.status_code == 200, first.text

        second = await c.post("/api/v1/auth/otp/verify", json={"identifier": PHONE, "otp": code})
        assert second.status_code == 401, "consumed challenge was reusable"

    rows = await _challenges_for(maker, user_id)
    assert len(rows) == 1
    assert rows[0].consumed_at is not None, "consumption was not persisted"


@pytest.mark.asyncio
async def test_sms_sent_only_after_challenge_committed(otp_env):
    """T5: the SMS send runs only after the challenge is committed, and the row is
    visible to a wholly separate session at send time."""
    maker = otp_env
    user_id = await _seed_org_admin(maker)

    class _VisibilityProbeProvider(OtpDeliveryProvider):
        def __init__(self, maker):
            self._maker = maker
            self.visible_at_send = None
            self.sent = []

        async def send_sms(self, mobile, code):
            # a SEPARATE session must already see the committed challenge
            async with self._maker() as s:
                rows = (await s.execute(select(OtpChallengeModel))).scalars().all()
                self.visible_at_send = len(rows) >= 1
            self.sent.append(("sms", mobile, code))

        async def send_email(self, email, code):  # pragma: no cover - unused
            self.sent.append(("email", email, code))

    probe = _VisibilityProbeProvider(maker)
    app.dependency_overrides[get_otp_provider] = lambda: probe

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        r1 = await c.post("/api/v1/auth/otp/request", json={"identifier": PHONE})
        assert r1.status_code == 200, r1.text

    assert probe.sent, "send_sms was never called"
    assert probe.visible_at_send is True, "challenge was not committed before send_sms ran"
    assert len(await _challenges_for(maker, user_id)) == 1


@pytest.mark.asyncio
async def test_precommit_error_rolls_back_challenge(otp_env, monkeypatch):
    """T4: if the request fails AFTER the challenge is flushed but BEFORE commit,
    the flushed row is rolled back on session close (no orphan challenge)."""
    maker = otp_env
    user_id = await _seed_org_admin(maker)

    import src.auth.router as router_mod
    real_create = router_mod.create_challenge

    async def _flush_then_fail(session, **kwargs):
        # perform the real add+flush, then fail before the endpoint's commit()
        await real_create(session, **kwargs)
        raise RuntimeError("boom before commit")

    monkeypatch.setattr(router_mod, "create_challenge", _flush_then_fail)

    mock = MockOtpProvider()
    app.dependency_overrides[get_otp_provider] = lambda: mock

    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        r = await c.post("/api/v1/auth/otp/request", json={"identifier": PHONE})
        assert r.status_code == 500, r.text

    rows = await _challenges_for(maker, user_id)
    assert rows == [], "flushed-but-uncommitted challenge was not rolled back"
    assert mock.sent == [], "send_sms must not run when creation failed"
