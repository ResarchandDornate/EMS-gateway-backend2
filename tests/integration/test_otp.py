from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from src.auth.models import OtpChallengeModel, UserModel
from src.auth.otp import create_challenge, verify_code
from src.auth.otp_provider import MockOtpProvider


async def _user(session):
    u = UserModel(phone="919999999999")
    session.add(u)
    await session.flush()
    return u


async def test_create_and_verify(session):
    u = await _user(session)
    code = await create_challenge(session, user_id=u.id, channel="sms", destination=u.phone)
    assert len(code) == 6 and code.isdigit()
    assert await verify_code(session, user_id=u.id, code=code) is True


async def test_wrong_code(session):
    u = await _user(session)
    await create_challenge(session, user_id=u.id, channel="sms", destination=u.phone)
    assert await verify_code(session, user_id=u.id, code="000000") is False


async def test_single_use(session):
    u = await _user(session)
    code = await create_challenge(session, user_id=u.id, channel="sms", destination=u.phone)
    assert await verify_code(session, user_id=u.id, code=code) is True
    assert await verify_code(session, user_id=u.id, code=code) is False


async def test_expired(session):
    u = await _user(session)
    code = await create_challenge(session, user_id=u.id, channel="sms", destination=u.phone)
    ch = (await session.execute(select(OtpChallengeModel))).scalars().first()
    ch.expires_at = datetime.now(timezone.utc) - timedelta(minutes=1)
    await session.flush()
    assert await verify_code(session, user_id=u.id, code=code) is False


async def test_mock_provider():
    p = MockOtpProvider()
    await p.send_sms("919999999999", "123456")
    assert ("sms", "919999999999", "123456") in p.sent