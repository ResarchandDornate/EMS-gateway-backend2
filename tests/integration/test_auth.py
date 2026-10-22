from src.auth.models import UserModel
from src.auth.security import hash_password
from src.auth.service import authenticate


async def test_authenticate(session):
    session.add(UserModel(email="a@x.com", password_hash=hash_password("pw")))
    await session.flush()
    assert await authenticate(session, "a@x.com", "pw") is not None
    assert await authenticate(session, "a@x.com", "bad") is None
    assert await authenticate(session, "nobody@x.com", "pw") is None


async def test_inactive_rejected(session):
    session.add(UserModel(email="b@x.com", password_hash=hash_password("pw"), is_active=False))
    await session.flush()
    assert await authenticate(session, "b@x.com", "pw") is None