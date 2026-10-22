from src.auth.models import UserModel
from src.rbac.models import RoleModel, UserRoleModel


async def _org_admin(session, phone="919999999999"):
    role = RoleModel(name="org_admin")
    user = UserModel(phone=phone)
    session.add_all([role, user])
    await session.flush()
    session.add(UserRoleModel(user_id=user.id, role_id=role.id))
    await session.flush()
    return user


async def test_otp_login_flow(client, session):
    c, mock = client
    user = await _org_admin(session)

    r1 = await c.post("/api/v1/auth/otp/request", json={"identifier": user.phone})
    assert r1.status_code == 200 and r1.json()["status"] == "otp_sent"
    _, _, code = mock.sent[-1]

    r2 = await c.post("/api/v1/auth/otp/verify",
                      json={"identifier": user.phone, "otp": code})
    assert r2.status_code == 200 and "access_token" in r2.json()


async def test_unknown_identifier_errors(client, session):
    c, mock = client
    r = await c.post("/api/v1/auth/otp/request", json={"identifier": "910000000000"})
    assert r.status_code == 404
    assert r.json()["detail"] == "User not registered"
    assert mock.sent == []