from sqlalchemy import select

from src.auth.models import UserModel
from src.auth.security import create_access_token
from src.rbac.models import RoleModel, UserRoleModel
from src.rbac.service import seed_roles_permissions


def auth_header(user):
    return {"Authorization": f"Bearer {create_access_token(user.id)}"}


async def make_super_admin(session, phone):
    user = UserModel(phone=phone)
    session.add(user)
    await session.flush()
    role = (await session.execute(
        select(RoleModel).where(RoleModel.name == "super_admin")
    )).scalar_one()
    session.add(UserRoleModel(user_id=user.id, role_id=role.id))
    await session.commit()
    return user


async def onboard(c, sa, org_key, admin_phone, site_keys):
    body = {
        "org_key": org_key, "name": org_key, "admin_phone": admin_phone,
        "sites": [{"site_key": k, "name": k} for k in site_keys],
    }
    r = await c.post("/api/v1/admin/organizations", json=body, headers=auth_header(sa))
    assert r.status_code == 201
    return r.json()


async def get_user(session, phone):
    return (await session.execute(
        select(UserModel).where(UserModel.phone == phone)
    )).scalar_one()


async def test_two_operator_limit(client, session):
    c, _ = client
    await seed_roles_permissions(session)
    await session.commit()
    sa = await make_super_admin(session, "9000000010")
    data = await onboard(c, sa, "cap", "9900000001", ["s1", "s2"])
    site_id = data["sites"][0]["id"]
    admin = await get_user(session, "9900000001")
    h = auth_header(admin)

    r1 = await c.post("/api/v1/org/operators", json={"phone": "9700000001", "site_ids": [site_id]}, headers=h)
    r2 = await c.post("/api/v1/org/operators", json={"phone": "9700000002", "site_ids": [site_id]}, headers=h)
    assert r1.status_code == 201
    assert r2.status_code == 201

    r3 = await c.post("/api/v1/org/operators", json={"phone": "9700000003", "site_ids": [site_id]}, headers=h)
    assert r3.status_code == 409


async def test_deactivate_frees_slot(client, session):
    c, _ = client
    await seed_roles_permissions(session)
    await session.commit()
    sa = await make_super_admin(session, "9000000011")
    data = await onboard(c, sa, "cap2", "9900000002", ["s1"])
    site_id = data["sites"][0]["id"]
    admin = await get_user(session, "9900000002")
    h = auth_header(admin)

    await c.post("/api/v1/org/operators", json={"phone": "9700000011", "site_ids": [site_id]}, headers=h)
    await c.post("/api/v1/org/operators", json={"phone": "9700000012", "site_ids": [site_id]}, headers=h)

    r = await c.post("/api/v1/org/operators", json={"phone": "9700000013", "site_ids": [site_id]}, headers=h)
    assert r.status_code == 409

    op = await get_user(session, "9700000011")
    rd = await c.post(f"/api/v1/org/operators/{op.id}/deactivate", headers=h)
    assert rd.status_code == 200

    r2 = await c.post("/api/v1/org/operators", json={"phone": "9700000013", "site_ids": [site_id]}, headers=h)
    assert r2.status_code == 201


async def test_cannot_assign_other_org_site(client, session):
    c, _ = client
    await seed_roles_permissions(session)
    await session.commit()
    sa = await make_super_admin(session, "9000000012")
    await onboard(c, sa, "orgA", "9900000003", ["a1"])
    b = await onboard(c, sa, "orgB", "9900000004", ["b1"])
    b_site = b["sites"][0]["id"]
    admin_a = await get_user(session, "9900000003")

    r = await c.post("/api/v1/org/operators",
                     json={"phone": "9700000020", "site_ids": [b_site]},
                     headers=auth_header(admin_a))
    assert r.status_code == 400


async def test_operator_cannot_manage_operators(client, session):
    c, _ = client
    await seed_roles_permissions(session)
    await session.commit()
    sa = await make_super_admin(session, "9000000013")
    data = await onboard(c, sa, "orgC", "9900000005", ["c1"])
    site_id = data["sites"][0]["id"]
    admin = await get_user(session, "9900000005")

    await c.post("/api/v1/org/operators", json={"phone": "9700000030", "site_ids": [site_id]}, headers=auth_header(admin))
    operator = await get_user(session, "9700000030")

    r = await c.post("/api/v1/org/operators", json={"phone": "9700000031", "site_ids": [site_id]}, headers=auth_header(operator))
    assert r.status_code == 403


async def test_super_admin_has_no_operator_context(client, session):
    c, _ = client
    await seed_roles_permissions(session)
    await session.commit()
    sa = await make_super_admin(session, "9000000014")

    r = await c.post("/api/v1/org/operators", json={"phone": "9700000040", "site_ids": []}, headers=auth_header(sa))
    assert r.status_code == 403