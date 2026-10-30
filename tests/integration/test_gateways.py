from sqlalchemy import select

from src.auth.models import AdminAuditModel, UserModel
from src.auth.security import create_access_token
from src.rbac.models import OrganizationPermissionModel, RoleModel, UserRoleModel
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


async def enable_perm(session, org_id, perm):
    row = (await session.execute(
        select(OrganizationPermissionModel)
        .where(OrganizationPermissionModel.organization_id == org_id)
        .where(OrganizationPermissionModel.permission == perm)
    )).scalar_one()
    row.allowed = True
    await session.commit()


async def test_super_admin_registers_gateway(client, session):
    c, _ = client
    await seed_roles_permissions(session)
    await session.commit()
    sa = await make_super_admin(session, "9000000020")
    data = await onboard(c, sa, "gw1", "9900000020", ["s1"])
    site_id = data["sites"][0]["id"]

    r = await c.post(f"/api/v1/admin/sites/{site_id}/gateways",
                     json={"gateway_key": "GW-0001"}, headers=auth_header(sa))
    assert r.status_code == 201
    body = r.json()
    assert body["client_id"].startswith("gw_")
    assert body["client_secret"]          # returned once

    # list must NOT expose the secret
    rl = await c.get(f"/api/v1/admin/sites/{site_id}/gateways", headers=auth_header(sa))
    assert rl.status_code == 200
    assert "client_secret" not in rl.json()[0]


async def test_duplicate_gateway_key(client, session):
    c, _ = client
    await seed_roles_permissions(session)
    await session.commit()
    sa = await make_super_admin(session, "9000000021")
    data = await onboard(c, sa, "gw2", "9900000021", ["s1"])
    site_id = data["sites"][0]["id"]

    r1 = await c.post(f"/api/v1/admin/sites/{site_id}/gateways",
                      json={"gateway_key": "GW-DUP"}, headers=auth_header(sa))
    assert r1.status_code == 201
    r2 = await c.post(f"/api/v1/admin/sites/{site_id}/gateways",
                      json={"gateway_key": "GW-DUP"}, headers=auth_header(sa))
    assert r2.status_code == 409


async def test_invalid_site(client, session):
    c, _ = client
    await seed_roles_permissions(session)
    await session.commit()
    sa = await make_super_admin(session, "9000000022")
    import uuid
    r = await c.post(f"/api/v1/admin/sites/{uuid.uuid4()}/gateways",
                     json={"gateway_key": "GW-X"}, headers=auth_header(sa))
    assert r.status_code == 404


async def test_org_admin_blocked_by_default(client, session):
    c, _ = client
    await seed_roles_permissions(session)
    await session.commit()
    sa = await make_super_admin(session, "9000000023")
    data = await onboard(c, sa, "gw3", "9900000023", ["s1"])
    site_id = data["sites"][0]["id"]
    admin = await get_user(session, "9900000023")

    # site:manage is OFF for the org by default -> Gate A denies
    r = await c.post(f"/api/v1/admin/sites/{site_id}/gateways",
                     json={"gateway_key": "GW-ORG"}, headers=auth_header(admin))
    assert r.status_code == 403


async def test_org_admin_cannot_register_even_with_org_permission(client, session):
    c, _ = client
    await seed_roles_permissions(session)
    await session.commit()
    sa = await make_super_admin(session, "9000000024")
    a = await onboard(c, sa, "orgA", "9900000024", ["a1"])
    b = await onboard(c, sa, "orgB", "9900000025", ["b1"])
    a_site = a["sites"][0]["id"]
    b_site = b["sites"][0]["id"]

    admin_a = await get_user(session, "9900000024")
    # even with the org permission switched on, org_admin's ROLE has no site:manage
    await enable_perm(session, admin_a.organization_id, "site:manage")
    r = await c.post(f"/api/v1/admin/sites/{a_site}/gateways",
                     json={"gateway_key": "GW-A"}, headers=auth_header(admin_a))
    assert r.status_code == 403

    # super admin can register gateways across organizations
    r1 = await c.post(f"/api/v1/admin/sites/{a_site}/gateways",
                      json={"gateway_key": "GW-A"}, headers=auth_header(sa))
    r2 = await c.post(f"/api/v1/admin/sites/{b_site}/gateways",
                      json={"gateway_key": "GW-B"}, headers=auth_header(sa))
    assert r1.status_code == 201
    assert r2.status_code == 201


async def test_operator_cannot_register(client, session):
    c, _ = client
    await seed_roles_permissions(session)
    await session.commit()
    sa = await make_super_admin(session, "9000000026")
    data = await onboard(c, sa, "gw4", "9900000026", ["s1"])
    site_id = data["sites"][0]["id"]
    admin = await get_user(session, "9900000026")

    await c.post("/api/v1/org/operators",
                 json={"phone": "9700000050", "site_ids": [site_id]}, headers=auth_header(admin))
    operator = await get_user(session, "9700000050")

    r = await c.post(f"/api/v1/admin/sites/{site_id}/gateways",
                     json={"gateway_key": "GW-OP"}, headers=auth_header(operator))
    assert r.status_code == 403


async def test_gateway_registration_audited(client, session):
    c, _ = client
    await seed_roles_permissions(session)
    await session.commit()
    sa = await make_super_admin(session, "9000000027")
    data = await onboard(c, sa, "gw5", "9900000027", ["s1"])
    site_id = data["sites"][0]["id"]

    await c.post(f"/api/v1/admin/sites/{site_id}/gateways",
                 json={"gateway_key": "GW-AUD"}, headers=auth_header(sa))

    rows = (await session.execute(
        select(AdminAuditModel).where(AdminAuditModel.action == "gateway_registered")
    )).scalars().all()
    assert len(rows) == 1
    assert rows[0].new_value["gateway_key"] == "GW-AUD"

async def test_list_exposes_last_seen_at_null_then_populated(client, session):
    c, _ = client
    await seed_roles_permissions(session)
    await session.commit()
    sa = await make_super_admin(session, "9000000028")
    data = await onboard(c, sa, "gw6", "9900000028", ["s1"])
    site_id = data["sites"][0]["id"]

    reg = await c.post(f"/api/v1/admin/sites/{site_id}/gateways",
                       json={"gateway_key": "GW-LS"}, headers=auth_header(sa))
    assert reg.status_code == 201
    client_id = reg.json()["client_id"]
    secret = reg.json()["client_secret"]

    # before first token issuance -> field present and NULL (select the row by unique client_id)
    rl = await c.get(f"/api/v1/admin/sites/{site_id}/gateways", headers=auth_header(sa))
    assert rl.status_code == 200
    row = next(r for r in rl.json() if r["client_id"] == client_id)
    assert "last_seen_at" in row and row["last_seen_at"] is None

    # successful token issuance records the heartbeat
    rt = await c.post("/api/v1/gateway/token",
                      json={"client_id": client_id, "client_secret": secret})
    assert rt.status_code == 200

    rl2 = await c.get(f"/api/v1/admin/sites/{site_id}/gateways", headers=auth_header(sa))
    assert rl2.status_code == 200
    row2 = next(r for r in rl2.json() if r["client_id"] == client_id)
    assert row2["last_seen_at"] is not None
