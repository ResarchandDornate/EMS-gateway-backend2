from sqlalchemy import select

from src.auth.models import UserModel
from src.auth.security import create_access_token
from src.rbac.models import RoleModel, UserRoleModel
from src.rbac.service import seed_org_permissions, seed_roles_permissions
from src.tenancy.organizations.models import OrganizationModel


def auth_header(user):
    return {"Authorization": f"Bearer {create_access_token(user.id)}"}


async def make_user(session, role_name, phone, org_id=None):
    user = UserModel(phone=phone, organization_id=org_id)
    session.add(user)
    await session.flush()
    role = (await session.execute(
        select(RoleModel).where(RoleModel.name == role_name)
    )).scalar_one()
    session.add(UserRoleModel(user_id=user.id, role_id=role.id))
    await session.commit()
    return user


async def test_super_admin_onboards_client(client, session):
    c, _ = client
    await seed_roles_permissions(session)
    await session.commit()
    admin = await make_user(session, "super_admin", "9000000000")

    body = {
        "org_key": "acme",
        "name": "Acme Power",
        "admin_phone": "9811111111",
        "sites": [
            {"site_key": "site-1", "name": "Plant 1"},
            {"site_key": "site-2", "name": "Plant 2"},
        ],
    }
    r = await c.post("/api/v1/admin/organizations", json=body, headers=auth_header(admin))
    assert r.status_code == 201
    data = r.json()
    assert data["org_key"] == "acme"
    assert len(data["sites"]) == 2

    org = (await session.execute(
        select(OrganizationModel).where(OrganizationModel.org_key == "acme")
    )).scalar_one()
    client_admin = (await session.execute(
        select(UserModel).where(UserModel.phone == "9811111111")
    )).scalar_one()
    assert client_admin.organization_id == org.id


async def test_org_admin_cannot_onboard(client, session):
    c, _ = client
    await seed_roles_permissions(session)
    await session.commit()
    org = OrganizationModel(org_key="o", name="O")
    session.add(org)
    await session.flush()
    await seed_org_permissions(session, org.id)
    await session.commit()
    user = await make_user(session, "org_admin", "9822222222", org.id)

    body = {"org_key": "x", "name": "X", "admin_phone": "9833333333", "sites": []}
    r = await c.post("/api/v1/admin/organizations", json=body, headers=auth_header(user))
    assert r.status_code == 403


async def test_duplicate_phone_rejected(client, session):
    c, _ = client
    await seed_roles_permissions(session)
    await session.commit()
    admin = await make_user(session, "super_admin", "9000000001")

    body = {"org_key": "acme1", "name": "Acme", "admin_phone": "9844444444", "sites": []}
    r1 = await c.post("/api/v1/admin/organizations", json=body, headers=auth_header(admin))
    assert r1.status_code == 201

    body2 = {"org_key": "acme2", "name": "Acme2", "admin_phone": "9844444444", "sites": []}
    r2 = await c.post("/api/v1/admin/organizations", json=body2, headers=auth_header(admin))
    assert r2.status_code == 409


async def test_list_clients(client, session):
    c, _ = client
    await seed_roles_permissions(session)
    await session.commit()
    admin = await make_user(session, "super_admin", "9000000002")

    body = {
        "org_key": "acme3",
        "name": "Acme3",
        "admin_phone": "9855555555",
        "sites": [{"site_key": "s1", "name": "S1"}],
    }
    await c.post("/api/v1/admin/organizations", json=body, headers=auth_header(admin))

    r = await c.get("/api/v1/admin/organizations", headers=auth_header(admin))
    assert r.status_code == 200
    assert any(o["org_key"] == "acme3" for o in r.json())
