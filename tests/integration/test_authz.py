from sqlalchemy import select

from src.auth.models import UserModel
from src.rbac.models import RoleModel, UserRoleModel, UserSiteAccessModel
from src.rbac.permissions import PERMISSIONS
from src.rbac.service import (
    get_gateway_site,
    get_user_permissions,
    get_user_sites,
    seed_org_permissions,
    seed_roles_permissions,
)
from src.tenancy.gateways.models import GatewayModel
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel


async def get_role(session, name):
    result = await session.execute(select(RoleModel).where(RoleModel.name == name))
    return result.scalar_one()


async def make_user(session, role_name, email, org_id=None):
    user = UserModel(email=email, organization_id=org_id)
    session.add(user)
    await session.flush()
    role = await get_role(session, role_name)
    session.add(UserRoleModel(user_id=user.id, role_id=role.id))
    await session.flush()
    return user


async def test_super_admin_gets_everything(session):
    await seed_roles_permissions(session)
    await session.flush()
    user = await make_user(session, "super_admin", "root@x.com")

    assert await get_user_permissions(session, user) == set(PERMISSIONS)
    assert await get_user_sites(session, user) is None


async def test_org_admin_limited_by_org(session):
    await seed_roles_permissions(session)
    await session.flush()
    org = OrganizationModel(org_key="o1", name="Org 1")
    session.add(org)
    await session.flush()
    await seed_org_permissions(session, org.id)
    await session.flush()
    user = await make_user(session, "org_admin", "admin@o1.com", org.id)

    permissions = await get_user_permissions(session, user)
    assert "telemetry:read" in permissions
    assert "command:write" not in permissions


async def test_missing_org_permissions_denies(session):
    await seed_roles_permissions(session)
    await session.flush()
    org = OrganizationModel(org_key="o2", name="Org 2")
    session.add(org)
    await session.flush()
    user = await make_user(session, "org_admin", "admin@o2.com", org.id)

    assert await get_user_permissions(session, user) == set()


async def test_operator_only_sees_assigned_site(session):
    await seed_roles_permissions(session)
    await session.flush()
    org = OrganizationModel(org_key="o3", name="Org 3")
    session.add(org)
    await session.flush()

    site_a = SiteModel(organization_id=org.id, site_key="A", name="Site A")
    site_b = SiteModel(organization_id=org.id, site_key="B", name="Site B")
    session.add_all([site_a, site_b])
    await session.flush()

    gateway = GatewayModel(
        site_id=site_b.id, gateway_key="gw-b",
        client_id="c", client_secret_hash="h", enabled=True,
    )
    session.add(gateway)
    await session.flush()

    user = await make_user(session, "site_operator", "op@o3.com", org.id)
    session.add(UserSiteAccessModel(user_id=user.id, site_id=site_a.id))
    await session.flush()

    sites = await get_user_sites(session, user)
    assert sites == {site_a.id}

    gateway_info = await get_gateway_site(session, gateway.id)
    assert gateway_info[0] == site_b.id
    assert site_b.id not in sites