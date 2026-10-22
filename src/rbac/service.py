from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.rbac.models import (OrganizationPermissionModel, PermissionModel,RoleModel, RolePermissionModel)
from src.rbac.permissions import (DEFAULT_ORG_PERMISSION_ALLOWED, PERMISSIONS, ROLE_PERMISSIONS, ROLES)
from src.tenancy.organizations.models import OrganizationModel
from src.rbac.models import UserRoleModel, UserSiteAccessModel
from src.tenancy.gateways.models import GatewayModel
from src.tenancy.sites.models import SiteModel

async def seed_roles_permissions(session: AsyncSession) -> None:
    """Idempotent: creates roles, permissions, and role_permissions if missing."""
    roles = {r.name: r for r in (await session.execute(select(RoleModel))).scalars()}
    for name in ROLES:
        if name not in roles:
            roles[name] = RoleModel(name=name)
            session.add(roles[name])

    perms = {p.name: p for p in (await session.execute(select(PermissionModel))).scalars()}
    for name in PERMISSIONS:
        if name not in perms:
            perms[name] = PermissionModel(name=name)
            session.add(perms[name])

    await session.flush()  # assign ids

    existing = {(rp.role_id, rp.permission_id)
                for rp in (await session.execute(select(RolePermissionModel))).scalars()}
    for role_name, perm_names in ROLE_PERMISSIONS.items():
        for pname in perm_names:
            key = (roles[role_name].id, perms[pname].id)
            if key not in existing:
                session.add(RolePermissionModel(role_id=key[0], permission_id=key[1]))


async def seed_org_permissions(session: AsyncSession, organization_id) -> int:
    """Fill missing organization_permissions rows for one org at safe defaults."""
    existing = {op.permission for op in (await session.execute(
        select(OrganizationPermissionModel).where(
            OrganizationPermissionModel.organization_id == organization_id))).scalars()}
    added = 0
    for perm in PERMISSIONS:
        if perm not in existing:
            session.add(OrganizationPermissionModel(
                organization_id=organization_id,
                permission=perm,
                allowed=DEFAULT_ORG_PERMISSION_ALLOWED.get(perm, False)))
            added += 1
    return added


async def backfill_org_permissions(session: AsyncSession) -> int:
    """Run after adding a new permission, or over all orgs. Never overwrites existing rows."""
    orgs = (await session.execute(select(OrganizationModel))).scalars().all()
    total = 0
    for org in orgs:
        total += await seed_org_permissions(session, org.id)
    return total


async def get_user_roles(session, user_id):
    result = await session.execute(
        select(RoleModel.name)
        .join(UserRoleModel, UserRoleModel.role_id == RoleModel.id)
        .where(UserRoleModel.user_id == user_id)
    )
    return set(result.scalars())


async def get_user_permissions(session, user):
    roles = await get_user_roles(session, user.id)
    if "super_admin" in roles:
        return set(PERMISSIONS)

    result = await session.execute(
        select(PermissionModel.name)
        .join(RolePermissionModel, RolePermissionModel.permission_id == PermissionModel.id)
        .join(UserRoleModel, UserRoleModel.role_id == RolePermissionModel.role_id)
        .where(UserRoleModel.user_id == user.id)
    )
    role_permissions = set(result.scalars())

    if user.organization_id is None:
        return set()

    result = await session.execute(
        select(OrganizationPermissionModel.permission)
        .where(OrganizationPermissionModel.organization_id == user.organization_id)
        .where(OrganizationPermissionModel.allowed.is_(True))
    )
    org_permissions = set(result.scalars())

    return role_permissions & org_permissions


async def get_gateway_site(session, gateway_id):
    result = await session.execute(
        select(GatewayModel.site_id, SiteModel.organization_id)
        .join(SiteModel, SiteModel.id == GatewayModel.site_id)
        .where(GatewayModel.id == gateway_id)
    )
    return result.first()


async def get_user_sites(session, user):
    roles = await get_user_roles(session, user.id)

    if "super_admin" in roles:
        return None

    if "org_admin" in roles and user.organization_id is not None:
        result = await session.execute(
            select(SiteModel.id).where(SiteModel.organization_id == user.organization_id)
        )
        return set(result.scalars())

    result = await session.execute(
        select(UserSiteAccessModel.site_id).where(UserSiteAccessModel.user_id == user.id)
    )
    return set(result.scalars())