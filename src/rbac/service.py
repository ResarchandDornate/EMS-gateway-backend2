from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.rbac.models import (OrganizationPermissionModel, PermissionModel,
                             RoleModel, RolePermissionModel)
from src.rbac.permissions import (DEFAULT_ORG_PERMISSION_ALLOWED, PERMISSIONS,
                                  ROLE_PERMISSIONS, ROLES)
from src.tenancy.organizations.models import OrganizationModel


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