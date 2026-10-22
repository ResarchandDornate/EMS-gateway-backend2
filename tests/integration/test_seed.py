from sqlalchemy import func, select

from src.rbac.models import (OrganizationPermissionModel, PermissionModel,
                             RoleModel, RolePermissionModel)
from src.rbac.permissions import PERMISSIONS, ROLE_PERMISSIONS, ROLES
from src.rbac.service import (backfill_org_permissions, seed_roles_permissions)
from src.tenancy.organizations.models import OrganizationModel


async def test_seed_is_idempotent(session):
    await seed_roles_permissions(session)
    await session.flush()
    await seed_roles_permissions(session)   # run twice
    await session.flush()

    roles = (await session.execute(select(func.count()).select_from(RoleModel))).scalar()
    perms = (await session.execute(select(func.count()).select_from(PermissionModel))).scalar()
    rp = (await session.execute(select(func.count()).select_from(RolePermissionModel))).scalar()

    assert roles == len(ROLES)
    assert perms == len(PERMISSIONS)
    assert rp == sum(len(v) for v in ROLE_PERMISSIONS.values())


async def test_backfill_fills_org_defaults(session):
    await seed_roles_permissions(session)
    org = OrganizationModel(org_key="o1", name="O1")
    session.add(org)
    await session.flush()

    added = await backfill_org_permissions(session)
    await session.flush()
    assert added == len(PERMISSIONS)

    # idempotent: second run adds nothing
    assert await backfill_org_permissions(session) == 0

    rows = {op.permission: op.allowed for op in (await session.execute(
        select(OrganizationPermissionModel).where(
            OrganizationPermissionModel.organization_id == org.id))).scalars()}
    assert rows["telemetry:read"] is True      # read allowed
    assert rows["command:write"] is False      # write denied by default