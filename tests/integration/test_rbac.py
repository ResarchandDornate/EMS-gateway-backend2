import pytest
from sqlalchemy.exc import IntegrityError

from src.auth.models import UserModel
from src.rbac.models import (RoleModel, UserRoleModel,
                             OrganizationPermissionModel)
from src.tenancy.organizations.models import OrganizationModel


async def test_user_org_nullable(session):
    # super_admin has no organization
    u = UserModel(email="admin@x.com", organization_id=None)
    session.add(u)
    await session.flush()
    assert u.organization_id is None


async def test_user_role_composite_pk_blocks_duplicate(session):
    u = UserModel(email="a@x.com")
    r = RoleModel(name="org_admin")
    session.add_all([u, r])
    await session.flush()
    session.add(UserRoleModel(user_id=u.id, role_id=r.id))
    await session.flush()
    # same pair again violates the composite primary key
    session.add(UserRoleModel(user_id=u.id, role_id=r.id))
    with pytest.raises(IntegrityError):
        await session.flush()


async def test_org_permission_unique(session):
    org = OrganizationModel(org_key="o1", name="O1")
    session.add(org)
    await session.flush()
    session.add(OrganizationPermissionModel(
        organization_id=org.id, permission="telemetry:read", allowed=True))
    await session.flush()
    # same (org, permission) again violates UniqueConstraint
    session.add(OrganizationPermissionModel(
        organization_id=org.id, permission="telemetry:read", allowed=False))
    with pytest.raises(IntegrityError):
        await session.flush()