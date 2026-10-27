"""Slice 2 - tenancy onboarding hardening tests.

Isolation (verified in tests/integration/conftest.py): the `session` fixture builds a
fresh engine per test, runs Base.metadata.create_all at setup and drop_all + dispose at
teardown, so committed helper rows are wiped between tests.

Route-level coverage (end-to-end 409 mapping): org_key (pre-check), site intra-request
dup (router dedup + atomicity), gateway_key (pre-check), operator duplicate-email net
(real 23505 -> exact users_email_key -> 409, distinct from phone).
Helper-level coverage (SQLSTATE + constraint-name extraction): a real 23505 violation.
Concurrency race: documented skip (not a flaky test).

Policy: org_key/site_key trim+lowercase; gateway_key trim-only/case-preserving;
client_id untouched; display names trim-only (not unique).
"""
import uuid

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from src.accounts import service
from src.auth.models import UserModel
from src.auth.security import create_access_token
from src.rbac.models import RoleModel, UserRoleModel
from src.rbac.service import seed_roles_permissions
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel


def _auth(user):
    return {"Authorization": f"Bearer {create_access_token(user.id)}"}


async def _super_admin(session, phone):
    user = UserModel(phone=phone)
    session.add(user)
    await session.flush()
    role = (await session.execute(
        select(RoleModel).where(RoleModel.name == "super_admin"))).scalar_one()
    session.add(UserRoleModel(user_id=user.id, role_id=role.id))
    await session.commit()
    return user


async def _get_user(session, phone):
    return (await session.execute(
        select(UserModel).where(UserModel.phone == phone))).scalar_one()


async def _post_org(c, sa, *, org_key, phone, sites=None, name=None, admin_email=None):
    body = {"org_key": org_key, "name": name or org_key, "admin_phone": phone,
            "admin_email": admin_email,
            "sites": [{"site_key": k, "name": k} for k in (sites or [])]}
    return await c.post("/api/v1/admin/organizations", json=body, headers=_auth(sa))


async def _onboard_site(c, sa, *, org_key, phone, site_key="s1"):
    r = await _post_org(c, sa, org_key=org_key, phone=phone, sites=[site_key])
    assert r.status_code == 201
    return r.json()["sites"][0]["id"]


# ------------------------------------------------------------------ org_key (route-level, pre-check)

async def test_org_key_case_variant_rejected_409(client, session):
    c, _ = client
    await seed_roles_permissions(session); await session.commit()
    sa = await _super_admin(session, "9000010001")
    r1 = await _post_org(c, sa, org_key="acme", phone="9900010001")
    r2 = await _post_org(c, sa, org_key="ACME", phone="9900010002", name="Acme Caps")
    assert r1.status_code == 201
    assert r2.status_code == 409
    keys = set((await session.execute(select(OrganizationModel.org_key))).scalars())
    assert keys == {"acme"}


async def test_org_key_whitespace_variant_rejected_409(client, session):
    c, _ = client
    await seed_roles_permissions(session); await session.commit()
    sa = await _super_admin(session, "9000010003")
    r1 = await _post_org(c, sa, org_key="beta", phone="9900010003")
    r2 = await _post_org(c, sa, org_key="  beta  ", phone="9900010004", name="Beta Spaced")
    assert r1.status_code == 201
    assert r2.status_code == 409
    keys = set((await session.execute(select(OrganizationModel.org_key))).scalars())
    assert keys == {"beta"}


async def test_org_key_exact_duplicate_returns_409_OK(client, session):
    c, _ = client
    await seed_roles_permissions(session); await session.commit()
    sa = await _super_admin(session, "9000010005")
    r1 = await _post_org(c, sa, org_key="gamma", phone="9900010005")
    r2 = await _post_org(c, sa, org_key="gamma", phone="9900010006", name="Gamma Two")
    assert r1.status_code == 201
    assert r2.status_code == 409


# ------------------------------------------------------------------ site_key intra-request + atomicity

async def test_site_key_case_variant_in_request_rejected_409(client, session):
    c, _ = client
    await seed_roles_permissions(session); await session.commit()
    sa = await _super_admin(session, "9000010007")
    r = await _post_org(c, sa, org_key="org-sc", phone="9900010007", sites=["s1", "S1"])
    assert r.status_code == 409


async def test_site_key_whitespace_variant_in_request_rejected_409(client, session):
    c, _ = client
    await seed_roles_permissions(session); await session.commit()
    sa = await _super_admin(session, "9000010008")
    r = await _post_org(c, sa, org_key="org-sw", phone="9900010008", sites=["s1", "  s1  "])
    assert r.status_code == 409


async def test_duplicate_site_key_single_request_rejected_409_atomic(client, session):
    # 409 AND nothing from THIS request persisted, scoped to its own organization:
    # no org row, no admin user, and no site rows belonging to that org (join on org_key,
    # since site_key is unique per-org, not globally).
    c, _ = client
    await seed_roles_permissions(session); await session.commit()
    sa = await _super_admin(session, "9000010009")
    r = await _post_org(c, sa, org_key="org-dup", phone="9900010009", sites=["s1", "s1"])
    assert r.status_code == 409
    org = (await session.execute(
        select(OrganizationModel).where(OrganizationModel.org_key == "org-dup"))).scalar_one_or_none()
    admin = (await session.execute(
        select(UserModel).where(UserModel.phone == "9900010009"))).scalar_one_or_none()
    site_n = (await session.execute(
        select(func.count())
        .select_from(SiteModel)
        .join(OrganizationModel, OrganizationModel.id == SiteModel.organization_id)
        .where(OrganizationModel.org_key == "org-dup"))).scalar()
    assert org is None and admin is None and site_n == 0


# ------------------------------------------------------------------ gateway_key (route-level)

async def test_gateway_key_case_variant_coexist_EXPECTED(client, session):
    c, _ = client
    await seed_roles_permissions(session); await session.commit()
    sa = await _super_admin(session, "9000010010")
    site_id = await _onboard_site(c, sa, org_key="org-gc", phone="9900010010")
    r1 = await c.post(f"/api/v1/admin/sites/{site_id}/gateways",
                      json={"gateway_key": "GW-1"}, headers=_auth(sa))
    r2 = await c.post(f"/api/v1/admin/sites/{site_id}/gateways",
                      json={"gateway_key": "gw-1"}, headers=_auth(sa))
    assert r1.status_code == 201
    assert r2.status_code == 201                          # distinct by case (preserved)


async def test_gateway_key_whitespace_variant_rejected_409(client, session):
    c, _ = client
    await seed_roles_permissions(session); await session.commit()
    sa = await _super_admin(session, "9000010011")
    site_id = await _onboard_site(c, sa, org_key="org-gw", phone="9900010011")
    r1 = await c.post(f"/api/v1/admin/sites/{site_id}/gateways",
                      json={"gateway_key": "GW-2"}, headers=_auth(sa))
    r2 = await c.post(f"/api/v1/admin/sites/{site_id}/gateways",
                      json={"gateway_key": "  GW-2  "}, headers=_auth(sa))
    assert r1.status_code == 201
    assert r2.status_code == 409


async def test_gateway_key_exact_duplicate_returns_409_OK(client, session):
    c, _ = client
    await seed_roles_permissions(session); await session.commit()
    sa = await _super_admin(session, "9000010012")
    site_id = await _onboard_site(c, sa, org_key="org-ge", phone="9900010012")
    r1 = await c.post(f"/api/v1/admin/sites/{site_id}/gateways",
                      json={"gateway_key": "GW-DUP"}, headers=_auth(sa))
    r2 = await c.post(f"/api/v1/admin/sites/{site_id}/gateways",
                      json={"gateway_key": "GW-DUP"}, headers=_auth(sa))
    assert r1.status_code == 201
    assert r2.status_code == 409


# ------------------------------------------------------------------ IntegrityError net, route-level: email vs phone

async def test_operator_duplicate_email_maps_to_email_409(client, session):
    # Email has NO pre-check, so a duplicate email reaches DB UNIQUE(email); the net must
    # map it to the EMAIL response via the exact constraint name (never phone).
    c, _ = client
    await seed_roles_permissions(session); await session.commit()
    sa = await _super_admin(session, "9000010013")
    site_id = await _onboard_site(c, sa, org_key="org-op", phone="9900010013")
    admin = await _get_user(session, "9900010013")
    r1 = await c.post("/api/v1/org/operators",
                      json={"phone": "9700000061", "email": "dup@x.com", "site_ids": [site_id]},
                      headers=_auth(admin))
    assert r1.status_code == 201
    r2 = await c.post("/api/v1/org/operators",
                      json={"phone": "9700000062", "email": "dup@x.com", "site_ids": [site_id]},
                      headers=_auth(admin))
    assert r2.status_code == 409
    assert "Email" in r2.json()["detail"]


# ------------------------------------------------------------------ helper-level: SQLSTATE + constraint name

async def test_unique_violation_helpers_on_real_db(session):
    # UNVERIFIED until run locally: asserts that the exact constraint name is recoverable
    # from the asyncpg exception cause-chain on THIS stack. If the `"org_key" in name`
    # assertion fails here, constraint-name extraction is not reliable and the gateway/
    # operator paths must fall back to re-raise-only (no guessing) per the agreed plan.
    session.add(OrganizationModel(org_key="dupe", name="A"))
    await session.commit()
    session.add(OrganizationModel(org_key="dupe", name="B"))
    with pytest.raises(IntegrityError) as ei:
        await session.flush()
    await session.rollback()
    assert service._is_unique_violation(ei.value) is True
    name = service._unique_constraint_name(ei.value)
    assert name is not None and "org_key" in name       # exact name recovered from the chain


# ------------------------------------------------------------------ documented concurrency limitation

@pytest.mark.skip(reason="LIMITATION: a true concurrent two-request TOCTOU race is not "
                         "deterministically reproducible in the single-event-loop "
                         "ASGITransport + shared-session setup. The IntegrityError->409 "
                         "net is covered by the operator email test and the helper test.")
async def test_concurrent_duplicate_org_key_race_500_LIMITATION():
    pass
