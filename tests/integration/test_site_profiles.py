"""Site Config registration API (super-admin only).

Pins the contract of /api/v1/site-profiles: super admins upsert and list profiles, the upsert is
idempotent per site, and any non-super-admin is rejected. The feature is additive, so these never
touch the tenancy tables beyond the site they reference.
"""
import uuid

from sqlalchemy import select

from src.auth.security import create_access_token
from src.auth.models import UserModel
from src.rbac.models import RoleModel, UserRoleModel, UserSiteAccessModel
from src.rbac.service import seed_roles_permissions
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel

BASE = "/api/v1/site-profiles"


async def _org(session, key):
    o = OrganizationModel(org_key=key, name=key); session.add(o); await session.flush()
    return o


async def _site(session, org, key):
    s = SiteModel(organization_id=org.id, site_key=key, name=key); session.add(s); await session.flush()
    return s


async def _user(session, role, email, org_id=None, sites=()):
    u = UserModel(email=email, organization_id=org_id, is_active=True); session.add(u); await session.flush()
    r = (await session.execute(select(RoleModel).where(RoleModel.name == role))).scalar_one()
    session.add(UserRoleModel(user_id=u.id, role_id=r.id))
    for s in sites:
        session.add(UserSiteAccessModel(user_id=u.id, site_id=s.id))
    await session.commit()
    return u


def _auth(u):
    return {"Authorization": f"Bearer {create_access_token(u.id)}"}


def _payload():
    return {
        "company_name": "Acme Power", "address": "Plot 9, Noida",
        "latitude": 28.57, "longitude": 77.32,
        "solar_capacity_kw": 500.0, "connection_voltage_kv": 11.0,
        "battery_capacity_kwh": 261.0, "pcs_capacity_kw": 125.0,
        "comm_pcs_ems": True, "comm_battery_ems": True,
    }


async def test_super_admin_upsert_and_get(client, session):
    c, _ = client
    await seed_roles_permissions(session); await session.flush()
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    sa = await _user(session, "super_admin", "root@sys")

    r = await c.post(f"{BASE}/{site.id}", json=_payload(), headers=_auth(sa))
    assert r.status_code == 200
    prof = r.json()["profile"]
    assert prof["company_name"] == "Acme Power"
    assert prof["comm_pcs_ems"] is True and prof["comm_pcs_battery"] is False
    assert prof["updated_by"] == "root@sys"

    g = await c.get(f"{BASE}/{site.id}", headers=_auth(sa))
    assert g.status_code == 200
    assert g.json()["profile"]["pcs_capacity_kw"] == 125.0


async def test_upsert_is_idempotent_per_site(client, session):
    c, _ = client
    await seed_roles_permissions(session); await session.flush()
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    sa = await _user(session, "super_admin", "root@sys")

    await c.post(f"{BASE}/{site.id}", json=_payload(), headers=_auth(sa))
    await c.post(f"{BASE}/{site.id}", json={**_payload(), "company_name": "Renamed"}, headers=_auth(sa))

    lst = await c.get(BASE, headers=_auth(sa))
    rows = [p for p in lst.json() if p["site_id"] == str(site.id)]
    assert len(rows) == 1 and rows[0]["company_name"] == "Renamed"


async def test_missing_profile_returns_null(client, session):
    c, _ = client
    await seed_roles_permissions(session); await session.flush()
    sa = await _user(session, "super_admin", "root@sys")
    g = await c.get(f"{BASE}/{uuid.uuid4()}", headers=_auth(sa))
    assert g.status_code == 200 and g.json()["profile"] is None


async def test_non_super_admin_forbidden(client, session):
    c, _ = client
    await seed_roles_permissions(session); await session.flush()
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    op = await _user(session, "site_operator", "op@o1", org.id, sites=[site])

    assert (await c.get(BASE, headers=_auth(op))).status_code == 403
    assert (await c.post(f"{BASE}/{site.id}", json=_payload(), headers=_auth(op))).status_code == 403


async def test_requires_auth(client, session):
    c, _ = client
    r = await c.get(BASE)
    assert r.status_code == 401
