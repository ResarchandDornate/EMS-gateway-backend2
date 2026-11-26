"""Automation config API (Strategy -> Automation editor).

Pins /api/v1/automation-config: a command:write holder on a reachable gateway's site saves the
schedule blob and reads it back, the upsert is idempotent per gateway and derives `enabled` from
the blob, and callers without command:write or site access are refused. Additive — one row per
gateway, read by nothing existing.
"""
import uuid

from sqlalchemy import select

from src.auth.security import create_access_token, hash_password
from src.auth.models import UserModel
from src.rbac.models import RoleModel, UserRoleModel, UserSiteAccessModel
from src.rbac.service import seed_roles_permissions
from src.tenancy.gateways.models import GatewayModel
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel

BASE = "/api/v1/automation-config"


async def _org(session, key):
    o = OrganizationModel(org_key=key, name=key); session.add(o); await session.flush()
    return o


async def _site(session, org, key):
    s = SiteModel(organization_id=org.id, site_key=key, name=key); session.add(s); await session.flush()
    return s


async def _gw(session, site, key, client_id):
    g = GatewayModel(site_id=site.id, gateway_key=key, client_id=client_id,
                     client_secret_hash=hash_password("x"), enabled=True)
    session.add(g); await session.flush()
    return g


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


def _config(enabled=True):
    return {
        "enabled": enabled, "timezone": "Asia/Kolkata", "days": [1, 2, 3, 4, 5, 6, 7],
        "charge": {"start": "09:00", "stop": "14:30"},
        "discharge": {"start": "15:00", "stop": "17:00"},
        "solar": {"start_above_kw": 100.0, "start_hold_seconds": 60,
                  "stop_below_kw": 80.0, "stop_hold_seconds": 30, "max_age_seconds": 15},
        "timing": {"preparation_lead_minutes": 15, "power_dispatch_lead_seconds": 120},
        "pairs": {f"pair_{i}": {"enabled": True, "charge_kw": 5.0, "discharge_kw": 5.0} for i in range(1, 5)},
    }


async def test_save_and_get(client, session):
    c, _ = client
    await seed_roles_permissions(session); await session.flush()
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    gw = await _gw(session, site, "gw", "c1")
    sa = await _user(session, "super_admin", "root@sys")

    r = await c.post(f"{BASE}?gateway_id={gw.id}", json=_config(True), headers=_auth(sa))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["enabled"] is True
    assert body["config"]["charge"]["start"] == "09:00"
    assert body["config"]["pairs"]["pair_3"]["charge_kw"] == 5.0

    g = await c.get(f"{BASE}?gateway_id={gw.id}", headers=_auth(sa))
    assert g.status_code == 200
    assert g.json()["config"]["discharge"]["stop"] == "17:00"


async def test_enabled_derived_and_idempotent(client, session):
    c, _ = client
    await seed_roles_permissions(session); await session.flush()
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    gw = await _gw(session, site, "gw", "c1")
    sa = await _user(session, "super_admin", "root@sys")

    await c.post(f"{BASE}?gateway_id={gw.id}", json=_config(True), headers=_auth(sa))
    r = await c.post(f"{BASE}?gateway_id={gw.id}", json=_config(False), headers=_auth(sa))
    assert r.json()["enabled"] is False          # derived from the blob
    # still one row for this gateway
    g = await c.get(f"{BASE}?gateway_id={gw.id}", headers=_auth(sa))
    assert g.json()["enabled"] is False


async def test_missing_returns_empty(client, session):
    c, _ = client
    await seed_roles_permissions(session); await session.flush()
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    gw = await _gw(session, site, "gw", "c1")
    sa = await _user(session, "super_admin", "root@sys")
    g = await c.get(f"{BASE}?gateway_id={gw.id}", headers=_auth(sa))
    assert g.status_code == 200 and g.json()["enabled"] is False and g.json()["config"] == {}


async def test_unknown_gateway_404(client, session):
    c, _ = client
    await seed_roles_permissions(session); await session.flush()
    sa = await _user(session, "super_admin", "root@sys")
    g = await c.get(f"{BASE}?gateway_id={uuid.uuid4()}", headers=_auth(sa))
    assert g.status_code == 404


async def test_without_command_write_forbidden(client, session):
    c, _ = client
    await seed_roles_permissions(session); await session.flush()
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    gw = await _gw(session, site, "gw", "c1")
    op = await _user(session, "site_operator", "op@o1", org.id, sites=[site])  # no command:write
    assert (await c.get(f"{BASE}?gateway_id={gw.id}", headers=_auth(op))).status_code == 403
    assert (await c.post(f"{BASE}?gateway_id={gw.id}", json=_config(), headers=_auth(op))).status_code == 403


async def test_requires_auth(client, session):
    c, _ = client
    r = await c.get(f"{BASE}?gateway_id={uuid.uuid4()}")
    assert r.status_code == 401
