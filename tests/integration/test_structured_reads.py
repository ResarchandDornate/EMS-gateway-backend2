"""Read-API tests for the dedicated per-stream tables (S3-S9): auth (401), site scope (403),
cross-site isolation, latest/history, filters, pagination, empty, invalid params. No Influx
is wired anywhere here -> these reads are PostgreSQL-only by construction."""
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from src.auth.security import create_access_token, hash_password
from src.auth.models import UserModel
from src.ingestion.health_models import GatewayHealthRecordModel
from src.ingestion.alarm_models import AlarmEventRecordModel
from src.ingestion.controller_models import ControllerStateRecordModel
from src.ingestion.command_result_models import CommandResultRecordModel
from src.ingestion.config_audit_models import ConfigurationAuditRecordModel
from src.rbac.models import RoleModel, UserRoleModel, UserSiteAccessModel
from src.rbac.service import seed_org_permissions, seed_roles_permissions
from src.tenancy.gateways.models import GatewayModel
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel

T0 = datetime(2026, 9, 1, tzinfo=timezone.utc)


async def _rbac(s): await seed_roles_permissions(s); await s.flush()
async def _org(s, k, perms=True):
    o = OrganizationModel(org_key=k, name=k); s.add(o); await s.flush()
    if perms: await seed_org_permissions(s, o.id); await s.flush()
    return o
async def _site(s, org, k):
    x = SiteModel(organization_id=org.id, site_key=k, name=k); s.add(x); await s.flush(); return x
async def _gw(s, site, k, c):
    g = GatewayModel(site_id=site.id, gateway_key=k, client_id=c,
                     client_secret_hash=hash_password("x"), enabled=True); s.add(g); await s.flush(); return g
async def _user(s, role, email, org_id=None, sites=()):
    u = UserModel(email=email, organization_id=org_id, is_active=True); s.add(u); await s.flush()
    r = (await s.execute(select(RoleModel).where(RoleModel.name == role))).scalar_one()
    s.add(UserRoleModel(user_id=u.id, role_id=r.id))
    for x in sites: s.add(UserSiteAccessModel(user_id=u.id, site_id=x.id))
    await s.commit(); return u
def _auth(u): return {"Authorization": f"Bearer {create_access_token(u.id)}"}
def _p(site, **kw): return {"site_id": str(site.id), **kw}


def _health(gw, org, site, rec_at, payload=None):
    return GatewayHealthRecordModel(gateway_id=gw.id, organization_id=org.id, site_id=site.id,
        message_id=uuid.uuid4(), substream="gateway", record_index=0, recorded_at=rec_at,
        payload=payload or {"status": "ok"})

def _alarm(gw, org, site, occ, severity="Critical", state="ACTIVE", event_type="ov"):
    return AlarmEventRecordModel(gateway_id=gw.id, organization_id=org.id, site_id=site.id,
        message_id=uuid.uuid4(), event_id=uuid.uuid4(), event_type=event_type, severity=severity,
        state=state, dedupe_key=f"{event_type}:{state}", correlation_id=None, occurred_at=occ,
        payload={"severity": severity})

def _ctrl(gw, org, site, i, rx, state="IDLE"):
    return ControllerStateRecordModel(gateway_id=gw.id, organization_id=org.id, site_id=site.id,
        message_id=uuid.uuid4(), record_index=i, controller_state=state, previous_state=None,
        decision=None, mode="auto", version=None, healthy=True, sample_time_utc=rx,
        received_at_utc=rx, payload={"mode": "auto"})

def _cmd(gw, org, site, i, req_at, status="accepted", ctype="set_soc"):
    return CommandResultRecordModel(gateway_id=gw.id, organization_id=org.id, site_id=site.id,
        message_id=uuid.uuid4(), record_index=i, command_id=uuid.uuid4(), correlation_id=None,
        command_type=ctype, final_status=status, requested_at_utc=req_at,
        gateway_received_at_utc=req_at, stored_at=req_at, requested_value=90, readback_value=None,
        payload={"final_status": status})

def _audit(gw, org, site, occ, change_source="local_api", setting_path="soc.max"):
    return ConfigurationAuditRecordModel(gateway_id=gw.id, organization_id=org.id, site_id=site.id,
        message_id=uuid.uuid4(), audit_id=uuid.uuid4(), occurred_at=occ, received_at_utc=occ,
        actor="admin", actor_role="org_admin", change_source=change_source, setting_path=setting_path,
        revision="1", correlation_id=None, old_value=80, new_value=90, payload={"actor": "admin"})


# ---------------- auth + scope ----------------
async def test_health_no_token_401(client, session):
    c, _ = client
    await _rbac(session); org = await _org(session, "o1"); site = await _site(session, org, "A"); await session.commit()
    assert (await c.get("/api/v1/health/latest", params=_p(site))).status_code == 401


async def test_alarms_cross_org_403(client, session):
    c, _ = client
    await _rbac(session)
    o1 = await _org(session, "o1"); await _site(session, o1, "A")
    o2 = await _org(session, "o2"); s2 = await _site(session, o2, "B")
    admin1 = await _user(session, "org_admin", "a@o1", o1.id)
    assert (await c.get("/api/v1/alarms", params=_p(s2), headers=_auth(admin1))).status_code == 403


async def test_cross_site_isolation(client, session):
    c, _ = client
    await _rbac(session); org = await _org(session, "o1")
    a = await _site(session, org, "A"); b = await _site(session, org, "B")
    ga = await _gw(session, a, "ga", "ca"); gb = await _gw(session, b, "gb", "cb")
    session.add(_alarm(ga, org, a, T0, event_type="A_only"))
    session.add(_alarm(gb, org, b, T0, event_type="B_only"))
    await session.commit()
    op = await _user(session, "site_operator", "op@o1", org.id, sites=[a])
    ra = await c.get("/api/v1/alarms", params=_p(a), headers=_auth(op))
    assert ra.status_code == 200
    assert [x["event_type"] for x in ra.json()["results"]] == ["A_only"]
    assert (await c.get("/api/v1/alarms", params=_p(b), headers=_auth(op))).status_code == 403


# ---------------- latest / history / pagination ----------------
async def test_health_latest_per_gateway_and_history(client, session):
    c, _ = client
    await _rbac(session); org = await _org(session, "o1"); site = await _site(session, org, "A")
    gw = await _gw(session, site, "g1", "c1")
    for k in range(3):
        session.add(_health(gw, org, site, T0 + timedelta(minutes=k), payload={"n": k}))
    await session.commit()
    admin = await _user(session, "org_admin", "a@o1", org.id)
    lat = await c.get("/api/v1/health/latest", params=_p(site), headers=_auth(admin))
    assert lat.status_code == 200 and len(lat.json()["results"]) == 1   # one gateway -> newest
    assert lat.json()["results"][0]["payload"]["n"] == 2
    hist = await c.get("/api/v1/health/history", params=_p(site, limit=2), headers=_auth(admin))
    j = hist.json(); assert len(j["results"]) == 2 and j["has_more"] is True


async def test_health_history_time_window_and_invalid_ts(client, session):
    c, _ = client
    await _rbac(session); org = await _org(session, "o1"); site = await _site(session, org, "A")
    gw = await _gw(session, site, "g1", "c1")
    for k in range(4):
        session.add(_health(gw, org, site, T0 + timedelta(hours=k)))
    await session.commit()
    admin = await _user(session, "org_admin", "a@o1", org.id)
    stop = (T0 + timedelta(hours=2)).isoformat()
    r = await c.get("/api/v1/health/history", params=_p(site, stop=stop), headers=_auth(admin))
    assert r.status_code == 200 and len(r.json()["results"]) == 2        # hours 0,1 (<stop)
    bad = await c.get("/api/v1/health/history", params=_p(site, start="nope"), headers=_auth(admin))
    assert bad.status_code == 422


# ---------------- filters ----------------
async def test_alarms_filters_and_empty(client, session):
    c, _ = client
    await _rbac(session); org = await _org(session, "o1"); site = await _site(session, org, "A")
    gw = await _gw(session, site, "g1", "c1")
    session.add(_alarm(gw, org, site, T0, severity="Critical", state="ACTIVE"))
    session.add(_alarm(gw, org, site, T0, severity="Warning", state="CLEARED", event_type="uv"))
    await session.commit()
    admin = await _user(session, "org_admin", "a@o1", org.id)
    crit = await c.get("/api/v1/alarms", params=_p(site, severity="Critical"), headers=_auth(admin))
    assert crit.status_code == 200 and len(crit.json()["results"]) == 1
    assert crit.json()["results"][0]["state"] == "ACTIVE"
    none = await c.get("/api/v1/alarms", params=_p(site, severity="Info"), headers=_auth(admin))
    assert none.status_code == 200 and none.json()["results"] == []      # empty, not error


async def test_commands_and_audit_filters(client, session):
    c, _ = client
    await _rbac(session); org = await _org(session, "o1"); site = await _site(session, org, "A")
    gw = await _gw(session, site, "g1", "c1")
    session.add(_cmd(gw, org, site, 0, T0, status="accepted"))
    session.add(_cmd(gw, org, site, 1, T0, status="failed"))
    session.add(_audit(gw, org, site, T0, change_source="local_api", setting_path="soc.max"))
    session.add(_audit(gw, org, site, T0, change_source="central_command", setting_path="mode"))
    await session.commit()
    admin = await _user(session, "org_admin", "a@o1", org.id)
    failed = await c.get("/api/v1/commands", params=_p(site, final_status="failed"), headers=_auth(admin))
    assert failed.status_code == 200 and len(failed.json()["results"]) == 1
    aud = await c.get("/api/v1/config-audit", params=_p(site, change_source="central_command"), headers=_auth(admin))
    assert aud.status_code == 200 and len(aud.json()["results"]) == 1
    assert aud.json()["results"][0]["setting_path"] == "mode"


async def test_controller_latest_state_filter(client, session):
    c, _ = client
    await _rbac(session); org = await _org(session, "o1"); site = await _site(session, org, "A")
    gw = await _gw(session, site, "g1", "c1")
    session.add(_ctrl(gw, org, site, 0, T0, state="IDLE"))
    session.add(_ctrl(gw, org, site, 1, T0 + timedelta(minutes=1), state="CHARGING"))
    await session.commit()
    admin = await _user(session, "org_admin", "a@o1", org.id)
    r = await c.get("/api/v1/controller/history", params=_p(site, controller_state="CHARGING"), headers=_auth(admin))
    assert r.status_code == 200 and len(r.json()["results"]) == 1
    assert r.json()["results"][0]["controller_state"] == "CHARGING"
