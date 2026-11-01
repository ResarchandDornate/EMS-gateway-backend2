"""Integration tests for the bucketed summary endpoints + relative/calendar ranges."""
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from src.auth.security import create_access_token, hash_password
from src.auth.models import UserModel
from src.ingestion.alarm_models import AlarmEventRecordModel
from src.ingestion.command_result_models import CommandResultRecordModel
from src.ingestion.config_audit_models import ConfigurationAuditRecordModel
from src.rbac.models import RoleModel, UserRoleModel, UserSiteAccessModel
from src.rbac.service import seed_org_permissions, seed_roles_permissions
from src.tenancy.gateways.models import GatewayModel
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel

UTC = timezone.utc
D0 = datetime(2026, 9, 1, tzinfo=UTC)


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


def _alarm(gw, org, site, occ, severity, state, et="ov"):
    return AlarmEventRecordModel(gateway_id=gw.id, organization_id=org.id, site_id=site.id,
        message_id=uuid.uuid4(), event_id=uuid.uuid4(), event_type=et, severity=severity,
        state=state, dedupe_key=f"{et}:{state}", correlation_id=None, occurred_at=occ, payload={})

def _cmd(gw, org, site, i, req, status):
    return CommandResultRecordModel(gateway_id=gw.id, organization_id=org.id, site_id=site.id,
        message_id=uuid.uuid4(), record_index=i, command_id=uuid.uuid4(), correlation_id=None,
        command_type="set_soc", final_status=status, requested_at_utc=req,
        gateway_received_at_utc=req, stored_at=req, requested_value=1, readback_value=None, payload={})

def _audit(gw, org, site, occ, change_source):
    return ConfigurationAuditRecordModel(gateway_id=gw.id, organization_id=org.id, site_id=site.id,
        message_id=uuid.uuid4(), audit_id=uuid.uuid4(), occurred_at=occ, received_at_utc=occ,
        actor="admin", actor_role=None, change_source=change_source, setting_path="p",
        revision=None, correlation_id=None, old_value=None, new_value=None, payload={})


async def _setup(session):
    await _rbac(session); org = await _org(session, "o1"); site = await _site(session, org, "A")
    gw = await _gw(session, site, "g1", "c1")
    admin = await _user(session, "org_admin", "a@o1", org.id)
    return org, site, gw, admin


async def test_alarms_summary_by_day_zerofill_and_breakdowns(client, session):
    c, _ = client
    org, site, gw, admin = await _setup(session)
    session.add(_alarm(gw, org, site, D0, "Critical", "ACTIVE"))
    session.add(_alarm(gw, org, site, D0 + timedelta(hours=6), "Warning", "CLEARED", et="uv"))
    session.add(_alarm(gw, org, site, D0 + timedelta(days=2), "Critical", "ACTIVE"))   # day 2; day 1 empty
    await session.commit()
    r = await c.get("/api/v1/alarms/summary",
                    params=_p(site, group_by="day", start="2026-09-01T00:00:00Z", stop="2026-09-04T00:00:00Z"),
                    headers=_auth(admin))
    assert r.status_code == 200, r.text
    b = {x["bucket"]: x for x in r.json()["buckets"]}
    assert list(b) == ["2026-09-01T00:00:00Z", "2026-09-02T00:00:00Z", "2026-09-03T00:00:00Z"]
    assert b["2026-09-01T00:00:00Z"]["count"] == 2
    assert b["2026-09-01T00:00:00Z"]["breakdowns"]["severity"] == {"Critical": 1, "Warning": 1}
    assert b["2026-09-01T00:00:00Z"]["breakdowns"]["state"] == {"ACTIVE": 1, "CLEARED": 1}
    assert b["2026-09-02T00:00:00Z"]["count"] == 0                      # zero-filled
    assert b["2026-09-03T00:00:00Z"]["count"] == 1


async def test_commands_summary_failed_breakdown(client, session):
    c, _ = client
    org, site, gw, admin = await _setup(session)
    session.add(_cmd(gw, org, site, 0, D0, "accepted"))
    session.add(_cmd(gw, org, site, 1, D0, "failed"))
    session.add(_cmd(gw, org, site, 2, D0 + timedelta(days=2), "failed"))
    await session.commit()
    r = await c.get("/api/v1/commands/summary",
                    params=_p(site, group_by="day", start="2026-09-01T00:00:00Z", stop="2026-09-04T00:00:00Z"),
                    headers=_auth(admin))
    assert r.status_code == 200, r.text
    b = {x["bucket"]: x for x in r.json()["buckets"]}
    assert b["2026-09-01T00:00:00Z"]["breakdowns"]["final_status"] == {"accepted": 1, "failed": 1}
    assert b["2026-09-03T00:00:00Z"]["breakdowns"]["final_status"] == {"failed": 1}


async def test_config_audit_summary_change_source(client, session):
    c, _ = client
    org, site, gw, admin = await _setup(session)
    session.add(_audit(gw, org, site, D0, "local_api"))
    session.add(_audit(gw, org, site, D0, "central_command"))
    await session.commit()
    r = await c.get("/api/v1/config-audit/summary",
                    params=_p(site, group_by="day", range="this_year"), headers=_auth(admin))
    assert r.status_code == 200, r.text
    hit = [x for x in r.json()["buckets"] if x["bucket"] == "2026-09-01T00:00:00Z"]
    assert hit and hit[0]["breakdowns"]["change_source"] == {"local_api": 1, "central_command": 1}


async def test_relative_range_on_history(client, session):
    c, _ = client
    org, site, gw, admin = await _setup(session)
    now = datetime.now(UTC)
    session.add(_alarm(gw, org, site, now - timedelta(days=2), "Critical", "ACTIVE", et="recent"))
    session.add(_alarm(gw, org, site, now - timedelta(days=400), "Critical", "ACTIVE", et="old"))
    await session.commit()
    r = await c.get("/api/v1/alarms", params=_p(site, range="this_year"), headers=_auth(admin))
    assert r.status_code == 200
    types = {x["event_type"] for x in r.json()["results"]}
    assert "recent" in types and "old" not in types


async def test_summary_invalid_and_excessive_and_conflict(client, session):
    c, _ = client
    org, site, gw, admin = await _setup(session)
    bad_unit = await c.get("/api/v1/alarms/summary", params=_p(site, group_by="fortnight", range="this_month"),
                           headers=_auth(admin))
    assert bad_unit.status_code == 422
    huge = await c.get("/api/v1/alarms/summary",
                       params=_p(site, group_by="hour", start="2000-01-01T00:00:00Z", stop="2026-01-01T00:00:00Z"),
                       headers=_auth(admin))
    assert huge.status_code == 422                                     # > max buckets
    no_bound = await c.get("/api/v1/alarms/summary", params=_p(site, group_by="day"), headers=_auth(admin))
    assert no_bound.status_code == 422                                 # summary needs a lower bound
    conflict = await c.get("/api/v1/alarms/summary",
                           params=_p(site, group_by="day", start="-7d", range="this_month"), headers=_auth(admin))
    assert conflict.status_code == 422


async def test_summary_tenant_isolation_403(client, session):
    c, _ = client
    await _rbac(session)
    o1 = await _org(session, "o1"); await _site(session, o1, "A")
    o2 = await _org(session, "o2"); s2 = await _site(session, o2, "B")
    admin1 = await _user(session, "org_admin", "a@o1", o1.id)
    r = await c.get("/api/v1/alarms/summary", params=_p(s2, group_by="day", range="this_month"),
                    headers=_auth(admin1))
    assert r.status_code == 403
