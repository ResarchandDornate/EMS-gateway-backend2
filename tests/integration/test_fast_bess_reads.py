"""S1 Fast BESS read API tests (PostgreSQL fast_bess_records).

No InfluxDB is wired in any test here: a successful S1 read with no Influx provider present
is itself proof that S1 reads come from PostgreSQL, not Influx. Covers auth (401), scope
(403 cross-org/cross-site), cross-site isolation, latest-snapshot semantics, history time
windows + pagination, empty results, invalid params, and verbatim payload preservation.
"""
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from src.auth.security import create_access_token, hash_password
from src.auth.models import UserModel
from src.ingestion.fast_bess_models import FastBessRecordModel
from src.rbac.models import RoleModel, UserRoleModel, UserSiteAccessModel
from src.rbac.service import seed_org_permissions, seed_roles_permissions
from src.tenancy.gateways.models import GatewayModel
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel

LATEST = "/api/v1/bess/fast/latest"
HISTORY = "/api/v1/bess/fast/history"
T0 = datetime(2026, 9, 1, 0, 0, tzinfo=timezone.utc)


# ---------- seeding helpers ----------
async def _rbac(session):
    await seed_roles_permissions(session); await session.flush()


async def _org(session, key, with_perms=True):
    o = OrganizationModel(org_key=key, name=key); session.add(o); await session.flush()
    if with_perms:
        await seed_org_permissions(session, o.id); await session.flush()
    return o


async def _site(session, org, key):
    s = SiteModel(organization_id=org.id, site_key=key, name=key); session.add(s); await session.flush()
    return s


async def _gw(session, site, key, client_id):
    g = GatewayModel(site_id=site.id, gateway_key=key, client_id=client_id,
                     client_secret_hash=hash_password("x"), enabled=True)
    session.add(g); await session.flush()
    return g


async def _user(session, role, email, org_id=None, active=True, sites=()):
    u = UserModel(email=email, organization_id=org_id, is_active=active); session.add(u); await session.flush()
    r = (await session.execute(select(RoleModel).where(RoleModel.name == role))).scalar_one()
    session.add(UserRoleModel(user_id=u.id, role_id=r.id))
    for s in sites:
        session.add(UserSiteAccessModel(user_id=u.id, site_id=s.id))
    await session.commit()
    return u


def _auth(u):
    return {"Authorization": f"Bearer {create_access_token(u.id)}"}


async def _fb(session, *, gw, org, site, bess_id="BESS-1", source_id="pcs1",
              recorded_at=T0, payload=None, substream="bess_fast", record_index=0):
    rec = FastBessRecordModel(
        gateway_id=gw.id, organization_id=org.id, site_id=site.id,
        message_id=uuid.uuid4(), substream=substream, record_index=record_index,
        source_id=source_id, bess_id=bess_id, recorded_at=recorded_at,
        payload=payload if payload is not None else {"soc": 55.5},
    )
    session.add(rec); await session.commit()
    return rec


# ================= auth + scope =================
async def test_no_token_401(client, session):
    c, _ = client
    await _rbac(session); org = await _org(session, "o1"); site = await _site(session, org, "A")
    await session.commit()
    r = await c.get(LATEST, params={"site_id": str(site.id)})
    assert r.status_code == 401


async def test_cross_org_403(client, session):
    c, _ = client
    await _rbac(session)
    org1 = await _org(session, "o1"); await _site(session, org1, "A")
    org2 = await _org(session, "o2"); s2 = await _site(session, org2, "B")
    admin1 = await _user(session, "org_admin", "a@o1", org1.id)
    r = await c.get(LATEST, params={"site_id": str(s2.id)}, headers=_auth(admin1))
    assert r.status_code == 403


# ================= latest snapshot =================
async def test_latest_newest_per_bess(client, session):
    c, _ = client
    await _rbac(session); org = await _org(session, "o1"); site = await _site(session, org, "A")
    gw = await _gw(session, site, "g1", "c1")
    admin = await _user(session, "org_admin", "a@o1", org.id)
    await _fb(session, gw=gw, org=org, site=site, bess_id="B1", recorded_at=T0, payload={"soc": 10})
    await _fb(session, gw=gw, org=org, site=site, bess_id="B1", recorded_at=T0 + timedelta(minutes=5), payload={"soc": 20})
    await _fb(session, gw=gw, org=org, site=site, bess_id="B2", recorded_at=T0 + timedelta(minutes=1), payload={"soc": 99})
    r = await c.get(LATEST, params={"site_id": str(site.id)}, headers=_auth(admin))
    assert r.status_code == 200
    by = {row["bess_id"]: row for row in r.json()["results"]}
    assert set(by) == {"B1", "B2"}
    assert by["B1"]["payload"] == {"soc": 20}      # newest for B1
    assert by["B2"]["payload"] == {"soc": 99}


async def test_latest_filter_by_bess_id(client, session):
    c, _ = client
    await _rbac(session); org = await _org(session, "o1"); site = await _site(session, org, "A")
    gw = await _gw(session, site, "g1", "c1")
    admin = await _user(session, "org_admin", "a@o1", org.id)
    await _fb(session, gw=gw, org=org, site=site, bess_id="B1")
    await _fb(session, gw=gw, org=org, site=site, bess_id="B2")
    r = await c.get(LATEST, params={"site_id": str(site.id), "bess_id": "B2"}, headers=_auth(admin))
    assert r.status_code == 200
    results = r.json()["results"]
    assert len(results) == 1 and results[0]["bess_id"] == "B2"


# ================= cross-site isolation =================
async def test_cross_site_isolation(client, session):
    c, _ = client
    await _rbac(session); org = await _org(session, "o1")
    a = await _site(session, org, "A"); b = await _site(session, org, "B")
    gwa = await _gw(session, a, "ga", "ca"); gwb = await _gw(session, b, "gb", "cb")
    op = await _user(session, "site_operator", "op@o1", org.id, sites=[a])
    await _fb(session, gw=gwa, org=org, site=a, bess_id="A1")
    await _fb(session, gw=gwb, org=org, site=b, bess_id="B1")
    # authorized site A -> only A's data
    ra = await c.get(LATEST, params={"site_id": str(a.id)}, headers=_auth(op))
    assert ra.status_code == 200
    assert [x["bess_id"] for x in ra.json()["results"]] == ["A1"]
    # unassigned site B -> 403 (no data leak)
    rb = await c.get(LATEST, params={"site_id": str(b.id)}, headers=_auth(op))
    assert rb.status_code == 403


# ================= history: window + paging + empty =================
async def test_history_window_and_paging(client, session):
    c, _ = client
    await _rbac(session); org = await _org(session, "o1"); site = await _site(session, org, "A")
    gw = await _gw(session, site, "g1", "c1")
    admin = await _user(session, "org_admin", "a@o1", org.id)
    for i in range(5):
        await _fb(session, gw=gw, org=org, site=site, bess_id="B1",
                  recorded_at=T0 + timedelta(minutes=i), record_index=i, payload={"i": i})
    # window excludes the last record (stop is exclusive), paging over the remaining 4
    stop = (T0 + timedelta(minutes=4)).isoformat()
    p1 = await c.get(HISTORY, params={"site_id": str(site.id), "stop": stop, "limit": 2, "offset": 0}, headers=_auth(admin))
    assert p1.status_code == 200
    j1 = p1.json(); assert len(j1["results"]) == 2 and j1["has_more"] is True
    assert j1["results"][0]["payload"]["i"] == 3     # newest-first within window
    p2 = await c.get(HISTORY, params={"site_id": str(site.id), "stop": stop, "limit": 2, "offset": 2}, headers=_auth(admin))
    j2 = p2.json(); assert len(j2["results"]) == 2 and j2["has_more"] is False


async def test_history_empty_results(client, session):
    c, _ = client
    await _rbac(session); org = await _org(session, "o1"); site = await _site(session, org, "A")
    admin = await _user(session, "org_admin", "a@o1", org.id)
    r = await c.get(HISTORY, params={"site_id": str(site.id)}, headers=_auth(admin))
    assert r.status_code == 200
    j = r.json(); assert j["results"] == [] and j["has_more"] is False


async def test_history_invalid_timestamp_422(client, session):
    c, _ = client
    await _rbac(session); org = await _org(session, "o1"); site = await _site(session, org, "A")
    admin = await _user(session, "org_admin", "a@o1", org.id)
    r = await c.get(HISTORY, params={"site_id": str(site.id), "start": "not-a-date"}, headers=_auth(admin))
    assert r.status_code == 422


# ================= payload fidelity (nested + mixed types) =================
async def test_payload_preserved_verbatim(client, session):
    c, _ = client
    await _rbac(session); org = await _org(session, "o1"); site = await _site(session, org, "A")
    gw = await _gw(session, site, "g1", "c1")
    admin = await _user(session, "org_admin", "a@o1", org.id)
    nested = {"soc": 55.5, "online": True, "faults": [1, 2, 3],
              "modules": {"m1": {"temp": 31.2, "state": "ok"}}, "note": None}
    await _fb(session, gw=gw, org=org, site=site, bess_id="B1", payload=nested)
    r = await c.get(LATEST, params={"site_id": str(site.id)}, headers=_auth(admin))
    assert r.status_code == 200
    assert r.json()["results"][0]["payload"] == nested      # nested + mixed types untouched
