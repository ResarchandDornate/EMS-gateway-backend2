"""S1 Fast BESS batch-slices read (POST /bess/fast/slices + svc.slices).

The slice reader returns the newest record in each [from, to) window in one query. These lock
its contract: newest-per-window parity, empty windows -> None, input order preserved, the same
site hard-filter as the other S1 readers, and the HTTP endpoint's auth.
"""
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from src.auth.security import create_access_token, hash_password
from src.auth.models import UserModel
from src.fast_bess import service as svc
from src.ingestion.fast_bess_models import FastBessRecordModel
from src.rbac.models import RoleModel, UserRoleModel, UserSiteAccessModel
from src.rbac.service import seed_org_permissions, seed_roles_permissions
from src.tenancy.gateways.models import GatewayModel
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel

SLICES = "/api/v1/bess/fast/slices"
T0 = datetime(2026, 9, 1, 0, 0, tzinfo=timezone.utc)
MIN = timedelta(minutes=1)


async def _org(session, key):
    o = OrganizationModel(org_key=key, name=key); session.add(o); await session.flush()
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


async def _fb(session, *, gw, org, site, at, bess_id="BESS-1", soc=0.0):
    rec = FastBessRecordModel(
        gateway_id=gw.id, organization_id=org.id, site_id=site.id,
        message_id=uuid.uuid4(), substream="bess_fast", record_index=0,
        source_id="pcs1", bess_id=bess_id, recorded_at=at, payload={"soc": soc},
    )
    session.add(rec); await session.commit()
    return rec


def _win(a_min, b_min):
    return (T0 + a_min * MIN, T0 + b_min * MIN)


async def test_slices_newest_per_window(session):
    await seed_roles_permissions(session); await session.flush()
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    gw = await _gw(session, site, "gw", "c1")
    for m, soc in [(0, 1.0), (5, 2.0), (12, 3.0), (20, 4.0), (50, 5.0)]:
        await _fb(session, gw=gw, org=org, site=site, at=T0 + m * MIN, soc=soc)

    windows = [_win(0, 15), _win(15, 30), _win(30, 45), _win(45, 60)]
    out = await svc.slices(session, site_id=site.id, bess_id="BESS-1", windows=windows)

    assert len(out) == 4
    assert out[0] is not None and out[0].payload["soc"] == 3.0   # newest of 0/5/12 -> 12
    assert out[1] is not None and out[1].payload["soc"] == 4.0   # 20
    assert out[2] is None                                        # empty window
    assert out[3] is not None and out[3].payload["soc"] == 5.0   # 50


async def test_slices_preserve_input_order(session):
    await seed_roles_permissions(session); await session.flush()
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    gw = await _gw(session, site, "gw", "c1")
    await _fb(session, gw=gw, org=org, site=site, at=T0 + 5 * MIN, soc=1.0)
    await _fb(session, gw=gw, org=org, site=site, at=T0 + 50 * MIN, soc=2.0)

    windows = [_win(45, 60), _win(0, 15)]
    out = await svc.slices(session, site_id=site.id, bess_id="BESS-1", windows=windows)
    assert out[0].payload["soc"] == 2.0   # first window asked = [45,60)
    assert out[1].payload["soc"] == 1.0   # second = [0,15)


async def test_slices_site_scoped(session):
    await seed_roles_permissions(session); await session.flush()
    org = await _org(session, "o1")
    site_a = await _site(session, org, "A"); site_b = await _site(session, org, "B")
    gw_a = await _gw(session, site_a, "gwa", "ca"); gw_b = await _gw(session, site_b, "gwb", "cb")
    await _fb(session, gw=gw_b, org=org, site=site_b, at=T0 + 10 * MIN, soc=9.0)
    out = await svc.slices(session, site_id=site_a.id, bess_id="BESS-1", windows=[_win(0, 15)])
    assert out == [None]
    await _fb(session, gw=gw_a, org=org, site=site_a, at=T0 + 8 * MIN, soc=1.0)
    out = await svc.slices(session, site_id=site_a.id, bess_id="BESS-1", windows=[_win(0, 15)])
    assert out[0] is not None and out[0].payload["soc"] == 1.0


async def test_slices_empty_windows(session):
    await seed_roles_permissions(session); await session.flush()
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    out = await svc.slices(session, site_id=site.id, bess_id="BESS-1", windows=[])
    assert out == []


async def test_slices_endpoint_ok(client, session):
    c, _ = client
    await seed_roles_permissions(session); await session.flush()
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    gw = await _gw(session, site, "gw", "c1")
    await _fb(session, gw=gw, org=org, site=site, at=T0 + 12 * MIN, soc=3.0)
    op = await _user(session, "site_operator", "op@o1", org.id, sites=[site])

    a = int((T0).timestamp() * 1000)
    b = int((T0 + 15 * MIN).timestamp() * 1000)
    mid = int((T0 + 30 * MIN).timestamp() * 1000)
    r = await c.post(SLICES, params={"site_id": str(site.id)},
                    json={"bess_id": "BESS-1", "windows": [[a, b], [b, mid]]}, headers=_auth(op))
    assert r.status_code == 200
    body = r.json()
    assert len(body["results"]) == 2
    assert len(body["results"][0]) == 1 and body["results"][0][0]["payload"]["soc"] == 3.0
    assert body["results"][1] == []

async def test_slices_endpoint_needs_auth(client, session):
    c, _ = client
    r = await c.post(SLICES, params={"site_id": str(uuid.uuid4())},
                    json={"bess_id": "BESS-1", "windows": [[0, 1000]]})
    assert r.status_code == 401
