"""Phase 10 Stage 1 — read-only telemetry API tests (auth/scope matrix + bounded reads).

Auth/scope/validation tests use a stub reader (deterministic, no Influx). Data tests use
a real InfluxGateway bound to telemetry_test (skip if Influx unreachable). The ingestion
gate stays OFF throughout — these reads are ungated.
"""
import types
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from influxdb_client import InfluxDBClient
from sqlalchemy import delete, select

from src.auth.security import create_access_token
from src.auth.models import UserModel
from src.config import get_settings
from src.influx import InfluxGateway, RegistryIndex, TelemetryPoint
from src.ingestion.providers import reset_registry_cache
from src.rbac.models import RoleModel, UserRoleModel, UserSiteAccessModel
from src.rbac.service import seed_org_permissions, seed_roles_permissions
from src.telemetry.providers import get_influx_gateway_read, get_registry_index_read
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel

LATEST = "/api/v1/telemetry/latest"
SERIES = "/api/v1/telemetry/series"

TEST_INDEX = RegistryIndex.from_rows([
    ("S2", "bms_extended", "rack_operating_state", "rack", "U16", "T2_1Y"),
    ("S2", "bms_extended", "mcb_feedback", "rack", "U16", "T2_1Y"),
    ("S2", "bms_extended", "rack_charge_energy_session", "rack", "FLOAT32", "T3_3Y"),  # T3 -> interim long bucket (200)
    ("S1", "bess_fast", "soc", "pcs_1", "Float", "T1_RAW_90D"),
])


@pytest.fixture(autouse=True)
def _reset():
    reset_registry_cache(); yield; reset_registry_cache()


# ---------- stub reader (no Influx) ----------
class _StubRead:
    def __init__(self, *, fail=False, no_call=False):
        self._s = types.SimpleNamespace(influx_bucket_raw="telemetry_test",
                                        influx_bucket_long="telemetry_test")
        self.fail = fail; self.no_call = no_call
        self.calls = []            # records (method, args, kwargs) so reads can be asserted

    @property
    def settings(self):
        return self._s

    async def _r(self):
        if self.no_call:
            raise AssertionError("InfluxDB queried on a request that should have been rejected")
        if self.fail:
            raise RuntimeError("influx down")
        return []

    async def latest_signal(self, *a, **k):
        self.calls.append(("latest_signal", a, k)); return await self._r()
    async def latest_by_asset(self, *a, **k):
        self.calls.append(("latest_by_asset", a, k)); return await self._r()
    async def series(self, *a, **k):
        self.calls.append(("series", a, k)); return await self._r()
    async def aggregate(self, *a, **k):
        self.calls.append(("aggregate", a, k)); return await self._r()


def _override(influx_obj, index=TEST_INDEX):
    from src.main import app
    app.dependency_overrides[get_registry_index_read] = lambda: index
    app.dependency_overrides[get_influx_gateway_read] = lambda: influx_obj


# ---------- DB helpers ----------
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


def _p(site_id, **over):
    base = {"site_id": str(site_id), "stream": "S2", "substream": "bms_extended",
            "signal_key": "rack_operating_state"}
    base.update(over)
    return base


# ================= auth order + permission (stub; no Influx) =================
async def test_no_token_401(client, session):
    c, _ = client
    _override(_StubRead(no_call=True))
    await _rbac(session)
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    r = await c.get(LATEST, params=_p(site.id))
    assert r.status_code == 401


async def test_deactivated_user_401(client, session):
    c, _ = client
    _override(_StubRead(no_call=True))
    await _rbac(session)
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    u = await _user(session, "org_admin", "dead@o1", org.id, active=False)
    r = await c.get(LATEST, params=_p(site.id), headers=_auth(u))
    assert r.status_code == 401


async def test_missing_telemetry_read_perm_403(client, session):
    c, _ = client
    _override(_StubRead(no_call=True))
    await _rbac(session)
    org = await _org(session, "o1", with_perms=False)     # org perms NOT seeded -> telemetry:read not effective
    site = await _site(session, org, "A")
    u = await _user(session, "org_admin", "a@o1", org.id)
    r = await c.get(LATEST, params=_p(site.id), headers=_auth(u))
    assert r.status_code == 403


async def test_non_super_without_org_fails_closed_403(client, session):
    c, _ = client
    _override(_StubRead(no_call=True))
    await _rbac(session)
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    u = await _user(session, "org_admin", "noorg@x", org_id=None)   # organization_id None
    r = await c.get(LATEST, params=_p(site.id), headers=_auth(u))
    assert r.status_code == 403


# ================= site scope (stub; empty 200 when authorized) =================
async def test_org_admin_own_vs_cross_org(client, session):
    c, _ = client
    _override(_StubRead())            # authorized path returns [] -> 200 empty
    await _rbac(session)
    org1 = await _org(session, "o1"); s1 = await _site(session, org1, "A")
    org2 = await _org(session, "o2"); s2 = await _site(session, org2, "B")
    admin1 = await _user(session, "org_admin", "a@o1", org1.id)
    assert (await c.get(LATEST, params=_p(s1.id), headers=_auth(admin1))).status_code == 200
    assert (await c.get(LATEST, params=_p(s2.id), headers=_auth(admin1))).status_code == 403  # cross-org


async def test_site_operator_assigned_vs_unassigned(client, session):
    c, _ = client
    _override(_StubRead())
    await _rbac(session)
    org = await _org(session, "o1")
    a = await _site(session, org, "A"); b = await _site(session, org, "B")
    op = await _user(session, "site_operator", "op@o1", org.id, sites=[a])
    assert (await c.get(LATEST, params=_p(a.id), headers=_auth(op))).status_code == 200
    assert (await c.get(LATEST, params=_p(b.id), headers=_auth(op))).status_code == 403


async def test_site_operator_multiple_sites(client, session):
    c, _ = client
    _override(_StubRead())
    await _rbac(session)
    org = await _org(session, "o1")
    a = await _site(session, org, "A"); b = await _site(session, org, "B")
    op = await _user(session, "site_operator", "op@o1", org.id, sites=[a, b])
    assert (await c.get(LATEST, params=_p(a.id), headers=_auth(op))).status_code == 200
    assert (await c.get(LATEST, params=_p(b.id), headers=_auth(op))).status_code == 200


async def test_site_access_revocation_next_request(client, session):
    c, _ = client
    _override(_StubRead())
    await _rbac(session)
    org = await _org(session, "o1"); a = await _site(session, org, "A")
    op = await _user(session, "site_operator", "op@o1", org.id, sites=[a])
    assert (await c.get(LATEST, params=_p(a.id), headers=_auth(op))).status_code == 200
    await session.execute(delete(UserSiteAccessModel).where(UserSiteAccessModel.user_id == op.id))
    await session.commit()
    assert (await c.get(LATEST, params=_p(a.id), headers=_auth(op))).status_code == 403


# ================= input validation (super_admin; stub no_call => must reject pre-query) =================
async def test_unknown_signal_422(client, session):
    c, _ = client
    _override(_StubRead(no_call=True))
    await _rbac(session)
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    su = await _user(session, "super_admin", "root@x")
    r = await c.get(LATEST, params=_p(site.id, signal_key="not_a_signal"), headers=_auth(su))
    assert r.status_code == 422


async def test_unsupported_stream_422(client, session):
    c, _ = client
    _override(_StubRead(no_call=True))
    await _rbac(session)
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    su = await _user(session, "super_admin", "root@x")
    r = await c.get(LATEST, params=_p(site.id, stream="S9", substream="x", signal_key="whatever"),
                    headers=_auth(su))
    assert r.status_code == 422


async def test_t3_signal_interim_reads_200(client, session):
    # INTERIM 2026-10-26: T3 resolves to the LONG bucket, so reads succeed (200) not 501.
    # Distinct stub bucket names prove long-vs-raw; the authorized site is forwarded to the
    # reader. (The actual Flux site filter is covered by the real-influx read tests.)
    c, _ = client
    stub = _StubRead()
    stub.settings.influx_bucket_raw = "telemetry_raw_test"
    stub.settings.influx_bucket_long = "telemetry_long_test"
    _override(stub)
    await _rbac(session)
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    su = await _user(session, "super_admin", "root@x")
    r = await c.get(LATEST, params=_p(site.id, signal_key="rack_charge_energy_session"),
                    headers=_auth(su))
    assert r.status_code == 200
    assert stub.calls and stub.calls[-1][0] == "latest_by_asset"
    bucket, _measurement, called_site, signal_key = stub.calls[-1][1]
    assert bucket == "telemetry_long_test"
    assert bucket != "telemetry_raw_test"
    assert called_site == str(site.id)
    assert signal_key == "rack_charge_energy_session"


async def test_unsafe_identifier_422(client, session):
    c, _ = client
    await _rbac(session)
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    su = await _user(session, "super_admin", "root@x")
    BAD = 'bad" or "1'
    sbase = {**_p(site.id), "start": "-1h"}
    # unsafe identifiers must be rejected BEFORE any InfluxDB query (no_call stub fails if queried)
    _override(_StubRead(no_call=True))
    assert (await c.get(LATEST, params=_p(site.id, asset_id=BAD), headers=_auth(su))).status_code == 422
    assert (await c.get(LATEST, params=_p(site.id, source_id=BAD), headers=_auth(su))).status_code == 422
    assert (await c.get(SERIES, params={**sbase, "asset_id": BAD}, headers=_auth(su))).status_code == 422
    assert (await c.get(SERIES, params={**sbase, "asset_id": "rack1", "source_id": BAD}, headers=_auth(su))).status_code == 422
    # valid identifiers still reach the gateway and succeed (empty result -> 200)
    _override(_StubRead())
    assert (await c.get(LATEST, params=_p(site.id, asset_id="rack1"), headers=_auth(su))).status_code == 200
    assert (await c.get(SERIES, params={**sbase, "asset_id": "rack1"}, headers=_auth(su))).status_code == 200


async def test_series_bad_ranges_422(client, session):
    c, _ = client
    _override(_StubRead(no_call=True))
    await _rbac(session)
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    su = await _user(session, "super_admin", "root@x")
    base = _p(site.id, asset_id="rack1")
    # reversed (start after stop)
    r1 = await c.get(SERIES, params={**base, "start": "2026-10-10T00:00:00Z",
                                     "stop": "2026-10-09T00:00:00Z"}, headers=_auth(su))
    # window > 7 days
    r2 = await c.get(SERIES, params={**base, "start": "2026-10-01T00:00:00Z",
                                     "stop": "2026-10-20T00:00:00Z"}, headers=_auth(su))
    # garbage start
    r3 = await c.get(SERIES, params={**base, "start": "not-a-time"}, headers=_auth(su))
    assert r1.status_code == 422 and r2.status_code == 422 and r3.status_code == 422


async def test_series_invalid_limit_422(client, session):
    c, _ = client
    _override(_StubRead(no_call=True))
    await _rbac(session)
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    su = await _user(session, "super_admin", "root@x")
    base = _p(site.id, asset_id="rack1")
    assert (await c.get(SERIES, params={**base, "start": "-1h", "limit": 0}, headers=_auth(su))).status_code == 422
    assert (await c.get(SERIES, params={**base, "start": "-1h", "limit": 6000}, headers=_auth(su))).status_code == 422


async def test_influx_error_503(client, session):
    c, _ = client
    _override(_StubRead(fail=True))
    await _rbac(session)
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    su = await _user(session, "super_admin", "root@x")
    r = await c.get(LATEST, params=_p(site.id, asset_id="rack1"), headers=_auth(su))
    assert r.status_code == 503


async def test_reads_work_while_ingestion_disabled(client, session):
    # gate OFF (default): telemetry read returns 200; ingest endpoint still 503
    c, _ = client
    _override(_StubRead())
    await _rbac(session)
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    su = await _user(session, "super_admin", "root@x")
    assert (await c.get(LATEST, params=_p(site.id), headers=_auth(su))).status_code == 200
    assert get_settings().ingest_enabled is False


# ================= real Influx (telemetry_test); skip if unreachable =================
@pytest.fixture
def real_influx():
    base = get_settings()
    client = InfluxDBClient(url=base.influx_url, token=base.influx_token, org=base.influx_org)
    try:
        if not client.ping():
            pytest.skip("InfluxDB not reachable")
        b = client.buckets_api()
        if b.find_bucket_by_name(base.influx_bucket_test) is None:
            b.create_bucket(bucket_name=base.influx_bucket_test, org=base.influx_org)
        ts = base.model_copy(update={"influx_bucket_raw": base.influx_bucket_test,
                                     "influx_bucket_long": base.influx_bucket_test})
        gw = InfluxGateway(settings=ts)
        assert gw.settings.influx_bucket_raw == base.influx_bucket_test
        assert gw.settings.influx_bucket_long == base.influx_bucket_test
        yield gw, base.influx_bucket_test
    finally:
        client.close()


def _pt(site, asset, source, key, val, ts):
    return TelemetryPoint("s2_bms_extended", "org_x", str(site.id), "gw_x",
                          source, asset, "bms_extended", key, val, "good", ts)


async def _cleanup(gw, bucket, site, t0, t1):
    async with gw._client() as c:
        await c.delete_api().delete(t0 - timedelta(seconds=5), t1 + timedelta(seconds=5),
                                    f'site_id="{site}"', bucket=bucket, org=gw._org)


async def test_super_admin_reads_value_roundtrip(client, session, real_influx):
    c, _ = client
    gw, bucket = real_influx
    _override(gw)
    await _rbac(session)
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    other = await _site(session, org, "B")
    su = await _user(session, "super_admin", "root@x")
    now = datetime.now(timezone.utc)
    try:
        await gw.write([_pt(site, "bms_rack_1", "src1", "rack_operating_state", 5, now)], TEST_INDEX)
        r = await c.get(LATEST, params=_p(site.id), headers=_auth(su))
        assert r.status_code == 200
        res = r.json()["results"]
        assert len(res) == 1 and res[0]["value"] == 5 and res[0]["asset_id"] == "bms_rack_1"
        # a different site (no data) returns empty, never site A's data
        r2 = await c.get(LATEST, params=_p(other.id), headers=_auth(su))
        assert r2.status_code == 200 and r2.json()["results"] == []
    finally:
        await _cleanup(gw, bucket, str(site.id), now, now)


async def test_unknown_asset_returns_empty(client, session, real_influx):
    c, _ = client
    gw, bucket = real_influx
    _override(gw)
    await _rbac(session)
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    su = await _user(session, "super_admin", "root@x")
    now = datetime.now(timezone.utc)
    try:
        await gw.write([_pt(site, "bms_rack_1", "src1", "rack_operating_state", 5, now)], TEST_INDEX)
        r = await c.get(LATEST, params=_p(site.id, asset_id="bms_rack_9"), headers=_auth(su))
        assert r.status_code == 200 and r.json()["results"] == []
    finally:
        await _cleanup(gw, bucket, str(site.id), now, now)


async def test_series_limit_enforced_truncated(client, session, real_influx):
    c, _ = client
    gw, bucket = real_influx
    _override(gw)
    await _rbac(session)
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    su = await _user(session, "super_admin", "root@x")
    now = datetime.now(timezone.utc)
    pts = [_pt(site, "bms_rack_1", "src1", "rack_operating_state", v, now - timedelta(minutes=m))
           for v, m in ((1, 2), (2, 1), (3, 0))]
    try:
        await gw.write(pts, TEST_INDEX)
        r = await c.get(SERIES, params=_p(site.id, asset_id="bms_rack_1", start="-1h", limit=2),
                        headers=_auth(su))
        assert r.status_code == 200
        body = r.json()
        assert len(body["points"]) == 2 and body["truncated"] is True
    finally:
        await _cleanup(gw, bucket, str(site.id), now - timedelta(minutes=2), now)


# ================= Phase 10 Stage 2: aggregation =================
AGGREGATE = "/api/v1/telemetry/aggregate"

NON_NUMERIC_INDEX = RegistryIndex.from_rows([
    ("S2", "bms_extended", "weird_text", "rack", "STRING", "T2_1Y"),
])
UNKNOWN_RETENTION_INDEX = RegistryIndex.from_rows([
    ("S2", "bms_extended", "weird_ret", "rack", "U16", "T9_X"),
])


def _ap(site_id, **over):
    base = {**_p(site_id), "asset_id": "bms_rack_1", "start": "-1h", "window": "5m", "fn": "mean"}
    base.update(over)
    return base


async def test_aggregate_unknown_signal_422(client, session):
    c, _ = client
    _override(_StubRead(no_call=True))
    await _rbac(session)
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    su = await _user(session, "super_admin", "root@x")
    assert (await c.get(AGGREGATE, params=_ap(site.id, signal_key="not_a_signal"),
                        headers=_auth(su))).status_code == 422


async def test_aggregate_bad_window_and_fn_422(client, session):
    c, _ = client
    _override(_StubRead(no_call=True))
    await _rbac(session)
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    su = await _user(session, "super_admin", "root@x")
    for w in ("0m", "abc", "5", "m", "100000w", "367d"):
        assert (await c.get(AGGREGATE, params=_ap(site.id, window=w),
                            headers=_auth(su))).status_code == 422
    assert (await c.get(AGGREGATE, params=_ap(site.id, fn="median"),
                        headers=_auth(su))).status_code == 422


async def test_aggregate_overlong_window_digits_422(client, session):
    c, _ = client
    _override(_StubRead(no_call=True))
    await _rbac(session)
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    su = await _user(session, "super_admin", "root@x")
    huge = "1" + "0" * 5000 + "s"
    assert (await c.get(AGGREGATE, params=_ap(site.id, window=huge),
                        headers=_auth(su))).status_code == 422


async def test_aggregate_non_numeric_type_422(client, session):
    c, _ = client
    _override(_StubRead(no_call=True), index=NON_NUMERIC_INDEX)
    await _rbac(session)
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    su = await _user(session, "super_admin", "root@x")
    for fn in ("mean", "min", "max", "first", "last", "count"):
        r = await c.get(AGGREGATE, params=_ap(site.id, signal_key="weird_text", fn=fn), headers=_auth(su))
        assert r.status_code == 422, (fn, r.text)


async def test_aggregate_unknown_retention_501(client, session):
    c, _ = client
    _override(_StubRead(no_call=True), index=UNKNOWN_RETENTION_INDEX)
    await _rbac(session)
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    su = await _user(session, "super_admin", "root@x")
    r = await c.get(AGGREGATE, params=_ap(site.id, signal_key="weird_ret"), headers=_auth(su))
    assert r.status_code == 501


async def test_aggregate_horizon_422(client, session):
    c, _ = client
    _override(_StubRead(no_call=True))
    await _rbac(session)
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    su = await _user(session, "super_admin", "root@x")
    assert (await c.get(AGGREGATE, params=_ap(site.id, start="-400d", window="1d"),
                        headers=_auth(su))).status_code == 422


async def test_aggregate_bucket_count_exactly_2000_ok(client, session):
    c, _ = client
    _override(_StubRead())
    await _rbac(session)
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    su = await _user(session, "super_admin", "root@x")
    p = {**_p(site.id), "asset_id": "bms_rack_1", "window": "1m", "fn": "mean",
         "start": "2026-01-01T00:00:00Z", "stop": "2026-01-02T09:20:00Z"}
    assert (await c.get(AGGREGATE, params=p, headers=_auth(su))).status_code == 200


async def test_aggregate_bucket_count_over_cap_422(client, session):
    c, _ = client
    _override(_StubRead(no_call=True))
    await _rbac(session)
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    su = await _user(session, "super_admin", "root@x")
    p = {**_p(site.id), "asset_id": "bms_rack_1", "window": "1m", "fn": "mean",
         "start": "2026-01-01T00:00:00Z", "stop": "2026-01-02T09:21:00Z"}
    assert (await c.get(AGGREGATE, params=p, headers=_auth(su))).status_code == 422


async def test_aggregate_bucket_count_fractional_boundary_422(client, session):
    c, _ = client
    _override(_StubRead(no_call=True))
    await _rbac(session)
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    su = await _user(session, "super_admin", "root@x")
    p = {**_p(site.id), "asset_id": "bms_rack_1", "window": "1m", "fn": "mean",
         "start": "2026-01-01T00:00:00Z", "stop": "2026-01-02T09:20:00.001Z"}
    assert (await c.get(AGGREGATE, params=p, headers=_auth(su))).status_code == 422


async def test_aggregate_unsafe_identifier_422(client, session):
    c, _ = client
    _override(_StubRead(no_call=True))
    await _rbac(session)
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    su = await _user(session, "super_admin", "root@x")
    assert (await c.get(AGGREGATE, params=_ap(site.id, asset_id='bad" or "1'),
                        headers=_auth(su))).status_code == 422


async def test_aggregate_org_admin_own_200_cross_403(client, session):
    c, _ = client
    _override(_StubRead())
    await _rbac(session)
    org1 = await _org(session, "o1"); s1 = await _site(session, org1, "A")
    org2 = await _org(session, "o2"); s2 = await _site(session, org2, "B")
    admin1 = await _user(session, "org_admin", "a@o1", org1.id)
    assert (await c.get(AGGREGATE, params=_ap(s1.id), headers=_auth(admin1))).status_code == 200
    assert (await c.get(AGGREGATE, params=_ap(s2.id), headers=_auth(admin1))).status_code == 403


async def test_aggregate_t3_routes_to_long_bucket(client, session):
    c, _ = client
    stub = _StubRead()
    stub.settings.influx_bucket_raw = "telemetry_raw_test"
    stub.settings.influx_bucket_long = "telemetry_long_test"
    _override(stub)
    await _rbac(session)
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    su = await _user(session, "super_admin", "root@x")
    r = await c.get(AGGREGATE, params=_ap(site.id, signal_key="rack_charge_energy_session",
                    start="-10m", window="1m", fn="last"), headers=_auth(su))
    assert r.status_code == 200
    assert stub.calls and stub.calls[-1][0] == "aggregate"
    bucket = stub.calls[-1][1][0]
    assert bucket == "telemetry_long_test"
    assert bucket != "telemetry_raw_test"


async def test_aggregate_all_functions_roundtrip(client, session, real_influx):
    c, _ = client
    gw, bucket = real_influx
    _override(gw)
    await _rbac(session)
    org = await _org(session, "o1"); site = await _site(session, org, "A")
    su = await _user(session, "super_admin", "root@x")
    now = datetime.now(timezone.utc)
    t0 = (now - timedelta(hours=1)).replace(minute=10, second=0, microsecond=0)
    pts = [_pt(site, "bms_rack_1", "src1", "rack_operating_state", v, t0 + timedelta(minutes=m))
           for v, m in ((2, 0), (4, 1), (9, 2))]
    expected = {"mean": 5.0, "min": 2, "max": 9, "first": 2, "last": 9, "count": 3}
    try:
        await gw.write(pts, TEST_INDEX)
        for fn, exp in expected.items():
            r = await c.get(AGGREGATE, params=_ap(site.id, asset_id="bms_rack_1",
                            start="-2h", window="1h", fn=fn), headers=_auth(su))
            assert r.status_code == 200, (fn, r.text)
            out = r.json()["points"]
            assert len(out) == 1, (fn, out)
            assert out[0]["value"] == exp, (fn, out[0]["value"])
    finally:
        await _cleanup(gw, bucket, str(site.id), t0 - timedelta(minutes=1), t0 + timedelta(minutes=3))


async def test_aggregate_sparse_gap_and_cross_site(client, session, real_influx):
    c, _ = client
    gw, bucket = real_influx
    _override(gw)
    await _rbac(session)
    org = await _org(session, "o1"); site = await _site(session, org, "A"); other = await _site(session, org, "B")
    su = await _user(session, "super_admin", "root@x")
    t0 = (datetime.now(timezone.utc).replace(microsecond=0, second=0)) - timedelta(minutes=5)
    pts = [
        _pt(site, "bms_rack_1", "src1", "rack_operating_state", 2, t0),
        _pt(site, "bms_rack_1", "src1", "rack_operating_state", 4, t0 + timedelta(seconds=20)),
        _pt(site, "bms_rack_1", "src1", "rack_operating_state", 9, t0 + timedelta(minutes=2)),
    ]
    try:
        await gw.write(pts, TEST_INDEX)
        r = await c.get(AGGREGATE, params=_ap(site.id, start="-10m", window="1m", fn="mean"),
                        headers=_auth(su))
        assert r.status_code == 200
        out = r.json()["points"]
        assert len(out) == 2
        vals = [p["value"] for p in out]
        assert 3.0 in vals and 9.0 in vals
        assert all(p["value"] is not None for p in out)
        r2 = await c.get(AGGREGATE, params=_ap(other.id, start="-10m", window="1m", fn="mean"),
                         headers=_auth(su))
        assert r2.status_code == 200 and r2.json()["points"] == []
    finally:
        await _cleanup(gw, bucket, str(site.id), t0 - timedelta(minutes=1), t0 + timedelta(minutes=3))


def test_agg_flux_fn_matches_field_type_normalization():
    from src.signals.types import field_type
    from src.telemetry import service as svc
    for data_type in ("FLOAT32", "Float", "U16", "S16", "U32"):
        ft = field_type(data_type)
        assert ft in ("float", "int")
        for fn in ("mean", "min", "max", "first", "last", "count"):
            assert svc.agg_flux_fn(fn, ft) == svc.AGG_FUNCS[fn]
    with pytest.raises(ValueError):
        svc.agg_flux_fn("median", "float")


def test_parse_window_rejects_overlong_digits():
    from src.telemetry import service as svc
    with pytest.raises(ValueError):
        svc.parse_window("1" + "0" * 5000 + "s")


def test_bucket_count_exact_half_open():
    from datetime import datetime, timedelta, timezone
    from src.telemetry import service as svc
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    m = timedelta(minutes=1)
    assert svc.bucket_count(t0, t0 + timedelta(minutes=2000), m) == 2000
    assert svc.bucket_count(t0, t0 + timedelta(minutes=2000, microseconds=1), m) == 2001
    assert svc.bucket_count(t0 + timedelta(seconds=30), t0 + timedelta(seconds=90), m) == 2
    assert svc.bucket_count(t0, t0 + m, m) == 1


def test_bucket_count_fractional_seconds():
    from datetime import datetime, timedelta, timezone
    from src.telemetry import service as svc
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    m = timedelta(minutes=1)
    assert svc.bucket_count(t0 + timedelta(seconds=30, microseconds=500000),
                            t0 + timedelta(minutes=2), m) == 2
    assert svc.bucket_count(t0 + timedelta(seconds=10, microseconds=250000),
                            t0 + timedelta(seconds=130, microseconds=750000), m) == 3
    assert svc.bucket_count(t0 + timedelta(seconds=59, microseconds=999999),
                            t0 + timedelta(seconds=60, microseconds=1), m) == 2
