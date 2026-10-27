"""Phase 9b-ii-b: InfluxGateway writer (test-bucket routing) + mark_stored CAS.

Strict isolation: the only writing InfluxGateway here is built from a settings override
where BOTH influx_bucket_raw and influx_bucket_long equal influx_bucket_test, so T1/T2
route only to telemetry_test. Real registry retention is NOT falsified. (Interim 2026-10-26: T3 now routes to the long bucket,
which here is also telemetry_test.) Influx tests skip ONLY when ping() shows the server is unreachable.
"""
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from influxdb_client import InfluxDBClient
from sqlalchemy import select

from src.config import get_settings
from src.influx import InfluxGateway, RegistryIndex, TelemetryPoint, rows
from src.ingestion.models import IngestMessageModel
from src.ingestion.service import IngestDecision, claim_or_resolve, mark_stored
from src.auth.security import hash_password
from src.tenancy.gateways.models import GatewayModel
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel

# verified-real registry rows (bms_extended T2, asset_type rack)
_IDX = RegistryIndex.from_rows([
    ("S2", "bms_extended", "rack_operating_state", "rack", "U16", "T2_1Y"),
    ("S2", "bms_extended", "mcb_feedback", "rack", "U16", "T2_1Y"),
])


async def _mk_gateway(session, *, org_key, site_key, gateway_key, client_id):
    org = OrganizationModel(org_key=org_key, name="Org")
    session.add(org); await session.flush()
    site = SiteModel(organization_id=org.id, site_key=site_key, name="Site")
    session.add(site); await session.flush()
    gw = GatewayModel(site_id=site.id, gateway_key=gateway_key, client_id=client_id,
                      client_secret_hash=hash_password("x"), enabled=True)
    session.add(gw); await session.commit()
    return gw


async def _claim(session, gw_id, mid, payload_hash="h1"):
    return await claim_or_resolve(session, gateway_id=gw_id, stream="S2", message_id=mid,
                                  sequence=1, record_count=1, payload_hash=payload_hash)


async def _row(session, mid):
    return (await session.execute(
        select(IngestMessageModel).where(IngestMessageModel.message_id == mid))).scalar_one()


def _pt(site, asset, source, key, val, ts):
    return TelemetryPoint("s2_bms_extended", "org1", site, "gw1", source, asset,
                          "bms_extended", key, val, "good", ts)


# ================= PostgreSQL-only: mark_stored CAS =================
async def test_mark_stored_cas_success(session):
    gw = await _mk_gateway(session, org_key="o1", site_key="s1", gateway_key="g1", client_id="c1")
    mid = uuid.uuid4()
    decision, token = await _claim(session, gw.id, mid)
    assert decision == IngestDecision.CLAIMED
    assert await mark_stored(session, gateway_id=gw.id, message_id=mid, token=token) is True
    row = await _row(session, mid)
    assert row.status == "stored" and row.stored_at is not None


async def test_mark_stored_wrong_token_leaves_row_unchanged(session):
    gw = await _mk_gateway(session, org_key="o1", site_key="s1", gateway_key="g1", client_id="c1")
    mid = uuid.uuid4()
    _, token = await _claim(session, gw.id, mid)
    before = await _row(session, mid)
    assert await mark_stored(session, gateway_id=gw.id, message_id=mid, token=uuid.uuid4()) is False
    after = await _row(session, mid)
    assert after.status == "pending" and after.stored_at is None and after.lease_owner == before.lease_owner


async def test_mark_stored_already_stored_is_false(session):
    gw = await _mk_gateway(session, org_key="o1", site_key="s1", gateway_key="g1", client_id="c1")
    mid = uuid.uuid4()
    _, token = await _claim(session, gw.id, mid)
    assert await mark_stored(session, gateway_id=gw.id, message_id=mid, token=token) is True
    # second call: already stored -> no row matches status='pending' -> False
    assert await mark_stored(session, gateway_id=gw.id, message_id=mid, token=token) is False


async def test_mark_stored_gateway_message_isolation(session):
    gw1 = await _mk_gateway(session, org_key="o1", site_key="s1", gateway_key="g1", client_id="c1")
    gw2 = await _mk_gateway(session, org_key="o2", site_key="s2", gateway_key="g2", client_id="c2")
    mid = uuid.uuid4()
    _, t1 = await _claim(session, gw1.id, mid)
    _, t2 = await _claim(session, gw2.id, mid)
    assert await mark_stored(session, gateway_id=gw1.id, message_id=mid, token=t1) is True
    r1 = (await session.execute(select(IngestMessageModel).where(
        IngestMessageModel.gateway_id == gw1.id, IngestMessageModel.message_id == mid))).scalar_one()
    r2 = (await session.execute(select(IngestMessageModel).where(
        IngestMessageModel.gateway_id == gw2.id, IngestMessageModel.message_id == mid))).scalar_one()
    assert r1.status == "stored" and r2.status == "pending"


# ================= deterministic (mocked) write failure: ledger stays retryable =================
async def test_write_failure_mocked_leaves_pending_then_retry_stores(session):
    gw = await _mk_gateway(session, org_key="o1", site_key="s1", gateway_key="g1", client_id="c1")
    mid = uuid.uuid4()
    _, token = await _claim(session, gw.id, mid)

    class BoomGateway:
        async def write(self, points, index):
            raise RuntimeError("influx unavailable")

    now = datetime.now(timezone.utc)
    with pytest.raises(RuntimeError):
        await BoomGateway().write([_pt("pytest-x", "rack1", "src1", "rack_operating_state", 5, now)], _IDX)
    # mark_stored was NOT called after the failure -> row stays pending (retryable)
    assert (await _row(session, mid)).status == "pending"
    # a later successful attempt can still store (idempotent retry path)
    assert await mark_stored(session, gateway_id=gw.id, message_id=mid, token=token) is True
    assert (await _row(session, mid)).status == "stored"


# ================= Influx integration (dedicated telemetry_test bucket) =================
@pytest.fixture
def influx_test():
    base = get_settings()
    client = InfluxDBClient(url=base.influx_url, token=base.influx_token, org=base.influx_org)
    try:
        if not client.ping():
            pytest.skip("InfluxDB not reachable")
        buckets = client.buckets_api()
        if buckets.find_bucket_by_name(base.influx_bucket_test) is None:
            buckets.create_bucket(bucket_name=base.influx_bucket_test, org=base.influx_org)
        test_settings = base.model_copy(update={
            "influx_bucket_raw": base.influx_bucket_test,
            "influx_bucket_long": base.influx_bucket_test,
        })
        gw = InfluxGateway(settings=test_settings)
        # fail-fast: this gateway must only ever target the test bucket
        assert gw._settings.influx_bucket_raw == base.influx_bucket_test
        assert gw._settings.influx_bucket_long == base.influx_bucket_test
        yield gw, base.influx_bucket_test
    finally:
        client.close()


def _assert_test_bucket(gw, bucket):
    tb = get_settings().influx_bucket_test
    assert bucket == tb
    assert gw._settings.influx_bucket_raw == tb and gw._settings.influx_bucket_long == tb


async def _cleanup(gw, bucket, site, t0, t1):
    async with gw._client() as c:
        await c.delete_api().delete(t0 - timedelta(seconds=1), t1 + timedelta(seconds=1),
                                    f'site_id="{site}"', bucket=bucket, org=gw._org)


async def test_write_roundtrip_to_test_bucket(influx_test):
    gw, bucket = influx_test
    _assert_test_bucket(gw, bucket)
    site = f"pytest-{uuid.uuid4().hex[:8]}"; now = datetime.now(timezone.utc)
    try:
        await gw.write([_pt(site, "rack1", "src1", "rack_operating_state", 5, now)], _IDX)
        res = rows(await gw.latest_by_asset(bucket, "s2_bms_extended", site, "rack_operating_state"))
        assert len(res) == 1 and res[0]["value"] == 5
    finally:
        await _cleanup(gw, bucket, site, now, now)


async def test_happy_path_claim_write_mark_stored(influx_test, session):
    gw, bucket = influx_test
    _assert_test_bucket(gw, bucket)
    gwrow = await _mk_gateway(session, org_key="o1", site_key="s1", gateway_key="g1", client_id="c1")
    mid = uuid.uuid4()
    decision, token = await _claim(session, gwrow.id, mid)
    assert decision == IngestDecision.CLAIMED
    site = f"pytest-{uuid.uuid4().hex[:8]}"; now = datetime.now(timezone.utc)
    try:
        await gw.write([_pt(site, "rack1", "src1", "rack_operating_state", 7, now)], _IDX)
        assert await mark_stored(session, gateway_id=gwrow.id, message_id=mid, token=token) is True
        res = rows(await gw.latest_by_asset(bucket, "s2_bms_extended", site, "rack_operating_state"))
        assert len(res) == 1 and res[0]["value"] == 7
        assert (await _row(session, mid)).status == "stored"
    finally:
        await _cleanup(gw, bucket, site, now, now)


async def test_write_failure_real_missing_bucket_leaves_pending(influx_test, session):
    gw, bucket = influx_test               # fixture ensures Influx is reachable (else skip)
    base = get_settings()
    bogus = f"nonexistent-{uuid.uuid4().hex[:8]}"
    # the failure target must be a verified-absent, non-production bucket
    client = InfluxDBClient(url=base.influx_url, token=base.influx_token, org=base.influx_org)
    try:
        assert client.buckets_api().find_bucket_by_name(bogus) is None
    finally:
        client.close()
    assert bogus not in (base.influx_bucket_raw, base.influx_bucket_long, base.influx_bucket_test)
    bad_settings = base.model_copy(update={"influx_bucket_raw": bogus, "influx_bucket_long": bogus})
    bad_gw = InfluxGateway(settings=bad_settings)

    gwrow = await _mk_gateway(session, org_key="o1", site_key="s1", gateway_key="g1", client_id="c1")
    mid = uuid.uuid4()
    _, token = await _claim(session, gwrow.id, mid)
    now = datetime.now(timezone.utc)
    with pytest.raises(Exception):         # real write failure (missing bucket)
        await bad_gw.write([_pt("pytest-nowrite", "rack1", "src1", "rack_operating_state", 5, now)], _IDX)
    # mark_stored not called -> ledger retryable
    assert (await _row(session, mid)).status == "pending"
