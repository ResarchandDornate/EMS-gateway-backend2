"""Phase 9b-ii-c endpoint tests: gate, auth order, state matrix, failure/retry, isolation.

Default gate is OFF (503). Enabled tests flip it via a get_settings override and override
the registry-index + Influx providers. Real-Influx happy path uses telemetry_test only;
all other states use PG + a stub writer so they are deterministic and need no Influx.
"""
import types as _types
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from influxdb_client import InfluxDBClient, Point
from sqlalchemy import func, select, update

from src.auth.security import create_gateway_token, hash_password
from src.config import get_settings
from src.influx import InfluxGateway, RegistryIndex, rows
from src.ingestion.models import IngestMessageModel
from src.ingestion.providers import (get_influx_gateway, get_registry_index,
                                      reset_registry_cache)
from src.ingestion.schemas import EnvelopeIn
from src.ingestion.service import canonical_hash
from src.tenancy.gateways.models import GatewayModel
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel
from src.telemetry_errors import UnsafeQueryInput

URL = "/api/v1/gateway/ingest"

TEST_INDEX = RegistryIndex.from_rows([
    ("S2", "bms_extended", "rack_operating_state", "rack", "U16", "T2_1Y"),
    ("S2", "bms_extended", "mcb_feedback", "rack", "U16", "T2_1Y"),
    ("S2", "bms_extended", "rack_charge_energy_session", "rack", "FLOAT32", "T3_3Y"),
])


# -------- autouse: never let the real registry cache leak across tests --------
@pytest.fixture(autouse=True)
def _reset_registry():
    reset_registry_cache()
    yield
    reset_registry_cache()


# -------- helpers --------
async def _mk(session, *, o, s, g, c):
    org = OrganizationModel(org_key=o, name="Org"); session.add(org); await session.flush()
    site = SiteModel(organization_id=org.id, site_key=s, name="Site"); session.add(site); await session.flush()
    gw = GatewayModel(site_id=site.id, gateway_key=g, client_id=c,
                      client_secret_hash=hash_password("x"), enabled=True)
    session.add(gw); await session.commit()
    return org, site, gw


def _auth(gw):
    return {"Authorization": f"Bearer {create_gateway_token(gw.id)}"}


def _rec(signal_key, value, *, source_id="src1", asset_id="rack1", quality="good",
         ts="2026-09-04T07:59:14+00:00"):
    return {"source_id": source_id, "asset_id": asset_id, "signal_key": signal_key,
            "value": value, "quality": quality, "timestamp": ts}


def _env(org, site, gw, *, stream="S2", substream="bms_extended", records=None, message_id=None):
    recs = records if records is not None else [_rec("rack_operating_state", 5)]
    return {"schema_version": "1.0", "message_id": str(message_id or uuid.uuid4()),
            "gateway_id": str(gw.id), "site_id": str(site.id), "organization_id": str(org.id),
            "stream": stream, "substream": substream, "sequence": 1,
            "created_at_utc": "2026-09-04T07:59:14+00:00", "record_count": len(recs), "records": recs}


class _StubInflux:
    """In-process writer: confines routing to telemetry_test via its settings; records or
    fails writes. Used for deterministic PG-only state/failure tests (no Influx server)."""
    def __init__(self, fail_write=False):
        self._s = _types.SimpleNamespace(influx_bucket_raw="telemetry_test",
                                         influx_bucket_long="telemetry_test",
                                         influx_bucket_3y="telemetry_test")
        self.fail_write = fail_write
        self.writes = []

    @property
    def settings(self):
        return self._s

    async def write_built(self, built):
        if self.fail_write:
            raise RuntimeError("influx down")
        self.writes.append(built)


@pytest.fixture
def enabled():
    from src.main import app
    enabled_settings = get_settings().model_copy(update={"ingest_enabled": True})
    app.dependency_overrides[get_settings] = lambda: enabled_settings
    app.dependency_overrides[get_registry_index] = lambda: TEST_INDEX
    reset_registry_cache()
    yield app
    for dep in (get_settings, get_registry_index, get_influx_gateway):
        app.dependency_overrides.pop(dep, None)
    reset_registry_cache()


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
                                     "influx_bucket_long": base.influx_bucket_test,
                                     "influx_bucket_3y": base.influx_bucket_test})
        gw = InfluxGateway(settings=ts)
        assert gw.settings.influx_bucket_raw == base.influx_bucket_test
        assert gw.settings.influx_bucket_long == base.influx_bucket_test
        yield gw, base.influx_bucket_test
    finally:
        client.close()


async def _cleanup(gw, bucket, site, t0, t1):
    async with gw._client() as c:
        await c.delete_api().delete(t0 - timedelta(seconds=1), t1 + timedelta(seconds=1),
                                    f'site_id="{site}"', bucket=bucket, org=gw._org)


async def _count(session):
    return (await session.execute(select(func.count()).select_from(IngestMessageModel))).scalar()


async def _row(session, mid):
    return (await session.execute(
        select(IngestMessageModel).where(IngestMessageModel.message_id == uuid.UUID(mid)))).scalar_one()


# ===================== pure guard (no Influx server) =====================
async def test_write_built_rejects_unconfigured_bucket():
    s = get_settings().model_copy(update={"influx_bucket_raw": "telemetry_test",
                                          "influx_bucket_long": "telemetry_test"})
    gw = InfluxGateway(settings=s)
    with pytest.raises(UnsafeQueryInput):
        await gw.write_built([("telemetry_1y", Point("s2_bms_extended"))])   # raises before any I/O


# ===================== auth order + gate disabled (default) =====================
async def test_no_token_gate_disabled_401(client, session):
    c, _ = client
    r = await c.post(URL, json={})
    assert r.status_code == 401


async def test_invalid_token_gate_disabled_401(client, session):
    c, _ = client
    r = await c.post(URL, json={}, headers={"Authorization": "Bearer garbage"})
    assert r.status_code == 401


async def test_valid_token_gate_disabled_503_no_downstream_work(client, session, monkeypatch):
    c, _ = client
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    import src.ingestion.router as R

    def _boom_sync(*a, **k):
        raise AssertionError("downstream called while gate disabled")

    async def _boom_async(*a, **k):
        raise AssertionError("downstream called while gate disabled")

    monkeypatch.setattr(R, "decode_body", _boom_sync)
    monkeypatch.setattr(R, "build_message_points", _boom_sync)
    monkeypatch.setattr(R, "canonical_hash", _boom_sync)
    monkeypatch.setattr(R, "claim_or_resolve", _boom_async)
    r = await c.post(URL, json=_env(org, site, gw), headers=_auth(gw))
    assert r.status_code == 503
    assert await _count(session) == 0          # no ledger claim


# ===================== enabled: happy path (real Influx, telemetry_test) =====================
async def test_happy_path_stored(client, session, enabled, real_influx):
    c, _ = client
    app = enabled
    gw_influx, bucket = real_influx
    app.dependency_overrides[get_influx_gateway] = lambda: gw_influx
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    now = datetime.now(timezone.utc)
    # record timestamp = now so the point lands inside latest_by_asset's default -1h window
    env = _env(org, site, gw, records=[_rec("rack_operating_state", 7, ts=now.isoformat())])
    try:
        r = await c.post(URL, json=env, headers=_auth(gw))
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["status"] == "stored" and body["record_count"] == 1
        res = rows(await gw_influx.latest_by_asset(bucket, "s2_bms_extended", str(site.id), "rack_operating_state"))
        assert len(res) == 1 and res[0]["value"] == 7
        assert (await _row(session, env["message_id"])).status == "stored"
    finally:
        await _cleanup(gw_influx, bucket, str(site.id), now - timedelta(seconds=60), now + timedelta(seconds=60))


# ===================== enabled: ledger states (PG + stub writer) =====================
async def test_duplicate_stored_no_write(client, session, enabled):
    c, _ = client; app = enabled
    stub = _StubInflux(); app.dependency_overrides[get_influx_gateway] = lambda: stub
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    env = _env(org, site, gw)
    h = canonical_hash(EnvelopeIn(**env).model_dump())
    session.add(IngestMessageModel(gateway_id=gw.id, message_id=uuid.UUID(env["message_id"]),
                                   stream="S2", sequence=1, record_count=1, payload_hash=h, status="stored"))
    await session.commit()
    r = await c.post(URL, json=env, headers=_auth(gw))
    assert r.status_code == 200 and r.json()["status"] == "duplicate"
    assert stub.writes == []


async def test_hash_mismatch_409_no_write(client, session, enabled):
    c, _ = client; app = enabled
    stub = _StubInflux(); app.dependency_overrides[get_influx_gateway] = lambda: stub
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    env = _env(org, site, gw)
    session.add(IngestMessageModel(gateway_id=gw.id, message_id=uuid.UUID(env["message_id"]),
                                   stream="S2", sequence=1, record_count=1, payload_hash="DIFFERENT", status="stored"))
    await session.commit()
    r = await c.post(URL, json=env, headers=_auth(gw))
    assert r.status_code == 409
    assert stub.writes == []


async def test_in_progress_409_no_write(client, session, enabled):
    c, _ = client; app = enabled
    stub = _StubInflux(); app.dependency_overrides[get_influx_gateway] = lambda: stub
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    env = _env(org, site, gw)
    h = canonical_hash(EnvelopeIn(**env).model_dump())
    session.add(IngestMessageModel(gateway_id=gw.id, message_id=uuid.UUID(env["message_id"]),
                                   stream="S2", sequence=1, record_count=1, payload_hash=h, status="pending",
                                   lease_owner=uuid.uuid4(),
                                   lease_expires_at=datetime.now(timezone.utc) + timedelta(minutes=5)))
    await session.commit()
    r = await c.post(URL, json=env, headers=_auth(gw))
    assert r.status_code == 409
    assert stub.writes == []


# ===================== enabled: rejections (no claim, no write) =====================
async def test_scope_mismatch_403(client, session, enabled):
    c, _ = client; app = enabled
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    env = _env(org, site, gw); env["organization_id"] = str(uuid.uuid4())
    r = await c.post(URL, json=env, headers=_auth(gw))
    assert r.status_code == 403
    assert await _count(session) == 0


async def test_deferred_stream_501(client, session, enabled):
    c, _ = client; app = enabled
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    env = _env(org, site, gw, stream="S5", substream="x", records=[_rec("whatever", 1)])
    r = await c.post(URL, json=env, headers=_auth(gw))
    assert r.status_code == 501
    assert await _count(session) == 0


async def test_unknown_stream_422(client, session, enabled):
    c, _ = client; app = enabled
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    env = _env(org, site, gw, stream="S42", substream="x", records=[_rec("whatever", 1)])
    r = await c.post(URL, json=env, headers=_auth(gw))
    assert r.status_code == 422
    assert await _count(session) == 0


async def test_invalid_telemetry_422_no_claim(client, session, enabled):
    c, _ = client; app = enabled
    stub = _StubInflux(); app.dependency_overrides[get_influx_gateway] = lambda: stub
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    env = _env(org, site, gw, records=[_rec("not_a_signal", 1)])
    r = await c.post(URL, json=env, headers=_auth(gw))
    assert r.status_code == 422
    assert await _count(session) == 0 and stub.writes == []


async def test_t3_stored_200(client, session, enabled):
    # A T3 signal no longer 501s; it is claimed and written to the configured 3-year bucket,
    # NOT raw or long. Distinct stub bucket names make routing unambiguous. Unrelated
    # validation/auth/idempotency behavior is unchanged.
    c, _ = client; app = enabled
    stub = _StubInflux()
    stub.settings.influx_bucket_raw = "telemetry_raw_test"
    stub.settings.influx_bucket_long = "telemetry_long_test"
    stub.settings.influx_bucket_3y = "telemetry_3y_test"
    app.dependency_overrides[get_influx_gateway] = lambda: stub
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    env = _env(org, site, gw, records=[_rec("rack_charge_energy_session", 1.0)])
    r = await c.post(URL, json=env, headers=_auth(gw))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "stored" and body["record_count"] == 1
    assert await _count(session) == 1
    assert len(stub.writes) == 1
    assert stub.writes[0][0][0] == "telemetry_3y_test"
    assert stub.writes[0][0][0] not in ("telemetry_raw_test", "telemetry_long_test")


async def test_malformed_json_400(client, session, enabled):
    c, _ = client; app = enabled
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    r = await c.post(URL, content=b"not json", headers=_auth(gw))
    assert r.status_code == 400
    assert await _count(session) == 0


async def test_bad_gzip_400(client, session, enabled):
    c, _ = client; app = enabled
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    r = await c.post(URL, content=b"not gzip", headers={**_auth(gw), "content-encoding": "gzip"})
    assert r.status_code == 400
    assert await _count(session) == 0


# ===================== enabled: infra failure + retry =====================
async def test_influx_write_failure_503_pending(client, session, enabled):
    c, _ = client; app = enabled
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux(fail_write=True)
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    env = _env(org, site, gw)
    r = await c.post(URL, json=env, headers=_auth(gw))
    assert r.status_code == 503
    assert (await _row(session, env["message_id"])).status == "pending"


async def test_mark_stored_false_503(client, session, enabled, monkeypatch):
    c, _ = client; app = enabled
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    import src.ingestion.router as R

    async def _false(*a, **k):
        return False

    monkeypatch.setattr(R, "mark_stored", _false)
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    env = _env(org, site, gw)
    r = await c.post(URL, json=env, headers=_auth(gw))
    assert r.status_code == 503
    assert (await _row(session, env["message_id"])).status == "pending"


async def test_retry_after_write_failure_converges(client, session, enabled):
    c, _ = client; app = enabled
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    env = _env(org, site, gw)
    # attempt 1: Influx down -> 503, row claimed pending
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux(fail_write=True)
    r1 = await c.post(URL, json=env, headers=_auth(gw))
    assert r1.status_code == 503
    assert (await _row(session, env["message_id"])).status == "pending"
    # simulate the lease expiring (recovery window)
    await session.execute(update(IngestMessageModel)
                          .where(IngestMessageModel.message_id == uuid.UUID(env["message_id"]))
                          .values(lease_expires_at=datetime.now(timezone.utc) - timedelta(minutes=1)))
    await session.commit()
    # attempt 2: Influx healthy -> reclaim, write, store
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    r2 = await c.post(URL, json=env, headers=_auth(gw))
    assert r2.status_code == 200 and r2.json()["status"] == "stored"
    assert (await _row(session, env["message_id"])).status == "stored"
