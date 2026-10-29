"""Phase 9b-ii-c endpoint tests: gate, auth order, state matrix, failure/retry, isolation.

Default gate is OFF (503). Enabled tests flip it via a get_settings override and override
the registry-index + Influx providers. Real-Influx happy path uses telemetry_test only;
all other states use PG + a stub writer so they are deterministic and need no Influx.
"""
import types as _types
import gzip
import json
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
from src.ingestion.s3_health import S3HealthEnvelopeIn
from src.ingestion.health_models import GatewayHealthRecordModel
from src.ingestion.s4_alarms import S4AlarmEnvelopeIn
from src.ingestion.alarm_models import AlarmEventRecordModel
from src.ingestion.controller_models import ControllerStateRecordModel
from src.ingestion.solis_models import SolisTelemetryRecordModel
from src.ingestion.edge_ai_models import EdgeAiInferenceRecordModel
from src.ingestion.config_audit_models import ConfigurationAuditRecordModel
from src.ingestion.s8_config_audit import S8ConfigAuditEnvelopeIn
from src.ingestion.command_result_models import CommandResultRecordModel
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


async def test_disabled_gateway_403_precedes_gate(client, session):
    # get_current_gateway rejects a disabled gateway (403) before the ingest gate (503, default off).
    c, _ = client
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    await session.execute(update(GatewayModel).where(GatewayModel.id == gw.id).values(enabled=False))
    await session.commit()
    r = await c.post(URL, json=_env(org, site, gw), headers=_auth(gw))
    assert r.status_code == 403
    assert await _count(session) == 0          # rejected at auth; no ledger claim


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


# ===================== P3 characterization: retry idempotency + auth-order =====================
# CHARACTERIZATION of current endpoint behavior, not a permanent gateway contract.

async def test_retry_duplicate_ignores_hash_excluded_metadata(client, session, enabled):
    """P3 CHARACTERIZATION: once a row is STORED, re-POSTing the SAME (gateway_id, message_id)
    with only a hash-excluded transport field changed (gateway_software_version) returns
    200 status="duplicate" and performs NO second telemetry write. Retry idempotency is keyed
    on (gateway_id, message_id) + payload hash; transport metadata does not affect it.
    gateway_id/message_id are held constant (same ledger identity); site_id/organization_id are
    NOT varied here because a scope change would 403 before the ledger (see the auth-order test)."""
    c, _ = client; app = enabled
    stub = _StubInflux(); app.dependency_overrides[get_influx_gateway] = lambda: stub
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    env = _env(org, site, gw)
    h = canonical_hash(EnvelopeIn(**env).model_dump())
    session.add(IngestMessageModel(gateway_id=gw.id, message_id=uuid.UUID(env["message_id"]),
                                   stream="S2", sequence=1, record_count=1, payload_hash=h, status="stored"))
    await session.commit()
    # retry: same ledger identity, only a hash-excluded transport field differs
    retry = {**env, "gateway_software_version": "9.9.9"}
    assert canonical_hash(EnvelopeIn(**retry).model_dump()) == h      # metadata does NOT alter the hash
    r = await c.post(URL, json=retry, headers=_auth(gw))
    assert r.status_code == 200 and r.json()["status"] == "duplicate"
    assert stub.writes == []                     # no second telemetry write
    assert await _count(session) == 1            # still exactly the one pre-seeded row


async def test_scope_mismatch_403_precedes_payload_hash(client, session, enabled, monkeypatch):
    """P3 CHARACTERIZATION: a changed scope identifier returns 403 BEFORE any payload-hash
    comparison or ledger claim. canonical_hash and claim_or_resolve are patched to raise; a 403
    (not an AssertionError/500) proves verify_scope runs first. Complements test_scope_mismatch_403
    by pinning the ORDER, not just the status code."""
    c, _ = client; app = enabled
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    import src.ingestion.router as R

    def _boom_sync(*a, **k):
        raise AssertionError("payload hashing reached despite scope mismatch")

    async def _boom_async(*a, **k):
        raise AssertionError("ledger claim reached despite scope mismatch")

    monkeypatch.setattr(R, "canonical_hash", _boom_sync)
    monkeypatch.setattr(R, "claim_or_resolve", _boom_async)
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    env = _env(org, site, gw); env["organization_id"] = str(uuid.uuid4())   # scope identifier changed
    r = await c.post(URL, json=env, headers=_auth(gw))
    assert r.status_code == 403
    assert await _count(session) == 0            # no ledger claim created


# ===================== enabled: VALID gzip body decodes like identity =====================
async def test_gzip_body_decoded_matches_identity(client, session, enabled):
    # Positive counterpart to test_bad_gzip_400: a valid Content-Encoding: gzip body must
    # decode to the same envelope and canonical hash as the identity path. Proven via the
    # ledger duplicate match (same hash -> 200 'duplicate', no write); needs no Influx.
    c, _ = client; app = enabled
    stub = _StubInflux(); app.dependency_overrides[get_influx_gateway] = lambda: stub
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    env = _env(org, site, gw)
    h = canonical_hash(EnvelopeIn(**env).model_dump())
    session.add(IngestMessageModel(gateway_id=gw.id, message_id=uuid.UUID(env['message_id']),
                                   stream="S2", sequence=1, record_count=1, payload_hash=h, status="stored"))
    await session.commit()
    body = gzip.compress(json.dumps(env).encode("utf-8"))
    r = await c.post(URL, content=body, headers={**_auth(gw), "content-encoding": "gzip"})
    assert r.status_code == 200 and r.json()["status"] == "duplicate", r.text
    assert stub.writes == []


# ===================== enabled: S1 end-to-end (raw bucket) counterpart to the S2 happy path =====================
async def test_happy_path_s1_bess_fast_stored(client, session, enabled, real_influx):
    # S1 counterpart to test_happy_path_stored (S2). Confirms an S1/bess_fast record routes
    # through the endpoint to the T1_RAW_90D (raw) bucket, lands in Influx, and the ledger row
    # goes 'stored'. Uses a LOCAL S1 index (the shared TEST_INDEX is S2-only).
    c, _ = client; app = enabled
    gw_influx, bucket = real_influx
    app.dependency_overrides[get_influx_gateway] = lambda: gw_influx
    app.dependency_overrides[get_registry_index] = lambda: RegistryIndex.from_rows([
        ("S1", "bess_fast", "soc", "pcs_1", "Float", "T1_RAW_90D"),
    ])
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    now = datetime.now(timezone.utc)
    env = _env(org, site, gw, stream="S1", substream="bess_fast",
               records=[_rec("soc", 55.5, asset_id="pcs1", ts=now.isoformat())])
    try:
        r = await c.post(URL, json=env, headers=_auth(gw))
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["status"] == "stored" and body["record_count"] == 1
        res = rows(await gw_influx.latest_by_asset(bucket, "s1_bess_fast", str(site.id), "soc"))
        assert len(res) == 1 and res[0]["value"] == 55.5
        assert (await _row(session, env["message_id"])).status == "stored"
    finally:
        await _cleanup(gw_influx, bucket, str(site.id), now - timedelta(seconds=60), now + timedelta(seconds=60))


# ===================== S3 structured-record endpoint (gateway_health -> JSONB) =====================
_S3_REC = {
    "status": "ok", "timestamp_utc": "2026-09-04T08:00:00.340928+00:00",
    "gateway_mode": "ems_read_write", "source_count": 2, "asset_count": 18,
    "online_asset_count": 18, "total_signal_count": 2844, "bad_signal_count": 0,
    "commands_enabled": True, "control_enabled": True, "storage_can_write": True,
    "storage": {"enabled": True, "type": "sqlite", "path": None, "reasons": []},
}


def _s3env(org, site, gw, *, records=None, message_id=None):
    recs = records if records is not None else [dict(_S3_REC)]
    return {"schema_version": "1.0", "message_id": str(message_id or uuid.uuid4()),
            "gateway_id": str(gw.id), "site_id": str(site.id), "organization_id": str(org.id),
            "stream": "S3", "substream": "gateway", "sequence": 1,
            "created_at_utc": "2026-09-04T08:00:00+00:00", "record_count": len(recs), "records": recs}


async def _health_count(session):
    return (await session.execute(select(func.count()).select_from(GatewayHealthRecordModel))).scalar()


async def test_s3_gate_disabled_503(client, session):
    c, _ = client
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    r = await c.post(URL, json=_s3env(org, site, gw), headers=_auth(gw))
    assert r.status_code == 503
    assert await _count(session) == 0 and await _health_count(session) == 0


async def test_s3_disabled_gateway_403(client, session):
    c, _ = client
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    await session.execute(update(GatewayModel).where(GatewayModel.id == gw.id).values(enabled=False))
    await session.commit()
    r = await c.post(URL, json=_s3env(org, site, gw), headers=_auth(gw))
    assert r.status_code == 403
    assert await _count(session) == 0 and await _health_count(session) == 0


async def test_s3_valid_stored_200(client, session, enabled):
    c, _ = client; app = enabled
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    env = _s3env(org, site, gw)
    r = await c.post(URL, json=env, headers=_auth(gw))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "stored" and body["record_count"] == 1
    assert (await _row(session, env["message_id"])).status == "stored"
    assert await _health_count(session) == 1


async def test_s3_duplicate_200(client, session, enabled):
    c, _ = client; app = enabled
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    env = _s3env(org, site, gw)
    h = canonical_hash(S3HealthEnvelopeIn(**env).model_dump(mode="json"))
    session.add(IngestMessageModel(gateway_id=gw.id, message_id=uuid.UUID(env["message_id"]),
                                   stream="S3", sequence=1, record_count=1, payload_hash=h, status="stored"))
    await session.commit()
    r = await c.post(URL, json=env, headers=_auth(gw))
    assert r.status_code == 200 and r.json()["status"] == "duplicate"
    assert await _health_count(session) == 0          # duplicate -> no new JSONB write


async def test_s3_hash_mismatch_409(client, session, enabled):
    c, _ = client; app = enabled
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    env = _s3env(org, site, gw)
    session.add(IngestMessageModel(gateway_id=gw.id, message_id=uuid.UUID(env["message_id"]),
                                   stream="S3", sequence=1, record_count=1, payload_hash="DIFFERENT", status="stored"))
    await session.commit()
    r = await c.post(URL, json=env, headers=_auth(gw))
    assert r.status_code == 409
    assert await _health_count(session) == 0


async def test_s3_scope_mismatch_403(client, session, enabled):
    c, _ = client; app = enabled
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    env = _s3env(org, site, gw); env["organization_id"] = str(uuid.uuid4())
    r = await c.post(URL, json=env, headers=_auth(gw))
    assert r.status_code == 403
    assert await _count(session) == 0 and await _health_count(session) == 0


async def test_s3_malformed_422_no_claim(client, session, enabled):
    c, _ = client; app = enabled
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    bad = dict(_S3_REC); bad.pop("status")                  # missing required scalar -> fail before claim
    env = _s3env(org, site, gw, records=[bad])
    r = await c.post(URL, json=env, headers=_auth(gw))
    assert r.status_code == 422
    assert await _count(session) == 0 and await _health_count(session) == 0


# ===================== S4 alarms/events endpoint (events -> JSONB) =====================
def _s4rec(**over):
    r = {"event_id": str(uuid.uuid4()), "timestamp_utc": "2026-09-04T08:00:00.5+00:00",
         "gateway_id": "g1", "site_id": "s1", "source_id": "external_ems_1",
         "event_type": "BMS_CELL_OVERVOLTAGE_ACTIVE", "severity": "Critical", "state": "ACTIVE",
         "message": "Cell overvoltage", "dedupe_key": "bms1:overvoltage"}
    r.update(over); return r


def _s4env(org, site, gw, *, records=None, message_id=None):
    recs = records if records is not None else [_s4rec()]
    return {"schema_version": "1.0", "message_id": str(message_id or uuid.uuid4()),
            "gateway_id": str(gw.id), "site_id": str(site.id), "organization_id": str(org.id),
            "stream": "S4", "substream": "events", "sequence": 1,
            "created_at_utc": "2026-09-04T08:00:00+00:00", "record_count": len(recs), "records": recs}


async def _alarm_count(session):
    return (await session.execute(select(func.count()).select_from(AlarmEventRecordModel))).scalar()


async def test_s4_gate_disabled_503(client, session):
    c, _ = client
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    r = await c.post(URL, json=_s4env(org, site, gw), headers=_auth(gw))
    assert r.status_code == 503
    assert await _count(session) == 0 and await _alarm_count(session) == 0


async def test_s4_disabled_gateway_403(client, session):
    c, _ = client
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    await session.execute(update(GatewayModel).where(GatewayModel.id == gw.id).values(enabled=False))
    await session.commit()
    r = await c.post(URL, json=_s4env(org, site, gw), headers=_auth(gw))
    assert r.status_code == 403
    assert await _count(session) == 0 and await _alarm_count(session) == 0


async def test_s4_valid_stored_200(client, session, enabled):
    c, _ = client; app = enabled
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    env = _s4env(org, site, gw)
    r = await c.post(URL, json=env, headers=_auth(gw))
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "stored" and r.json()["record_count"] == 1
    assert (await _row(session, env["message_id"])).status == "stored"
    assert await _alarm_count(session) == 1


async def test_s4_duplicate_200(client, session, enabled):
    c, _ = client; app = enabled
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    env = _s4env(org, site, gw)
    h = canonical_hash(S4AlarmEnvelopeIn(**env).model_dump(mode="json"))
    session.add(IngestMessageModel(gateway_id=gw.id, message_id=uuid.UUID(env["message_id"]),
                                   stream="S4", sequence=1, record_count=1, payload_hash=h, status="stored"))
    await session.commit()
    r = await c.post(URL, json=env, headers=_auth(gw))
    assert r.status_code == 200 and r.json()["status"] == "duplicate"
    assert await _alarm_count(session) == 0          # duplicate -> no JSONB write


async def test_s4_hash_mismatch_409(client, session, enabled):
    c, _ = client; app = enabled
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    env = _s4env(org, site, gw)
    session.add(IngestMessageModel(gateway_id=gw.id, message_id=uuid.UUID(env["message_id"]),
                                   stream="S4", sequence=1, record_count=1, payload_hash="DIFFERENT", status="stored"))
    await session.commit()
    r = await c.post(URL, json=env, headers=_auth(gw))
    assert r.status_code == 409
    assert await _alarm_count(session) == 0


async def test_s4_scope_mismatch_403(client, session, enabled):
    c, _ = client; app = enabled
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    env = _s4env(org, site, gw); env["organization_id"] = str(uuid.uuid4())
    r = await c.post(URL, json=env, headers=_auth(gw))
    assert r.status_code == 403
    assert await _count(session) == 0 and await _alarm_count(session) == 0


async def test_s4_malformed_422_no_claim(client, session, enabled):
    c, _ = client; app = enabled
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    bad = _s4rec(); bad.pop("severity")                     # missing required enum -> fail before claim
    env = _s4env(org, site, gw, records=[bad])
    r = await c.post(URL, json=env, headers=_auth(gw))
    assert r.status_code == 422
    assert await _count(session) == 0 and await _alarm_count(session) == 0


# ===================== S5 remains DEFERRED (controller_state slice not wired to endpoint) =====================
def _s5env(org, site, gw, *, message_id=None):
    return {"schema_version": "1.0", "message_id": str(message_id or uuid.uuid4()),
            "gateway_id": str(gw.id), "site_id": str(site.id), "organization_id": str(org.id),
            "stream": "S5", "substream": "controller", "sequence": 1,
            "created_at_utc": "2026-09-04T08:00:00+00:00", "record_count": 1,
            "records": [{"timestamp_utc": "2026-09-04T08:00:05+00:00", "mode": "LIVE",
                         "healthy": True, "controller": {"state": "NORMAL"}}]}


async def _controller_count(session):
    return (await session.execute(select(func.count()).select_from(ControllerStateRecordModel))).scalar()


async def test_s5_deferred_501_no_write(client, session, enabled):
    # S5 persistence exists at the service layer but is NOT routed: endpoint must still 501,
    # with no ledger claim and no controller_state row.
    c, _ = client; app = enabled
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    r = await c.post(URL, json=_s5env(org, site, gw), headers=_auth(gw))
    assert r.status_code == 501
    assert await _count(session) == 0 and await _controller_count(session) == 0


# ===================== S6 remains DEFERRED (solis_telemetry slice not wired to endpoint) =====================
def _s6env(org, site, gw, *, message_id=None):
    return {"schema_version": "1.0", "message_id": str(message_id or uuid.uuid4()),
            "gateway_id": str(gw.id), "site_id": str(site.id), "organization_id": str(org.id),
            "stream": "S6", "substream": "solis", "sequence": 1,
            "created_at_utc": "2026-09-04T08:00:00+00:00", "record_count": 1,
            "records": [{"timestamp_utc": "2026-09-04T08:00:15+00:00",
                         "solis": {"online": True, "state": "ON", "power_mode": "DERATED"}}]}


async def _solis_count(session):
    return (await session.execute(select(func.count()).select_from(SolisTelemetryRecordModel))).scalar()


async def test_s6_deferred_501_no_write(client, session, enabled):
    # S6 persistence exists at the service layer but is NOT routed: endpoint must still 501,
    # with no ledger claim and no solis_telemetry row.
    c, _ = client; app = enabled
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    r = await c.post(URL, json=_s6env(org, site, gw), headers=_auth(gw))
    assert r.status_code == 501
    assert await _count(session) == 0 and await _solis_count(session) == 0


# ===================== S7 remains DEFERRED (edge_ai_inference slice not wired to endpoint) =====================
def _s7env(org, site, gw, *, message_id=None):
    return {"schema_version": "1.0", "message_id": str(message_id or uuid.uuid4()),
            "gateway_id": str(gw.id), "site_id": str(site.id), "organization_id": str(org.id),
            "stream": "S7", "substream": "per_bess", "sequence": 1,
            "created_at_utc": "2026-09-28T04:58:23+00:00", "record_count": 1,
            "records": [{"phase": "8", "cycle_time": "2026-09-28T04:58:23+00:00", "cycle_index": 14533,
                         "model_info": {"model_type": "IsolationForest", "feature_count": 33}}]}


async def _edge_ai_count(session):
    return (await session.execute(select(func.count()).select_from(EdgeAiInferenceRecordModel))).scalar()


async def test_s7_deferred_501_no_write(client, session, enabled):
    # S7 persistence exists at the service layer but is NOT routed: endpoint must still 501,
    # with no ledger claim and no edge_ai_inference row.
    c, _ = client; app = enabled
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    r = await c.post(URL, json=_s7env(org, site, gw), headers=_auth(gw))
    assert r.status_code == 501
    assert await _count(session) == 0 and await _edge_ai_count(session) == 0


# ===================== S8 configuration_audit endpoint (ACTIVE -> JSONB) =====================
def _s8rec(**over):
    r = {"audit_id": str(uuid.uuid4()), "timestamp_utc": "2026-10-12T04:21:09.512000+00:00",
         "gateway_id": "g1", "site_id": "s1", "actor": "kedar@ornatesolar.com",
         "actor_role": "admin", "change_source": "central_command",
         "setting_path": "controller.high_limit", "old_value": 85, "new_value": 90,
         "revision": 42, "reason": "raised ceiling", "correlation_id": "cmd-7781"}
    r.update(over); return r


def _s8env(org, site, gw, *, records=None, message_id=None):
    recs = records if records is not None else [_s8rec()]
    return {"schema_version": "1.0", "message_id": str(message_id or uuid.uuid4()),
            "gateway_id": str(gw.id), "site_id": str(site.id), "organization_id": str(org.id),
            "stream": "S8", "substream": "config_audit", "sequence": 1,
            "created_at_utc": "2026-10-12T04:21:09+00:00", "record_count": len(recs), "records": recs}


async def _audit_count(session):
    return (await session.execute(select(func.count()).select_from(ConfigurationAuditRecordModel))).scalar()


async def test_s8_gate_disabled_503(client, session):
    c, _ = client
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    r = await c.post(URL, json=_s8env(org, site, gw), headers=_auth(gw))
    assert r.status_code == 503
    assert await _count(session) == 0 and await _audit_count(session) == 0


async def test_s8_disabled_gateway_403(client, session):
    c, _ = client
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    await session.execute(update(GatewayModel).where(GatewayModel.id == gw.id).values(enabled=False))
    await session.commit()
    r = await c.post(URL, json=_s8env(org, site, gw), headers=_auth(gw))
    assert r.status_code == 403
    assert await _count(session) == 0 and await _audit_count(session) == 0


async def test_s8_valid_stored_200(client, session, enabled):
    c, _ = client; app = enabled
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    env = _s8env(org, site, gw)
    r = await c.post(URL, json=env, headers=_auth(gw))
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "stored" and r.json()["record_count"] == 1
    assert (await _row(session, env["message_id"])).status == "stored"
    assert await _audit_count(session) == 1


async def test_s8_duplicate_200(client, session, enabled):
    c, _ = client; app = enabled
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    env = _s8env(org, site, gw)
    h = canonical_hash(S8ConfigAuditEnvelopeIn(**env).model_dump(mode="json"))
    session.add(IngestMessageModel(gateway_id=gw.id, message_id=uuid.UUID(env["message_id"]),
                                   stream="S8", sequence=1, record_count=1, payload_hash=h, status="stored"))
    await session.commit()
    r = await c.post(URL, json=env, headers=_auth(gw))
    assert r.status_code == 200 and r.json()["status"] == "duplicate"
    assert await _audit_count(session) == 0          # duplicate -> no JSONB write


async def test_s8_hash_mismatch_409(client, session, enabled):
    c, _ = client; app = enabled
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    env = _s8env(org, site, gw)
    session.add(IngestMessageModel(gateway_id=gw.id, message_id=uuid.UUID(env["message_id"]),
                                   stream="S8", sequence=1, record_count=1, payload_hash="DIFFERENT", status="stored"))
    await session.commit()
    r = await c.post(URL, json=env, headers=_auth(gw))
    assert r.status_code == 409
    assert await _audit_count(session) == 0


async def test_s8_scope_mismatch_403(client, session, enabled):
    c, _ = client; app = enabled
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    env = _s8env(org, site, gw); env["organization_id"] = str(uuid.uuid4())
    r = await c.post(URL, json=env, headers=_auth(gw))
    assert r.status_code == 403
    assert await _count(session) == 0 and await _audit_count(session) == 0


async def test_s8_malformed_enum_422_no_claim(client, session, enabled):
    c, _ = client; app = enabled
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    env = _s8env(org, site, gw, records=[_s8rec(change_source="model_update")])  # Option A reject
    r = await c.post(URL, json=env, headers=_auth(gw))
    assert r.status_code == 422
    assert await _count(session) == 0 and await _audit_count(session) == 0


async def test_s8_persistence_failure_503(client, session, enabled, monkeypatch):
    # Write failure after validation+claim -> 503; no audit row persisted (message left retryable).
    c, _ = client; app = enabled
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    import src.ingestion.config_audit_service as CAS

    async def _boom(*a, **k):
        raise RuntimeError("db down")

    monkeypatch.setattr(CAS, "_upsert_rows", _boom)
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    r = await c.post(URL, json=_s8env(org, site, gw), headers=_auth(gw))
    assert r.status_code == 503
    assert await _audit_count(session) == 0


# ===================== S9 remains DEFERRED (command_results slice not wired to endpoint) =====================
def _s9env(org, site, gw, *, message_id=None):
    return {"schema_version": "1.0", "message_id": str(message_id or uuid.uuid4()),
            "gateway_id": str(gw.id), "site_id": str(site.id), "organization_id": str(org.id),
            "stream": "S9", "substream": "commands", "sequence": 1,
            "created_at_utc": "2026-10-12T04:21:09+00:00", "record_count": 1,
            "records": [{"command_id": str(uuid.uuid4()), "gateway_id": "g1", "site_id": "s1",
                         "requested_at_utc": "2026-10-12T04:21:09+00:00",
                         "received_at_utc": "2026-10-12T04:21:10+00:00",
                         "requested_by": "u", "requested_role": "admin", "command_type": "noop",
                         "requested_value": {}, "gateway_validation_ok": True,
                         "readback_requested": False, "verified": None, "final_status": "accepted"}]}


async def _command_result_count(session):
    return (await session.execute(select(func.count()).select_from(CommandResultRecordModel))).scalar()


async def test_s9_deferred_501_no_write(client, session, enabled):
    # S9 persistence exists at the service layer but is NOT routed: endpoint must still 501,
    # with no ledger claim and no command_result row.
    c, _ = client; app = enabled
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    org, site, gw = await _mk(session, o="o1", s="s1", g="g1", c="c1")
    r = await c.post(URL, json=_s9env(org, site, gw), headers=_auth(gw))
    assert r.status_code == 501
    assert await _count(session) == 0 and await _command_result_count(session) == 0
