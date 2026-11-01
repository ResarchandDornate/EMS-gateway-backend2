"""ARCHITECTURE CONFORMANCE: every gateway stream lands in its correct store.

Decided architecture:
  S1  fast_bess_telemetry        -> PostgreSQL JSONB  (fast_bess_records)   [mixed-type]
  S2  general_asset_telemetry    -> InfluxDB ONLY      (time-series)         [numeric]
  S3  gateway_health             -> PostgreSQL JSONB  (gateway_health_records)     [dedicated]
  S4  alarms_events              -> PostgreSQL JSONB  (alarm_event_records)        [dedicated]
  S5  soc_controller             -> PostgreSQL JSONB  (controller_state_records)   [dedicated]
  S6  solis                      -> PostgreSQL JSONB  (solis_telemetry_records)    [dedicated]
  S7  edge_ai                    -> PostgreSQL JSONB  (edge_ai_inference_records)  [dedicated]
  S8  configuration_audit        -> PostgreSQL JSONB  (configuration_audit_records)[dedicated]
  S9  command_results            -> PostgreSQL JSONB  (command_result_records)     [dedicated]
  S10 battery_charge_discharge   -> PostgreSQL JSONB  (stream_records)  [generic; unconfirmed]
  S2/combiner_box_meter          -> EXCLUDED (not stored)
  anything else                  -> rejected, retryable (kept by gateway)

Every test asserts the record is in the RIGHT store AND absent from the wrong ones.
Gate OFF by default; flipped via a get_settings override. Influx is an in-process stub
that records writes, so no Influx server is needed.
"""
import types as _types
import uuid

import pytest
from sqlalchemy import func, select

from src.auth.security import create_gateway_token, hash_password
from src.config import get_settings
from src.influx import RegistryIndex
from src.ingestion.models import IngestMessageModel
from src.ingestion.fast_bess_models import FastBessRecordModel
from src.ingestion.stream_models import StreamRecordModel
from src.ingestion.health_models import GatewayHealthRecordModel
from src.ingestion.alarm_models import AlarmEventRecordModel
from src.ingestion.controller_models import ControllerStateRecordModel
from src.ingestion.solis_models import SolisTelemetryRecordModel
from src.ingestion.edge_ai_models import EdgeAiInferenceRecordModel
from src.ingestion.config_audit_models import ConfigurationAuditRecordModel
from src.ingestion.command_result_models import CommandResultRecordModel
from src.ingestion.providers import (get_influx_gateway, get_registry_index,
                                      reset_registry_cache)
from src.tenancy.gateways.models import GatewayModel
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel

URL = "/api/v1/gateway/ingest"
TS = "2026-09-04T07:59:14+00:00"
TS_Z = "2026-10-27T06:00:00.123456Z"


@pytest.fixture(autouse=True)
def _reset_registry():
    reset_registry_cache()
    yield
    reset_registry_cache()


class _StubInflux:
    def __init__(self):
        self._s = _types.SimpleNamespace(influx_bucket_raw="telemetry_test",
                                         influx_bucket_long="telemetry_test",
                                         influx_bucket_3y="telemetry_test")
        self.writes = []

    @property
    def settings(self):
        return self._s

    async def write_built(self, built):
        self.writes.append(built)


@pytest.fixture
def enabled():
    from src.main import app
    enabled_settings = get_settings().model_copy(update={"ingest_enabled": True})
    stub = _StubInflux()
    app.dependency_overrides[get_settings] = lambda: enabled_settings
    app.dependency_overrides[get_registry_index] = lambda: RegistryIndex.from_rows([])
    app.dependency_overrides[get_influx_gateway] = lambda: stub
    reset_registry_cache()
    yield app, stub
    for dep in (get_settings, get_registry_index, get_influx_gateway):
        app.dependency_overrides.pop(dep, None)
    reset_registry_cache()


async def _mk(session, *, o="o1", s="s1", g="g1", c="c1"):
    org = OrganizationModel(org_key=o, name="Org"); session.add(org); await session.flush()
    site = SiteModel(organization_id=org.id, site_key=s, name="Site"); session.add(site); await session.flush()
    gw = GatewayModel(site_id=site.id, gateway_key=g, client_id=c,
                      client_secret_hash=hash_password("x"), enabled=True)
    session.add(gw); await session.commit()
    return org, site, gw


def _auth(gw):
    return {"Authorization": f"Bearer {create_gateway_token(gw.id)}"}


# --- message builders (as the real gateway sends) ---
def _sig(value, quality="good"):
    return {"value": value, "unit": None, "quality": quality, "category": "normalized_fast",
            "display_name": None, "updated_utc": TS_Z, "age_ms": None}


def _s1_rec():
    return {"record_type": "fast_bess_sample", "timestamp_utc": TS_Z,
            "source_id": "external_ems_1", "bess_id": "bess_1",
            "pcs_asset_id": "ems1_pcs", "bms_asset_id": "ems1_bms", "quality": "good",
            "pcs_values": {"running": _sig(False), "active_power_kw": _sig(0.0),
                           "charging": _sig(None)},
            "bms_values": {"state": _sig("Normal"), "blocking_reasons": _sig([]),
                           "soh": _sig(98.2)}}


def _s2_rec(signal="rack_operating_state", value=2):
    return {"record_type": "general_asset_signal", "source_id": "lineage_source",
            "runtime_asset_id": "rack_1", "signal": signal, "value": value,
            "unit": None, "quality": "good", "source_updated_utc": TS_Z}


def _generic_rec():
    return {"source_id": "src", "timestamp_utc": TS_Z, "flag": True, "note": "hello",
            "nested": {"a": 1}, "list": [1, 2], "null_field": None}


def _s3_valid():
    return {"status": "ok", "timestamp_utc": TS_Z, "gateway_mode": "ems_read_write",
            "source_count": 2, "asset_count": 18, "online_asset_count": 18,
            "total_signal_count": 2844, "bad_signal_count": 0, "commands_enabled": True,
            "control_enabled": True, "storage_can_write": True,
            "storage": {"enabled": True, "type": "sqlite", "reasons": []}}

def _s4_valid():
    return {"event_id": str(uuid.uuid4()), "timestamp_utc": TS_Z, "gateway_id": "k",
            "site_id": "k", "source_id": "pcs1", "event_type": "over_voltage",
            "severity": "Critical", "state": "ACTIVE", "message": "x", "dedupe_key": "ov:ACTIVE"}

def _s5_valid():
    return {"timestamp_utc": TS_Z, "mode": "auto", "healthy": True,
            "controller": {"state": "IDLE", "previous_state": "CHARGING", "decision": "hold"}}

def _s6_valid():
    return {"timestamp_utc": TS_Z, "solis": {"state": "running"}, "power": 12.3}

def _s7_valid():
    return {"cycle_time": TS_Z, "model_info": {"name": "anom-v1"}, "sources": [], "key_features": {}}

def _s8_valid():
    return {"audit_id": str(uuid.uuid4()), "timestamp_utc": TS_Z, "gateway_id": "k", "site_id": "k",
            "actor": "admin", "actor_role": "org_admin", "change_source": "local_api",
            "setting_path": "soc.max", "old_value": 80, "new_value": 90, "revision": 3,
            "reason": None, "correlation_id": None}

def _s9_valid():
    return {"command_id": str(uuid.uuid4()), "gateway_id": "k", "site_id": "k",
            "requested_at_utc": TS_Z, "received_at_utc": TS_Z, "requested_by": "admin",
            "requested_role": "org_admin", "command_type": "set_soc_limit", "requested_value": 90,
            "gateway_validation_ok": True, "readback_requested": False, "verified": None,
            "final_status": "accepted"}


def _msg(stream, substream, records, *, message_id=None, sequence=0):
    m = {"schema_version": "1.0", "message_id": str(message_id or uuid.uuid4()),
         "gateway_id": "kalpa-ems-gateway-001", "site_id": "kalpa",  # ignored
         "stream": stream, "sequence": sequence, "created_at_utc": TS,
         "record_count": len(records), "priority": "P2", "records": records}
    if substream is not None:
        m["substream"] = substream
    return m


def _batch(messages):
    return {"request_id": "req-1", "gateway_id": "kalpa-ems-gateway-001",
            "sent_at_utc": TS, "messages": messages}


async def _ledger(session):
    return (await session.execute(select(func.count()).select_from(IngestMessageModel))).scalar()

async def _fb(session):
    return (await session.execute(select(FastBessRecordModel))).scalars().all()

async def _sr(session):
    return (await session.execute(select(StreamRecordModel))).scalars().all()

def _points(stub):
    return [p for w in stub.writes for p in w]


# ===================== S1 -> PostgreSQL JSONB (not Influx) =====================

async def test_S1_fast_bess_goes_to_postgres_jsonb_only(client, session, enabled):
    c, _ = client
    app, stub = enabled
    org, site, gw = await _mk(session)
    mid = uuid.uuid4()
    r = await c.post(URL, json=_batch([_msg("fast_bess_telemetry", "bess_fast", [_s1_rec()],
                                            message_id=mid)]), headers=_auth(gw))
    assert r.status_code == 200, r.text
    assert r.json()["accepted"] == [{"message_id": str(mid), "status": "accepted"}]
    fb = await _fb(session)
    assert len(fb) == 1                           # row exists in fast_bess_records
    assert fb[0].gateway_id == gw.id and fb[0].payload["bms_values"]["state"]["value"] == "Normal"
    assert len(await _sr(session)) == 0          # NOT in the generic Postgres table
    assert _points(stub) == []                   # NOT in Influx


# ===================== S2 -> InfluxDB only (not Postgres rows) =====================

async def test_S2_telemetry_goes_to_influx_only(client, session, enabled):
    c, _ = client
    app, stub = enabled
    org, site, gw = await _mk(session)
    mid = uuid.uuid4()
    r = await c.post(URL, json=_batch([_msg("general_asset_telemetry", "bms_extended",
                                            [_s2_rec(), _s2_rec("soc", 55)], message_id=mid)]),
                     headers=_auth(gw))
    assert r.status_code == 200, r.text
    assert r.json()["accepted"] == [{"message_id": str(mid), "status": "accepted"}]
    assert len(_points(stub)) == 2               # two numeric signals -> two Influx points
    assert len(await _sr(session)) == 0          # NOT in Postgres stream_records
    assert len(await _fb(session)) == 0          # NOT in fast_bess
    assert await _ledger(session) == 1           # ledger still records it (idempotency)


# ===================== S3-S9 -> dedicated per-stream tables (not generic, not Influx) =====================

_DEDICATED = {
    "S3": ("gateway_health", "gateway", _s3_valid, GatewayHealthRecordModel),
    "S4": ("alarms_events", "events", _s4_valid, AlarmEventRecordModel),
    "S5": ("soc_controller", "controller", _s5_valid, ControllerStateRecordModel),
    "S6": ("solis", "solis", _s6_valid, SolisTelemetryRecordModel),
    "S7": ("edge_ai", "per_bess", _s7_valid, EdgeAiInferenceRecordModel),
    "S8": ("configuration_audit", "gateway", _s8_valid, ConfigurationAuditRecordModel),
    "S9": ("command_results", "commands", _s9_valid, CommandResultRecordModel),
}


async def _count(session, model):
    return (await session.execute(select(func.count()).select_from(model))).scalar()


@pytest.mark.parametrize("code", list(_DEDICATED))
async def test_structured_streams_go_to_dedicated_tables(client, session, enabled, code):
    c, _ = client
    app, stub = enabled
    org, site, gw = await _mk(session)
    legacy, substream, make, model = _DEDICATED[code]
    mid = uuid.uuid4()
    r = await c.post(URL, json=_batch([_msg(legacy, substream, [make()], message_id=mid)]),
                     headers=_auth(gw))
    assert r.status_code == 200, r.text
    assert r.json()["accepted"] == [{"message_id": str(mid), "status": "accepted"}], r.json()
    assert await _count(session, model) == 1          # landed in its DEDICATED table
    assert len(await _sr(session)) == 0               # NOT in generic stream_records
    assert len(await _fb(session)) == 0               # NOT in fast_bess
    assert _points(stub) == []                        # NOT in Influx


async def test_S10_unconfirmed_stays_in_stream_records(client, session, enabled):
    c, _ = client
    app, stub = enabled
    org, site, gw = await _mk(session)
    mid = uuid.uuid4()
    r = await c.post(URL, json=_batch([_msg("battery_charge_discharge_control", "pair_controller",
                                            [_generic_rec()], message_id=mid)]), headers=_auth(gw))
    assert r.status_code == 200, r.text
    assert r.json()["accepted"] == [{"message_id": str(mid), "status": "accepted"}]
    sr = await _sr(session)
    assert len(sr) == 1 and sr[0].stream == "S10" and sr[0].payload["flag"] is True  # verbatim
    assert _points(stub) == [] and len(await _fb(session)) == 0


# ===================== exclusions & rejections (no data loss) =====================

async def test_S2_combiner_box_meter_excluded(client, session, enabled):
    c, _ = client
    app, stub = enabled
    org, site, gw = await _mk(session)
    r = await c.post(URL, json=_batch([_msg("general_asset_telemetry", "combiner_box_meter",
                                            [_s2_rec()], message_id=uuid.uuid4())]), headers=_auth(gw))
    assert r.status_code == 200, r.text
    assert r.json()["accepted"] == []
    rej = r.json()["rejected"][0]
    assert rej["error_code"] == "substream_excluded" and rej["retryable"] is True
    assert len(await _sr(session)) == 0 and len(await _fb(session)) == 0 and _points(stub) == []


async def test_unsupported_stream_retryable_reject(client, session, enabled):
    c, _ = client
    app, _stub = enabled
    org, site, gw = await _mk(session)
    r = await c.post(URL, json=_batch([_msg("some_future_stream", "x", [{"a": 1}],
                                            message_id=uuid.uuid4())]), headers=_auth(gw))
    assert r.status_code == 200, r.text
    assert r.json()["accepted"] == []
    assert r.json()["rejected"][0]["error_code"] == "stream_not_yet_supported"
    assert r.json()["rejected"][0]["retryable"] is True
    assert await _ledger(session) == 0


async def test_S2_missing_substream_rejected(client, session, enabled):
    c, _ = client
    app, _stub = enabled
    org, site, gw = await _mk(session)
    r = await c.post(URL, json=_batch([_msg("general_asset_telemetry", None, [_s2_rec()],
                                            message_id=uuid.uuid4())]), headers=_auth(gw))
    assert r.status_code == 200, r.text
    assert r.json()["accepted"] == [] and r.json()["rejected"][0]["retryable"] is True


async def test_missing_message_id_rejected(client, session, enabled):
    c, _ = client
    app, _stub = enabled
    org, site, gw = await _mk(session)
    bad = {"schema_version": "1.0", "gateway_id": "k", "site_id": "k",
           "stream": "fast_bess_telemetry", "substream": "bess_fast", "sequence": 1,
           "created_at_utc": TS, "records": [_s1_rec()]}
    r = await c.post(URL, json=_batch([bad]), headers=_auth(gw))
    assert r.status_code == 200, r.text
    assert r.json()["accepted"] == [] and r.json()["rejected"][0]["retryable"] is True


# ===================== idempotency & structural =====================

async def test_S1_duplicate_already_processed(client, session, enabled):
    c, _ = client
    app, stub = enabled
    org, site, gw = await _mk(session)
    batch = _batch([_msg("fast_bess_telemetry", "bess_fast", [_s1_rec()], message_id=uuid.uuid4())])
    r1 = await c.post(URL, json=batch, headers=_auth(gw)); assert r1.json()["accepted"][0]["status"] == "accepted"
    r2 = await c.post(URL, json=batch, headers=_auth(gw)); assert r2.json()["accepted"][0]["status"] == "already_processed"
    assert len(await _fb(session)) == 1 and await _ledger(session) == 1


async def test_batch_empty_messages_422(client, session, enabled):
    c, _ = client
    _, _, gw = await _mk(session)
    assert (await c.post(URL, json=_batch([]), headers=_auth(gw))).status_code == 422

async def test_batch_malformed_json_400(client, session, enabled):
    c, _ = client
    _, _, gw = await _mk(session)
    r = await c.post(URL, content=b"{not json", headers={**_auth(gw), "Content-Type": "application/json"})
    assert r.status_code == 400

async def test_single_envelope_unknown_stream_422(client, session, enabled):
    c, _ = client
    _, _, gw = await _mk(session)
    body = {"schema_version": "1.0", "message_id": str(uuid.uuid4()), "gateway_id": str(gw.id),
            "site_id": "x", "organization_id": "x", "stream": "weird", "substream": "bess_fast",
            "sequence": 0, "created_at_utc": TS, "record_count": 0, "records": []}
    assert (await c.post(URL, json=body, headers=_auth(gw))).status_code == 422


# ===================== one mixed batch routes each stream correctly =====================

async def test_mixed_batch_routes_each_stream(client, session, enabled):
    c, _ = client
    app, stub = enabled
    org, site, gw = await _mk(session)
    s1, s2, s4, s10 = (uuid.uuid4() for _ in range(4))
    batch = _batch([
        _msg("fast_bess_telemetry", "bess_fast", [_s1_rec()], message_id=s1),
        _msg("general_asset_telemetry", "pcs_extended", [_s2_rec("active_power_kw", 12.3)], message_id=s2),
        _msg("alarms_events", "events", [_s4_valid()], message_id=s4),
        _msg("battery_charge_discharge_control", "pair_controller", [_generic_rec()], message_id=s10),
    ])
    r = await c.post(URL, json=batch, headers=_auth(gw))
    assert r.status_code == 200, r.text
    assert {a["status"] for a in r.json()["accepted"]} == {"accepted"} and len(r.json()["accepted"]) == 4
    assert r.json()["rejected"] == []
    # S1 -> fast_bess_records
    assert {x.message_id for x in await _fb(session)} == {s1}
    # S4 -> dedicated alarm_event_records; only S10 remains in generic stream_records
    sr = {x.stream: x.message_id for x in await _sr(session)}
    assert sr == {"S10": s10}
    alarms = (await session.execute(select(AlarmEventRecordModel))).scalars().all()
    assert len(alarms) == 1 and alarms[0].message_id == s4
    # S2 -> Influx only
    assert len(_points(stub)) == 1
