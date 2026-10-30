"""Regression tests for legacy TransportBatch adaptation on POST /api/v1/gateway/ingest.

Covers the exact production blocker (a legacy batch -> 422 "unsupported stream" before the
fix), nested fast_bess_sample -> 63-signal explode, flat S1 conversion, duplicate retry,
unmapped stream (S2 kept deferred), invalid payloads, identity stamping (scope), and that
the single-message endpoint is unchanged.

Gate is OFF by default; these flip it via a get_settings override and override the
registry-index + Influx providers, like test_ingest_endpoint.py. No Influx server needed:
an in-process stub writer records writes.
"""
import types as _types
import uuid

import pytest
from sqlalchemy import func, select

from src.auth.security import create_access_token, create_gateway_token, hash_password
from src.config import get_settings
from src.influx import RegistryIndex
from src.ingestion.models import IngestMessageModel
from src.ingestion.providers import (get_influx_gateway, get_registry_index,
                                      reset_registry_cache)
from src.tenancy.gateways.models import GatewayModel
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel

URL = "/api/v1/gateway/ingest"
TS = "2026-09-04T07:59:14+00:00"
TS_Z = "2026-09-04T07:59:14.500Z"

# S1/bess_fast signals used here (subset of the 63 seeded). pcs_* tagged asset_type "pcs",
# bms_* "bms"; all numeric FLOAT32, T1 raw retention.
S1_INDEX = RegistryIndex.from_rows([
    ("S1", "bess_fast", "soc", "pcs", "FLOAT32", "T1_RAW_90D"),
    ("S1", "bess_fast", "charge_discharge_power", "pcs", "FLOAT32", "T1_RAW_90D"),
    ("S1", "bess_fast", "fault_status", "pcs", "FLOAT32", "T1_RAW_90D"),
    ("S1", "bess_fast", "soh", "bms", "FLOAT32", "T1_RAW_90D"),
    ("S1", "bess_fast", "contactor_fault", "bms", "FLOAT32", "T1_RAW_90D"),
])


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
    app.dependency_overrides[get_registry_index] = lambda: S1_INDEX
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


def _flat_rec(signal_key, value, *, source_id="pcs1", asset_id="bank1", quality="good", ts=TS):
    return {"source_id": source_id, "asset_id": asset_id, "signal_key": signal_key,
            "value": value, "quality": quality, "timestamp": ts}


def _nested_rec(*, source_id="external_ems_1", pcs=None, bms=None,
                pcs_asset="ems1_pcs", bms_asset="ems1_bms", quality="good", ts=TS_Z,
                include_pcs_asset=True, include_bms_asset=True, include_source=True):
    rec = {"bess_id": "bess_1", "record_type": "fast_bess_sample", "quality": quality,
           "timestamp_utc": ts}
    if include_source:
        rec["source_id"] = source_id
    if pcs is not None:
        rec["pcs_values"] = pcs
        if include_pcs_asset:
            rec["pcs_asset_id"] = pcs_asset
    if bms is not None:
        rec["bms_values"] = bms
        if include_bms_asset:
            rec["bms_asset_id"] = bms_asset
    return rec


def _legacy_msg(records, *, message_id=None, sequence=0, stream="fast_bess_telemetry"):
    return {"schema_version": "1.0", "message_id": str(message_id or uuid.uuid4()),
            "gateway_id": "kalpa-ems-gateway-001", "site_id": "kalpa",   # must be ignored
            "stream": stream, "substream": "bess_fast", "sequence": sequence,
            "created_at_utc": TS, "records": records}


def _batch(messages, *, request_id="req-1", gateway_id="kalpa-ems-gateway-001"):
    return {"request_id": request_id, "gateway_id": gateway_id, "sent_at_utc": TS,
            "messages": messages}


async def _count(session):
    return (await session.execute(select(func.count()).select_from(IngestMessageModel))).scalar()


# ---------------- individual endpoint is unweakened ----------------

async def test_single_unknown_stream_still_422(client, session, enabled):
    c, _ = client
    _, _, gw = await _mk(session)
    body = {"schema_version": "1.0", "message_id": str(uuid.uuid4()),
            "gateway_id": str(gw.id), "site_id": "x", "organization_id": "x",
            "stream": "weird", "substream": "bess_fast", "sequence": 0,
            "created_at_utc": TS, "record_count": 0, "records": []}
    r = await c.post(URL, json=body, headers=_auth(gw))
    assert r.status_code == 422

async def test_body_without_stream_or_messages_422(client, session, enabled):
    c, _ = client
    _, _, gw = await _mk(session)
    r = await c.post(URL, json={"foo": "bar"}, headers=_auth(gw))
    assert r.status_code == 422
    assert await _count(session) == 0


# ---------------- nested fast_bess_sample -> 63-signal explode (the real shape) ----------------

async def test_legacy_batch_nested_s1_exploded_stored(client, session, enabled):
    c, _ = client
    app, stub = enabled
    org, site, gw = await _mk(session)
    mid = uuid.uuid4()
    rec = _nested_rec(
        pcs={"soc": 55.0, "charge_discharge_power": -12.5, "fault_status": 0},  # 0 preserved
        bms={"soh": 98.2, "contactor_fault": 0},
    )
    r = await c.post(URL, json=_batch([_legacy_msg([rec], message_id=mid, sequence=7)]),
                     headers=_auth(gw))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["accepted"] == 1 and body["rejected"] == 0
    assert body["results"][0]["status"] == "stored"
    # one ledger row for the message; identity stamped from token, NOT "kalpa-*"
    row = (await session.execute(select(IngestMessageModel))).scalar_one()
    assert row.gateway_id == gw.id and row.message_id == mid and row.status == "stored"
    assert row.sequence == 7 and row.record_count == 5          # 3 pcs + 2 bms exploded
    assert len(stub.writes) == 1 and len(stub.writes[0]) == 5   # 5 points built


async def test_nested_duplicate_retry_200_no_rewrite(client, session, enabled):
    c, _ = client
    app, stub = enabled
    org, site, gw = await _mk(session)
    batch = _batch([_legacy_msg([_nested_rec(pcs={"soc": 55.0})], message_id=uuid.uuid4())])
    r1 = await c.post(URL, json=batch, headers=_auth(gw)); assert r1.status_code == 200, r1.text
    assert r1.json()["results"][0]["status"] == "stored"
    r2 = await c.post(URL, json=batch, headers=_auth(gw)); assert r2.status_code == 200, r2.text
    assert r2.json()["results"][0]["status"] == "duplicate"
    assert await _count(session) == 1 and len(stub.writes) == 1   # no second row / write


async def test_nested_unknown_signal_rejected_422(client, session, enabled):
    c, _ = client
    app, _ = enabled
    org, site, gw = await _mk(session)
    batch = _batch([_legacy_msg([_nested_rec(pcs={"not_a_real_signal": 1.0})], message_id=uuid.uuid4())])
    r = await c.post(URL, json=batch, headers=_auth(gw))
    assert r.status_code == 422, r.text          # UnknownSignal -> whole message rejected
    assert r.json()["results"][0]["status"] == "rejected"
    assert await _count(session) == 0


async def test_nested_missing_asset_id_rejected_422(client, session, enabled):
    c, _ = client
    app, _ = enabled
    org, site, gw = await _mk(session)
    rec = _nested_rec(pcs={"soc": 55.0}, include_pcs_asset=False)   # pcs_values but no pcs_asset_id
    r = await c.post(URL, json=_batch([_legacy_msg([rec], message_id=uuid.uuid4())]), headers=_auth(gw))
    assert r.status_code == 422, r.text
    res = r.json()["results"][0]
    assert res["status"] == "rejected" and "pcs_asset_id" in res["detail"]
    assert await _count(session) == 0


async def test_unrecognized_record_shape_rejected_422(client, session, enabled):
    c, _ = client
    app, _ = enabled
    org, site, gw = await _mk(session)
    rec = {"source_id": "x", "weird_values": {"soc": 1}}   # neither flat nor pcs/bms nested
    r = await c.post(URL, json=_batch([_legacy_msg([rec], message_id=uuid.uuid4())]), headers=_auth(gw))
    assert r.status_code == 422, r.text
    assert "will not infer a mapping" in r.json()["results"][0]["detail"]
    assert await _count(session) == 0


# ---------------- flat records still accepted ----------------

async def test_legacy_batch_flat_s1_stored_200(client, session, enabled):
    c, _ = client
    app, stub = enabled
    org, site, gw = await _mk(session)
    mid = uuid.uuid4()
    batch = _batch([_legacy_msg(
        [_flat_rec("soc", 55.0), _flat_rec("fault_status", 0)], message_id=mid, sequence=3)])
    r = await c.post(URL, json=batch, headers=_auth(gw))
    assert r.status_code == 200, r.text
    assert r.json()["results"][0]["status"] == "stored"
    row = (await session.execute(select(IngestMessageModel))).scalar_one()
    assert row.gateway_id == gw.id and row.record_count == 2 and row.sequence == 3


# ---------------- S2 / unmapped streams stay deferred ----------------

async def test_legacy_batch_unmapped_stream_rejected_422(client, session, enabled):
    c, _ = client
    app, _ = enabled
    org, site, gw = await _mk(session)
    batch = _batch([_legacy_msg([_flat_rec("soc", 55.0)],
                                message_id=uuid.uuid4(), stream="general_asset_telemetry")])
    r = await c.post(URL, json=batch, headers=_auth(gw))
    assert r.status_code == 422, r.text
    assert "not handled" in r.json()["results"][0]["detail"]
    assert await _count(session) == 0


# ---------------- malformed batch bodies ----------------

async def test_batch_empty_messages_422(client, session, enabled):
    c, _ = client
    _, _, gw = await _mk(session)
    r = await c.post(URL, json=_batch([]), headers=_auth(gw))
    assert r.status_code == 422

async def test_batch_malformed_json_400(client, session, enabled):
    c, _ = client
    _, _, gw = await _mk(session)
    r = await c.post(URL, content=b"{not json", headers={**_auth(gw), "Content-Type": "application/json"})
    assert r.status_code == 400


# ---------------- mixed batch: good + poison -> overall 422, good one stored ----------------

async def test_legacy_batch_mixed_overall_422_partial_stored(client, session, enabled):
    c, _ = client
    app, stub = enabled
    org, site, gw = await _mk(session)
    good_id = uuid.uuid4()
    batch = _batch([
        _legacy_msg([_nested_rec(pcs={"soc": 55.0})], message_id=good_id, sequence=1),
        _legacy_msg([_nested_rec(pcs={"soc": 1}, include_source=False)],   # missing source_id
                    message_id=uuid.uuid4(), sequence=2),
    ])
    r = await c.post(URL, json=batch, headers=_auth(gw))
    assert r.status_code == 422, r.text
    body = r.json()
    assert body["accepted"] == 1 and body["rejected"] == 1
    statuses = {x["message_id"]: x["status"] for x in body["results"]}
    assert statuses[str(good_id)] == "stored"
    row = (await session.execute(
        select(IngestMessageModel).where(IngestMessageModel.message_id == good_id))).scalar_one()
    assert row.status == "stored"
