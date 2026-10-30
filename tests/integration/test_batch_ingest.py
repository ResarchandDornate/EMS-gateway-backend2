"""Regression tests for the legacy TransportBatch adapter on POST /api/v1/gateway/ingest.

New behaviour (confirmed against the Kalpa backend pack):
  * fast_bess_telemetry -> S1 structured JSONB, stored VERBATIM (mixed-type nested
    per-signal objects: bool/str/null/list preserved); no explode, no registry lookup.
  * Response is the gateway's ACK contract: HTTP 200 with accepted[]/rejected[].
  * Unsupported streams (and bad messages) -> rejected with retryable:true (kept by the
    gateway, no data loss). Nothing is permanently rejected.
  * `accepted` is emitted only after the row is committed.
The single-message endpoint and numeric routing are unchanged.

Gate OFF by default; flipped here via a get_settings override. No Influx needed.
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
    app.dependency_overrides[get_settings] = lambda: enabled_settings
    app.dependency_overrides[get_registry_index] = lambda: RegistryIndex.from_rows([])
    app.dependency_overrides[get_influx_gateway] = lambda: _StubInflux()
    reset_registry_cache()
    yield app
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


def _sig(value, quality="good"):
    # one per-signal object, as the real gateway emits
    return {"value": value, "unit": None, "quality": quality, "category": "normalized_fast",
            "display_name": None, "updated_utc": TS_Z, "age_ms": None}


def _nested_rec(*, source_id="external_ems_1", bess_id="bess_1"):
    # mixed types: bool, str, null, list, float, int - all must survive verbatim
    return {
        "schema_version": 1, "record_type": "fast_bess_sample", "sample_source": "cache",
        "timestamp_utc": TS_Z, "source_id": source_id, "bess_id": bess_id,
        "pcs_asset_id": f"{source_id}_pcs", "bms_asset_id": f"{source_id}_bms",
        "quality": "good",
        "pcs_values": {
            "running": _sig(False), "charging": _sig(None), "active_power_kw": _sig(0.0),
            "raw_status": _sig(0),
        },
        "bms_values": {
            "state": _sig("Normal"), "blocking_reasons": _sig([]), "soh": _sig(98.2),
        },
    }


def _legacy_msg(records, *, message_id=None, sequence=0, stream="fast_bess_telemetry",
                priority="P2"):
    return {"schema_version": "1.0", "message_id": str(message_id or uuid.uuid4()),
            "gateway_id": "kalpa-ems-gateway-001", "site_id": "kalpa",   # must be ignored
            "stream": stream, "substream": "bess_fast", "sequence": sequence,
            "created_at_utc": TS, "record_count": len(records), "priority": priority,
            "records": records}


def _batch(messages, *, request_id="req-1"):
    return {"request_id": request_id, "gateway_id": "kalpa-ems-gateway-001",
            "sent_at_utc": TS, "messages": messages}


async def _ledger_count(session):
    return (await session.execute(select(func.count()).select_from(IngestMessageModel))).scalar()


async def _fb_rows(session):
    return (await session.execute(select(FastBessRecordModel))).scalars().all()


# ---------------- S1 verbatim store + ACK accepted ----------------

async def test_s1_nested_stored_verbatim_accepted(client, session, enabled):
    c, _ = client
    org, site, gw = await _mk(session)
    mid = uuid.uuid4()
    r = await c.post(URL, json=_batch([_legacy_msg([_nested_rec()], message_id=mid, sequence=7)]),
                     headers=_auth(gw))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["rejected"] == []
    assert body["accepted"] == [{"message_id": str(mid), "status": "accepted"}]
    # one ledger row + one fast_bess row; identity stamped from token, not "kalpa-*"
    assert await _ledger_count(session) == 1
    rows = await _fb_rows(session)
    assert len(rows) == 1
    row = rows[0]
    assert row.gateway_id == gw.id and row.message_id == mid and row.record_index == 0
    assert row.source_id == "external_ems_1" and row.bess_id == "bess_1"
    # verbatim payload: bool / null / list / str / numbers all preserved exactly
    p = row.payload
    assert p["pcs_values"]["running"]["value"] is False
    assert p["pcs_values"]["charging"]["value"] is None
    assert p["bms_values"]["state"]["value"] == "Normal"
    assert p["bms_values"]["blocking_reasons"]["value"] == []
    assert p["bms_values"]["soh"]["value"] == 98.2


async def test_s1_multi_record_rows(client, session, enabled):
    c, _ = client
    org, site, gw = await _mk(session)
    msg = _legacy_msg([_nested_rec(bess_id="bess_1"), _nested_rec(bess_id="bess_2")],
                      message_id=uuid.uuid4())
    r = await c.post(URL, json=_batch([msg]), headers=_auth(gw))
    assert r.status_code == 200, r.text
    assert r.json()["accepted"][0]["status"] == "accepted"
    rows = await _fb_rows(session)
    assert len(rows) == 2 and {x.record_index for x in rows} == {0, 1}


async def test_s1_duplicate_already_processed(client, session, enabled):
    c, _ = client
    org, site, gw = await _mk(session)
    batch = _batch([_legacy_msg([_nested_rec()], message_id=uuid.uuid4())])
    r1 = await c.post(URL, json=batch, headers=_auth(gw)); assert r1.status_code == 200, r1.text
    assert r1.json()["accepted"][0]["status"] == "accepted"
    r2 = await c.post(URL, json=batch, headers=_auth(gw)); assert r2.status_code == 200, r2.text
    assert r2.json()["accepted"][0]["status"] == "already_processed"
    assert await _ledger_count(session) == 1 and len(await _fb_rows(session)) == 1


# ---------------- unsupported streams -> retryable reject, nothing lost ----------------

async def test_unsupported_stream_retryable_reject(client, session, enabled):
    c, _ = client
    org, site, gw = await _mk(session)
    r = await c.post(URL, json=_batch([_legacy_msg([{"x": 1}], message_id=uuid.uuid4(),
                                                   stream="general_asset_telemetry")]),
                     headers=_auth(gw))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["accepted"] == []
    rej = body["rejected"][0]
    assert rej["error_code"] == "stream_not_yet_supported" and rej["retryable"] is True
    assert await _ledger_count(session) == 0 and len(await _fb_rows(session)) == 0


async def test_missing_message_id_retryable_reject(client, session, enabled):
    c, _ = client
    org, site, gw = await _mk(session)
    bad = {"schema_version": "1.0", "gateway_id": "kalpa", "site_id": "kalpa",
           "stream": "fast_bess_telemetry", "substream": "bess_fast", "sequence": 1,
           "created_at_utc": TS, "records": [_nested_rec()]}      # no message_id
    r = await c.post(URL, json=_batch([bad]), headers=_auth(gw))
    assert r.status_code == 200, r.text
    assert r.json()["accepted"] == []
    assert r.json()["rejected"][0]["retryable"] is True
    assert await _ledger_count(session) == 0


async def test_mixed_batch_good_and_unsupported(client, session, enabled):
    c, _ = client
    org, site, gw = await _mk(session)
    good = uuid.uuid4()
    batch = _batch([
        _legacy_msg([_nested_rec()], message_id=good, sequence=1),
        _legacy_msg([{"x": 1}], message_id=uuid.uuid4(), sequence=2, stream="general_asset_telemetry"),
    ])
    r = await c.post(URL, json=batch, headers=_auth(gw))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["accepted"] == [{"message_id": str(good), "status": "accepted"}]
    assert len(body["rejected"]) == 1 and body["rejected"][0]["error_code"] == "stream_not_yet_supported"
    rows = await _fb_rows(session)
    assert len(rows) == 1 and rows[0].message_id == good


# ---------------- structural failures + single-message routing unchanged ----------------

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
    assert await _ledger_count(session) == 0
