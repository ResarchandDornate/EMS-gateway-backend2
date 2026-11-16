"""Regression tests for the S2 batch silent-data-loss defect (audit finding D-01).

Before the fix, an S2 batch message whose records failed shaping produced zero Influx
points but was still claimed, marked STORED, and ACKed `accepted` -> the gateway deleted
data that was never persisted. The fix makes `build_influx_points` raise `InvalidValue`
on any unshapeable (non-null) record, so the message is rejected `retryable:true` BEFORE
it is claimed/stored. A null-valued record remains a legitimate skip (no loss).

Scenarios: zero-point (all-null), invalid-record, partial-invalid, mixed-validity batch,
reject-then-corrected-retry, and duplicate. Uses a stub Influx so no live server is needed.
"""
import types as _types
import uuid

import pytest
from sqlalchemy import func, select

from src.auth.security import create_gateway_token, hash_password
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
TS_Z = "2026-10-27T06:00:00.123456Z"


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


async def _mk(session):
    org = OrganizationModel(org_key="o1", name="Org"); session.add(org); await session.flush()
    site = SiteModel(organization_id=org.id, site_key="s1", name="Site"); session.add(site)
    await session.flush()
    gw = GatewayModel(site_id=site.id, gateway_key="g1", client_id="c1",
                      client_secret_hash=hash_password("x"), enabled=True)
    session.add(gw); await session.commit()
    return gw


def _auth(gw):
    return {"Authorization": f"Bearer {create_gateway_token(gw.id)}"}


def _s2_rec(signal="rack_operating_state", value=2):
    return {"record_type": "general_asset_signal", "source_id": "lineage_source",
            "runtime_asset_id": "rack_1", "signal": signal, "value": value,
            "unit": None, "quality": "good", "source_updated_utc": TS_Z}


def _msg(records, *, message_id=None):
    return {"schema_version": "1.0", "message_id": str(message_id or uuid.uuid4()),
            "gateway_id": "kalpa-ems-gateway-001", "site_id": "kalpa",
            "stream": "general_asset_telemetry", "substream": "bms_extended",
            "sequence": 0, "created_at_utc": TS, "record_count": len(records),
            "priority": "P2", "records": records}


def _batch(messages):
    return {"request_id": "req-1", "gateway_id": "kalpa-ems-gateway-001",
            "sent_at_utc": TS, "messages": messages}


async def _ledger_count(session):
    return (await session.execute(select(func.count()).select_from(IngestMessageModel))).scalar()


def _points(stub):
    return [p for w in stub.writes for p in w]


# ---------------------------------------------------------------------------

async def test_all_null_records_accepted_with_zero_points(client, session, enabled):
    """A message whose records are ALL null readings is a legitimate zero-point store:
    accepted, ledger records it, no points written. (Boundary: null != invalid.)"""
    c, _ = client
    _, stub = enabled
    gw = await _mk(session)
    mid = uuid.uuid4()
    r = await c.post(URL, json=_batch([_msg([_s2_rec(value=None), _s2_rec(signal="x", value=None)],
                                            message_id=mid)]), headers=_auth(gw))
    assert r.status_code == 200
    assert r.json()["accepted"] == [{"message_id": str(mid), "status": "accepted"}]
    assert r.json()["rejected"] == []
    assert await _ledger_count(session) == 1
    assert len(_points(stub)) == 0                 # nothing to store, nothing lost


async def test_invalid_record_is_rejected_and_not_stored(client, session, enabled):
    """A single invalid record (missing 'signal') -> whole message rejected retryable,
    NOT claimed, NOT stored, no points. This is the core silent-loss regression."""
    c, _ = client
    _, stub = enabled
    gw = await _mk(session)
    mid = uuid.uuid4()
    r = await c.post(URL, json=_batch([_msg([_s2_rec(signal=None, value=5)], message_id=mid)]),
                     headers=_auth(gw))
    assert r.status_code == 200
    assert r.json()["accepted"] == []
    rej = r.json()["rejected"]
    assert len(rej) == 1 and rej[0]["message_id"] == str(mid)
    assert rej[0]["retryable"] is True and rej[0]["error_code"] == "invalid_payload"
    assert await _ledger_count(session) == 0       # never claimed -> gateway keeps it
    assert len(_points(stub)) == 0


async def test_partial_invalid_rejects_whole_message(client, session, enabled):
    """One valid + one invalid record -> the ENTIRE message is rejected (not partially
    stored), so the gateway resends all of it. No point is written for the valid record."""
    c, _ = client
    _, stub = enabled
    gw = await _mk(session)
    mid = uuid.uuid4()
    recs = [_s2_rec(signal="good_signal", value=1), _s2_rec(signal=None, value=2)]
    r = await c.post(URL, json=_batch([_msg(recs, message_id=mid)]), headers=_auth(gw))
    assert r.json()["accepted"] == []
    assert r.json()["rejected"][0]["retryable"] is True
    assert await _ledger_count(session) == 0
    assert len(_points(stub)) == 0


async def test_mixed_validity_batch_stores_only_the_valid_message(client, session, enabled):
    """A batch with a valid message A and an invalid message B: A is accepted and its
    points written; B is rejected retryable and never stored. Ledger has only A."""
    c, _ = client
    _, stub = enabled
    gw = await _mk(session)
    a, b = uuid.uuid4(), uuid.uuid4()
    msg_a = _msg([_s2_rec(signal="sig_a", value=10)], message_id=a)
    msg_b = _msg([_s2_rec(signal=None, value=11)], message_id=b)
    r = await c.post(URL, json=_batch([msg_a, msg_b]), headers=_auth(gw))
    body = r.json()
    assert body["accepted"] == [{"message_id": str(a), "status": "accepted"}]
    assert [x["message_id"] for x in body["rejected"]] == [str(b)]
    assert body["rejected"][0]["retryable"] is True
    assert await _ledger_count(session) == 1       # only A
    assert len(_points(stub)) == 1                 # only A's single point


async def test_rejected_then_corrected_resend_is_stored(client, session, enabled):
    """Retry scenario: a rejected (never-claimed) message can be resent with the SAME
    message_id, corrected, and is then accepted and stored — idempotency intact."""
    c, _ = client
    _, stub = enabled
    gw = await _mk(session)
    mid = uuid.uuid4()
    bad = await c.post(URL, json=_batch([_msg([_s2_rec(signal=None, value=1)], message_id=mid)]),
                       headers=_auth(gw))
    assert bad.json()["rejected"][0]["retryable"] is True
    assert await _ledger_count(session) == 0
    good = await c.post(URL, json=_batch([_msg([_s2_rec(signal="fixed", value=1)], message_id=mid)]),
                        headers=_auth(gw))
    assert good.json()["accepted"] == [{"message_id": str(mid), "status": "accepted"}]
    assert await _ledger_count(session) == 1
    assert len(_points(stub)) == 1


async def test_duplicate_valid_message_not_double_stored(client, session, enabled):
    """A valid message sent twice: first accepted, second already_processed; the ledger
    keeps one row and the point is not written a second time."""
    c, _ = client
    _, stub = enabled
    gw = await _mk(session)
    mid = uuid.uuid4()
    m = _batch([_msg([_s2_rec(signal="dup", value=7)], message_id=mid)])
    first = await c.post(URL, json=m, headers=_auth(gw))
    second = await c.post(URL, json=m, headers=_auth(gw))
    assert first.json()["accepted"] == [{"message_id": str(mid), "status": "accepted"}]
    assert second.json()["accepted"] == [{"message_id": str(mid), "status": "already_processed"}]
    assert await _ledger_count(session) == 1
    assert len(_points(stub)) == 1                 # duplicate is not re-written
