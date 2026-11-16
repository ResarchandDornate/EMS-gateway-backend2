"""Regression tests for the dead-letter queue (audit finding D-13).

A PERMANENT S2 write failure durably captures the message in ingest_dead_letter, transitions
the ledger row pending -> failed, and rejects the gateway NON-retryable (no infinite loop).
A TRANSIENT failure is NOT dead-lettered: the row stays pending and the gateway keeps
retrying. Manual replay restores a parked message. No automatic replay exists.
"""
import types as _types
import uuid

import pytest
from sqlalchemy import select

from src.auth.security import create_gateway_token, hash_password
from src.config import get_settings
from src.gateway.service import resolve_gateway_identity
from src.ingestion.dead_letter_models import IngestDeadLetterModel
from src.ingestion.dead_letter_service import record_dead_letter, replay_dead_letter
from src.ingestion.generic_stream import GenericStreamEnvelopeIn
from src.ingestion.models import IngestMessageModel
from src.ingestion.providers import (get_influx_gateway, get_registry_index,
                                      reset_registry_cache)
from src.influx import RegistryIndex
from src.tenancy.gateways.models import GatewayModel
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel

URL = "/api/v1/gateway/ingest"
TS = "2026-09-04T07:59:14+00:00"
TS_Z = "2026-10-27T06:00:00.123456Z"


class _Influx:
    """Configurable stub. mode: 'ok' | 'permanent' | 'transient'. Fast retry knobs."""
    def __init__(self, mode="ok"):
        self.mode = mode
        self._s = _types.SimpleNamespace(
            influx_bucket_raw="telemetry_test", influx_bucket_long="telemetry_test",
            influx_bucket_3y="telemetry_test", influx_write_retries=0,
            influx_retry_budget_ms=100, influx_timeout_ms=100)
        self.writes = []

    @property
    def settings(self):
        return self._s

    async def write_built(self, built):
        if self.mode == "permanent":
            raise RuntimeError("deterministic permanent write failure")   # unknown -> permanent
        if self.mode == "transient":
            raise ConnectionError("network blip")                         # -> transient
        self.writes.append(built)


@pytest.fixture
def influx():
    return _Influx("ok")


@pytest.fixture
def enabled(influx):
    from src.main import app
    enabled_settings = get_settings().model_copy(update={"ingest_enabled": True})
    app.dependency_overrides[get_settings] = lambda: enabled_settings
    app.dependency_overrides[get_registry_index] = lambda: RegistryIndex.from_rows([])
    app.dependency_overrides[get_influx_gateway] = lambda: influx
    reset_registry_cache()
    yield app, influx
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
            "gateway_id": "k", "site_id": "k", "stream": "general_asset_telemetry",
            "substream": "bms_extended", "sequence": 0, "created_at_utc": TS,
            "record_count": len(records), "priority": "P2", "records": records}


def _batch(messages):
    return {"request_id": "r", "gateway_id": "k", "sent_at_utc": TS, "messages": messages}


async def _dlq_rows(session):
    return (await session.execute(select(IngestDeadLetterModel))).scalars().all()


async def _ledger_status(session, mid):
    return (await session.execute(
        select(IngestMessageModel.status).where(IngestMessageModel.message_id == mid))).scalar()


# ---------------------------------------------------------------------------

async def test_permanent_failure_dead_letters_and_rejects_nonretryable(client, session, enabled, influx):
    influx.mode = "permanent"
    c, _ = client
    gw = await _mk(session)
    mid = uuid.uuid4()
    r = await c.post(URL, json=_batch([_msg([_s2_rec()], message_id=mid)]), headers=_auth(gw))
    rej = r.json()["rejected"]
    assert r.json()["accepted"] == []
    assert len(rej) == 1 and rej[0]["retryable"] is False
    assert rej[0]["error_code"] == "permanently_failed"
    rows = await _dlq_rows(session)
    assert len(rows) == 1
    assert rows[0].status == "parked" and rows[0].attempts == 1
    assert rows[0].error_code == "influx_write_permanent" and rows[0].payload["records"]
    assert await _ledger_status(session, mid) == "failed"


async def test_transient_failure_stays_pending_and_retryable(client, session, enabled, influx):
    influx.mode = "transient"
    c, _ = client
    gw = await _mk(session)
    mid = uuid.uuid4()
    r = await c.post(URL, json=_batch([_msg([_s2_rec()], message_id=mid)]), headers=_auth(gw))
    assert r.json()["rejected"][0]["retryable"] is True
    assert await _dlq_rows(session) == []
    assert await _ledger_status(session, mid) == "pending"


async def test_resent_dead_lettered_message_is_nonretryable_no_loop(client, session, enabled, influx):
    influx.mode = "permanent"
    c, _ = client
    gw = await _mk(session)
    mid = uuid.uuid4()
    msg = _batch([_msg([_s2_rec()], message_id=mid)])
    first = await c.post(URL, json=msg, headers=_auth(gw))
    assert first.json()["rejected"][0]["retryable"] is False
    # gateway (buggy) resends the same message: it is idempotently re-dropped, never looped,
    # and is NOT dead-lettered a second time (attempts stays 1).
    second = await c.post(URL, json=msg, headers=_auth(gw))
    assert second.json()["accepted"] == []
    assert second.json()["rejected"][0]["retryable"] is False
    rows = await _dlq_rows(session)
    assert len(rows) == 1 and rows[0].attempts == 1


async def test_record_dead_letter_is_idempotent(client, session, enabled, influx):
    """A repeat permanent failure of the SAME message increments attempts on one DLQ row
    rather than inserting a duplicate. First failure goes through the endpoint; the second
    calls the service directly with the canonical envelope rebuilt from the stored payload."""
    influx.mode = "permanent"
    c, _ = client
    gw = await _mk(session)
    mid = uuid.uuid4()
    await c.post(URL, json=_batch([_msg([_s2_rec()], message_id=mid)]), headers=_auth(gw))
    (dl,) = await _dlq_rows(session)
    assert dl.attempts == 1
    identity = await resolve_gateway_identity(session, gw)
    env = GenericStreamEnvelopeIn(**dl.payload)
    await record_dead_letter(session, gw=gw, identity=identity, envelope=env, token=uuid.uuid4(),
                             error_code="influx_write_permanent", detail="again")
    session.expire_all()                       # drop stale identity-map copies (expire_on_commit=False)
    rows = await _dlq_rows(session)
    assert len(rows) == 1 and rows[0].attempts == 2
    assert await _ledger_status(session, mid) == "failed"


async def test_manual_replay_restores_message(client, session, enabled, influx):
    influx.mode = "permanent"
    c, _ = client
    gw = await _mk(session)
    mid = uuid.uuid4()
    await c.post(URL, json=_batch([_msg([_s2_rec(value=9)], message_id=mid)]), headers=_auth(gw))
    (dl,) = await _dlq_rows(session)
    assert dl.status == "parked"
    # operator fixes the backend; manual replay now succeeds
    influx.mode = "ok"
    identity = await resolve_gateway_identity(session, gw)
    ok = await replay_dead_letter(session, dlq_id=dl.id, gw=gw, identity=identity, influx=influx,
                                  envelope_factory=lambda p: GenericStreamEnvelopeIn(**p))
    assert ok is True
    await session.refresh(dl)
    assert dl.status == "replayed" and dl.replayed_at is not None
    assert await _ledger_status(session, mid) == "stored"
    assert len(influx.writes) == 1
