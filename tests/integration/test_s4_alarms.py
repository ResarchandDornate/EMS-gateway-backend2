"""S4 alarms/events structured-record slice: schema validation, JSONB persistence,
message-level and event-level idempotency. Service-level; PG only, no Influx."""
import uuid

import pytest
from pydantic import ValidationError
from sqlalchemy import func, select

from src.auth.security import hash_password
from src.ingestion.alarm_models import AlarmEventRecordModel
from src.ingestion.alarm_service import persist_alarm_events
from src.ingestion.s4_alarms import S4AlarmEventRecord, S4AlarmEnvelopeIn
from src.ingestion.service import IngestDecision
from src.telemetry_errors import InvalidValue
from src.tenancy.gateways.models import GatewayModel
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel


def _rec(**over):
    r = {"event_id": str(uuid.uuid4()), "timestamp_utc": "2026-09-04T08:00:00.5+00:00",
         "gateway_id": "g1", "site_id": "s1", "source_id": "external_ems_1",
         "event_type": "BMS_CELL_OVERVOLTAGE_ACTIVE", "severity": "Critical", "state": "ACTIVE",
         "message": "Cell overvoltage", "dedupe_key": "bms1:overvoltage",
         "payload": {"vendor_code": 42, "nested": {"x": None}, "arr": [1, 2]}}
    r.update(over); return r


async def _mk(session):
    org = OrganizationModel(org_key="o1", name="Org"); session.add(org); await session.flush()
    site = SiteModel(organization_id=org.id, site_key="s1", name="Site"); session.add(site); await session.flush()
    gw = GatewayModel(site_id=site.id, gateway_key="g1", client_id="c1",
                      client_secret_hash=hash_password("x"), enabled=True)
    session.add(gw); await session.commit()
    return org, site, gw


def _ident(org, site, gw):
    return {"gateway_id": gw.id, "site_id": site.id, "organization_id": org.id}


def _env(org, site, gw, *, records=None, message_id=None):
    recs = records if records is not None else [_rec()]
    return S4AlarmEnvelopeIn(
        schema_version="1.0", message_id=message_id or uuid.uuid4(),
        gateway_id=str(gw.id), site_id=str(site.id), organization_id=str(org.id),
        stream="S4", substream="events", sequence=1,
        created_at_utc="2026-09-04T08:00:00+00:00", record_count=len(recs), records=recs)


async def _count(session):
    return (await session.execute(select(func.count()).select_from(AlarmEventRecordModel))).scalar_one()


# ---- schema ----

def test_schema_accepts_and_preserves_payload():
    d = S4AlarmEventRecord(**_rec()).model_dump(mode="json")
    assert d["severity"] == "Critical" and d["state"] == "ACTIVE"
    assert d["payload"]["nested"]["x"] is None and d["payload"]["arr"] == [1, 2]


def test_schema_rejects_missing_required():
    bad = _rec(); bad.pop("dedupe_key")
    with pytest.raises(ValidationError):
        S4AlarmEventRecord(**bad)


def test_schema_rejects_bad_enum():
    with pytest.raises(ValidationError):
        S4AlarmEventRecord(**_rec(severity="critical"))    # wrong case
    with pytest.raises(ValidationError):
        S4AlarmEventRecord(**_rec(state="RAISED"))          # not in contract enum


def test_schema_requires_source_id_key_but_allows_null():
    S4AlarmEventRecord(**_rec(source_id=None))              # null value OK (key present)
    bad = _rec(); bad.pop("source_id")
    with pytest.raises(ValidationError):
        S4AlarmEventRecord(**bad)                           # key missing -> error


# ---- persistence + idempotency ----

async def test_persist_stores_event(session):
    org, site, gw = await _mk(session)
    eid = str(uuid.uuid4())
    env = _env(org, site, gw, records=[_rec(event_id=eid)])
    d = await persist_alarm_events(session, gw=gw, identity=_ident(org, site, gw), envelope=env)
    assert d == IngestDecision.CLAIMED
    row = (await session.execute(select(AlarmEventRecordModel))).scalar_one()
    assert str(row.event_id) == eid and row.event_type == "BMS_CELL_OVERVOLTAGE_ACTIVE"
    assert row.severity == "Critical" and row.state == "ACTIVE"
    assert row.payload["payload"]["arr"] == [1, 2]
    assert row.occurred_at.year == 2026


async def test_duplicate_message_idempotent(session):
    org, site, gw = await _mk(session)
    mid = uuid.uuid4(); ident = _ident(org, site, gw)
    recs = [_rec()]          # same message_id AND same payload -> canonical hash matches -> duplicate
    d1 = await persist_alarm_events(session, gw=gw, identity=ident, envelope=_env(org, site, gw, message_id=mid, records=recs))
    d2 = await persist_alarm_events(session, gw=gw, identity=ident, envelope=_env(org, site, gw, message_id=mid, records=recs))
    assert d1 == IngestDecision.CLAIMED and d2 == IngestDecision.DUPLICATE_STORED
    assert await _count(session) == 1


async def test_same_event_new_message_no_duplicate(session):
    # outbox resend: same event_id, different message_id -> event upserts, one row (last-write-wins)
    org, site, gw = await _mk(session); ident = _ident(org, site, gw)
    eid = str(uuid.uuid4())
    await persist_alarm_events(session, gw=gw, identity=ident,
                               envelope=_env(org, site, gw, records=[_rec(event_id=eid, message="first")]))
    await persist_alarm_events(session, gw=gw, identity=ident,
                               envelope=_env(org, site, gw, records=[_rec(event_id=eid, message="second")]))
    assert await _count(session) == 1
    row = (await session.execute(select(AlarmEventRecordModel))).scalar_one()
    assert row.payload["message"] == "second"


async def test_intra_message_duplicate_event_id_rejected(session):
    org, site, gw = await _mk(session)
    eid = str(uuid.uuid4())
    env = _env(org, site, gw, records=[_rec(event_id=eid), _rec(event_id=eid)])
    with pytest.raises(InvalidValue):
        await persist_alarm_events(session, gw=gw, identity=_ident(org, site, gw), envelope=env)
    assert await _count(session) == 0          # failed before any write
