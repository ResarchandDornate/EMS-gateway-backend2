"""S6 Solis solis_telemetry slice (telemetry ONLY): schema validation, timestamp handling, JSONB
verbatim preservation, message-level idempotency. Service-level; PG only. solis_events is NOT
implemented; S6 remains deferred at the endpoint (tested there). No promoted field mapping or
inverter identity is asserted beyond the contract's timestamp path."""
import uuid

import pytest
from pydantic import ValidationError
from sqlalchemy import func, select

from src.auth.security import hash_password
from src.ingestion.s6_solis import S6SolisEnvelopeIn, S6SolisTelemetryRecord
from src.ingestion.service import IngestDecision
from src.ingestion.solis_models import SolisTelemetryRecordModel
from src.ingestion.solis_service import persist_solis_telemetry
from src.telemetry_errors import InvalidValue
from src.tenancy.gateways.models import GatewayModel
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel


def _rec(**over):
    r = {"available": True, "timestamp_utc": "2026-09-04T08:00:15+00:00",
         "controller_state": "BOTH_SOLAR_DERATED",
         "decision": "both_above_90_keep_bess_on_solar_derated",
         "solis": {"enabled": True, "online": True, "state": "ON", "target_state": "ON",
                   "power_mode": "DERATED", "target_power_kw": 20.0, "active_power_w": 20000,
                   "active_power_kw": 20.0, "power_limit_percent_raw": 2000,
                   "power_limit_percent": 20.0, "limiting_status_raw": None,
                   "serial_port": "/dev/ems_solis_rtu", "unit_id": 1,
                   "communication_quality": "GOOD", "last_error": None, "command_id": None}}
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
    return S6SolisEnvelopeIn(
        schema_version="1.0", message_id=message_id or uuid.uuid4(),
        gateway_id=str(gw.id), site_id=str(site.id), organization_id=str(org.id),
        stream="S6", substream="solis", sequence=1,
        created_at_utc="2026-09-04T08:00:00+00:00", record_count=len(recs), records=recs)


async def _count(session):
    return (await session.execute(select(func.count()).select_from(SolisTelemetryRecordModel))).scalar_one()


# ---- schema ----

def test_schema_accepts_and_preserves_verbatim():
    d = S6SolisTelemetryRecord(**_rec()).model_dump(mode="json")
    assert d["solis"]["power_mode"] == "DERATED"
    assert d["solis"]["power_limit_percent_raw"] == 2000
    assert d["solis"]["limiting_status_raw"] is None           # null preserved
    assert d["solis"]["communication_quality"] == "GOOD"
    assert d["controller_state"] == "BOTH_SOLAR_DERATED"
    assert d["solis"]["command_id"] is None


def test_schema_allows_missing_timestamp_and_sparse():
    r = _rec(); r.pop("timestamp_utc"); r["solis"] = {"online": False}
    d = S6SolisTelemetryRecord(**r).model_dump(mode="json")
    assert "timestamp_utc" not in d or d["timestamp_utc"] is None
    assert d["solis"] == {"online": False}


def test_envelope_rejects_wrong_stream():
    with pytest.raises(ValidationError):
        S6SolisEnvelopeIn(
            schema_version="1.0", message_id=uuid.uuid4(), gateway_id="g", site_id="s",
            organization_id="o", stream="S5", substream="solis", sequence=1,
            created_at_utc="2026-09-04T08:00:00+00:00", record_count=1, records=[_rec()])


# ---- persistence ----

async def test_persist_stores_verbatim(session):
    org, site, gw = await _mk(session)
    d = await persist_solis_telemetry(session, gw=gw, identity=_ident(org, site, gw), envelope=_env(org, site, gw))
    assert d == IngestDecision.CLAIMED
    row = (await session.execute(select(SolisTelemetryRecordModel))).scalar_one()
    assert row.sample_time_utc.year == 2026 and row.sample_time_utc.utcoffset().total_seconds() == 0
    assert row.received_at_utc is not None
    assert row.payload["solis"]["power_limit_percent"] == 20.0     # full record verbatim
    assert row.payload["solis"]["serial_port"] == "/dev/ems_solis_rtu"


async def test_timestamp_absent_null_sample_time(session):
    org, site, gw = await _mk(session)
    r = _rec(); r.pop("timestamp_utc")
    await persist_solis_telemetry(session, gw=gw, identity=_ident(org, site, gw),
                                  envelope=_env(org, site, gw, records=[r]))
    row = (await session.execute(select(SolisTelemetryRecordModel))).scalar_one()
    assert row.sample_time_utc is None and row.received_at_utc is not None
    assert row.payload["solis"]["online"] is True


async def test_malformed_timestamp_rejected_before_claim(session):
    org, site, gw = await _mk(session)
    env = _env(org, site, gw, records=[_rec(timestamp_utc="not-a-date")])
    with pytest.raises(InvalidValue):
        await persist_solis_telemetry(session, gw=gw, identity=_ident(org, site, gw), envelope=env)
    assert await _count(session) == 0          # failed before any write


async def test_duplicate_message_idempotent(session):
    org, site, gw = await _mk(session)
    mid = uuid.uuid4(); ident = _ident(org, site, gw); recs = [_rec()]
    d1 = await persist_solis_telemetry(session, gw=gw, identity=ident, envelope=_env(org, site, gw, message_id=mid, records=recs))
    d2 = await persist_solis_telemetry(session, gw=gw, identity=ident, envelope=_env(org, site, gw, message_id=mid, records=recs))
    assert d1 == IngestDecision.CLAIMED and d2 == IngestDecision.DUPLICATE_STORED
    assert await _count(session) == 1
