"""S10 battery_charge_discharge_control / pair_controller slice: schema validation, promoted-column
mapping, JSONB verbatim preservation, timestamp handling, and message-level idempotency. Service
level; PostgreSQL only. Mirrors the S5/S6 structured-stream tests."""
import uuid

import pytest
from pydantic import ValidationError
from sqlalchemy import func, select

from src.auth.security import hash_password
from src.ingestion.s10_charge_discharge import S10ChargeDischargeEnvelopeIn, S10PairStatusRecord
from src.ingestion.service import IngestDecision
from src.ingestion.charge_discharge_models import ChargeDischargeRecordModel
from src.ingestion.charge_discharge_service import persist_charge_discharge
from src.telemetry_errors import InvalidValue
from src.tenancy.gateways.models import GatewayModel
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel


def _rec(**over):
    r = {"timestamp_utc": "2026-11-13T08:00:15+00:00",
         "application": "pair_controller", "application_version": "1.9",
         "available": True, "pair_id": 1, "stage": "RUNNING", "run_status": "running",
         "direction": "charge", "requested_power_kw": 50.0, "commanded_power_kw": 48.0,
         "actual_power_kw": 47.5, "rack_voltage_v": 1367.3, "rack_soc_percent": 51.9,
         "battery_ready": True, "pcs_running": True, "hard_blocked": False, "platform_ready": True,
         "parallel_operation_supported": True, "raw_modbus_write_bypass": False,
         "errors": [], "active_pairs": [1, 2], "starting_pairs": []}
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
    return S10ChargeDischargeEnvelopeIn(
        schema_version="1.0", message_id=message_id or uuid.uuid4(),
        gateway_id=str(gw.id), site_id=str(site.id), organization_id=str(org.id),
        stream="S10", substream="pair_controller", sequence=1,
        created_at_utc="2026-11-13T08:00:00+00:00", record_count=len(recs), records=recs)


async def _count(session):
    return (await session.execute(select(func.count()).select_from(ChargeDischargeRecordModel))).scalar_one()


# ---- schema ----

def test_schema_accepts_and_preserves_verbatim():
    d = S10PairStatusRecord(**_rec()).model_dump(mode="json")
    assert d["run_status"] == "running"
    assert d["actual_power_kw"] == 47.5
    assert d["active_pairs"] == [1, 2]              # extra preserved
    assert d["errors"] == []                        # extra preserved
    assert d["raw_modbus_write_bypass"] is False


def test_envelope_rejects_wrong_stream():
    with pytest.raises(ValidationError):
        S10ChargeDischargeEnvelopeIn(
            schema_version="1.0", message_id=uuid.uuid4(), gateway_id="g", site_id="s",
            organization_id="o", stream="S5", substream="pair_controller", sequence=1,
            created_at_utc="2026-11-13T08:00:00+00:00", record_count=1, records=[_rec()])


# ---- persistence ----

async def test_persist_promotes_columns_and_stores_verbatim(session):
    org, site, gw = await _mk(session)
    d = await persist_charge_discharge(session, gw=gw, identity=_ident(org, site, gw), envelope=_env(org, site, gw))
    assert d == IngestDecision.CLAIMED
    row = (await session.execute(select(ChargeDischargeRecordModel))).scalar_one()
    assert row.pair_id == "1" and row.run_status == "running" and row.direction == "charge"
    assert row.actual_power_kw == 47.5 and row.rack_soc_percent == 51.9
    assert row.available is True and row.hard_blocked is False
    assert row.sample_time_utc.year == 2026 and row.sample_time_utc.utcoffset().total_seconds() == 0
    assert row.received_at_utc is not None
    assert row.payload["active_pairs"] == [1, 2]   # full record verbatim


async def test_timestamp_absent_null_sample_time(session):
    org, site, gw = await _mk(session)
    r = _rec(); r.pop("timestamp_utc")
    await persist_charge_discharge(session, gw=gw, identity=_ident(org, site, gw),
                                   envelope=_env(org, site, gw, records=[r]))
    row = (await session.execute(select(ChargeDischargeRecordModel))).scalar_one()
    assert row.sample_time_utc is None and row.received_at_utc is not None


async def test_malformed_timestamp_rejected_before_claim(session):
    org, site, gw = await _mk(session)
    env = _env(org, site, gw, records=[_rec(timestamp_utc="not-a-date")])
    with pytest.raises(InvalidValue):
        await persist_charge_discharge(session, gw=gw, identity=_ident(org, site, gw), envelope=env)
    assert await _count(session) == 0


async def test_four_pairs_one_message(session):
    org, site, gw = await _mk(session)
    recs = [_rec(pair_id=p, direction="charge" if p % 2 else "discharge") for p in (1, 2, 3, 4)]
    await persist_charge_discharge(session, gw=gw, identity=_ident(org, site, gw),
                                   envelope=_env(org, site, gw, records=recs))
    assert await _count(session) == 4


async def test_duplicate_message_idempotent(session):
    org, site, gw = await _mk(session)
    mid = uuid.uuid4(); ident = _ident(org, site, gw); recs = [_rec()]
    d1 = await persist_charge_discharge(session, gw=gw, identity=ident, envelope=_env(org, site, gw, message_id=mid, records=recs))
    d2 = await persist_charge_discharge(session, gw=gw, identity=ident, envelope=_env(org, site, gw, message_id=mid, records=recs))
    assert d1 == IngestDecision.CLAIMED and d2 == IngestDecision.DUPLICATE_STORED
    assert await _count(session) == 1
