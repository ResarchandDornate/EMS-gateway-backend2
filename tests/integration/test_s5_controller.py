"""S5 SOC-controller STATE snapshot slice (controller_state ONLY): schema validation, nullable
fields, JSONB verbatim preservation, timestamp promotion, message-level idempotency. Service-level;
PG only. controller_events is NOT implemented; S5 remains deferred at the endpoint (tested there)."""
import uuid

import pytest
from pydantic import ValidationError
from sqlalchemy import func, select

from src.auth.security import hash_password
from src.ingestion.controller_models import ControllerStateRecordModel
from src.ingestion.controller_service import persist_controller_state
from src.ingestion.s5_controller import S5ControllerEnvelopeIn, S5ControllerStateRecord
from src.ingestion.service import IngestDecision
from src.telemetry_errors import InvalidValue
from src.tenancy.gateways.models import GatewayModel
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel


def _rec(**over):
    r = {"available": True, "timestamp_utc": "2026-09-04T08:00:05+00:00",
         "version": "v1.14_soc_solar_derate_90_98", "mode": "LIVE", "healthy": True, "error": None,
         "controller": {"state": "BOTH_SOLAR_DERATED", "previous_state": "NORMAL",
                        "decision": "both_above_90_keep_bess_on_solar_derated",
                        "solar_off_reason": None,
                        "trend": {"window_sec": 60.0, "sample_count": 12, "avg_soc_delta": 0.0}},
         "thresholds": {"derate_soc_percent": 90.0, "high_soc_percent": 98.0, "low_cutoff_enabled": False},
         "bess": {"X": {"host": "192.168.100.151", "soc_percent": 92.5, "online": True, "target_state": "ON"}},
         "solis": {"enabled": True, "power_mode": "DERATED", "target_power_kw": 20.0,
                   "active_power_w": 20000, "limiting_status_raw": None},
         "action_plan": [{"kind": "solar", "id": "Solis", "target": "ON_DERATED"}]}
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
    return S5ControllerEnvelopeIn(
        schema_version="1.0", message_id=message_id or uuid.uuid4(),
        gateway_id=str(gw.id), site_id=str(site.id), organization_id=str(org.id),
        stream="S5", substream="controller", sequence=1,
        created_at_utc="2026-09-04T08:00:00+00:00", record_count=len(recs), records=recs)


async def _count(session):
    return (await session.execute(select(func.count()).select_from(ControllerStateRecordModel))).scalar_one()


# ---- schema ----

def test_schema_accepts_and_preserves_nested():
    d = S5ControllerStateRecord(**_rec()).model_dump(mode="json")
    assert d["controller"]["state"] == "BOTH_SOLAR_DERATED"
    assert d["controller"]["trend"]["sample_count"] == 12          # nested-nested preserved
    assert d["thresholds"]["high_soc_percent"] == 98.0             # extra object preserved
    assert d["bess"]["X"]["soc_percent"] == 92.5
    assert d["action_plan"][0]["target"] == "ON_DERATED"          # array preserved
    assert d["error"] is None and d["solis"]["limiting_status_raw"] is None


def test_schema_allows_nullable_fields():
    r = _rec(healthy=None, mode=None, version=None,
             controller={"state": None, "previous_state": None, "decision": None})
    d = S5ControllerStateRecord(**r).model_dump(mode="json")
    assert d["healthy"] is None and d["mode"] is None and d["controller"]["state"] is None


def test_envelope_rejects_wrong_stream():
    with pytest.raises(ValidationError):
        S5ControllerEnvelopeIn(
            schema_version="1.0", message_id=uuid.uuid4(), gateway_id="g", site_id="s",
            organization_id="o", stream="S4", substream="controller", sequence=1,
            created_at_utc="2026-09-04T08:00:00+00:00", record_count=1, records=[_rec()])


# ---- persistence ----

async def test_persist_promotes_and_stores(session):
    org, site, gw = await _mk(session)
    env = _env(org, site, gw)
    d = await persist_controller_state(session, gw=gw, identity=_ident(org, site, gw), envelope=env)
    assert d == IngestDecision.CLAIMED
    row = (await session.execute(select(ControllerStateRecordModel))).scalar_one()
    assert row.controller_state == "BOTH_SOLAR_DERATED" and row.previous_state == "NORMAL"
    assert row.decision == "both_above_90_keep_bess_on_solar_derated"
    assert row.mode == "LIVE" and row.version == "v1.14_soc_solar_derate_90_98" and row.healthy is True
    assert row.sample_time_utc.year == 2026 and row.sample_time_utc.utcoffset().total_seconds() == 0
    assert row.received_at_utc is not None
    assert row.payload["thresholds"]["derate_soc_percent"] == 90.0   # full record verbatim
    assert row.payload["controller"]["trend"]["window_sec"] == 60.0


async def test_persist_nullable_promotion(session):
    org, site, gw = await _mk(session)
    r = _rec(healthy=None, mode=None, version=None,
             controller={"state": None, "previous_state": None, "decision": None})
    await persist_controller_state(session, gw=gw, identity=_ident(org, site, gw),
                                   envelope=_env(org, site, gw, records=[r]))
    row = (await session.execute(select(ControllerStateRecordModel))).scalar_one()
    assert row.controller_state is None and row.mode is None and row.healthy is None
    assert row.payload["available"] is True                         # still stored verbatim


async def test_timestamp_absent_null_sample_time(session):
    org, site, gw = await _mk(session)
    r = _rec(); r.pop("timestamp_utc")
    await persist_controller_state(session, gw=gw, identity=_ident(org, site, gw),
                                   envelope=_env(org, site, gw, records=[r]))
    row = (await session.execute(select(ControllerStateRecordModel))).scalar_one()
    assert row.sample_time_utc is None and row.received_at_utc is not None


async def test_malformed_timestamp_rejected_before_claim(session):
    org, site, gw = await _mk(session)
    env = _env(org, site, gw, records=[_rec(timestamp_utc="not-a-date")])
    with pytest.raises(InvalidValue):
        await persist_controller_state(session, gw=gw, identity=_ident(org, site, gw), envelope=env)
    assert await _count(session) == 0          # failed before any write


async def test_duplicate_message_idempotent(session):
    org, site, gw = await _mk(session)
    mid = uuid.uuid4(); ident = _ident(org, site, gw); recs = [_rec()]
    d1 = await persist_controller_state(session, gw=gw, identity=ident, envelope=_env(org, site, gw, message_id=mid, records=recs))
    d2 = await persist_controller_state(session, gw=gw, identity=ident, envelope=_env(org, site, gw, message_id=mid, records=recs))
    assert d1 == IngestDecision.CLAIMED and d2 == IngestDecision.DUPLICATE_STORED
    assert await _count(session) == 1
