"""S7 Edge-AI edge_ai_inference slice (inference ONLY): schema validation, cycle_time handling,
verbatim nested JSONB preservation, message-level idempotency. Service-level; PG only.
edge_ai_anomaly_context is NOT implemented; S7 remains deferred at the endpoint (tested there).
No disputed promoted mapping is asserted beyond the contract's cycle_time anchor."""
import uuid

import pytest
from pydantic import ValidationError
from sqlalchemy import func, select

from src.auth.security import hash_password
from src.ingestion.edge_ai_models import EdgeAiInferenceRecordModel
from src.ingestion.edge_ai_service import persist_edge_ai_inference
from src.ingestion.s7_edge_ai import S7EdgeAiEnvelopeIn, S7EdgeAiInferenceRecord
from src.ingestion.service import IngestDecision
from src.telemetry_errors import InvalidValue
from src.tenancy.gateways.models import GatewayModel
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel


def _rec(**over):
    r = {"phase": "8", "cycle_time": "2026-09-28T04:58:23+00:00", "cycle_index": 14533,
         "cycle_latency_ms": 30187.22, "edge_ai_status": "ok", "latest_age_sec": 0.8,
         "model_info": {"version": "v0_3", "model_type": "IsolationForest",
                        "feature_count": 33, "threshold": 0.5691796048965233,
                        "threshold_direction": "anomaly_score_ml >= threshold means anomaly"},
         "sources": [{"ok": True, "source_id": "external_ems_1", "bess_id": "X",
                      "ml_predicted_anomaly": False, "anomaly_score_ml": 0.12, "threshold": 0.5691,
                      "inference_latency_ms": 12.4,
                      "key_features": {"bms_cell_voltage_spread": 0.02,
                                       "pcs_fault_flag_count": 0.0}}]}
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
    return S7EdgeAiEnvelopeIn(
        schema_version="1.0", message_id=message_id or uuid.uuid4(),
        gateway_id=str(gw.id), site_id=str(site.id), organization_id=str(org.id),
        stream="S7", substream="per_bess", sequence=1,
        created_at_utc="2026-09-28T04:58:23+00:00", record_count=len(recs), records=recs)


async def _count(session):
    return (await session.execute(select(func.count()).select_from(EdgeAiInferenceRecordModel))).scalar_one()


# ---- schema ----

def test_schema_accepts_and_preserves_nested():
    d = S7EdgeAiInferenceRecord(**_rec()).model_dump(mode="json")
    assert d["model_info"]["model_type"] == "IsolationForest"
    assert d["model_info"]["feature_count"] == 33
    assert d["sources"][0]["key_features"]["bms_cell_voltage_spread"] == 0.02   # nested-nested
    assert d["sources"][0]["ml_predicted_anomaly"] is False
    assert d["cycle_index"] == 14533 and d["edge_ai_status"] == "ok"


def test_schema_allows_missing_cycle_time_and_sparse():
    r = _rec(); r.pop("cycle_time"); r["sources"] = []
    d = S7EdgeAiInferenceRecord(**r).model_dump(mode="json")
    assert ("cycle_time" not in d) or (d["cycle_time"] is None)
    assert d["sources"] == []


def test_envelope_rejects_wrong_stream():
    with pytest.raises(ValidationError):
        S7EdgeAiEnvelopeIn(
            schema_version="1.0", message_id=uuid.uuid4(), gateway_id="g", site_id="s",
            organization_id="o", stream="S6", substream="per_bess", sequence=1,
            created_at_utc="2026-09-28T04:58:23+00:00", record_count=1, records=[_rec()])


# ---- persistence ----

async def test_persist_stores_verbatim_with_cycle_time(session):
    org, site, gw = await _mk(session)
    d = await persist_edge_ai_inference(session, gw=gw, identity=_ident(org, site, gw), envelope=_env(org, site, gw))
    assert d == IngestDecision.CLAIMED
    row = (await session.execute(select(EdgeAiInferenceRecordModel))).scalar_one()
    assert row.sample_time_utc.year == 2026 and row.sample_time_utc.utcoffset().total_seconds() == 0
    assert row.received_at_utc is not None
    assert row.payload["model_info"]["threshold"] == 0.5691796048965233       # full record verbatim
    assert row.payload["sources"][0]["key_features"]["pcs_fault_flag_count"] == 0.0
    assert row.payload["cycle_index"] == 14533


async def test_cycle_time_absent_null_sample_time(session):
    org, site, gw = await _mk(session)
    r = _rec(); r.pop("cycle_time")
    await persist_edge_ai_inference(session, gw=gw, identity=_ident(org, site, gw),
                                    envelope=_env(org, site, gw, records=[r]))
    row = (await session.execute(select(EdgeAiInferenceRecordModel))).scalar_one()
    assert row.sample_time_utc is None and row.received_at_utc is not None
    assert row.payload["phase"] == "8"


async def test_malformed_cycle_time_rejected_before_claim(session):
    org, site, gw = await _mk(session)
    env = _env(org, site, gw, records=[_rec(cycle_time="not-a-date")])
    with pytest.raises(InvalidValue):
        await persist_edge_ai_inference(session, gw=gw, identity=_ident(org, site, gw), envelope=env)
    assert await _count(session) == 0          # failed before any write


async def test_duplicate_message_idempotent(session):
    org, site, gw = await _mk(session)
    mid = uuid.uuid4(); ident = _ident(org, site, gw); recs = [_rec()]
    d1 = await persist_edge_ai_inference(session, gw=gw, identity=ident, envelope=_env(org, site, gw, message_id=mid, records=recs))
    d2 = await persist_edge_ai_inference(session, gw=gw, identity=ident, envelope=_env(org, site, gw, message_id=mid, records=recs))
    assert d1 == IngestDecision.CLAIMED and d2 == IngestDecision.DUPLICATE_STORED
    assert await _count(session) == 1
