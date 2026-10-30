"""S3 gateway_health structured-record slice: schema validation, JSONB persistence,
idempotency. Service-level (endpoint still deferred/501). PG only; no Influx."""
import uuid

import pytest
from pydantic import ValidationError
from sqlalchemy import func, select

from src.auth.security import hash_password
from src.ingestion.health_models import GatewayHealthRecordModel
from src.ingestion.health_service import persist_health_message
from src.ingestion.s3_health import S3GatewayHealthRecord, S3HealthEnvelopeIn
from src.ingestion.service import IngestDecision
from src.tenancy.gateways.models import GatewayModel
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel

# A realistic deployed health record: scalars + nested objects + array + bool + null.
_VALID = {
    "status": "ok", "timestamp_utc": "2026-09-04T08:00:00.340928+00:00",
    "gateway_mode": "ems_read_write", "source_count": 2, "asset_count": 18,
    "online_asset_count": 18, "total_signal_count": 2844, "bad_signal_count": 0,
    "commands_enabled": True, "control_enabled": True, "storage_can_write": True,
    "storage": {"enabled": True, "type": "sqlite", "path": None, "reasons": [],
                "free_space_mb": 1024.5},
    "sources": [{"source_id": "external_ems_1", "host": "192.168.100.151", "port": 502}],
}


async def _mk(session):
    org = OrganizationModel(org_key="o1", name="Org"); session.add(org); await session.flush()
    site = SiteModel(organization_id=org.id, site_key="s1", name="Site"); session.add(site); await session.flush()
    gw = GatewayModel(site_id=site.id, gateway_key="g1", client_id="c1",
                      client_secret_hash=hash_password("x"), enabled=True)
    session.add(gw); await session.commit()
    return org, site, gw


def _identity(org, site, gw):
    return {"gateway_id": gw.id, "site_id": site.id, "organization_id": org.id}


def _env(org, site, gw, *, records=None, message_id=None, sequence=1):
    recs = records if records is not None else [dict(_VALID)]
    return S3HealthEnvelopeIn(
        schema_version="1.0", message_id=message_id or uuid.uuid4(),
        gateway_id=str(gw.id), site_id=str(site.id), organization_id=str(org.id),
        stream="S3", substream="gateway", sequence=sequence,
        created_at_utc="2026-09-04T08:00:00+00:00", record_count=len(recs), records=recs)


async def _count(session):
    return (await session.execute(select(func.count()).select_from(GatewayHealthRecordModel))).scalar_one()


# ---- schema validation ----

def test_schema_accepts_and_preserves_nested():
    rec = S3GatewayHealthRecord(**_VALID)
    dumped = rec.model_dump(mode="json")
    assert dumped["storage"]["type"] == "sqlite"
    assert dumped["storage"]["path"] is None           # null preserved
    assert dumped["storage"]["reasons"] == []           # array preserved
    assert dumped["sources"][0]["port"] == 502          # nested object preserved
    assert dumped["commands_enabled"] is True           # bool preserved


def test_schema_rejects_missing_required():
    bad = dict(_VALID); bad.pop("status")
    with pytest.raises(ValidationError):
        S3GatewayHealthRecord(**bad)


def test_schema_rejects_wrong_types():
    with pytest.raises(ValidationError):
        S3GatewayHealthRecord(**{**_VALID, "source_count": "2"})      # str not int (strict)
    with pytest.raises(ValidationError):
        S3GatewayHealthRecord(**{**_VALID, "commands_enabled": 1})    # int not bool (strict)


# ---- persistence + idempotency ----

async def test_persist_stores_jsonb(session):
    org, site, gw = await _mk(session)
    env = _env(org, site, gw)
    decision = await persist_health_message(session, gw=gw, identity=_identity(org, site, gw), envelope=env)
    assert decision == IngestDecision.CLAIMED
    row = (await session.execute(select(GatewayHealthRecordModel))).scalar_one()
    assert row.gateway_id == gw.id and row.organization_id == org.id and row.site_id == site.id
    assert row.record_index == 0 and row.substream == "gateway"
    assert row.payload["storage"]["reasons"] == [] and row.payload["storage"]["path"] is None
    assert row.recorded_at.year == 2026 and row.recorded_at.utcoffset().total_seconds() == 0


async def test_persist_duplicate_is_idempotent(session):
    org, site, gw = await _mk(session)
    mid = uuid.uuid4()
    ident = _identity(org, site, gw)
    d1 = await persist_health_message(session, gw=gw, identity=ident, envelope=_env(org, site, gw, message_id=mid))
    d2 = await persist_health_message(session, gw=gw, identity=ident, envelope=_env(org, site, gw, message_id=mid))
    assert d1 == IngestDecision.CLAIMED
    assert d2 == IngestDecision.DUPLICATE_STORED
    assert await _count(session) == 1          # no duplicate row


async def test_persist_hash_mismatch(session):
    org, site, gw = await _mk(session)
    mid = uuid.uuid4()
    ident = _identity(org, site, gw)
    await persist_health_message(session, gw=gw, identity=ident, envelope=_env(org, site, gw, message_id=mid))
    altered = dict(_VALID); altered["status"] = "degraded"
    d = await persist_health_message(session, gw=gw, identity=ident,
                                     envelope=_env(org, site, gw, message_id=mid, records=[altered]))
    assert d == IngestDecision.HASH_MISMATCH
    assert await _count(session) == 1          # mismatch did not write a second payload
