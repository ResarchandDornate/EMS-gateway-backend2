"""S8 configuration_audit slice: schema validation (enum, required/null), JSONB verbatim
preservation, message-level + event-level idempotency. Service-level; PG only."""
import uuid

import pytest
from pydantic import ValidationError
from sqlalchemy import func, select

from src.auth.security import hash_password
from src.ingestion.config_audit_models import ConfigurationAuditRecordModel
from src.ingestion.config_audit_service import persist_config_audit
from src.ingestion.s8_config_audit import S8ConfigAuditEnvelopeIn, S8ConfigAuditRecord
from src.ingestion.service import IngestDecision
from src.telemetry_errors import InvalidValue
from src.tenancy.gateways.models import GatewayModel
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel


def _rec(**over):
    r = {"audit_id": str(uuid.uuid4()), "timestamp_utc": "2026-10-12T04:21:09.512000+00:00",
         "gateway_id": "g1", "site_id": "s1", "actor": "kedar@ornatesolar.com",
         "actor_role": "admin", "change_source": "central_command",
         "setting_path": "controller.high_limit", "old_value": 85, "new_value": 90,
         "revision": 42, "reason": "raised ceiling for ToD window", "correlation_id": "cmd-7781"}
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
    return S8ConfigAuditEnvelopeIn(
        schema_version="1.0", message_id=message_id or uuid.uuid4(),
        gateway_id=str(gw.id), site_id=str(site.id), organization_id=str(org.id),
        stream="S8", substream="config_audit", sequence=1,
        created_at_utc="2026-10-12T04:21:09+00:00", record_count=len(recs), records=recs)


async def _count(session):
    return (await session.execute(select(func.count()).select_from(ConfigurationAuditRecordModel))).scalar_one()


# ---- schema ----

def test_schema_accepts_and_preserves():
    d = S8ConfigAuditRecord(**_rec(old_value={"mode": "peak", "kw": 90}, new_value=None)).model_dump(mode="json")
    assert d["change_source"] == "central_command"
    assert d["old_value"] == {"mode": "peak", "kw": 90}      # object preserved
    assert d["new_value"] is None                            # null preserved


def test_schema_rejects_bad_change_source():
    with pytest.raises(ValidationError):
        S8ConfigAuditRecord(**_rec(change_source="model_update"))   # Option A: not accepted
    with pytest.raises(ValidationError):
        S8ConfigAuditRecord(**_rec(change_source="remote"))


def test_schema_requires_old_and_new_value_keys_but_allows_null():
    S8ConfigAuditRecord(**_rec(old_value=None, new_value=None))      # null OK (keys present)
    bad = _rec(); bad.pop("old_value")
    with pytest.raises(ValidationError):
        S8ConfigAuditRecord(**bad)                                    # missing key -> error
    bad2 = _rec(); bad2.pop("new_value")
    with pytest.raises(ValidationError):
        S8ConfigAuditRecord(**bad2)


def test_schema_rejects_missing_required():
    for k in ("audit_id", "setting_path", "actor", "timestamp_utc"):
        bad = _rec(); bad.pop(k)
        with pytest.raises(ValidationError):
            S8ConfigAuditRecord(**bad)


def test_schema_allows_nullable_fields():
    S8ConfigAuditRecord(**_rec(actor_role=None, reason=None, correlation_id=None))


# ---- persistence ----

async def test_persist_stores_and_promotes(session):
    org, site, gw = await _mk(session)
    aid = str(uuid.uuid4())
    env = _env(org, site, gw, records=[_rec(audit_id=aid, old_value={"x": 1}, new_value=90, revision=6)])
    d = await persist_config_audit(session, gw=gw, identity=_ident(org, site, gw), envelope=env)
    assert d == IngestDecision.CLAIMED
    row = (await session.execute(select(ConfigurationAuditRecordModel))).scalar_one()
    assert str(row.audit_id) == aid and row.change_source == "central_command"
    assert row.setting_path == "controller.high_limit" and row.actor == "kedar@ornatesolar.com"
    assert row.revision == "6"                               # int coerced to string column
    assert row.old_value == {"x": 1} and row.new_value == 90  # arbitrary JSON promoted
    assert row.occurred_at.year == 2026
    assert row.payload["reason"] == "raised ceiling for ToD window"   # full record verbatim


async def test_same_audit_new_message_no_duplicate(session):
    org, site, gw = await _mk(session); ident = _ident(org, site, gw)
    aid = str(uuid.uuid4())
    await persist_config_audit(session, gw=gw, identity=ident,
                               envelope=_env(org, site, gw, records=[_rec(audit_id=aid, new_value=90)]))
    await persist_config_audit(session, gw=gw, identity=ident,
                               envelope=_env(org, site, gw, records=[_rec(audit_id=aid, new_value=95)]))
    assert await _count(session) == 1
    row = (await session.execute(select(ConfigurationAuditRecordModel))).scalar_one()
    assert row.new_value == 95                               # last-write-wins


async def test_intra_message_duplicate_audit_id_rejected(session):
    org, site, gw = await _mk(session)
    aid = str(uuid.uuid4())
    env = _env(org, site, gw, records=[_rec(audit_id=aid), _rec(audit_id=aid)])
    with pytest.raises(InvalidValue):
        await persist_config_audit(session, gw=gw, identity=_ident(org, site, gw), envelope=env)
    assert await _count(session) == 0
