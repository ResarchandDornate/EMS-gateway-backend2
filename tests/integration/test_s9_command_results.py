"""S9 command_results slice: schema validation (final_status enum, open command_type, required/
null, arbitrary JSON values), JSONB verbatim preservation, dual timestamps (gateway received_at_utc
vs backend stored_at), message-level idempotency, and transition-history preservation (same
command_id across messages -> distinct rows; NO command-keyed collapse). Service-level; PG only."""
import uuid

import pytest
from pydantic import ValidationError
from sqlalchemy import func, select

from src.auth.security import hash_password
from src.ingestion.command_result_models import CommandResultRecordModel
from src.ingestion.command_result_service import persist_command_result
from src.ingestion.s9_command_results import FinalStatus, S9CommandEnvelopeIn, S9CommandResultRecord
from src.ingestion.service import IngestDecision
from src.telemetry_errors import InvalidValue
from src.tenancy.gateways.models import GatewayModel
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel


def _rec(**over):
    r = {"command_id": str(uuid.uuid4()), "gateway_id": "g1", "site_id": "s1",
         "requested_at_utc": "2026-10-12T04:21:09+00:00",
         "received_at_utc": "2026-10-12T04:21:10+00:00",
         "requested_by": "kedar@ornatesolar.com", "requested_role": "admin",
         "command_type": "set_controller_high_limit", "requested_value": {"high_limit": 90},
         "gateway_validation_ok": True, "readback_requested": True, "verified": None,
         "final_status": "accepted", "correlation_id": "cmd-7781"}
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
    return S9CommandEnvelopeIn(
        schema_version="1.0", message_id=message_id or uuid.uuid4(),
        gateway_id=str(gw.id), site_id=str(site.id), organization_id=str(org.id),
        stream="S9", substream="commands", sequence=1,
        created_at_utc="2026-10-12T04:21:09+00:00", record_count=len(recs), records=recs)


async def _count(session):
    return (await session.execute(select(func.count()).select_from(CommandResultRecordModel))).scalar_one()


# ---- schema ----

def test_schema_accepts_and_preserves():
    d = S9CommandResultRecord(**_rec(requested_value={"register": 3051, "value": 2000},
                                     readback_value=None)).model_dump(mode="json")
    assert d["final_status"] == "accepted"
    assert d["requested_value"] == {"register": 3051, "value": 2000}   # object preserved
    assert d["readback_value"] is None and d["verified"] is None


def test_schema_accepts_all_six_statuses():
    for s in ("accepted", "rejected", "executing", "completed", "failed", "timeout"):
        S9CommandResultRecord(**_rec(final_status=s))
    assert {m.value for m in FinalStatus} == {"accepted", "rejected", "executing", "completed", "failed", "timeout"}


def test_schema_rejects_bad_status():
    with pytest.raises(ValidationError):
        S9CommandResultRecord(**_rec(final_status="queued"))      # not a contract state
    with pytest.raises(ValidationError):
        S9CommandResultRecord(**_rec(final_status="cancelled"))


def test_schema_command_type_is_open_string():
    S9CommandResultRecord(**_rec(command_type="any_vendor_specific_command"))   # not enumerated in S9


def test_schema_rejects_missing_unconditional_required():
    for k in ("command_id", "requested_at_utc", "received_at_utc", "final_status",
              "command_type", "requested_value", "gateway_validation_ok", "verified"):
        bad = _rec(); bad.pop(k)
        with pytest.raises(ValidationError):
            S9CommandResultRecord(**bad)


def test_schema_conditional_fields_optional():
    # a rejected command carries none of the execution/write/failure conditionals -> still valid
    S9CommandResultRecord(**_rec(final_status="rejected"))


# ---- persistence ----

async def test_persist_stores_and_separates_timestamps(session):
    org, site, gw = await _mk(session)
    env = _env(org, site, gw)
    d = await persist_command_result(session, gw=gw, identity=_ident(org, site, gw), envelope=env)
    assert d == IngestDecision.CLAIMED
    row = (await session.execute(select(CommandResultRecordModel))).scalar_one()
    assert row.final_status == "accepted" and row.command_type == "set_controller_high_limit"
    assert row.gateway_received_at_utc.year == 2026        # contract received_at_utc preserved
    assert row.stored_at is not None                        # backend store time, separate column
    assert row.requested_value == {"high_limit": 90}
    assert row.payload["requested_by"] == "kedar@ornatesolar.com"   # full record verbatim


async def test_transition_history_preserved_not_collapsed(session):
    # same command_id, two lifecycle updates in two messages -> TWO rows (no command-keyed collapse)
    org, site, gw = await _mk(session); ident = _ident(org, site, gw)
    cid = str(uuid.uuid4())
    await persist_command_result(session, gw=gw, identity=ident,
                                 envelope=_env(org, site, gw, records=[_rec(command_id=cid, final_status="accepted")]))
    await persist_command_result(session, gw=gw, identity=ident,
                                 envelope=_env(org, site, gw, records=[_rec(command_id=cid, final_status="completed")]))
    assert await _count(session) == 2
    statuses = {r.final_status for r in (await session.execute(select(CommandResultRecordModel))).scalars()}
    assert statuses == {"accepted", "completed"}            # both transitions retained


async def test_duplicate_message_idempotent(session):
    org, site, gw = await _mk(session)
    mid = uuid.uuid4(); ident = _ident(org, site, gw); recs = [_rec()]
    d1 = await persist_command_result(session, gw=gw, identity=ident, envelope=_env(org, site, gw, message_id=mid, records=recs))
    d2 = await persist_command_result(session, gw=gw, identity=ident, envelope=_env(org, site, gw, message_id=mid, records=recs))
    assert d1 == IngestDecision.CLAIMED and d2 == IngestDecision.DUPLICATE_STORED
    assert await _count(session) == 1


async def test_malformed_timestamp_rejected_before_claim(session):
    org, site, gw = await _mk(session)
    env = _env(org, site, gw, records=[_rec(requested_at_utc="not-a-date")])
    with pytest.raises(InvalidValue):
        await persist_command_result(session, gw=gw, identity=_ident(org, site, gw), envelope=env)
    assert await _count(session) == 0
