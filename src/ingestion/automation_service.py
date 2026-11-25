"""S10 automation_status / automation_events persistence (CG-28).

Mirrors charge_discharge_service: validate-before-claim, the shared ingest_messages ledger
for message-level idempotency, then an upsert. automation_status upserts per
(gateway_id, message_id, record_index) like the other S10 records; automation_events upserts
ON CONFLICT (gateway_id, event_id) DO NOTHING so a retried message never duplicates an event.
"""
import uuid
from datetime import datetime, timezone

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.ingestion.automation_models import AutomationEventRecordModel, AutomationStatusRecordModel
from src.ingestion.health_service import StoreNotConfirmed
from src.ingestion.s3_health import parse_record_timestamp
from src.ingestion.s10_charge_discharge import S10ChargeDischargeEnvelopeIn
from src.ingestion.service import (IngestDecision, canonical_hash, claim_or_resolve, mark_stored)
from src.telemetry_errors import InvalidValue

_RESOLVED = {IngestDecision.DUPLICATE_STORED, IngestDecision.HASH_MISMATCH, IngestDecision.IN_PROGRESS}


def _ts(v):
    return parse_record_timestamp(v) if v else None


# ── automation_status ──────────────────────────────────────────────────────

def _status_rows(gw_id, identity: dict, envelope: S10ChargeDischargeEnvelopeIn) -> list[dict]:
    now = datetime.now(timezone.utc)
    rows = []
    for i, rec in enumerate(envelope.records):
        data = rec.model_dump(mode="json")
        rows.append(dict(
            id=uuid.uuid4(),
            gateway_id=gw_id,
            organization_id=identity["organization_id"],
            site_id=identity["site_id"],
            message_id=envelope.message_id,
            record_index=i,
            automation_enabled=data.get("automation_enabled"),
            config_revision=data.get("config_revision"),
            supervisor_alive=data.get("supervisor_alive"),
            phase=data.get("phase"),
            direction=data.get("direction"),
            fault_latched=data.get("fault_latched"),
            safe_stop_requested=data.get("safe_stop_requested"),
            sample_time_utc=_ts(data.get("timestamp_utc")),
            received_at_utc=now,
            payload=data,
        ))
    return rows


async def _upsert_status(session, rows: list[dict]) -> None:
    if rows:
        stmt = pg_insert(AutomationStatusRecordModel).values(rows)
        cols = ("organization_id", "site_id", "automation_enabled", "config_revision",
                "supervisor_alive", "phase", "direction", "fault_latched", "safe_stop_requested",
                "sample_time_utc", "received_at_utc", "payload")
        stmt = stmt.on_conflict_do_update(
            index_elements=["gateway_id", "message_id", "record_index"],
            set_={c: getattr(stmt.excluded, c) for c in cols} | {"updated_at": func.now()},
        )
        await session.execute(stmt)
    await session.commit()


async def persist_automation_status(session, *, gw, identity: dict,
                                    envelope: S10ChargeDischargeEnvelopeIn) -> IngestDecision:
    rows = _status_rows(gw.id, identity, envelope)
    payload_hash = canonical_hash(envelope.model_dump(mode="json"))
    decision, token = await claim_or_resolve(
        session, gateway_id=gw.id, stream="S10", message_id=envelope.message_id,
        sequence=envelope.sequence, record_count=envelope.record_count,
        payload_hash=payload_hash, substream="automation_status")
    if decision in _RESOLVED:
        return decision
    await _upsert_status(session, rows)
    stored = await mark_stored(session, gateway_id=gw.id, message_id=envelope.message_id, token=token)
    if not stored:
        raise StoreNotConfirmed("store not confirmed; retry")
    return decision


# ── automation_events ──────────────────────────────────────────────────────

def _event_rows(gw_id, identity: dict, envelope: S10ChargeDischargeEnvelopeIn) -> list[dict]:
    now = datetime.now(timezone.utc)
    rows = []
    for rec in envelope.records:
        data = rec.model_dump(mode="json")
        event_id = data.get("event_id")
        if not event_id:
            raise InvalidValue("automation event missing event_id")
        rows.append(dict(
            id=uuid.uuid4(),
            gateway_id=gw_id,
            organization_id=identity["organization_id"],
            site_id=identity["site_id"],
            message_id=envelope.message_id,
            event_id=str(event_id),
            event_type=data.get("event_type"),
            phase=data.get("phase"),
            direction=data.get("direction"),
            timestamp_utc=_ts(data.get("timestamp_utc")),
            received_at_utc=now,
            payload=data,
        ))
    return rows


async def _upsert_events(session, rows: list[dict]) -> None:
    if rows:
        # Each event is sent once; a retried message must not duplicate it.
        stmt = pg_insert(AutomationEventRecordModel).values(rows)
        stmt = stmt.on_conflict_do_nothing(index_elements=["gateway_id", "event_id"])
        await session.execute(stmt)
    await session.commit()


async def persist_automation_events(session, *, gw, identity: dict,
                                    envelope: S10ChargeDischargeEnvelopeIn) -> IngestDecision:
    rows = _event_rows(gw.id, identity, envelope)                     # validate-before-claim
    payload_hash = canonical_hash(envelope.model_dump(mode="json"))
    decision, token = await claim_or_resolve(
        session, gateway_id=gw.id, stream="S10", message_id=envelope.message_id,
        sequence=envelope.sequence, record_count=envelope.record_count,
        payload_hash=payload_hash, substream="automation_events")
    if decision in _RESOLVED:
        return decision
    await _upsert_events(session, rows)
    stored = await mark_stored(session, gateway_id=gw.id, message_id=envelope.message_id, token=token)
    if not stored:
        raise StoreNotConfirmed("store not confirmed; retry")
    return decision
