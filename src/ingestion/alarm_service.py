"""S4 alarms/events persistence (structured JSONB path).

Reuses the ingest_messages ledger for message-level idempotency/leasing exactly like the
S3 path (claim_or_resolve -> write -> mark_stored). The write is an idempotent upsert keyed
on (gateway_id, event_id): the same event redelivered in a different message overwrites in
place (last-write-wins), so outbox resends never duplicate events. event_id uniqueness is
also enforced WITHIN a message. Timestamps are validated before the ledger claim.
"""
import uuid

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.ingestion.alarm_models import AlarmEventRecordModel
from src.ingestion.health_service import StoreNotConfirmed
from src.ingestion.s3_health import parse_record_timestamp
from src.ingestion.s4_alarms import S4AlarmEnvelopeIn
from src.ingestion.service import (IngestDecision, canonical_hash, claim_or_resolve,
                                   mark_stored)
from src.telemetry_errors import InvalidValue

_RESOLVED = {IngestDecision.DUPLICATE_STORED, IngestDecision.HASH_MISMATCH,
             IngestDecision.IN_PROGRESS}


def _build_rows(gw_id, identity: dict, envelope: S4AlarmEnvelopeIn) -> list[dict]:
    """Pure: validate timestamps, enforce intra-message event_id uniqueness, build row dicts.
    Raises InvalidValue (-> 422) before any ledger write."""
    rows = []
    seen = set()
    for rec in envelope.records:
        data = rec.model_dump(mode="json")          # event_id/enums -> json-safe; extras preserved
        eid = data["event_id"]
        if eid in seen:
            raise InvalidValue(f"duplicate event_id within message: {eid}")
        seen.add(eid)
        rows.append(dict(
            id=uuid.uuid4(),
            gateway_id=gw_id,
            organization_id=identity["organization_id"],
            site_id=identity["site_id"],
            message_id=envelope.message_id,
            event_id=rec.event_id,
            event_type=data["event_type"],
            severity=data["severity"],
            state=data["state"],
            dedupe_key=data["dedupe_key"],
            correlation_id=data.get("correlation_id"),
            occurred_at=parse_record_timestamp(data["timestamp_utc"]),
            payload=data,
        ))
    return rows


async def _upsert_rows(session, rows: list[dict]) -> None:
    if rows:
        stmt = pg_insert(AlarmEventRecordModel).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["gateway_id", "event_id"],
            set_={
                "organization_id": stmt.excluded.organization_id,
                "site_id": stmt.excluded.site_id,
                "message_id": stmt.excluded.message_id,
                "event_type": stmt.excluded.event_type,
                "severity": stmt.excluded.severity,
                "state": stmt.excluded.state,
                "dedupe_key": stmt.excluded.dedupe_key,
                "correlation_id": stmt.excluded.correlation_id,
                "occurred_at": stmt.excluded.occurred_at,
                "payload": stmt.excluded.payload,
                "updated_at": func.now(),
            },
        )
        await session.execute(stmt)
    await session.commit()


async def persist_alarm_events(session, *, gw, identity: dict,
                               envelope: S4AlarmEnvelopeIn) -> IngestDecision:
    """Full S4 ingest decision+persist. Returns the ledger IngestDecision. Mirrors the
    S3 path's phase order: validate -> hash -> claim -> write -> mark_stored."""
    rows = _build_rows(gw.id, identity, envelope)                      # validate-before-claim
    payload_hash = canonical_hash(envelope.model_dump(mode="json"))

    decision, token = await claim_or_resolve(
        session, gateway_id=gw.id, stream="S4", message_id=envelope.message_id,
        sequence=envelope.sequence, record_count=envelope.record_count,
        payload_hash=payload_hash)
    if decision in _RESOLVED:
        return decision

    await _upsert_rows(session, rows)
    stored = await mark_stored(session, gateway_id=gw.id,
                               message_id=envelope.message_id, token=token)
    if not stored:
        raise StoreNotConfirmed("store not confirmed; retry")
    return decision
