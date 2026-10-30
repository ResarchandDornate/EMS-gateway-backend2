"""S1 fast_bess_telemetry persistence (structured JSONB path).

Reuses the ingest_messages ledger for idempotency/leasing exactly like S3 health:
claim_or_resolve -> upsert JSONB -> mark_stored (lease-owned CAS). Records are stored
VERBATIM (nested objects, booleans, strings, nulls, arrays preserved). A retried message
overwrites in place on (gateway_id, message_id, record_index). Record timestamps are
resolved BEFORE the ledger claim so a bad record never leaves a stuck 'pending' row.
"""
import uuid

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.ingestion.fast_bess_models import FastBessRecordModel
from src.ingestion.health_service import StoreNotConfirmed
from src.ingestion.s1_fast_bess import S1FastBessEnvelopeIn
from src.ingestion.s3_health import parse_record_timestamp
from src.ingestion.service import (IngestDecision, canonical_hash, claim_or_resolve,
                                   mark_stored)
from src.telemetry_errors import InvalidValue

_RESOLVED = {IngestDecision.DUPLICATE_STORED, IngestDecision.HASH_MISMATCH,
             IngestDecision.IN_PROGRESS}


def _str_or_none(v):
    return v if isinstance(v, str) else None


def _build_rows(gw_id, identity: dict, envelope: S1FastBessEnvelopeIn) -> list[dict]:
    """Pure: resolve a recorded_at per record and build verbatim JSONB rows.
    Raises InvalidValue (-> rejected) before any ledger write."""
    rows = []
    for i, rec in enumerate(envelope.records):
        if not isinstance(rec, dict):
            raise InvalidValue(f"record[{i}] is not a JSON object")
        ts = rec.get("timestamp_utc")
        if not (isinstance(ts, str) and ts):
            ts = envelope.created_at_utc                   # fallback: envelope time
        recorded_at = parse_record_timestamp(ts)           # InvalidValue if unparseable
        rows.append(dict(
            id=uuid.uuid4(),
            gateway_id=gw_id,
            organization_id=identity["organization_id"],
            site_id=identity["site_id"],
            message_id=envelope.message_id,
            substream=envelope.substream,
            record_index=i,
            source_id=_str_or_none(rec.get("source_id")),
            bess_id=_str_or_none(rec.get("bess_id")),
            recorded_at=recorded_at,
            payload=rec,                                   # verbatim
        ))
    return rows


async def _upsert_rows(session, rows: list[dict]) -> None:
    if rows:
        stmt = pg_insert(FastBessRecordModel).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["gateway_id", "message_id", "record_index"],
            set_={
                "organization_id": stmt.excluded.organization_id,
                "site_id": stmt.excluded.site_id,
                "substream": stmt.excluded.substream,
                "source_id": stmt.excluded.source_id,
                "bess_id": stmt.excluded.bess_id,
                "recorded_at": stmt.excluded.recorded_at,
                "payload": stmt.excluded.payload,
                "updated_at": func.now(),
            },
        )
        await session.execute(stmt)
    await session.commit()


async def persist_fast_bess_message(session, *, gw, identity: dict,
                                    envelope: S1FastBessEnvelopeIn) -> IngestDecision:
    """Full S1 ingest decision+persist. Order: validate -> hash -> claim -> write(JSONB)
    -> mark_stored. Returns a RESOLVED decision (duplicate/hash-mismatch/in-progress)
    without writing; otherwise writes and returns CLAIMED/RECLAIMED. Raises
    StoreNotConfirmed if the lease was lost mid-write (retryable)."""
    rows = _build_rows(gw.id, identity, envelope)                      # validate-before-claim
    payload_hash = canonical_hash(envelope.model_dump(mode="json"))

    decision, token = await claim_or_resolve(
        session, gateway_id=gw.id, stream="S1", message_id=envelope.message_id,
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
