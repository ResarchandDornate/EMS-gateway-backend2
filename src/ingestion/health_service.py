"""S3 gateway_health persistence (structured JSONB path).

Reuses the ingest_messages ledger for idempotency/leasing exactly like the numeric
S1/S2 path: claim_or_resolve -> write -> mark_stored (lease-owned CAS). The 'write'
is an idempotent JSONB upsert on (gateway_id, message_id, record_index), so a retried
message overwrites in place (last-write-wins) rather than duplicating. Timestamps are
validated BEFORE the ledger claim so a bad record never leaves a stuck 'pending' row.
"""
import uuid

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.ingestion.health_models import GatewayHealthRecordModel
from src.ingestion.s3_health import S3HealthEnvelopeIn, parse_record_timestamp
from src.ingestion.service import (IngestDecision, canonical_hash, claim_or_resolve,
                                   mark_stored)

_RESOLVED = {IngestDecision.DUPLICATE_STORED, IngestDecision.HASH_MISMATCH,
             IngestDecision.IN_PROGRESS}


class StoreNotConfirmed(Exception):
    """Post-write lease CAS (mark_stored) matched 0 rows: our lease was stolen mid-write.
    The row stays 'pending' and is retryable; callers map this to HTTP 503."""


def _build_rows(gw_id, identity: dict, envelope: S3HealthEnvelopeIn) -> list[dict]:
    """Pure: validate timestamps and build row dicts. Raises InvalidValue (-> 422)
    before any ledger write."""
    rows = []
    for i, rec in enumerate(envelope.records):
        data = rec.model_dump(mode="json")          # preserves nested objects/arrays/null/bool
        rows.append(dict(
            id=uuid.uuid4(),
            gateway_id=gw_id,
            organization_id=identity["organization_id"],
            site_id=identity["site_id"],
            message_id=envelope.message_id,
            substream=envelope.substream,
            record_index=i,
            recorded_at=parse_record_timestamp(data["timestamp_utc"]),
            payload=data,
        ))
    return rows


async def _upsert_rows(session, rows: list[dict]) -> None:
    if rows:
        stmt = pg_insert(GatewayHealthRecordModel).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["gateway_id", "message_id", "record_index"],
            set_={
                "organization_id": stmt.excluded.organization_id,
                "site_id": stmt.excluded.site_id,
                "substream": stmt.excluded.substream,
                "recorded_at": stmt.excluded.recorded_at,
                "payload": stmt.excluded.payload,
                "updated_at": func.now(),
            },
        )
        await session.execute(stmt)
    await session.commit()


async def persist_health_message(session, *, gw, identity: dict,
                                 envelope: S3HealthEnvelopeIn) -> IngestDecision:
    """Full S3 ingest decision+persist. Returns the ledger IngestDecision.
    Mirrors the numeric router's phase order: validate -> hash -> claim -> write ->
    mark_stored. NOT wired into the HTTP endpoint yet (S3 stays deferred)."""
    rows = _build_rows(gw.id, identity, envelope)                      # validate-before-claim
    payload_hash = canonical_hash(envelope.model_dump(mode="json"))

    decision, token = await claim_or_resolve(
        session, gateway_id=gw.id, stream="S3", message_id=envelope.message_id,
        sequence=envelope.sequence, record_count=envelope.record_count,
        payload_hash=payload_hash)
    if decision in _RESOLVED:
        return decision

    # CLAIMED or RECLAIMED -> write JSONB rows, then confirm via lease-owned CAS.
    await _upsert_rows(session, rows)
    stored = await mark_stored(session, gateway_id=gw.id,
                               message_id=envelope.message_id, token=token)
    if not stored:                      # lease lost mid-write -> row stays pending, retryable
        raise StoreNotConfirmed("store not confirmed; retry")
    return decision
