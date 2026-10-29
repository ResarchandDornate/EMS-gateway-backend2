"""S6 Solis operational-telemetry persistence (structured JSONB path).

solis_telemetry ONLY. Reuses the ingest_messages ledger for message-level idempotency exactly
like S3/S5 (claim_or_resolve -> write -> mark_stored), with an idempotent upsert on
(gateway_id, message_id, record_index). The full record is stored verbatim; only time anchors are
promoted (sample_time_utc from the contract's timestamp_utc; received_at_utc backend-assigned). No
per-snapshot id, cross-message dedup, promoted field mapping, or inverter identity is invented.
Timestamps are validated before the ledger claim.
"""
import uuid
from datetime import datetime, timezone

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.ingestion.health_service import StoreNotConfirmed
from src.ingestion.s3_health import parse_record_timestamp
from src.ingestion.s6_solis import S6SolisEnvelopeIn
from src.ingestion.service import (IngestDecision, canonical_hash, claim_or_resolve,
                                   mark_stored)
from src.ingestion.solis_models import SolisTelemetryRecordModel

_RESOLVED = {IngestDecision.DUPLICATE_STORED, IngestDecision.HASH_MISMATCH,
             IngestDecision.IN_PROGRESS}


def _build_rows(gw_id, identity: dict, envelope: S6SolisEnvelopeIn) -> list[dict]:
    """Pure: parse the contract-defined timestamp, build row dicts. Raises InvalidValue (-> 422)
    on an unparseable timestamp_utc, before any ledger write."""
    now = datetime.now(timezone.utc)
    rows = []
    for i, rec in enumerate(envelope.records):
        data = rec.model_dump(mode="json")          # full record verbatim
        ts = data.get("timestamp_utc")
        sample = parse_record_timestamp(ts) if ts else None
        rows.append(dict(
            id=uuid.uuid4(),
            gateway_id=gw_id,
            organization_id=identity["organization_id"],
            site_id=identity["site_id"],
            message_id=envelope.message_id,
            record_index=i,
            sample_time_utc=sample,
            received_at_utc=now,
            payload=data,
        ))
    return rows


async def _upsert_rows(session, rows: list[dict]) -> None:
    if rows:
        stmt = pg_insert(SolisTelemetryRecordModel).values(rows)
        cols = ("organization_id", "site_id", "sample_time_utc", "received_at_utc", "payload")
        stmt = stmt.on_conflict_do_update(
            index_elements=["gateway_id", "message_id", "record_index"],
            set_={c: getattr(stmt.excluded, c) for c in cols} | {"updated_at": func.now()},
        )
        await session.execute(stmt)
    await session.commit()


async def persist_solis_telemetry(session, *, gw, identity: dict,
                                  envelope: S6SolisEnvelopeIn) -> IngestDecision:
    """Full S6 solis-telemetry decision+persist. Returns the ledger IngestDecision. Mirrors the
    S3/S5 phase order: validate -> hash -> claim -> write -> mark_stored. NOT wired into the HTTP
    endpoint (S6 stays deferred/501)."""
    rows = _build_rows(gw.id, identity, envelope)                      # validate-before-claim
    payload_hash = canonical_hash(envelope.model_dump(mode="json"))

    decision, token = await claim_or_resolve(
        session, gateway_id=gw.id, stream="S6", message_id=envelope.message_id,
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
