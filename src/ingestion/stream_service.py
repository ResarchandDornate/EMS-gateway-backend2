"""Generic VERBATIM persistence for northbound streams S2..S10 (structured JSONB path).

Reuses the ingest_messages ledger (claim_or_resolve -> upsert -> mark_stored) exactly like
the S1 fast_bess and S3 health paths. Records are stored verbatim. recorded_at is resolved
best-effort from common record timestamp fields, falling back to the envelope
created_at_utc, so a stream whose record uses a different time field is never rejected.
"""
import uuid

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.ingestion.stream_models import StreamRecordModel
from src.ingestion.health_service import StoreNotConfirmed
from src.ingestion.generic_stream import GenericStreamEnvelopeIn
from src.ingestion.s3_health import parse_record_timestamp
from src.ingestion.service import (IngestDecision, canonical_hash, claim_or_resolve,
                                   mark_stored)
from src.telemetry_errors import InvalidValue

_RESOLVED = {IngestDecision.DUPLICATE_STORED, IngestDecision.HASH_MISMATCH,
             IngestDecision.IN_PROGRESS}

_TS_FIELDS = ("timestamp_utc", "source_updated_utc", "observed_at_utc",
              "recorded_at", "event_time_utc", "sample_time_utc")


def _str_or_none(v):
    return v if isinstance(v, str) else None


def _recorded_at(rec: dict, envelope: GenericStreamEnvelopeIn):
    for k in _TS_FIELDS:
        v = rec.get(k)
        if isinstance(v, str) and v:
            try:
                return parse_record_timestamp(v)
            except InvalidValue:
                continue
    return parse_record_timestamp(envelope.created_at_utc)   # envelope fallback


def _build_rows(gw_id, identity: dict, envelope: GenericStreamEnvelopeIn) -> list[dict]:
    rows = []
    for i, rec in enumerate(envelope.records):
        if not isinstance(rec, dict):
            raise InvalidValue(f"record[{i}] is not a JSON object")
        rows.append(dict(
            id=uuid.uuid4(),
            gateway_id=gw_id,
            organization_id=identity["organization_id"],
            site_id=identity["site_id"],
            message_id=envelope.message_id,
            stream=envelope.stream,
            substream=envelope.substream,
            record_index=i,
            source_id=_str_or_none(rec.get("source_id")),
            recorded_at=_recorded_at(rec, envelope),
            payload=rec,                                   # verbatim
        ))
    return rows


async def _upsert_rows(session, rows: list[dict]) -> None:
    if rows:
        stmt = pg_insert(StreamRecordModel).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["gateway_id", "message_id", "record_index"],
            set_={
                "organization_id": stmt.excluded.organization_id,
                "site_id": stmt.excluded.site_id,
                "stream": stmt.excluded.stream,
                "substream": stmt.excluded.substream,
                "source_id": stmt.excluded.source_id,
                "recorded_at": stmt.excluded.recorded_at,
                "payload": stmt.excluded.payload,
                "updated_at": func.now(),
            },
        )
        await session.execute(stmt)
    await session.commit()


async def persist_stream_message(session, *, gw, identity: dict,
                                 envelope: GenericStreamEnvelopeIn) -> IngestDecision:
    """Full S2..S10 ingest decision+persist. Order: validate -> hash -> claim -> write
    -> mark_stored. RESOLVED decisions return without writing; otherwise writes verbatim
    JSONB and returns CLAIMED/RECLAIMED. Raises StoreNotConfirmed on a lost lease."""
    rows = _build_rows(gw.id, identity, envelope)
    payload_hash = canonical_hash(envelope.model_dump(mode="json"))

    decision, token = await claim_or_resolve(
        session, gateway_id=gw.id, stream=envelope.stream, message_id=envelope.message_id,
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
