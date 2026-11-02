"""Persistence for northbound streams S2..S10.

Two paths, per the backend architecture:
  * persist_stream_message  - S3..S10 structured/event data -> VERBATIM PostgreSQL JSONB
    (stream_records). Reuses the ingest_messages ledger (claim -> upsert -> mark_stored).
  * persist_influx_only     - S2 numeric telemetry -> InfluxDB ONLY. The ledger still
    records the message (idempotency/ACK) in PostgreSQL, but no JSONB row is written.

Records stored verbatim; recorded_at resolved best-effort from common timestamp fields.
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
    return parse_record_timestamp(envelope.created_at_utc)


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
            payload=rec,
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
    """S3..S10 -> verbatim PostgreSQL JSONB. claim -> upsert -> mark_stored."""
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


async def persist_influx_only(session, *, gw, identity: dict,
                              envelope: GenericStreamEnvelopeIn,
                              influx, influx_points) -> IngestDecision:
    """S2 numeric telemetry -> InfluxDB ONLY. The ledger records the message for
    idempotency/ACK (PostgreSQL), but NO JSONB row is written. claim -> write Influx ->
    mark_stored. 'accepted' only after the Influx write succeeds."""
    payload_hash = canonical_hash(envelope.model_dump(mode="json"))

    decision, token = await claim_or_resolve(
        session, gateway_id=gw.id, stream=envelope.stream, message_id=envelope.message_id,
        sequence=envelope.sequence, record_count=envelope.record_count,
        payload_hash=payload_hash)
    if decision in _RESOLVED:
        return decision

    if influx is not None and influx_points:
        await influx.write_built(influx_points)      # time-series to Influx (raises -> retry)
    stored = await mark_stored(session, gateway_id=gw.id,
                               message_id=envelope.message_id, token=token)
    if not stored:
        raise StoreNotConfirmed("store not confirmed; retry")
    return decision
