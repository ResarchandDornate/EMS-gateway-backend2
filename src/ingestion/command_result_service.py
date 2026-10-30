"""S9 command_results persistence (structured JSONB path).

Reuses the ingest_messages ledger for MESSAGE-level idempotency like S3/S5-S8 (claim_or_resolve ->
write -> mark_stored). The write upserts on (gateway_id, message_id, record_index) ONLY -- this is
message-scoped, so a retried message is idempotent while the same command_id across different
messages is retained as distinct rows (lifecycle transition history preserved). NO command-keyed
latest-status-wins upsert is performed and no transition-identity model is assumed. The contract's
received_at_utc (gateway receipt) is preserved; stored_at is the separate backend store time.
Both required timestamps are validated before the ledger claim.
"""
import uuid
from datetime import datetime, timezone

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.ingestion.command_result_models import CommandResultRecordModel
from src.ingestion.health_service import StoreNotConfirmed
from src.ingestion.s3_health import parse_record_timestamp
from src.ingestion.s9_command_results import S9CommandEnvelopeIn
from src.ingestion.service import (IngestDecision, canonical_hash, claim_or_resolve,
                                   mark_stored)

_RESOLVED = {IngestDecision.DUPLICATE_STORED, IngestDecision.HASH_MISMATCH,
             IngestDecision.IN_PROGRESS}


def _build_rows(gw_id, identity: dict, envelope: S9CommandEnvelopeIn) -> list[dict]:
    """Pure: validate the two required timestamps, build row dicts. Raises InvalidValue (-> 422)
    before any ledger write. No intra-message command_id dedup (history is preserved)."""
    now = datetime.now(timezone.utc)
    rows = []
    for i, rec in enumerate(envelope.records):
        data = rec.model_dump(mode="json")          # full record verbatim (uuid/enum json-safe, extras kept)
        rows.append(dict(
            id=uuid.uuid4(),
            gateway_id=gw_id,
            organization_id=identity["organization_id"],
            site_id=identity["site_id"],
            message_id=envelope.message_id,
            record_index=i,
            command_id=rec.command_id,
            correlation_id=data.get("correlation_id"),
            command_type=data.get("command_type"),
            final_status=data.get("final_status"),
            requested_at_utc=parse_record_timestamp(data["requested_at_utc"]),
            gateway_received_at_utc=parse_record_timestamp(data["received_at_utc"]),
            stored_at=now,
            requested_value=data.get("requested_value"),
            readback_value=data.get("readback_value"),
            payload=data,
        ))
    return rows


async def _upsert_rows(session, rows: list[dict]) -> None:
    if rows:
        stmt = pg_insert(CommandResultRecordModel).values(rows)
        cols = ("organization_id", "site_id", "command_id", "correlation_id", "command_type",
                "final_status", "requested_at_utc", "gateway_received_at_utc", "stored_at",
                "requested_value", "readback_value", "payload")
        stmt = stmt.on_conflict_do_update(
            index_elements=["gateway_id", "message_id", "record_index"],
            set_={c: getattr(stmt.excluded, c) for c in cols} | {"updated_at": func.now()},
        )
        await session.execute(stmt)
    await session.commit()


async def persist_command_result(session, *, gw, identity: dict,
                                 envelope: S9CommandEnvelopeIn) -> IngestDecision:
    """Full S9 decision+persist. Returns the ledger IngestDecision. Mirrors the S3/S8 phase order:
    validate -> hash -> claim -> write -> mark_stored. NOT wired into the HTTP endpoint (S9 stays
    deferred/501)."""
    rows = _build_rows(gw.id, identity, envelope)                      # validate-before-claim
    payload_hash = canonical_hash(envelope.model_dump(mode="json"))

    decision, token = await claim_or_resolve(
        session, gateway_id=gw.id, stream="S9", message_id=envelope.message_id,
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
