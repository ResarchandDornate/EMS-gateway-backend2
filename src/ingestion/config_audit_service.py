"""S8 configuration-audit persistence (structured JSONB path).

Reuses the ingest_messages ledger for message-level idempotency like S3/S4 (claim_or_resolve ->
write -> mark_stored). The write is an idempotent upsert keyed on (gateway_id, audit_id): the same
audit event redelivered in a different message overwrites in place (last-write-wins). audit_id
uniqueness is also enforced WITHIN a message. The full record is stored verbatim; contract fields
are promoted. Timestamps are validated before the ledger claim.
"""
import uuid
from datetime import datetime, timezone

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.ingestion.config_audit_models import ConfigurationAuditRecordModel
from src.ingestion.health_service import StoreNotConfirmed
from src.ingestion.s3_health import parse_record_timestamp
from src.ingestion.s8_config_audit import S8ConfigAuditEnvelopeIn
from src.ingestion.service import (IngestDecision, canonical_hash, claim_or_resolve,
                                   mark_stored)
from src.telemetry_errors import InvalidValue

_RESOLVED = {IngestDecision.DUPLICATE_STORED, IngestDecision.HASH_MISMATCH,
             IngestDecision.IN_PROGRESS}


def _build_rows(gw_id, identity: dict, envelope: S8ConfigAuditEnvelopeIn) -> list[dict]:
    """Pure: validate timestamps, enforce intra-message audit_id uniqueness, promote + build rows.
    Raises InvalidValue (-> 422) before any ledger write."""
    now = datetime.now(timezone.utc)
    rows = []
    seen = set()
    for rec in envelope.records:
        data = rec.model_dump(mode="json")          # enums/uuid json-safe; old/new_value + extras verbatim
        aid = data["audit_id"]
        if aid in seen:
            raise InvalidValue(f"duplicate audit_id within message: {aid}")
        seen.add(aid)
        rev = data.get("revision")
        rows.append(dict(
            id=uuid.uuid4(),
            gateway_id=gw_id,
            organization_id=identity["organization_id"],
            site_id=identity["site_id"],
            message_id=envelope.message_id,
            audit_id=rec.audit_id,
            occurred_at=parse_record_timestamp(data["timestamp_utc"]),
            received_at_utc=now,
            actor=data.get("actor"),
            actor_role=data.get("actor_role"),
            change_source=data.get("change_source"),
            setting_path=data.get("setting_path"),
            revision=(None if rev is None else str(rev)),
            correlation_id=data.get("correlation_id"),
            old_value=data.get("old_value"),
            new_value=data.get("new_value"),
            payload=data,
        ))
    return rows


async def _upsert_rows(session, rows: list[dict]) -> None:
    if rows:
        stmt = pg_insert(ConfigurationAuditRecordModel).values(rows)
        cols = ("organization_id", "site_id", "message_id", "occurred_at", "received_at_utc",
                "actor", "actor_role", "change_source", "setting_path", "revision",
                "correlation_id", "old_value", "new_value", "payload")
        stmt = stmt.on_conflict_do_update(
            index_elements=["gateway_id", "audit_id"],
            set_={c: getattr(stmt.excluded, c) for c in cols} | {"updated_at": func.now()},
        )
        await session.execute(stmt)
    await session.commit()


async def persist_config_audit(session, *, gw, identity: dict,
                               envelope: S8ConfigAuditEnvelopeIn) -> IngestDecision:
    """Full S8 decision+persist. Returns the ledger IngestDecision. Mirrors the S4 phase order:
    validate -> hash -> claim -> write -> mark_stored."""
    rows = _build_rows(gw.id, identity, envelope)                      # validate-before-claim
    payload_hash = canonical_hash(envelope.model_dump(mode="json"))

    decision, token = await claim_or_resolve(
        session, gateway_id=gw.id, stream="S8", message_id=envelope.message_id,
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
