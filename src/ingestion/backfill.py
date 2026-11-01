"""Idempotent, resumable backfill of generic ``stream_records`` rows into the dedicated
per-stream tables (S3-S9). Option C migration.

Design guarantees (per the approved plan):
  * SOURCE IS NEVER TOUCHED. Only reads stream_records; never updates/deletes/truncates it.
  * Reuses each stream's own _build_rows + _upsert_rows, so a backfilled row is byte-identical
    to one written by live batch routing (same extraction, same ON CONFLICT upsert).
  * BYPASSES the ingest_messages ledger on purpose: these messages are already resolved
    ('stored') in the ledger, so the normal persister would short-circuit and write nothing.
    Idempotency/resume here comes from the dedicated tables' own unique keys (upsert).
  * Resumable: a message already fully present in the dedicated table is skipped; a message
    whose write was interrupted before commit is simply reprocessed (upsert is last-write-wins).
  * No silent skips: a record that fails its stream validator is pinpointed (message_id,
    record_index, reason) in the report; the whole affected message is left un-migrated (its
    rows stay in stream_records, fully recoverable) rather than partially written.
  * Verifies per message after writing: destination rows/keys must match the migrated records.

S10/D1 are intentionally excluded (no authoritative contract / out of scope).
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field

from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.ingestion.stream_models import StreamRecordModel
from src.telemetry_errors import InvalidValue

# dedicated models
from src.ingestion.health_models import GatewayHealthRecordModel
from src.ingestion.alarm_models import AlarmEventRecordModel
from src.ingestion.controller_models import ControllerStateRecordModel
from src.ingestion.solis_models import SolisTelemetryRecordModel
from src.ingestion.edge_ai_models import EdgeAiInferenceRecordModel
from src.ingestion.config_audit_models import ConfigurationAuditRecordModel
from src.ingestion.command_result_models import CommandResultRecordModel

# per-stream envelope + record validators
from src.ingestion.s3_health import S3HealthEnvelopeIn, S3GatewayHealthRecord
from src.ingestion.s4_alarms import S4AlarmEnvelopeIn, S4AlarmEventRecord
from src.ingestion.s5_controller import S5ControllerEnvelopeIn, S5ControllerStateRecord
from src.ingestion.s6_solis import S6SolisEnvelopeIn, S6SolisTelemetryRecord
from src.ingestion.s7_edge_ai import S7EdgeAiEnvelopeIn, S7EdgeAiInferenceRecord
from src.ingestion.s8_config_audit import S8ConfigAuditEnvelopeIn, S8ConfigAuditRecord
from src.ingestion.s9_command_results import S9CommandEnvelopeIn, S9CommandResultRecord

# reuse each service's exact extraction + upsert (never reimplemented here)
from src.ingestion import (health_service, alarm_service, controller_service,
                           solis_service, edge_ai_service, config_audit_service,
                           command_result_service)
from src.ingestion.schemas import SUPPORTED_SCHEMA_VERSIONS

_SCHEMA_VERSION = sorted(SUPPORTED_SCHEMA_VERSIONS)[-1]   # envelope-level only; never persisted


@dataclass(frozen=True)
class _Spec:
    code: str
    envelope_cls: type
    record_cls: type
    service: object                 # module exposing _build_rows / _upsert_rows
    model: type                     # dedicated destination model
    key: str                        # "record_index" | "event_id" | "audit_id"


SPECS: dict[str, _Spec] = {
    "S3": _Spec("S3", S3HealthEnvelopeIn, S3GatewayHealthRecord, health_service,
                GatewayHealthRecordModel, "record_index"),
    "S4": _Spec("S4", S4AlarmEnvelopeIn, S4AlarmEventRecord, alarm_service,
                AlarmEventRecordModel, "event_id"),
    "S5": _Spec("S5", S5ControllerEnvelopeIn, S5ControllerStateRecord, controller_service,
                ControllerStateRecordModel, "record_index"),
    "S6": _Spec("S6", S6SolisEnvelopeIn, S6SolisTelemetryRecord, solis_service,
                SolisTelemetryRecordModel, "record_index"),
    "S7": _Spec("S7", S7EdgeAiEnvelopeIn, S7EdgeAiInferenceRecord, edge_ai_service,
                EdgeAiInferenceRecordModel, "record_index"),
    "S8": _Spec("S8", S8ConfigAuditEnvelopeIn, S8ConfigAuditRecord, config_audit_service,
                ConfigurationAuditRecordModel, "audit_id"),
    "S9": _Spec("S9", S9CommandEnvelopeIn, S9CommandResultRecord, command_result_service,
                CommandResultRecordModel, "record_index"),
}


@dataclass
class BackfillReport:
    stream: str
    source_rows: int = 0
    source_messages: int = 0
    migrated_messages: int = 0
    migrated_rows: int = 0
    skipped_already_present: int = 0
    failed_messages: int = 0
    verified_messages: int = 0
    errors: list[dict] = field(default_factory=list)      # {message_id, record_index, reason}
    discrepancies: list[dict] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors and not self.discrepancies

    def as_dict(self) -> dict:
        return {
            "stream": self.stream, "source_rows": self.source_rows,
            "source_messages": self.source_messages,
            "migrated_messages": self.migrated_messages, "migrated_rows": self.migrated_rows,
            "skipped_already_present": self.skipped_already_present,
            "failed_messages": self.failed_messages, "verified_messages": self.verified_messages,
            "errors": self.errors, "discrepancies": self.discrepancies, "ok": self.ok,
        }


async def _message_ids(session, code):
    """Distinct (gateway_id, message_id) for the stream, deterministically ordered (resumable)."""
    stmt = (select(StreamRecordModel.gateway_id, StreamRecordModel.message_id)
            .where(StreamRecordModel.stream == code)
            .group_by(StreamRecordModel.gateway_id, StreamRecordModel.message_id)
            .order_by(StreamRecordModel.gateway_id, StreamRecordModel.message_id))
    return [(g, m) for g, m in (await session.execute(stmt)).all()]


async def _source_rows(session, code, gateway_id, message_id):
    """All source rows for one message, in record_index order (verbatim payloads)."""
    stmt = (select(StreamRecordModel)
            .where(StreamRecordModel.stream == code,
                   StreamRecordModel.gateway_id == gateway_id,
                   StreamRecordModel.message_id == message_id)
            .order_by(StreamRecordModel.record_index))
    return list((await session.execute(stmt)).scalars().all())


def _reconstruct_envelope(spec, gateway_id, message_id, substream, org_id, site_id, payloads):
    """Rebuild the stream-specific envelope from stored rows. Envelope-level scaffolding
    (schema_version/sequence/created_at_utc) is NOT persisted to the dedicated table; only
    `records` (the verbatim payloads) and the promoted columns derived from them are."""
    env = {
        "schema_version": _SCHEMA_VERSION,
        "message_id": str(message_id),
        "gateway_id": str(gateway_id),
        "site_id": str(site_id),
        "organization_id": str(org_id),
        "stream": spec.code,
        "substream": (substream or "")[:64],
        "sequence": 0,
        "created_at_utc": "1970-01-01T00:00:00+00:00",
        "record_count": len(payloads),
        "records": payloads,
    }
    return spec.envelope_cls(**env)


async def _already_present(session, spec, gateway_id, message_id, valid_count, key_values):
    """True if this message is already fully in the destination (resume skip)."""
    m = spec.model
    if spec.key == "record_index":
        n = (await session.execute(
            select(func.count()).select_from(m)
            .where(m.gateway_id == gateway_id, m.message_id == message_id))).scalar() or 0
        return n >= valid_count and valid_count > 0
    col = getattr(m, spec.key)
    n = (await session.execute(
        select(func.count()).select_from(m)
        .where(m.gateway_id == gateway_id, col.in_(list(key_values))))).scalar() or 0
    return n >= len(key_values) and len(key_values) > 0


async def _verify_message(session, spec, gateway_id, message_id, valid_count, key_values) -> bool:
    """Confirm the destination now holds the migrated records for this message."""
    m = spec.model
    if spec.key == "record_index":
        n = (await session.execute(
            select(func.count()).select_from(m)
            .where(m.gateway_id == gateway_id, m.message_id == message_id))).scalar() or 0
        return n >= valid_count
    col = getattr(m, spec.key)
    n = (await session.execute(
        select(func.count()).select_from(m)
        .where(m.gateway_id == gateway_id, col.in_(list(key_values))))).scalar() or 0
    return n >= len(key_values)


def _diagnose(spec, rows, rep: BackfillReport, message_id):
    """Per-record validation to pinpoint which record(s) failed and why (no silent skip)."""
    found = False
    for r in rows:
        try:
            spec.record_cls(**r.payload)
        except (ValidationError, InvalidValue, TypeError, ValueError) as e:
            found = True
            rep.errors.append({"message_id": str(message_id), "record_index": r.record_index,
                               "reason": str(e).splitlines()[0][:300]})
    if not found:   # envelope-level failure not attributable to a single record
        rep.errors.append({"message_id": str(message_id), "record_index": None,
                           "reason": "envelope validation failed (count/scaffold)"})


async def backfill_stream(session: AsyncSession, *, stream: str) -> BackfillReport:
    """Backfill one stream (S3-S9) from stream_records into its dedicated table. Idempotent
    and resumable; returns a BackfillReport. Does not touch stream_records."""
    spec = SPECS.get(stream)
    if spec is None:
        raise ValueError(f"backfill not supported for stream {stream!r} (S3-S9 only)")
    rep = BackfillReport(stream=stream)

    rep.source_rows = (await session.execute(
        select(func.count()).select_from(StreamRecordModel)
        .where(StreamRecordModel.stream == stream))).scalar() or 0

    for gateway_id, message_id in await _message_ids(session, stream):
        rep.source_messages += 1
        rows = await _source_rows(session, stream, gateway_id, message_id)
        if not rows:
            continue
        payloads = [r.payload for r in rows]
        substream = rows[0].substream
        org_id, site_id = rows[0].organization_id, rows[0].site_id

        try:
            envelope = _reconstruct_envelope(spec, gateway_id, message_id, substream,
                                             org_id, site_id, payloads)
            built = spec.service._build_rows(gateway_id,
                                             {"organization_id": org_id, "site_id": site_id},
                                             envelope)
        except (ValidationError, InvalidValue, TypeError, ValueError):
            rep.failed_messages += 1
            _diagnose(spec, rows, rep, message_id)
            continue

        key_values = ({b["record_index"] for b in built} if spec.key == "record_index"
                      else {b[spec.key] for b in built})

        if await _already_present(session, spec, gateway_id, message_id, len(built), key_values):
            rep.skipped_already_present += 1
            rep.verified_messages += 1
            continue

        await spec.service._upsert_rows(session, built)    # own transaction (commits)

        if await _verify_message(session, spec, gateway_id, message_id, len(built), key_values):
            rep.migrated_messages += 1
            rep.migrated_rows += len(built)
            rep.verified_messages += 1
        else:
            rep.discrepancies.append({"message_id": str(message_id),
                                      "reason": "post-write verification count mismatch"})
    return rep


async def backfill_all(session: AsyncSession, streams=("S3", "S4", "S5", "S6", "S7", "S8", "S9")):
    return {s: await backfill_stream(session, stream=s) for s in streams}
