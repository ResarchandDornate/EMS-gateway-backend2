"""S10 battery_charge_discharge_control / pair_controller persistence (structured JSONB path).

Mirrors S5/S6: the shared ingest_messages ledger gives message-level idempotency
(claim_or_resolve -> upsert on (gateway_id, message_id, record_index) -> mark_stored). Promoted
per-pair columns are derived from the raw record at the contract's defined paths; the full record
is stored verbatim. Timestamps are validated before the ledger claim so a bad record never leaves a
stuck 'pending' row.
"""
import uuid
from datetime import datetime, timezone

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.ingestion.charge_discharge_models import ChargeDischargeRecordModel
from src.ingestion.health_service import StoreNotConfirmed
from src.ingestion.s3_health import parse_record_timestamp
from src.ingestion.s10_charge_discharge import S10ChargeDischargeEnvelopeIn
from src.ingestion.service import (IngestDecision, canonical_hash, claim_or_resolve,
                                   mark_stored)

_RESOLVED = {IngestDecision.DUPLICATE_STORED, IngestDecision.HASH_MISMATCH,
             IngestDecision.IN_PROGRESS}


def _s(v):
    return None if v is None else str(v)


def _build_rows(gw_id, identity: dict, envelope: S10ChargeDischargeEnvelopeIn) -> list[dict]:
    """Pure: promote defined per-pair fields, parse sample time, build row dicts. Raises
    InvalidValue (-> 422) on an unparseable timestamp, before any ledger write."""
    now = datetime.now(timezone.utc)
    rows = []
    for i, rec in enumerate(envelope.records):
        data = rec.model_dump(mode="json")          # full record verbatim (nested + extras)
        ts = data.get("timestamp_utc")
        sample = parse_record_timestamp(ts) if ts else None
        rows.append(dict(
            id=uuid.uuid4(),
            gateway_id=gw_id,
            organization_id=identity["organization_id"],
            site_id=identity["site_id"],
            message_id=envelope.message_id,
            record_index=i,
            pair_id=_s(data.get("pair_id")),
            stage=data.get("stage"),
            run_status=data.get("run_status"),
            direction=data.get("direction"),
            requested_power_kw=data.get("requested_power_kw"),
            commanded_power_kw=data.get("commanded_power_kw"),
            actual_power_kw=data.get("actual_power_kw"),
            rack_voltage_v=data.get("rack_voltage_v"),
            rack_soc_percent=data.get("rack_soc_percent"),
            available=data.get("available"),
            battery_ready=data.get("battery_ready"),
            pcs_running=data.get("pcs_running"),
            hard_blocked=data.get("hard_blocked"),
            platform_ready=data.get("platform_ready"),
            sample_time_utc=sample,
            received_at_utc=now,
            payload=data,
        ))
    return rows


async def _upsert_rows(session, rows: list[dict]) -> None:
    if rows:
        stmt = pg_insert(ChargeDischargeRecordModel).values(rows)
        cols = ("organization_id", "site_id", "pair_id", "stage", "run_status", "direction",
                "requested_power_kw", "commanded_power_kw", "actual_power_kw", "rack_voltage_v",
                "rack_soc_percent", "available", "battery_ready", "pcs_running", "hard_blocked",
                "platform_ready", "sample_time_utc", "received_at_utc", "payload")
        stmt = stmt.on_conflict_do_update(
            index_elements=["gateway_id", "message_id", "record_index"],
            set_={c: getattr(stmt.excluded, c) for c in cols} | {"updated_at": func.now()},
        )
        await session.execute(stmt)
    await session.commit()


async def persist_charge_discharge(session, *, gw, identity: dict,
                                   envelope: S10ChargeDischargeEnvelopeIn) -> IngestDecision:
    """Full S10 charge/discharge pair-status persist. Returns the ledger IngestDecision. Mirrors
    the S5/S6 phase order: validate -> hash -> claim -> write -> mark_stored."""
    rows = _build_rows(gw.id, identity, envelope)                      # validate-before-claim
    payload_hash = canonical_hash(envelope.model_dump(mode="json"))

    decision, token = await claim_or_resolve(
        session, gateway_id=gw.id, stream="S10", message_id=envelope.message_id,
        sequence=envelope.sequence, record_count=envelope.record_count,
        payload_hash=payload_hash, substream=getattr(envelope, "substream", None))
    if decision in _RESOLVED:
        return decision

    await _upsert_rows(session, rows)
    stored = await mark_stored(session, gateway_id=gw.id,
                               message_id=envelope.message_id, token=token)
    if not stored:
        raise StoreNotConfirmed("store not confirmed; retry")
    return decision
