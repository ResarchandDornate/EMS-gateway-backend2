"""Dead-letter capture, listing and MANUAL replay for permanently-failed ingestion (D-13).

`record_dead_letter` is called from the ingest write path when a write fails PERMANENTLY:
it durably stores the message (verbatim payload) and transitions the ledger row
pending -> failed via the owner-scoped CAS. Both steps are committed so a 'failed' ledger
row is always backed by a replayable DLQ row.

`replay_dead_letter` is MANUAL only (operator-invoked, S2). It re-attempts the write and,
on success, marks the DLQ row 'replayed' and the ledger row failed -> stored. There is NO
automatic reaper that replays on a timer - that would risk the exact infinite loop this
feature prevents. A sweeper that only *reports* parked rows is safe and can be added later.
"""
import uuid
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.ingestion.dead_letter_models import IngestDeadLetterModel
from src.ingestion.influx_projection import build_influx_points
from src.ingestion.models import IngestMessageModel
from src.ingestion.service import mark_failed

_DETAIL_MAX = 2000


async def record_dead_letter(session, *, gw, identity, envelope, token,
                             error_code: str, detail: str | None = None) -> None:
    """Durably capture a permanently-failed message and transition its ledger row to 'failed'.

    Idempotent on (gateway_id, message_id): a repeat permanent failure increments `attempts`
    and refreshes the payload/detail instead of inserting a duplicate. The ledger transition
    is the owner-scoped mark_failed CAS, so a stolen/already-resolved row is left untouched
    and historical rows are never affected."""
    now = datetime.now(timezone.utc)
    payload = envelope.model_dump(mode="json")
    tbl = IngestDeadLetterModel.__table__
    detail = detail[:_DETAIL_MAX] if detail else None
    stmt = (
        pg_insert(IngestDeadLetterModel)
        .values(id=uuid.uuid4(), gateway_id=gw.id, message_id=envelope.message_id,
                stream=(getattr(envelope, "stream", None) or ""),
                substream=getattr(envelope, "substream", None),
                error_code=error_code, detail=detail, attempts=1, payload=payload,
                status="parked", first_failed_at=now, last_failed_at=now)
        .on_conflict_do_update(
            constraint="uq_dlq_gateway_message",
            set_={"attempts": tbl.c.attempts + 1, "last_failed_at": now,
                  "error_code": error_code, "detail": detail, "payload": payload,
                  "status": "parked"})
    )
    await session.execute(stmt)
    await session.commit()
    # Backed by the DLQ row above; only the lease owner can fail its own row.
    await mark_failed(session, gateway_id=gw.id, message_id=envelope.message_id, token=token)


async def list_dead_letters(session, *, gateway_id=None, status: str = "parked", limit: int = 100):
    stmt = select(IngestDeadLetterModel).where(IngestDeadLetterModel.status == status)
    if gateway_id is not None:
        stmt = stmt.where(IngestDeadLetterModel.gateway_id == gateway_id)
    stmt = stmt.order_by(IngestDeadLetterModel.last_failed_at.desc()).limit(min(limit, 500))
    return (await session.execute(stmt)).scalars().all()


async def replay_dead_letter(session, *, dlq_id, gw, identity, influx, envelope_factory) -> bool:
    """MANUAL replay of one parked S2 dead-letter row. Rebuilds the points from the stored
    payload and re-writes them; on success marks the DLQ row 'replayed' and the ledger row
    failed -> stored. Returns True on a successful replay, False if the row is missing/not
    parked or the rebuild/write fails (the row stays parked for another attempt).

    `envelope_factory(payload) -> envelope` reconstructs the typed envelope from the stored
    JSON (the caller supplies it so this module stays free of envelope-schema imports)."""
    dl = await session.get(IngestDeadLetterModel, dlq_id)
    if dl is None or dl.status != "parked":
        return False
    try:
        from src.signals.service import load_registry_index   # local import avoids import cycle
        envelope = envelope_factory(dl.payload)
        idx = await load_registry_index(session)
        points = build_influx_points(envelope, identity, influx.settings, idx.t3_signal_keys(), resolve_point_id=idx.resolve_point_id)
    except Exception:
        return False                                  # still unshapeable -> leave parked
    if points:
        try:
            await influx.write_built(points)
        except Exception:
            return False                              # write still failing -> leave parked
    now = datetime.now(timezone.utc)
    dl.status = "replayed"
    dl.replayed_at = now
    await session.execute(
        update(IngestMessageModel)
        .where(IngestMessageModel.gateway_id == dl.gateway_id,
               IngestMessageModel.message_id == dl.message_id,
               IngestMessageModel.status == "failed")
        .values(status="stored", stored_at=now))
    await session.commit()
    return True
