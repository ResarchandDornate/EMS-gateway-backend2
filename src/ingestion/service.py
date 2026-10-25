"""Telemetry ingestion ledger (Phase 9b-i).

Lease model
-----------
Each accepted message row carries a time-bounded processing lease
(lease_owner token + lease_expires_at). A claim grants LEASE_TTL_SECONDS of
exclusive ownership. Crash recovery: an expired lease is stolen via an atomic
compare-and-swap on (lease_owner, lease_expires_at).

9b-ii - ownership protocol for the write path (NOT implemented here)
  1. mark_stored is a CAS:
         UPDATE ... SET status='stored', stored_at=now()
         WHERE id=? AND lease_owner=<my token> AND status='pending'
     If the lease was stolen mid-write, the CAS matches 0 rows and the worker
     MUST NOT acknowledge 'stored' - it treats its write as superseded.
  2. Lease renewal / over-run: LEASE_TTL_SECONDS is set above p99 single-message
     write latency; large batches renew (heartbeat) the lease by extending
     lease_expires_at under the same CAS guard.
  3. Deterministic Influx point identity on retries: a record always maps to the
     same series+timestamp - tags (organization_id, site_id, gateway_id,
     source_id, asset_id, substream), field (signal_key), time (record
     timestamp). Influx is last-write-wins per point, so a re-write overwrites
     with identical data rather than duplicating.
No DB transaction is held open across the Influx write.
"""
import hashlib
import json
import uuid
from datetime import datetime, timedelta, timezone
from enum import Enum

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.ingestion.models import IngestMessageModel
from src.ingestion.schemas import DEFERRED_STREAMS, INGEST_STREAMS

LEASE_TTL_SECONDS = 60     # must exceed p99 end-to-end single-message write latency


class IngestDecision(str, Enum):
    CLAIMED = "claimed"
    RECLAIMED = "reclaimed"
    DUPLICATE_STORED = "duplicate_stored"
    IN_PROGRESS = "in_progress"
    HASH_MISMATCH = "hash_mismatch"       # -> 409 at HTTP layer (9b-ii)


def route_stream(stream: str) -> str:
    if stream in INGEST_STREAMS:
        return "ingest"
    if stream in DEFERRED_STREAMS:
        return "deferred"
    return "unknown"


def verify_scope(identity: dict, *, gateway_id, site_id, organization_id) -> bool:
    """Envelope scope must equal the authenticated gateway's DB-resolved identity."""
    return (
        str(identity["gateway_id"]) == str(gateway_id)
        and str(identity["site_id"]) == str(site_id)
        and str(identity["organization_id"]) == str(organization_id)
    )


def canonical_hash(envelope: dict) -> str:
    """Deterministic SHA-256 over telemetry-bearing content only (sorted keys,
    no whitespace). Excludes identity (validated separately), compression and
    gateway_software_version."""
    canon = {
        "schema_version": envelope["schema_version"],
        "stream": envelope["stream"],
        "substream": envelope["substream"],
        "sequence": envelope["sequence"],
        "created_at_utc": envelope["created_at_utc"],
        "record_count": envelope["record_count"],
        "records": envelope["records"],
    }
    blob = json.dumps(canon, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


async def claim_or_resolve(session, *, gateway_id, stream, message_id,
                           sequence, record_count, payload_hash,
                           lease_ttl_seconds=LEASE_TTL_SECONDS):
    """Ledger state machine. Returns (IngestDecision, lease_token | None).

    9b-i scope: performs only the claim/lease decision. It NEVER marks 'stored'
    and NEVER triggers an Influx write - those belong to 9b-ii.

    Transaction contract: the function owns its transaction boundaries. It
    COMMITS on the write paths (CLAIMED; and the steal yielding RECLAIMED or a
    lost-race IN_PROGRESS) and ROLLS BACK on the read-only paths (HASH_MISMATCH,
    DUPLICATE_STORED, active-lease IN_PROGRESS). On any exception it ATTEMPTS a
    rollback and re-raises the ORIGINAL exception; if the rollback itself fails,
    that secondary error is suppressed so the original database error
    propagates. Rollback is attempted, not guaranteed. The function never
    swallows a database error or returns a successful decision after one. On
    normal return the session has no open transaction.
    """
    now = datetime.now(timezone.utc)
    token = uuid.uuid4()
    expires = now + timedelta(seconds=lease_ttl_seconds)

    try:
        claim = (
            pg_insert(IngestMessageModel)
            .values(id=uuid.uuid4(), gateway_id=gateway_id, message_id=message_id,
                    stream=stream, sequence=sequence, record_count=record_count,
                    payload_hash=payload_hash, status="pending",
                    lease_owner=token, lease_expires_at=expires)
            .on_conflict_do_nothing(index_elements=["gateway_id", "message_id"])
            .returning(IngestMessageModel.id)
        )
        inserted = (await session.execute(claim)).scalar_one_or_none()
        if inserted is not None:
            await session.commit()
            return IngestDecision.CLAIMED, token

        row = (await session.execute(
            select(IngestMessageModel)
            .where(IngestMessageModel.gateway_id == gateway_id,
                   IngestMessageModel.message_id == message_id)
        )).scalar_one()

        if row.payload_hash != payload_hash:
            await session.rollback()
            return IngestDecision.HASH_MISMATCH, None
        if row.status == "stored":
            await session.rollback()
            return IngestDecision.DUPLICATE_STORED, None
        if row.lease_expires_at is not None and row.lease_expires_at > now:
            await session.rollback()
            return IngestDecision.IN_PROGRESS, None

        steal = (await session.execute(
            update(IngestMessageModel)
            .where(IngestMessageModel.id == row.id,
                   IngestMessageModel.status == "pending",
                   IngestMessageModel.lease_owner == row.lease_owner,
                   IngestMessageModel.lease_expires_at == row.lease_expires_at)
            .values(lease_owner=token, lease_expires_at=expires)
            .returning(IngestMessageModel.id)
        )).scalar_one_or_none()
        await session.commit()
        return (IngestDecision.RECLAIMED, token) if steal is not None else (IngestDecision.IN_PROGRESS, None)
    except Exception:
        try:
            await session.rollback()
        except Exception:
            pass          # keep the original error; don't let a failed rollback mask it
        raise


async def mark_stored(session, *, gateway_id, message_id, token) -> bool:
    """Phase 9b-ii CAS: flip the ledger row to 'stored' ONLY if this worker still owns
    the lease. Matches (gateway_id, message_id, status='pending', lease_owner=token) and
    sets status='stored' + stored_at. Returns True iff exactly one row changed.

    A lost lease / already-stored / wrong message -> 0 rows -> False, row left unchanged;
    the caller MUST NOT acknowledge 'stored' on False. 9b-ii callers invoke this only AFTER
    a successful Influx write: an Influx failure means this is never called and the row
    stays 'pending' (retryable). A successful Influx write then this returning False (lease
    stolen) or raising (DB down) is NOT acked stored -> the lease expires, a retry reclaims
    and re-writes the same deterministic points (last-write-wins, no duplication) and then
    stores. At-least-once with idempotent points, NOT distributed exactly-once.
    """
    try:
        row = (await session.execute(
            update(IngestMessageModel)
            .where(IngestMessageModel.gateway_id == gateway_id,
                   IngestMessageModel.message_id == message_id,
                   IngestMessageModel.status == "pending",
                   IngestMessageModel.lease_owner == token)
            .values(status="stored", stored_at=datetime.now(timezone.utc))
            .returning(IngestMessageModel.id)
        )).scalar_one_or_none()
        await session.commit()
        return row is not None
    except Exception:
        try:
            await session.rollback()
        except Exception:
            pass          # keep the original error; a failed rollback must not mask it
        raise
