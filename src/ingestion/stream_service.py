"""Persistence for northbound streams S2..S10.

Two paths, per the backend architecture:
  * persist_stream_message  - S3..S10 structured/event data -> VERBATIM PostgreSQL JSONB
    (stream_records). Reuses the ingest_messages ledger (claim -> upsert -> mark_stored).
  * persist_influx_only     - S2 numeric telemetry -> InfluxDB ONLY. The ledger still
    records the message (idempotency/ACK) in PostgreSQL, but no JSONB row is written.

Records stored verbatim; recorded_at resolved best-effort from common timestamp fields.
"""
import asyncio
import time
import uuid

import structlog
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.ingestion.stream_models import StreamRecordModel
from src.ingestion.health_service import StoreNotConfirmed
from src.ingestion.generic_stream import GenericStreamEnvelopeIn
from src.ingestion.s3_health import parse_record_timestamp
from src.ingestion.service import (IngestDecision, canonical_hash, claim_or_resolve,
                                   mark_stored)
from src.telemetry_errors import InvalidValue, UnsafeQueryInput

# Influx SDK error hierarchy (influxdb-client 1.50.0): ApiException subclasses InfluxDBError,
# which carries .response (-> HTTP .status) and .retry_after. Network/transport failures from
# the async (aiohttp) client surface as aiohttp.ClientError / asyncio.TimeoutError.
from influxdb_client.client.exceptions import InfluxDBError
try:                                              # optional: classify transport errors
    import aiohttp as _aiohttp
    _AIOHTTP_CLIENT_ERRORS: tuple = (_aiohttp.ClientError,)
except Exception:                                 # pragma: no cover - aiohttp always present with [async]
    _AIOHTTP_CLIENT_ERRORS = ()

_RESOLVED = {IngestDecision.DUPLICATE_STORED, IngestDecision.HASH_MISMATCH,
             IngestDecision.IN_PROGRESS}

_log = structlog.get_logger("ingest.s2")

# Retry ONLY explicitly recognized transient failures: network/connection errors, timeouts,
# HTTP 429, and HTTP 5xx (plus a connection-level InfluxDBError that carries no HTTP response).
# Local deterministic config/validation errors and ANY unknown exception fail fast.
_TRANSIENT_NET = (ConnectionError, OSError, TimeoutError,
                  asyncio.TimeoutError) + _AIOHTTP_CLIENT_ERRORS


def _http_status(exc) -> "int | None":
    resp = getattr(exc, "response", None)
    st = getattr(resp, "status", None)
    try:
        return int(st) if st is not None else None
    except (TypeError, ValueError):
        return None


def _retry_after_seconds(exc) -> "float | None":
    ra = getattr(exc, "retry_after", None)
    try:
        return float(ra) if ra is not None else None
    except (TypeError, ValueError):
        return None


def _classify_write_error(exc: BaseException) -> str:
    """'transient' (retry) or 'permanent' (fail fast). CancelledError is handled separately
    by the caller and never reaches here. Only EXPLICITLY recognized transient failures are
    retried: network/connection errors, timeouts, HTTP 429, and HTTP 5xx (plus a
    connection-level InfluxDBError that carries no HTTP response). Everything else -- all 4xx
    and ANY unknown exception -- fails fast as 'permanent', so a deterministic defect is
    surfaced immediately instead of burning the retry budget and being masked."""
    if isinstance(exc, (UnsafeQueryInput, InvalidValue)):
        return "permanent"                        # our deterministic config/validation
    if isinstance(exc, InfluxDBError):            # ApiException is a subclass
        st = _http_status(exc)
        if st is None:
            return "transient"                    # connection-level (no HTTP response) == network
        if st == 429 or st >= 500:
            return "transient"                    # rate limited / server-side
        return "permanent"                        # 4xx (auth/bad request/not found/...) & any other
    if isinstance(exc, _TRANSIENT_NET):
        return "transient"                        # network reset / refused / timeout
    return "permanent"                            # UNKNOWN -> fail fast (surface the defect)


async def _write_s2_with_retry(influx, points, *, envelope, gw) -> None:
    """Write S2 points to InfluxDB under a genuine TOTAL wall-clock deadline, retrying only
    TRANSIENT failures (classified via _classify_write_error).

    Budget semantics: `influx_retry_budget_ms` is the TOTAL operation budget INCLUDING the
    first attempt. Each attempt runs under asyncio.wait_for(min(influx_timeout_ms, remaining)),
    so no attempt can exceed the remaining budget and a slow attempt is cancelled at the
    deadline -- wait_for cancels and awaits the inner write, leaving no orphaned task. Worst
    case total duration ~= influx_retry_budget_ms (+ scheduling slack), kept < the 60 s lease
    by a config validator and intended <= the gateway HTTP timeout.

    On a permanent error, exhausted retries, or the deadline, it re-raises -> the caller
    returns a retryable reject and the ledger row stays 'pending' (never acked without a
    confirmed write). External CancelledError propagates immediately, never retried.

    Forward-only: operates solely on the in-flight request while `points` is held in memory;
    it never scans or mutates ledger rows, so historical/pending rows are untouched. Retries
    re-send the same deterministic points (idempotent, last-write-wins -- no duplicate)."""
    s = getattr(influx, "settings", None)
    retries = int(getattr(s, "influx_write_retries", 0) or 0)
    budget_ms = float(getattr(s, "influx_retry_budget_ms", 0) or 0)
    per_attempt_s = float(getattr(s, "influx_timeout_ms", 20_000) or 20_000) / 1000.0
    deadline = (time.monotonic() + budget_ms / 1000.0) if budget_ms > 0 else None
    attempt = 0
    while True:
        remaining = None if deadline is None else (deadline - time.monotonic())
        if remaining is not None and remaining <= 0:
            _log.warning("s2_influx_write_deadline_exhausted",
                         message_id=str(envelope.message_id), gateway_id=str(gw.id),
                         stream=envelope.stream, attempt=attempt + 1, retryable=True)
            raise TimeoutError("s2 influx write exceeded total retry budget")
        cap = per_attempt_s if remaining is None else min(per_attempt_s, remaining)
        t0 = time.monotonic()
        try:
            await asyncio.wait_for(influx.write_built(points), timeout=cap)
            if attempt:
                _log.info("s2_influx_write_recovered",
                          message_id=str(envelope.message_id), gateway_id=str(gw.id),
                          stream=envelope.stream, attempts=attempt + 1,
                          duration_ms=round((time.monotonic() - t0) * 1000, 2))
            return
        except asyncio.CancelledError:
            raise                                 # external cancellation: never retry/swallow
        except Exception as exc:
            klass = _classify_write_error(exc)
            over_deadline = deadline is not None and time.monotonic() >= deadline
            will_retry = (klass == "transient" and attempt < retries and not over_deadline)
            # Never swallow: every failure is surfaced; on exhaustion/permanent we re-raise so
            # the caller returns a retryable reject (the row stays 'pending').
            _log.warning("s2_influx_write_failed",
                         message_id=str(envelope.message_id), gateway_id=str(gw.id),
                         stream=envelope.stream, points=len(points),
                         error_category=type(exc).__name__, error_class=klass,
                         http_status=_http_status(exc), attempt=attempt + 1,
                         duration_ms=round((time.monotonic() - t0) * 1000, 2),
                         will_retry=will_retry, retryable=True)
            if not will_retry:
                raise
            attempt += 1
            backoff = min(0.2 * (2 ** (attempt - 1)), 0.5)          # capped exponential backoff
            ra = _retry_after_seconds(exc)
            if ra is not None:
                backoff = max(backoff, ra)                          # honor Retry-After (429)
            if deadline is not None:                                # never sleep past the deadline
                backoff = min(backoff, max(0.0, deadline - time.monotonic()))
                if backoff <= 0:
                    raise
            await asyncio.sleep(backoff)

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
        # In-request, bounded, idempotent recovery of transient write failures (forward-only;
        # never touches historical/pending rows). On exhaustion it re-raises -> retryable reject.
        await _write_s2_with_retry(influx, influx_points, envelope=envelope, gw=gw)
    stored = await mark_stored(session, gateway_id=gw.id,
                               message_id=envelope.message_id, token=token)
    if not stored:
        # Lease lost between the (successful) write and the CAS: never acknowledge stored.
        _log.warning("s2_store_not_confirmed",
                     message_id=str(envelope.message_id), gateway_id=str(gw.id),
                     stream=envelope.stream, retryable=True)
        raise StoreNotConfirmed("store not confirmed; retry")
    _log.debug("s2_stored", message_id=str(envelope.message_id),
               gateway_id=str(gw.id), decision=decision.value)
    return decision
