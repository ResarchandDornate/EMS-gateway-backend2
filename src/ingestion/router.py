import json

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import get_session
from src.gateway.service import resolve_gateway_identity
from src.ingestion.batch import (MAX_BATCH_MESSAGES, BatchMessageError,
                                  adapt_batch_message, is_batch_body)
from src.ingestion.body import BodyError, decode_body
from src.ingestion.mapping import build_message_points
from src.ingestion.providers import (get_influx_gateway, get_registry_index,
                                      require_ingest_enabled)
from src.ingestion.schemas import EnvelopeIn, IngestAck
from src.ingestion.s3_health import S3HealthEnvelopeIn
from src.ingestion.health_service import StoreNotConfirmed, persist_health_message
from src.ingestion.s4_alarms import S4AlarmEnvelopeIn
from src.ingestion.alarm_service import persist_alarm_events
from src.ingestion.s8_config_audit import S8ConfigAuditEnvelopeIn
from src.ingestion.config_audit_service import persist_config_audit
from src.ratelimit import ingest_rate_limit
from src.ingestion.service import (IngestDecision, canonical_hash, claim_or_resolve,
                                   mark_stored, route_stream, verify_scope)
from src.telemetry_errors import (InfluxTelemetryError, InvalidValue, RetentionNotConfigured,
                                   UnknownRetentionClass, UnresolvedMetadata)

router = APIRouter(prefix="/api/v1/gateway", tags=["ingestion"])


class _NumericError(Exception):
    """Internal: a numeric-path failure carrying the HTTP status + detail that the single
    endpoint would have raised. Lets the single path and the batch path share the exact
    same ledger/write pipeline (no duplicated storage logic) while each maps failures to
    its own response shape."""

    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail)


async def _process_numeric_envelope(envelope, *, session, gw, identity, index, influx) -> str:
    """Validate -> build points -> ledger claim -> Influx write -> confirm stored, for a
    single canonical S1/S2 envelope whose scope has already been established. Returns
    "stored" or "duplicate"; raises _NumericError for every failure mode with the same
    status codes the endpoint has always used. This IS the numeric pipeline; both callers
    reuse it, and it reuses the existing ingestion service + idempotency ledger."""
    # ---- Phase B: validate + build all points (no ledger, no write) ----
    try:
        built = build_message_points(envelope, identity, index, influx.settings)
    except (RetentionNotConfigured, UnknownRetentionClass):
        raise _NumericError(status.HTTP_501_NOT_IMPLEMENTED, "retention class not configured")
    except UnresolvedMetadata:
        raise _NumericError(status.HTTP_500_INTERNAL_SERVER_ERROR, "registry metadata incomplete")
    except InfluxTelemetryError:
        raise _NumericError(status.HTTP_422_UNPROCESSABLE_CONTENT, "invalid telemetry record")

    payload_hash = canonical_hash(envelope.model_dump())

    # ---- Phase C: ledger claim (DB infra errors -> 503, never 422) ----
    try:
        decision, token = await claim_or_resolve(
            session, gateway_id=gw.id, stream=envelope.stream, message_id=envelope.message_id,
            sequence=envelope.sequence, record_count=envelope.record_count, payload_hash=payload_hash)
    except Exception:
        raise _NumericError(status.HTTP_503_SERVICE_UNAVAILABLE, "ingestion temporarily unavailable")

    if decision == IngestDecision.DUPLICATE_STORED:
        return "duplicate"
    if decision == IngestDecision.HASH_MISMATCH:
        raise _NumericError(status.HTTP_409_CONFLICT, "message_id reused with a different payload")
    if decision == IngestDecision.IN_PROGRESS:
        raise _NumericError(status.HTTP_409_CONFLICT, "message is already being processed")

    # decision is CLAIMED or RECLAIMED
    # ---- Phase D: write to Influx, then confirm stored via lease-owned CAS ----
    try:
        await influx.write_built(built)
    except Exception:
        raise _NumericError(status.HTTP_503_SERVICE_UNAVAILABLE, "telemetry store temporarily unavailable")
    try:
        stored = await mark_stored(session, gateway_id=gw.id,
                                   message_id=envelope.message_id, token=token)
    except Exception:
        raise _NumericError(status.HTTP_503_SERVICE_UNAVAILABLE, "ingestion temporarily unavailable")
    if not stored:                                  # lease lost -> do NOT acknowledge stored
        raise _NumericError(status.HTTP_503_SERVICE_UNAVAILABLE, "store not confirmed; retry")
    return "stored"


@router.post("/ingest", response_model=IngestAck)
async def ingest(
    request: Request,
    _rl=Depends(ingest_rate_limit),          # rate limiter BEFORE gate + ledger claim (shadow by default)
    gw=Depends(require_ingest_enabled),          # auth (401/403) -> gate (503) -> here
    session: AsyncSession = Depends(get_session),
    index=Depends(get_registry_index),
    influx=Depends(get_influx_gateway),
):
    """Phase 9b-ii-c live ingestion, behind the default-disabled gate.

    PROVISIONAL: record keys (source_id, asset_id, signal_key, value, quality, timestamp)
    and per-record timestamp semantics are NOT gateway-contract-confirmed; the
    default-disabled gate is the mitigation. Order is validate-before-claim.

    Accepts either a single canonical envelope (unchanged) or a legacy TransportBatch
    wrapper (request_id/gateway_id/sent_at_utc/messages[]); the batch path is a pure
    adapter over this same pipeline and does not weaken the single-message contract.
    """
    # ---- Phase A: body + stream routing (no ledger, no write) ----
    try:
        raw = await request.body()
        decoded = decode_body(raw, request.headers.get("content-encoding"))
    except BodyError as e:
        raise HTTPException(e.status_code, "request body rejected")
    try:
        body = json.loads(decoded)
    except json.JSONDecodeError:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "malformed JSON body")

    # Legacy batch wrapper (no top-level stream, has messages[]) -> adapter path.
    if is_batch_body(body):
        return await _ingest_batch(body, session, gw, index, influx)

    stream = body.get("stream") if isinstance(body, dict) else None
    route = route_stream(stream if isinstance(stream, str) else "")
    if route == "deferred":
        raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, f"stream {stream} not supported yet")
    if route == "structured":
        return await _ingest_structured(body, session, gw)
    if route != "ingest":
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "unsupported stream")

    # ---- numeric S1/S2 path (single canonical envelope) ----
    try:
        envelope = EnvelopeIn(**body)
    except (ValidationError, TypeError, ValueError):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "invalid envelope")

    # trusted DB-derived scope; client IDs are never trusted for authorization
    identity = await resolve_gateway_identity(session, gw)
    if not verify_scope(identity, gateway_id=envelope.gateway_id,
                        site_id=envelope.site_id, organization_id=envelope.organization_id):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "envelope scope does not match gateway")

    try:
        kind = await _process_numeric_envelope(
            envelope, session=session, gw=gw, identity=identity, index=index, influx=influx)
    except _NumericError as e:
        raise HTTPException(e.status_code, e.detail)
    return IngestAck(status=kind, message_id=envelope.message_id,
                     record_count=envelope.record_count)


async def _ingest_batch(body, session, gw, index, influx) -> JSONResponse:
    """Legacy TransportBatch adapter. Splits the batch, resolves the authenticated
    gateway's DB identity ONCE, adapts each message to a canonical S1 envelope (stamping
    the trusted identity, never the caller's strings), and runs each through the shared
    numeric pipeline + idempotency ledger. Per-message results are returned so a partial
    failure never false-acks: HTTP 200 only when every message is stored/duplicate; 503 if
    any message hit a retryable infra error (the gateway should retry the whole batch -
    already-stored messages dedupe); otherwise 422 (a permanent per-message rejection the
    gateway must fix, with a diagnostic per message). Nothing is acknowledged as stored
    unless its own ledger row was confirmed stored."""
    messages = body.get("messages")
    if not isinstance(messages, list) or not messages:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "batch has no messages")
    if len(messages) > MAX_BATCH_MESSAGES:
        raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, "batch has too many messages")

    # trusted DB-derived scope; legacy batch IDs are never trusted for authorization
    identity = await resolve_gateway_identity(session, gw)

    results = []
    for i, msg in enumerate(messages):
        try:
            canonical = adapt_batch_message(msg, identity)
        except BatchMessageError as e:
            results.append({"index": i, "message_id": (str(e.message_id) if e.message_id is not None else None),
                            "status": "rejected", "http_status": e.status_code, "detail": e.detail})
            continue

        try:
            envelope = EnvelopeIn(**canonical)
        except (ValidationError, TypeError, ValueError):
            results.append({"index": i, "message_id": canonical.get("message_id"),
                            "status": "rejected", "http_status": status.HTTP_422_UNPROCESSABLE_CONTENT,
                            "detail": "adapted envelope failed canonical validation"})
            continue

        # scope is stamped from the DB identity, so this matches by construction; verified
        # defensively to keep the single authorization path authoritative.
        if not verify_scope(identity, gateway_id=envelope.gateway_id,
                            site_id=envelope.site_id, organization_id=envelope.organization_id):
            results.append({"index": i, "message_id": str(envelope.message_id),
                            "status": "rejected", "http_status": status.HTTP_403_FORBIDDEN,
                            "detail": "envelope scope does not match gateway"})
            continue

        try:
            kind = await _process_numeric_envelope(
                envelope, session=session, gw=gw, identity=identity, index=index, influx=influx)
            results.append({"index": i, "message_id": str(envelope.message_id),
                            "status": kind, "http_status": status.HTTP_200_OK, "detail": None})
        except _NumericError as e:
            results.append({"index": i, "message_id": str(envelope.message_id),
                            "status": "rejected", "http_status": e.status_code, "detail": e.detail})

    accepted = sum(1 for r in results if r["status"] in ("stored", "duplicate"))
    rejected = sum(1 for r in results if r["status"] == "rejected")
    if rejected == 0:
        overall = status.HTTP_200_OK
    elif any(r["http_status"] == status.HTTP_503_SERVICE_UNAVAILABLE for r in results):
        overall = status.HTTP_503_SERVICE_UNAVAILABLE
    else:
        overall = status.HTTP_422_UNPROCESSABLE_CONTENT

    return JSONResponse(status_code=overall, content={
        "request_id": body.get("request_id"),
        "message_count": len(messages),
        "accepted": accepted,
        "rejected": rejected,
        "results": results,
    })


_STRUCTURED_HANDLERS = {
    "S3": (S3HealthEnvelopeIn, persist_health_message),
    "S4": (S4AlarmEnvelopeIn, persist_alarm_events),
    "S8": (S8ConfigAuditEnvelopeIn, persist_config_audit),
}


async def _ingest_structured(body, session, gw) -> IngestAck:
    """Structured-record ingest (S3 gateway_health, S4 alarms/events): validate -> scope ->
    persist to PostgreSQL JSONB via the shared ledger. Mirrors the numeric path's auth/scope/
    idempotency and HTTP error semantics. Malformed payloads fail before any ledger claim."""
    stream = body.get("stream") if isinstance(body, dict) else None
    envelope_cls, persist = _STRUCTURED_HANDLERS[stream]
    try:
        envelope = envelope_cls(**body)
    except (ValidationError, TypeError, ValueError):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "invalid envelope")

    # trusted DB-derived scope; client IDs are never trusted for authorization
    identity = await resolve_gateway_identity(session, gw)
    if not verify_scope(identity, gateway_id=envelope.gateway_id,
                        site_id=envelope.site_id, organization_id=envelope.organization_id):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "envelope scope does not match gateway")

    try:
        decision = await persist(session, gw=gw, identity=identity, envelope=envelope)
    except InvalidValue:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "invalid record")
    except StoreNotConfirmed:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "store not confirmed; retry")
    except Exception:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "ingestion temporarily unavailable")

    if decision == IngestDecision.DUPLICATE_STORED:
        return IngestAck(status="duplicate", message_id=envelope.message_id,
                         record_count=envelope.record_count)
    if decision == IngestDecision.HASH_MISMATCH:
        raise HTTPException(status.HTTP_409_CONFLICT, "message_id reused with a different payload")
    if decision == IngestDecision.IN_PROGRESS:
        raise HTTPException(status.HTTP_409_CONFLICT, "message is already being processed")
    return IngestAck(status="stored", message_id=envelope.message_id,
                     record_count=envelope.record_count)
