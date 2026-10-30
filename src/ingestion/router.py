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
from src.ingestion.s1_fast_bess import S1FastBessEnvelopeIn
from src.ingestion.fast_bess_service import persist_fast_bess_message
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
    """Internal: a numeric-path failure carrying the HTTP status + detail the single
    endpoint raises. Lets the single-message path share the exact ledger/write pipeline."""

    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail)


def _format_validation_error(e) -> str:
    """Compact, field-level description of a pydantic/validation failure (no bulk dump)."""
    if isinstance(e, ValidationError):
        return "; ".join(
            f"{'.'.join(str(p) for p in er['loc'])}: {er['msg']} (got {repr(er.get('input'))[:80]})"
            for er in e.errors()[:8]
        )
    return f"{type(e).__name__}: {e}"


async def _process_numeric_envelope(envelope, *, session, gw, identity, index, influx) -> str:
    """Validate -> build points -> ledger claim -> Influx write -> confirm stored, for a
    single canonical S1/S2 envelope whose scope has already been established. Returns
    "stored" or "duplicate"; raises _NumericError for every failure mode. Used only by
    the single-message endpoint; the batch path uses the structured S1 service."""
    try:
        built = build_message_points(envelope, identity, index, influx.settings)
    except (RetentionNotConfigured, UnknownRetentionClass):
        raise _NumericError(status.HTTP_501_NOT_IMPLEMENTED, "retention class not configured")
    except UnresolvedMetadata:
        raise _NumericError(status.HTTP_500_INTERNAL_SERVER_ERROR, "registry metadata incomplete")
    except InfluxTelemetryError:
        raise _NumericError(status.HTTP_422_UNPROCESSABLE_CONTENT, "invalid telemetry record")

    payload_hash = canonical_hash(envelope.model_dump())

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

    try:
        await influx.write_built(built)
    except Exception:
        raise _NumericError(status.HTTP_503_SERVICE_UNAVAILABLE, "telemetry store temporarily unavailable")
    try:
        stored = await mark_stored(session, gateway_id=gw.id,
                                   message_id=envelope.message_id, token=token)
    except Exception:
        raise _NumericError(status.HTTP_503_SERVICE_UNAVAILABLE, "ingestion temporarily unavailable")
    if not stored:
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
    """Live ingestion behind the default-disabled gate.

    Accepts either a single canonical envelope (unchanged numeric/structured routing) or a
    legacy TransportBatch wrapper (request_id/gateway_id/sent_at_utc/messages[]). The batch
    path is a pure adapter that stores S1 fast_bess verbatim as structured JSONB and returns
    the gateway's accepted[]/rejected[] ACK contract; it does not weaken the single-message
    contract.
    """
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
        return await _ingest_batch(body, session, gw)

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


async def _ingest_batch(body, session, gw) -> JSONResponse:
    """Legacy TransportBatch adapter. Returns the gateway's ACK contract (HTTP 200 with
    accepted[]/rejected[]). fast_bess_telemetry -> S1 structured JSONB, stored verbatim;
    unsupported streams and bad messages -> rejected with retryable:true so the gateway
    keeps them (no data loss, nothing permanently dropped). A message is reported in
    `accepted` ONLY after its row is committed (mark_stored)."""
    messages = body.get("messages")
    if not isinstance(messages, list) or not messages:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "batch has no messages")
    if len(messages) > MAX_BATCH_MESSAGES:
        raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, "batch has too many messages")

    # trusted DB-derived scope; legacy batch IDs are never trusted for authorization
    identity = await resolve_gateway_identity(session, gw)

    accepted: list[dict] = []
    rejected: list[dict] = []

    def _reject(message_id, error_code, detail, *, retryable=True):
        rejected.append({
            "message_id": (str(message_id) if message_id is not None else None),
            "error_code": error_code, "detail": detail, "retryable": retryable,
        })

    for msg in messages:
        try:
            canonical = adapt_batch_message(msg, identity)
        except BatchMessageError as e:
            # unsupported stream / missing fields -> keep on the gateway (retryable)
            _reject(e.message_id, e.error_code, e.detail, retryable=True)
            continue

        try:
            envelope = S1FastBessEnvelopeIn(**canonical)
        except (ValidationError, TypeError, ValueError) as e:
            _reject(canonical.get("message_id"), "invalid_payload",
                    _format_validation_error(e), retryable=True)
            continue

        # scope is stamped from the DB identity, so this matches by construction; verified
        # defensively to keep the single authorization path authoritative.
        if not verify_scope(identity, gateway_id=envelope.gateway_id,
                            site_id=envelope.site_id, organization_id=envelope.organization_id):
            _reject(str(envelope.message_id), "invalid_payload",
                    "envelope scope does not match gateway", retryable=True)
            continue

        try:
            decision = await persist_fast_bess_message(
                session, gw=gw, identity=identity, envelope=envelope)
        except InvalidValue as e:
            _reject(str(envelope.message_id), "invalid_payload", str(e), retryable=True)
            continue
        except StoreNotConfirmed:
            _reject(str(envelope.message_id), "temporary_backend_unavailable",
                    "store not confirmed; retry", retryable=True)
            continue
        except Exception:
            _reject(str(envelope.message_id), "temporary_backend_unavailable",
                    "ingestion temporarily unavailable", retryable=True)
            continue

        if decision == IngestDecision.DUPLICATE_STORED:
            accepted.append({"message_id": str(envelope.message_id), "status": "already_processed"})
        elif decision == IngestDecision.HASH_MISMATCH:
            _reject(str(envelope.message_id), "message_id_reused",
                    "message_id reused with a different payload", retryable=True)
        elif decision == IngestDecision.IN_PROGRESS:
            _reject(str(envelope.message_id), "temporary_backend_unavailable",
                    "message is already being processed", retryable=True)
        else:  # CLAIMED / RECLAIMED -> stored this delivery
            accepted.append({"message_id": str(envelope.message_id), "status": "accepted"})

    return JSONResponse(status_code=status.HTTP_200_OK,
                        content={"accepted": accepted, "rejected": rejected})


_STRUCTURED_HANDLERS = {
    "S3": (S3HealthEnvelopeIn, persist_health_message),
    "S4": (S4AlarmEnvelopeIn, persist_alarm_events),
    "S8": (S8ConfigAuditEnvelopeIn, persist_config_audit),
}


async def _ingest_structured(body, session, gw) -> IngestAck:
    """Structured-record ingest (S3 gateway_health, S4 alarms/events, S8 config audit):
    validate -> scope -> persist to PostgreSQL JSONB via the shared ledger. Mirrors the
    numeric path's auth/scope/idempotency and HTTP error semantics."""
    stream = body.get("stream") if isinstance(body, dict) else None
    envelope_cls, persist = _STRUCTURED_HANDLERS[stream]
    try:
        envelope = envelope_cls(**body)
    except (ValidationError, TypeError, ValueError):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "invalid envelope")

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
