import json

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import get_session
from src.gateway.service import resolve_gateway_identity
from src.ingestion.body import BodyError, decode_body
from src.ingestion.mapping import build_message_points
from src.ingestion.providers import (get_influx_gateway, get_registry_index,
                                      require_ingest_enabled)
from src.ingestion.schemas import EnvelopeIn, IngestAck
from src.ratelimit import ingest_rate_limit
from src.ingestion.service import (IngestDecision, canonical_hash, claim_or_resolve,
                                   mark_stored, route_stream, verify_scope)
from src.telemetry_errors import (InfluxTelemetryError, RetentionNotConfigured,
                                   UnknownRetentionClass, UnresolvedMetadata)

router = APIRouter(prefix="/api/v1/gateway", tags=["ingestion"])


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
    """
    # ---- Phase A: body + envelope (no ledger, no write) ----
    try:
        raw = await request.body()
        decoded = decode_body(raw, request.headers.get("content-encoding"))
    except BodyError as e:
        raise HTTPException(e.status_code, "request body rejected")
    try:
        envelope = EnvelopeIn(**json.loads(decoded))
    except json.JSONDecodeError:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "malformed JSON body")
    except (ValidationError, TypeError, ValueError):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "invalid envelope")

    # trusted DB-derived scope; client IDs are never trusted for authorization
    identity = await resolve_gateway_identity(session, gw)
    if not verify_scope(identity, gateway_id=envelope.gateway_id,
                        site_id=envelope.site_id, organization_id=envelope.organization_id):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "envelope scope does not match gateway")

    route = route_stream(envelope.stream)
    if route == "deferred":
        raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, f"stream {envelope.stream} not supported yet")
    if route != "ingest":
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "unsupported stream")

    # ---- Phase B: validate + build all points (no ledger, no write) ----
    try:
        built = build_message_points(envelope, identity, index, influx.settings)
    except (RetentionNotConfigured, UnknownRetentionClass):
        raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, "retention class not configured")
    except UnresolvedMetadata:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "registry metadata incomplete")
    except InfluxTelemetryError:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "invalid telemetry record")

    payload_hash = canonical_hash(envelope.model_dump())

    # ---- Phase C: ledger claim (DB infra errors -> 503, never 422) ----
    try:
        decision, token = await claim_or_resolve(
            session, gateway_id=gw.id, stream=envelope.stream, message_id=envelope.message_id,
            sequence=envelope.sequence, record_count=envelope.record_count, payload_hash=payload_hash)
    except Exception:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "ingestion temporarily unavailable")

    if decision == IngestDecision.DUPLICATE_STORED:
        return IngestAck(status="duplicate", message_id=envelope.message_id,
                         record_count=envelope.record_count)
    if decision == IngestDecision.HASH_MISMATCH:
        raise HTTPException(status.HTTP_409_CONFLICT, "message_id reused with a different payload")
    if decision == IngestDecision.IN_PROGRESS:
        raise HTTPException(status.HTTP_409_CONFLICT, "message is already being processed")

    # decision is CLAIMED or RECLAIMED
    # ---- Phase D: write to Influx, then confirm stored via lease-owned CAS ----
    try:
        await influx.write_built(built)
    except Exception:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "telemetry store temporarily unavailable")
    try:
        stored = await mark_stored(session, gateway_id=gw.id,
                                   message_id=envelope.message_id, token=token)
    except Exception:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "ingestion temporarily unavailable")
    if not stored:                                  # lease lost -> do NOT acknowledge stored
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "store not confirmed; retry")
    return IngestAck(status="stored", message_id=envelope.message_id,
                     record_count=envelope.record_count)
