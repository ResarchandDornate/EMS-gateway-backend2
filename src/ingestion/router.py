from fastapi import APIRouter, Depends, HTTPException, status

from src.gateway.dependencies import get_current_gateway

router = APIRouter(prefix="/api/v1/gateway", tags=["ingestion"])


@router.post("/ingest")
async def ingest(gw=Depends(get_current_gateway)):
    """Phase 9b-i: DISABLED (intended safety boundary).

    This endpoint performs gateway bearer authentication ONLY (via
    get_current_gateway) and then returns 503. It does NOT decode the body,
    does NOT parse the envelope, does NOT check envelope scope, and does NOT
    touch the ledger. No message is marked stored and no Influx write occurs.

    The envelope schema, gzip/body handling, scope check and the ledger state
    machine exist and are covered by SERVICE-LEVEL tests only. They are NOT
    wired into this endpoint; endpoint-level validation and persistence are
    deferred to 9b-ii.
    """
    raise HTTPException(
        status.HTTP_503_SERVICE_UNAVAILABLE,
        "Telemetry ingestion is not yet enabled (pending Phase 9b-ii)",
    )
