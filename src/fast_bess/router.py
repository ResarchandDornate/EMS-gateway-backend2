"""S1 Fast BESS read API (PostgreSQL). Auth + site-scoping via require_site_read.

GET /api/v1/bess/fast/latest   - newest record per bess_id at the site (snapshot)
GET /api/v1/bess/fast/history  - historical records, time-filtered, paginated

site_id is supplied and verified by require_site_read (401 unauth / 403 wrong site). All rows
are hard-filtered to the authorized site. No InfluxDB is touched for S1.
"""
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import get_session
from src.fast_bess import service as svc
from src.fast_bess.schemas import (FastBessHistoryResponse, FastBessLatestResponse,
                                    FastBessRecord)
from src.telemetry.dependencies import require_site_read
from src.structured_reads.ranges import RangeError, resolve_range

router = APIRouter(prefix="/api/v1/bess/fast", tags=["bess-fast"])

MAX_LIMIT = 1000
DEFAULT_LATEST_LIMIT = 200
DEFAULT_HISTORY_LIMIT = 500


def _parse_ts(name: str, value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT,
                            f"invalid {name} timestamp (expected ISO 8601)")
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)   # naive -> UTC


def _dto(r) -> FastBessRecord:
    return FastBessRecord(gateway_id=r.gateway_id, message_id=r.message_id,
                          substream=r.substream, record_index=r.record_index,
                          source_id=r.source_id, bess_id=r.bess_id,
                          recorded_at=r.recorded_at, payload=r.payload)


@router.get("/latest", response_model=FastBessLatestResponse)
async def latest(
    gateway_id: uuid.UUID | None = Query(None),
    source_id: str | None = Query(None, max_length=128),
    bess_id: str | None = Query(None, max_length=128),
    limit: int = Query(DEFAULT_LATEST_LIMIT, ge=1, le=MAX_LIMIT),
    site_id: uuid.UUID = Depends(require_site_read),      # 401/403 + authorized site
    session: AsyncSession = Depends(get_session),
):
    rows = await svc.latest(session, site_id=site_id, gateway_id=gateway_id,
                            source_id=source_id, bess_id=bess_id, limit=limit)
    return FastBessLatestResponse(site_id=site_id, results=[_dto(r) for r in rows])


@router.get("/history", response_model=FastBessHistoryResponse)
async def history(
    gateway_id: uuid.UUID | None = Query(None),
    source_id: str | None = Query(None, max_length=128),
    bess_id: str | None = Query(None, max_length=128),
    start: str | None = Query(None, description="ISO 8601; inclusive lower bound"),
    stop: str | None = Query(None, description="ISO 8601; exclusive upper bound"),
    range_: str | None = Query(None, alias="range",
                               description="this_week|this_month|this_year, or rolling in start (-7d/-6mo)"),
    limit: int = Query(DEFAULT_HISTORY_LIMIT, ge=1, le=MAX_LIMIT),
    offset: int = Query(0, ge=0),
    site_id: uuid.UUID = Depends(require_site_read),
    session: AsyncSession = Depends(get_session),
):
    try:
        start_dt, stop_dt = resolve_range(start, stop, range_)
    except RangeError as e:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(e))
    rows, has_more = await svc.history(session, site_id=site_id, gateway_id=gateway_id,
                                       source_id=source_id, bess_id=bess_id,
                                       start=start_dt, stop=stop_dt, limit=limit, offset=offset)
    return FastBessHistoryResponse(site_id=site_id, limit=limit, offset=offset,
                                   has_more=has_more, results=[_dto(r) for r in rows])
