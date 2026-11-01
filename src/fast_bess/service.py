"""S1 Fast BESS read queries (async SQLAlchemy over PostgreSQL fast_bess_records).

Security: every query is filtered by the authorized site_id resolved by require_site_read.
gateway_id/source_id/bess_id are narrowing filters *inside* that site, so a gateway_id from
another site simply matches nothing — no cross-site leakage. No raw SQL, no full-table loads.
"""
import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.ingestion.fast_bess_models import FastBessRecordModel as M


def _narrow(stmt, *, gateway_id, source_id, bess_id):
    if gateway_id is not None:
        stmt = stmt.where(M.gateway_id == gateway_id)
    if source_id is not None:
        stmt = stmt.where(M.source_id == source_id)
    if bess_id is not None:
        stmt = stmt.where(M.bess_id == bess_id)
    return stmt


async def latest(
    session: AsyncSession,
    *,
    site_id: uuid.UUID,
    gateway_id: uuid.UUID | None = None,
    source_id: str | None = None,
    bess_id: str | None = None,
    limit: int = 200,
) -> list[M]:
    """Newest record per bess_id at the site (current snapshot). DISTINCT ON is Postgres-native."""
    stmt = select(M).where(M.site_id == site_id)
    stmt = _narrow(stmt, gateway_id=gateway_id, source_id=source_id, bess_id=bess_id)
    stmt = stmt.order_by(M.bess_id, M.recorded_at.desc()).distinct(M.bess_id).limit(limit)
    return list((await session.execute(stmt)).scalars().all())


async def history(
    session: AsyncSession,
    *,
    site_id: uuid.UUID,
    gateway_id: uuid.UUID | None = None,
    source_id: str | None = None,
    bess_id: str | None = None,
    start=None,
    stop=None,
    limit: int = 500,
    offset: int = 0,
) -> tuple[list[M], bool]:
    """Historical records (newest first), optional [start, stop) window, limit/offset paging.
    Fetches limit+1 to compute has_more without a full COUNT. Returns (page, has_more)."""
    stmt = select(M).where(M.site_id == site_id)
    stmt = _narrow(stmt, gateway_id=gateway_id, source_id=source_id, bess_id=bess_id)
    if start is not None:
        stmt = stmt.where(M.recorded_at >= start)
    if stop is not None:
        stmt = stmt.where(M.recorded_at < stop)
    stmt = stmt.order_by(M.recorded_at.desc()).offset(offset).limit(limit + 1)
    rows = list((await session.execute(stmt)).scalars().all())
    has_more = len(rows) > limit
    return rows[:limit], has_more
