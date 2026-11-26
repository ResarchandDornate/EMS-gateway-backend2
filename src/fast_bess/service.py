"""S1 Fast BESS read queries (async SQLAlchemy over PostgreSQL fast_bess_records).

Security: every query is filtered by the authorized site_id resolved by require_site_read.
gateway_id/source_id/bess_id are narrowing filters *inside* that site, so a gateway_id from
another site simply matches nothing — no cross-site leakage. No raw SQL, no full-table loads.
"""
import uuid

from sqlalchemy import Integer, TIMESTAMP, column, select, true, values
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

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


async def slices(
    session: AsyncSession,
    *,
    site_id: uuid.UUID,
    windows: list[tuple],
    gateway_id: uuid.UUID | None = None,
    source_id: str | None = None,
    bess_id: str | None = None,
) -> list[M | None]:
    """Newest record in each [from, to) window, in ONE query. Returns a list aligned to
    `windows`; each entry is that window's newest record or None. A LATERAL join does one
    index seek per window off (site_id, bess_id, recorded_at), so this is the single-round-trip
    equivalent of N separate limit=1 reads. Same site hard-filter as the other readers."""
    if not windows:
        return []
    w = values(
        column("idx", Integer),
        column("from_ts", TIMESTAMP(timezone=True)),
        column("to_ts", TIMESTAMP(timezone=True)),
        name="w",
    ).data([(i, f, t) for i, (f, t) in enumerate(windows)])

    inner = select(M).where(M.site_id == site_id,
                            M.recorded_at >= w.c.from_ts, M.recorded_at < w.c.to_ts)
    inner = _narrow(inner, gateway_id=gateway_id, source_id=source_id, bess_id=bess_id)
    newest = inner.order_by(M.recorded_at.desc()).limit(1).lateral("newest")
    N = aliased(M, newest)

    stmt = (select(w.c.idx, N)
            .select_from(w.join(N, true(), isouter=True))
            .order_by(w.c.idx))
    out: list[M | None] = [None] * len(windows)
    for idx, rec in (await session.execute(stmt)).all():
        if 0 <= idx < len(windows):
            out[idx] = rec
    return out
