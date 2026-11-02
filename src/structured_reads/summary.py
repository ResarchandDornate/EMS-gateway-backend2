"""Bucketed count summaries for the event streams (S4/S8/S9). Aggregation is done in SQL
(date_trunc + GROUP BY, UTC); only the already-small grouped result reaches the app, which
zero-fills the continuous bucket axis so charts render gap-free. Never loads raw rows.
"""
from datetime import timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.structured_reads import ranges

MAX_BUCKETS = 1000


def _key(dt):
    # Postgres timezone('UTC', ts) returns a NAIVE UTC wall-clock value; only convert
    # when tz-aware (the axis side). Never reinterpret a naive value as local time.
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)
    return dt.replace(tzinfo=None)


def _iso(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


async def summarize(session: AsyncSession, model, *, site_id, time_col, unit,
                    start, stop, dims: dict, eq_filters):
    """dims: {name: Column} breakdown dimensions. eq_filters: list[(Column, value|None)].
    Returns a list of {bucket, count, breakdowns:{name:{value:count}}} over every bucket in
    [start, stop), zero-filled. Raises ranges.RangeError if the window is too large."""
    axis = ranges.enumerate_buckets(start, stop, unit, MAX_BUCKETS)     # guard + zero-fill axis

    bucket_expr = func.date_trunc(unit, func.timezone("UTC", time_col)).label("bucket")
    dim_cols = list(dims.values())
    stmt = select(bucket_expr, *dim_cols, func.count().label("n")).where(
        model.site_id == site_id, time_col >= start, time_col < stop)
    for col, val in eq_filters:
        if val is not None:
            stmt = stmt.where(col == val)
    stmt = stmt.group_by(bucket_expr, *dim_cols)

    acc = {}   # bucket_key -> {"count": int, "breakdowns": {name: {val: n}}}
    for row in (await session.execute(stmt)).all():
        b = row[0]
        dim_vals = row[1:1 + len(dims)]
        n = row[-1]
        e = acc.setdefault(_key(b), {"count": 0, "breakdowns": {k: {} for k in dims}})
        e["count"] += n
        for name, val in zip(dims, dim_vals):
            key = "null" if val is None else str(val)
            e["breakdowns"][name][key] = e["breakdowns"][name].get(key, 0) + n

    out = []
    for a in axis:
        e = acc.get(_key(a))
        out.append({"bucket": _iso(a),
                    "count": e["count"] if e else 0,
                    "breakdowns": e["breakdowns"] if e else {k: {} for k in dims}})
    return out
