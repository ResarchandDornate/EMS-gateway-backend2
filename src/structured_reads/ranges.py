"""Shared time-range grammar for the PostgreSQL read APIs (S1, S3-S9). UTC throughout;
stop is exclusive. Pure functions (no DB / no FastAPI) so they unit-test directly.

A window is expressed as EITHER:
  * absolute ISO-8601 in start/stop  (back-compatible), OR
  * a rolling token in start:  -<n>h | -<n>d | -<n>w | -<n>mo | -<n>y
    (h/d/w are fixed-length; mo/y are calendar math, leap-year safe), OR
  * a calendar keyword in `range`: this_week | this_month | this_year  (period-to-date).

`range` is mutually exclusive with start/stop. Invalid or conflicting input raises
RangeError, which the routers map to HTTP 422.
"""
import calendar
import re
from datetime import datetime, timedelta, timezone

UNITS = ("hour", "day", "week", "month", "year")
_ROLLING = re.compile(r"^-(\d+)(h|d|w|mo|y)$")
_CALENDAR = ("this_week", "this_month", "this_year")


class RangeError(ValueError):
    """Bad or conflicting time-range parameters (-> HTTP 422)."""


def _now(now):
    return now or datetime.now(timezone.utc)


def _add_months(dt: datetime, n: int) -> datetime:
    m0 = dt.month - 1 + n
    y = dt.year + m0 // 12
    m = m0 % 12 + 1
    d = min(dt.day, calendar.monthrange(y, m)[1])     # clamp (e.g. Jan 31 -> Feb 28/29)
    return dt.replace(year=y, month=m, day=d)


def _parse_point(token: str, now: datetime) -> datetime:
    s = (token or "").strip()
    m = _ROLLING.match(s)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        if unit == "h":
            return now - timedelta(hours=n)
        if unit == "d":
            return now - timedelta(days=n)
        if unit == "w":
            return now - timedelta(weeks=n)
        if unit == "mo":
            return _add_months(now, -n)
        return _add_months(now, -12 * n)              # y
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        raise RangeError(f"invalid timestamp {token!r} (ISO 8601 or -7d/-4w/-6mo/-1y)")
    return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)


def _calendar_start(kind: str, now: datetime) -> datetime:
    if kind == "this_week":
        d = now - timedelta(days=now.weekday())       # Monday
        return d.replace(hour=0, minute=0, second=0, microsecond=0)
    if kind == "this_month":
        return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)   # this_year


def resolve_range(start: str | None, stop: str | None, range_: str | None, now: datetime | None = None):
    """Return (start_dt|None, stop_dt|None) in UTC. Unbounded (None,None) when nothing given,
    preserving the existing history endpoints' default. Raises RangeError on bad/conflicting input."""
    now = _now(now)
    if range_ is not None:
        if start is not None or stop is not None:
            raise RangeError("`range` cannot be combined with start/stop")
        if range_ not in _CALENDAR:
            raise RangeError(f"invalid range {range_!r} (one of {_CALENDAR})")
        return _calendar_start(range_, now), now
    start_dt = _parse_point(start, now) if start is not None else None
    stop_dt = _parse_point(stop, now) if stop is not None else None
    if start_dt is not None and stop_dt is not None and start_dt >= stop_dt:
        raise RangeError("start must be before stop")
    return start_dt, stop_dt


def resolve_bounded(start, stop, range_, now=None):
    """Like resolve_range but for summaries: a lower bound is required and stop defaults to now."""
    now = _now(now)
    start_dt, stop_dt = resolve_range(start, stop, range_, now)
    if start_dt is None:
        raise RangeError("a summary requires a lower bound (start=-30d, range=this_month, ...)")
    return start_dt, (stop_dt or now)


# ---- bucket math (must mirror Postgres date_trunc in UTC) ----
def validate_unit(unit: str) -> str:
    if unit not in UNITS:
        raise RangeError(f"invalid group_by {unit!r} (one of {UNITS})")
    return unit


def trunc(dt: datetime, unit: str) -> datetime:
    dt = dt.astimezone(timezone.utc)
    if unit == "hour":
        return dt.replace(minute=0, second=0, microsecond=0)
    if unit == "day":
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    if unit == "week":
        d = dt - timedelta(days=dt.weekday())
        return d.replace(hour=0, minute=0, second=0, microsecond=0)
    if unit == "month":
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)   # year


def step(dt: datetime, unit: str) -> datetime:
    if unit == "hour":
        return dt + timedelta(hours=1)
    if unit == "day":
        return dt + timedelta(days=1)
    if unit == "week":
        return dt + timedelta(weeks=1)
    if unit == "month":
        return _add_months(dt, 1)
    return _add_months(dt, 12)                        # year


def enumerate_buckets(start: datetime, stop: datetime, unit: str, max_buckets: int = 1000):
    """Ordered UTC bucket boundaries covering [start, stop). Raises RangeError if the window
    would exceed max_buckets (guards against expensive queries)."""
    b = trunc(start, unit)
    out = []
    while b < stop:
        out.append(b)
        if len(out) > max_buckets:
            raise RangeError(f"range too large for group_by={unit} (> {max_buckets} buckets)")
        b = step(b, unit)
    return out
