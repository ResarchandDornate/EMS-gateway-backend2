"""Phase 10 read helpers: stream support, bounded-window parsing, Flux-time formatting."""
import re
from datetime import datetime, timedelta, timezone

from src.influx import parse_contract_timestamp
from src.streams import INGEST_STREAMS

# Telemetry read supports exactly the ingest-eligible streams (single source: src/streams).
SUPPORTED_STREAMS = INGEST_STREAMS
MAX_WINDOW = timedelta(days=7)
DEFAULT_LIMIT = 1000
MAX_LIMIT = 5000

_REL = re.compile(r"^-(\d+)(s|m|h|d|w)$")
_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


def resolve_start(start: str, now: datetime) -> datetime:
    """A supported relative range (e.g. -6h, -7d) or an RFC3339 timestamp (raises
    InvalidTimestamp on anything else)."""
    m = _REL.match(start)
    if m:
        return now - timedelta(seconds=int(m.group(1)) * _UNIT_SECONDS[m.group(2)])
    return parse_contract_timestamp(start)


def resolve_window(start: str, stop: str | None):
    """Return (start_dt, stop_dt) in UTC or raise ValueError/InvalidTimestamp for an
    invalid, reversed, or over-7-day window."""
    now = datetime.now(timezone.utc)
    start_dt = resolve_start(start, now)
    stop_dt = parse_contract_timestamp(stop) if stop else now
    if start_dt >= stop_dt:
        raise ValueError("start must be before stop")
    if stop_dt - start_dt > MAX_WINDOW:
        raise ValueError("time window exceeds 7 days")
    return start_dt, stop_dt


def to_flux_ts(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


# ---- Phase 10 Stage 2: time-bucketed aggregation ----
MAX_AGG_WINDOW = timedelta(days=366)
MAX_BUCKETS = 2000
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_MAX_WINDOW_DIGITS = 9   # the 366-day cap is 31_622_400s (8 digits); >9 digits is always invalid

# public fn name -> fixed Flux function (NEVER interpolate client input)
AGG_FUNCS = {
    "mean": "mean", "min": "min", "max": "max",
    "first": "first", "last": "last", "count": "count",
}
# fn -> the field_type() values it is valid for. field_type() returns exactly
# "float" or "int" (src/signals/types.py); every current registry signal is numeric,
# and the gate still rejects any future non-numeric type with 422.
_AGG_FN_TYPES = {
    "mean":  {"float", "int"},
    "min":   {"float", "int"},
    "max":   {"float", "int"},
    "first": {"float", "int"},
    "last":  {"float", "int"},
    "count": {"float", "int"},
}

_WINDOW = re.compile(r"^(\d+)(s|m|h|d|w)$")


def parse_window(window: str) -> timedelta:
    """Positive bucket interval, capped at MAX_AGG_WINDOW. Raises ValueError on a bad
    format, non-positive value, or an interval too large -- including an extremely long
    digit string, rejected by length BEFORE int() so a pathological input maps to 422."""
    m = _WINDOW.match(window or "")
    if not m:
        raise ValueError(f"invalid window: {window!r}")
    digits = m.group(1)
    if len(digits) > _MAX_WINDOW_DIGITS:
        raise ValueError(f"window value too large: {window!r}")
    n = int(digits)
    if n <= 0:
        raise ValueError(f"window must be positive: {window!r}")
    secs = n * _UNIT_SECONDS[m.group(2)]
    if secs > int(MAX_AGG_WINDOW.total_seconds()):
        raise ValueError(f"window exceeds maximum of 366 days: {window!r}")
    return timedelta(seconds=secs)


def flux_every(every: timedelta) -> str:
    """Whole-second Flux duration literal for aggregateWindow."""
    return f"{int(every.total_seconds())}s"


def agg_flux_fn(fn: str, field_type_name: str) -> str:
    """Map a public fn to its fixed Flux function, rejecting (ValueError) an unknown fn
    or an fn not valid for the resolved signal's field type."""
    flux = AGG_FUNCS.get(fn)
    if flux is None:
        raise ValueError(f"unsupported aggregate function: {fn!r}")
    if field_type_name not in _AGG_FN_TYPES[fn]:
        raise ValueError(f"function {fn!r} not valid for field type {field_type_name!r}")
    return flux


def _epoch_us(dt: datetime) -> int:
    """Exact integer microseconds since the UTC epoch (no float rounding)."""
    return (dt.astimezone(timezone.utc) - _EPOCH) // timedelta(microseconds=1)


def bucket_count(start_dt: datetime, stop_dt: datetime, every: timedelta) -> int:
    """Exact count of epoch/UTC-aligned windows of width `every` overlapping the half-open
    interval [start, stop), via integer microsecond arithmetic. Exact (not merely
    conservative): an exact stop boundary is excluded; a partial boundary window at either
    end is counted once; fractional-second timestamps honored at us resolution."""
    w_us = int(every.total_seconds()) * 1_000_000
    s = _epoch_us(start_dt)
    e = _epoch_us(stop_dt)
    first = s // w_us
    first_empty = -(-e // w_us)
    return first_empty - first


def resolve_agg(start: str, stop: str | None, window: str):
    """Validate an aggregation request BEFORE any InfluxDB call. Returns
    (start_dt, stop_dt, every) in UTC, or raises ValueError/InvalidTimestamp for a bad
    format, reversed/empty range, over-366-day horizon, bad/oversized window, or more than
    MAX_BUCKETS aligned output buckets."""
    now = datetime.now(timezone.utc)
    start_dt = resolve_start(start, now)
    stop_dt = parse_contract_timestamp(stop) if stop else now
    if start_dt >= stop_dt:
        raise ValueError("start must be before stop")
    if stop_dt - start_dt > MAX_AGG_WINDOW:
        raise ValueError("time window exceeds 366 days")
    every = parse_window(window)
    if bucket_count(start_dt, stop_dt, every) > MAX_BUCKETS:
        raise ValueError(f"aggregation exceeds {MAX_BUCKETS} buckets")
    return start_dt, stop_dt, every
