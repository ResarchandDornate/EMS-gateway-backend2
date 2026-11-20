from __future__ import annotations

import re
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone

from influxdb_client import Point
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

from src.config import get_settings
from src.signals.types import QUALITY_SUFFIX, coerce, validate_quality
from src.telemetry_errors import (
    AmbiguousPointId,
    InvalidTimestamp,
    RegistryConflict,
    RetentionNotConfigured,
    SignalMismatch,
    UnknownPointId,
    UnknownRetentionClass,
    UnknownSignal,
    UnresolvedMetadata,
    UnsafeQueryInput,
)

# Tags, verbatim from the frozen contract. asset_id = "Runtime Asset",
# source_id = "Source ID". Both are TAGS -> distinct series, never merged.
TAG_KEYS = ("organization_id", "site_id", "gateway_id", "source_id", "asset_id", "substream")
_REQUIRED = ("measurement",) + TAG_KEYS + ("signal_key",)

_IDENT = re.compile(r"^[A-Za-z0-9_.:\-]+$")
_RANGE = re.compile(r"^-?\d+(ns|us|ms|s|m|h|d|w|mo|y)$")
_AGG_FLUX_FNS = frozenset({"mean", "min", "max", "first", "last", "count"})


def measurement_for(stream: str, substream: str) -> str:
    return f"{stream.lower()}_{substream}"


def bucket_for(retention_class: str, settings) -> str:
    """Code-level routing only (NOT the actual Influx retention policy, which must be
    verified in InfluxDB; see src/influx_provision.py). T1->raw, T2->long, T3->the
    dedicated 3-year bucket. Unknown classes and a missing/blank T3 bucket fail closed.

    The T3 bucket's enforced retention (proposed 1095 d) is pending production
    confirmation; it is verified against the configured seconds at provisioning time.
    The registry keeps the T3_3Y classification unchanged."""
    if retention_class == "T1_RAW_90D":
        return settings.influx_bucket_raw
    if retention_class == "T2_1Y":
        return settings.influx_bucket_long
    if retention_class == "T3_3Y":
        bucket = getattr(settings, "influx_bucket_3y", "") or ""
        if not str(bucket).strip():
            raise RetentionNotConfigured("T3_3Y bucket not configured")
        return bucket
    raise UnknownRetentionClass(f"Unknown retention class: {retention_class!r}")


def parse_contract_timestamp(value) -> datetime:
    """Parse ISO-8601 offset-aware timestamp. Semantic validation (invalid calendar
    dates and offsets are rejected), preserve instant, normalize to UTC, reject naive."""
    if not isinstance(value, str):
        raise InvalidTimestamp("timestamp must be an ISO-8601 string")
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as e:
        raise InvalidTimestamp(f"invalid ISO-8601 timestamp: {value!r}") from e
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise InvalidTimestamp("timestamp must include a UTC offset (no naive datetimes)")
    return dt.astimezone(timezone.utc)


def _require_time(ts) -> datetime:
    if not isinstance(ts, datetime):
        raise InvalidTimestamp("timestamp must be a datetime (record Source Updated UTC)")
    if ts.tzinfo is None or ts.utcoffset() is None:
        raise InvalidTimestamp("timestamp must be timezone-aware")
    return ts.astimezone(timezone.utc)


def _ident(name: str, value) -> str:
    if not isinstance(value, str) or not value.strip() or not _IDENT.match(value):
        raise UnsafeQueryInput(f"invalid {name}: {value!r}")
    return value


def _range(value) -> str:
    if isinstance(value, str) and _RANGE.match(value):
        return value
    try:
        parse_contract_timestamp(value)        # semantic RFC3339 validation
    except InvalidTimestamp:
        raise UnsafeQueryInput(f"unsafe range: {value!r}")
    return value


@dataclass(frozen=True)
class RegistrySignal:
    primary_stream: str
    substream: str
    asset_type: str | None
    data_type: str
    retention_class: str


_AMBIGUOUS_POINT_ID = object()   # sentinel: a point_id mapped to >1 distinct signal


@dataclass
class RegistryIndex:
    """Trusted signal metadata from signal_registry, keyed by (measurement, signal_key).

    Also carries an additive point_id -> (measurement, signal_key) index built from the
    populated registry point_id column (T2 CleanEdge + T3 Kalpa). The point_id lookup is
    optional and never alters the existing (measurement, signal_key) resolution."""
    _by_key: dict
    _by_point_id: dict = field(default_factory=dict)

    @classmethod
    def from_rows(cls, rows) -> "RegistryIndex":
        # rows: (primary_stream, substream, signal_key, asset_type, data_type, retention_class[, point_id])
        d = {}
        pid_map = {}
        for row in rows:
            ps, sub, key, asset_type, dt, rc = row[0], row[1], row[2], row[3], row[4], row[5]
            point_id = row[6] if len(row) > 6 else None
            k = (measurement_for(ps, sub), key)
            if k in d:
                raise RegistryConflict(f"duplicate (measurement, signal_key): {k}")
            d[k] = RegistrySignal(ps, sub, asset_type, dt, rc)
            pid = point_id.strip() if isinstance(point_id, str) else point_id
            if pid:                                   # only populated, verified point_ids
                if pid in pid_map and pid_map[pid] != k:
                    pid_map[pid] = _AMBIGUOUS_POINT_ID
                elif pid not in pid_map:
                    pid_map[pid] = k
        return cls(d, pid_map)

    def resolve(self, measurement: str, signal_key: str) -> RegistrySignal:
        try:
            return self._by_key[(measurement, signal_key)]
        except KeyError:
            raise UnknownSignal(f"signal not in registry: {measurement}/{signal_key}")

    def resolve_point_id(self, point_id: str):
        """point_id -> (measurement, signal_key, RegistrySignal). Additive and exact; never
        falls back to a different signal. Raises UnknownPointId when the id is absent or
        unpopulated, AmbiguousPointId when it maps to more than one registry signal."""
        entry = self._by_point_id.get(point_id)
        if entry is None:
            raise UnknownPointId(f"point_id not in registry: {point_id}")
        if entry is _AMBIGUOUS_POINT_ID:
            raise AmbiguousPointId(f"point_id resolves to multiple signals: {point_id}")
        measurement, signal_key = entry
        return measurement, signal_key, self._by_key[entry]

    def t3_signal_keys(self) -> frozenset:
        """Signal keys whose registry retention_class is T3_3Y — the writer's source of
        truth for 3-year routing, consistent with bucket_for. Cached (registry is
        process-lived)."""
        t3 = getattr(self, "_t3_cache", None)
        if t3 is None:
            t3 = frozenset(sk for (_m, sk), sig in self._by_key.items()
                           if sig.retention_class == "T3_3Y")
            self._t3_cache = t3
        return t3

    def __len__(self) -> int:
        return len(self._by_key)


@dataclass
class TelemetryPoint:
    measurement: str
    organization_id: str
    site_id: str
    gateway_id: str
    source_id: str
    asset_id: str
    substream: str
    signal_key: str
    value: object
    quality: str | None
    timestamp: datetime
    asset_type: str | None = None     # optional cross-check; registry asset_type is authoritative


def _validate_identities(p: TelemetryPoint) -> None:
    for name in _REQUIRED:
        v = getattr(p, name)
        if not isinstance(v, str) or not v.strip():
            raise UnsafeQueryInput(f"{name} is required and must be a non-empty string")
        if not _IDENT.match(v):
            raise UnsafeQueryInput(f"invalid {name}: {v!r}")


def build_point(index: RegistryIndex, p: TelemetryPoint, settings):
    """Validate + resolve trusted metadata, build (bucket, Point). Raises before any I/O.
    asset_type is always resolved from the registry (cannot be bypassed); a point-supplied
    asset_type must match it."""
    _validate_identities(p)
    sig = index.resolve(p.measurement, p.signal_key)                 # UnknownSignal
    if p.substream != sig.substream:
        raise SignalMismatch(f"substream mismatch for {p.signal_key}: "
                             f"point={p.substream!r} registry={sig.substream!r}")
    if sig.asset_type is None or not str(sig.asset_type).strip():
        raise UnresolvedMetadata(f"asset_type missing in registry for {p.measurement}/{p.signal_key}")
    if p.asset_type is not None and p.asset_type != sig.asset_type:
        raise SignalMismatch(f"asset_type mismatch for {p.signal_key}: "
                             f"point={p.asset_type!r} registry={sig.asset_type!r}")
    ts = _require_time(p.timestamp)
    val = coerce(sig.data_type, p.value)                             # trusted type + range
    q = validate_quality(p.quality)
    bucket = bucket_for(sig.retention_class, settings)               # trusted retention routing
    pt = Point(p.measurement)
    for k in TAG_KEYS:
        pt = pt.tag(k, getattr(p, k))
    pt = pt.field(p.signal_key, val)
    if q is not None:
        pt = pt.field(f"{p.signal_key}{QUALITY_SUFFIX}", q)
    return bucket, pt.time(ts)


class InfluxGateway:
    """Low-level telemetry store. NOT an authorization boundary — Phase 11 read APIs
    must apply trusted, DB-derived organization/site scope before calling this.

    Conflict behavior: point identity is (measurement + full tag set + timestamp +
    field key); a repeat/identical write overwrites in place (last-write-wins). POINT-level
    overwrite only, NOT message-level idempotency (Phase 9, Postgres).
    """

    def __init__(self, settings=None, client: InfluxDBClientAsync | None = None):
        self._settings = settings or get_settings()
        self._url = self._settings.influx_url
        self._token = self._settings.influx_token
        self._org = self._settings.influx_org
        self._timeout_ms = int(getattr(self._settings, "influx_timeout_ms", 20_000))
        self._shared = client     # app-lifespan shared client (reused); None => per-call

    def _client(self) -> InfluxDBClientAsync:
        # Explicit timeout: a hung write fails fast and well under the 60 s ingest lease,
        # instead of stalling the request and leaving the ledger row pending indefinitely.
        return InfluxDBClientAsync(url=self._url, token=self._token, org=self._org,
                                   timeout=self._timeout_ms)

    @asynccontextmanager
    async def _acquire(self):
        """Yield a client for one operation. Reuse the shared (app-lifespan) client when
        present -- its connection pool avoids opening a fresh connection per message under
        load -- and never close it here. With no shared client, create and close a per-call
        client (backward-compatible with tests and non-app callers)."""
        if self._shared is not None:
            yield self._shared
        else:
            c = self._client()
            try:
                yield c
            finally:
                await c.close()

    @property
    def settings(self):
        return self._settings

    async def health(self) -> bool:
        async with self._client() as c:
            return await c.ping()

    async def write(self, points: list[TelemetryPoint], index: RegistryIndex) -> None:
        """Route each point to its registry-derived bucket. No caller-supplied bucket.
        ALL points are validated and built before any I/O (reject-before-write), so a
        mixed-retention batch with one bad point writes nothing."""
        by_bucket: dict[str, list] = defaultdict(list)
        for p in points:
            bucket, pt = build_point(index, p, self._settings)
            by_bucket[bucket].append(pt)
        async with self._acquire() as c:
            wapi = c.write_api()
            for bucket, recs in by_bucket.items():
                await wapi.write(bucket=bucket, record=recs)

    async def write_built(self, built) -> None:
        """Write pre-built (bucket, Point) tuples (from build_message_points) without
        rebuilding. Destinations are restricted to THIS gateway's configured retention
        buckets; a tuple naming any other bucket is refused (never writes to an arbitrary
        non-configured bucket)."""
        allowed = {self._settings.influx_bucket_raw, self._settings.influx_bucket_long,
                   self._settings.influx_bucket_3y}
        by_bucket: dict[str, list] = defaultdict(list)
        for bucket, pt in built:
            if bucket not in allowed:
                raise UnsafeQueryInput(f"refusing write to non-configured bucket: {bucket!r}")
            by_bucket[bucket].append(pt)
        async with self._acquire() as c:
            wapi = c.write_api()
            for bucket, recs in by_bucket.items():
                await wapi.write(bucket=bucket, record=recs)

    async def query(self, flux: str):
        async with self._client() as c:
            return await c.query_api().query(flux)

    async def latest_signal(self, bucket, measurement, site_id, signal_key,
                            asset_id=None, source_id=None, start="-1h", point_id=None):
        _ident("bucket", bucket); _ident("measurement", measurement); _ident("site_id", site_id)
        _ident("signal_key", signal_key); _range(start)
        extra = ""
        if asset_id:
            extra += f' and r.asset_id == "{_ident("asset_id", asset_id)}"'
        if source_id:
            extra += f' and r.source_id == "{_ident("source_id", source_id)}"'
        if point_id:
            extra += f' and r.point_id == "{_ident("point_id", point_id)}"'
        flux = (
            f'from(bucket: "{bucket}")\n'
            f'  |> range(start: {start})\n'
            f'  |> filter(fn: (r) => r._measurement == "{measurement}" '
            f'and r.site_id == "{site_id}" and (r._field == "{signal_key}" or r._field == "{signal_key}{QUALITY_SUFFIX}"){extra})\n'
            f'  |> group(columns: ["_field"])\n'
            f'  |> last()'
        )
        return await self.query(flux)

    async def latest_by_asset(self, bucket, measurement, site_id, signal_key, start="-1h", point_id=None):
        _ident("bucket", bucket); _ident("measurement", measurement); _ident("site_id", site_id)
        _ident("signal_key", signal_key); _range(start)
        extra = f' and r.point_id == "{_ident("point_id", point_id)}"' if point_id else ""
        flux = (
            f'from(bucket: "{bucket}")\n'
            f'  |> range(start: {start})\n'
            f'  |> filter(fn: (r) => r._measurement == "{measurement}" '
            f'and r.site_id == "{site_id}" and (r._field == "{signal_key}" or r._field == "{signal_key}{QUALITY_SUFFIX}"){extra})\n'
            f'  |> group(columns: ["asset_id", "source_id", "_field"])\n'
            f'  |> last()'
        )
        return await self.query(flux)

    async def series(self, bucket, measurement, site_id, signal_key, start, stop,
                     asset_id=None, source_id=None, limit=1000, point_id=None):
        """Bounded, site-scoped time-series read. All identifiers/ranges are guarded;
        never queries without site_id. The caller enforces window + limit caps; this
        method also refuses a non-positive limit. Points are time-sorted ascending."""
        _ident("bucket", bucket); _ident("measurement", measurement); _ident("site_id", site_id)
        _ident("signal_key", signal_key); _range(start); _range(stop)
        if not isinstance(limit, int) or limit < 1:
            raise UnsafeQueryInput(f"invalid limit: {limit!r}")
        extra = ""
        if asset_id:
            extra += f' and r.asset_id == "{_ident("asset_id", asset_id)}"'
        if source_id:
            extra += f' and r.source_id == "{_ident("source_id", source_id)}"'
        if point_id:
            extra += f' and r.point_id == "{_ident("point_id", point_id)}"'
        flux = (
            f'from(bucket: "{bucket}")\n'
            f'  |> range(start: {start}, stop: {stop})\n'
            f'  |> filter(fn: (r) => r._measurement == "{measurement}" '
            f'and r.site_id == "{site_id}" and r._field == "{signal_key}"{extra})\n'
            f'  |> sort(columns: ["_time"])\n'
            f'  |> limit(n: {int(limit)})'
        )
        return await self.query(flux)

    async def aggregate(self, bucket, measurement, site_id, signal_key,
                        start, stop, every, flux_fn, asset_id, source_id=None, point_id=None):
        """Bounded, site-scoped, time-bucketed read. SPARSE: createEmpty is false, so
        only windows containing data are returned (missing timestamps = gaps, not zero).
        The caller validates window/fn/bucket-count and maps fn to a fixed Flux name; this
        method guards every identifier and never queries without site_id. Bucket timestamp
        = window start."""
        _ident("bucket", bucket); _ident("measurement", measurement); _ident("site_id", site_id)
        _ident("signal_key", signal_key); _range(start); _range(stop); _range(every)
        if flux_fn not in _AGG_FLUX_FNS:
            raise UnsafeQueryInput(f"unsupported aggregate function: {flux_fn!r}")
        extra = f' and r.asset_id == "{_ident("asset_id", asset_id)}"'
        if source_id:
            extra += f' and r.source_id == "{_ident("source_id", source_id)}"'
        if point_id:
            extra += f' and r.point_id == "{_ident("point_id", point_id)}"'
        flux = (
            f'from(bucket: "{bucket}")\n'
            f'  |> range(start: {start}, stop: {stop})\n'
            f'  |> filter(fn: (r) => r._measurement == "{measurement}" '
            f'and r.site_id == "{site_id}" and r._field == "{signal_key}"{extra})\n'
            f'  |> aggregateWindow(every: {every}, fn: {flux_fn}, createEmpty: false, timeSrc: "_start")\n'
            f'  |> sort(columns: ["_time"])'
        )
        return await self.query(flux)


def rows(tables):
    out = []
    for t in tables:
        for rec in t.records:
            out.append({
                "asset_id": rec.values.get("asset_id"),
                "source_id": rec.values.get("source_id"),
                "field": rec.get_field(),
                "value": rec.get_value(),
                "time": rec.get_time(),
            })
    return out