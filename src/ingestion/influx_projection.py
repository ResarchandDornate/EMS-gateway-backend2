"""Project real S2 numeric telemetry records into native-typed InfluxDB points.

Per the backend architecture, ONLY S2 (general_asset_telemetry numeric signals) goes to
InfluxDB. S1 is mixed-type structured and stays in PostgreSQL JSONB; S3..S10 are structured
/ event data and also stay in PostgreSQL. Mixed-type structured records are never forced
into InfluxDB.

S2 -> one point per record, measurement s2_<substream>, asset tag = runtime_asset_id, long
(1y) bucket - except signals whose registry retention_class is T3_3Y (shared registry via
t3_signal_keys; the hardcoded set below is a fallback). Native
field types (float / bool / string; null readings skipped; lists/dicts JSON-encoded).
Registry is bypassed (real signal names do not match it).

Validity contract (fix for the batch S2 silent-loss defect): a record whose ``value`` is
null is a NULL READING and is intentionally not stored (not a loss). ANY OTHER unshapeable
record - a non-object record, a missing/blank ``signal``, an unencodable ``value``, or a
record with no parseable timestamp - is INVALID and raises ``InvalidValue`` so the whole
message is rejected BEFORE it is claimed/stored. This mirrors the single-message numeric
path (``build_message_points``), so a malformed S2 record can no longer be dropped while
the message is ACKed ``accepted``. The caller already rejects ``InvalidValue`` as
``retryable:true`` (gateway keeps the data).
"""
import json

from influxdb_client import Point

from src.influx import QUALITY_SUFFIX, measurement_for
from src.ingestion.s3_health import parse_record_timestamp
from src.telemetry_errors import InvalidValue
from src.telemetry_errors import UnknownPointId, AmbiguousPointId

# FALLBACK T3 set (used only when the registry-derived t3_signal_keys is not supplied).
# 12 energy/capacity signals -> T3 (3-year) bucket (energy_capacity_retention.csv).
ENERGY_T3_SIGNALS = frozenset({
    "system_charge_energy_session", "system_discharge_energy_session",
    "system_full_charge_capacity", "system_full_discharge_capacity",
    "system_accumulated_charge_energy", "system_accumulated_discharge_energy",
    "rack_charge_energy_session", "rack_discharge_energy_session",
    "rack_full_charge_capacity", "rack_full_discharge_capacity",
    "rack_accumulated_charge_energy", "rack_accumulated_discharge_energy",
})

# Signal-namespaced SECURE meter metadata (S2.11 solar_meter / S2.12 load_meter). Each is written
# as <signal_key><suffix> so multiple signals that share one (measurement, tag-set, timestamp)
# NEVER collide on a field key -- the same collision-safe discipline as QUALITY_SUFFIX. Absent on
# every non-SECURE record, so existing CleanEdge series are byte-identical.
_SECURE_FIELD_SUFFIXES = (
    ("runtime_signal_id", "_rsid"),
    ("observed_at_utc", "_obs_utc"),
    ("source_updated_utc", "_src_utc"),
    ("source_data_age_ms", "_age_ms"),
)


def _field(v):
    """Native Influx field value, or None to skip this signal."""
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return float(v)                       # uniform numeric type (avoids int/float clash)
    if isinstance(v, str):
        return v
    if v is None:
        return None
    try:
        return json.dumps(v, ensure_ascii=False, default=str)   # list/dict -> JSON string
    except Exception:
        return None


def _ts(*candidates):
    for c in candidates:
        if isinstance(c, str) and c:
            try:
                return parse_record_timestamp(c)
            except InvalidValue:
                continue
    return None


def _point(meas, tags: dict, signal_key, fv, quality, dt, extra_fields=None):
    """Build a Point from an ALREADY-VALIDATED field value `fv` (never None) and `dt`.

    extra_fields: optional {field_key: value} of signal-namespaced SECURE metadata. Keys are
    already <signal_key><suffix>, so they never clash with another signal's value at the same
    timestamp. None/empty for every non-SECURE record -> identical output to before."""
    pt = Point(meas)
    for k, val in tags.items():
        if val:
            pt = pt.tag(k, str(val))
    pt = pt.field(signal_key, fv)
    if isinstance(quality, str) and quality:
        pt = pt.field(f"{signal_key}{QUALITY_SUFFIX}", quality)
    for k, v in (extra_fields or {}).items():
        ev = _field(v)
        if ev is not None:
            pt = pt.field(k, ev)
    return pt.time(dt)


def _s2_points(envelope, identity, settings, t3_signal_keys=None, resolve_point_id=None):
    """Project every S2 record into (bucket, Point). A null-valued record is skipped
    (null reading); any other unshapeable record raises InvalidValue so the whole message
    is rejected before claim/store."""
    meas = measurement_for("S2", envelope.substream)
    base = {"organization_id": str(identity["organization_id"]),
            "site_id": str(identity["site_id"]),
            "gateway_id": str(identity["gateway_id"]),
            "substream": envelope.substream}
    out = []
    for i, rec in enumerate(envelope.records):
        if not isinstance(rec, dict):
            raise InvalidValue(f"S2 record[{i}] is not an object")
        value = rec.get("value")
        if value is None:
            continue                                   # null reading: not stored, not a loss
        signal = rec.get("signal")
        if not isinstance(signal, str) or not signal:
            raise InvalidValue(f"S2 record[{i}] has a missing or blank 'signal'")
        fv = _field(value)
        if fv is None:                                 # non-null value that cannot be encoded
            raise InvalidValue(f"S2 signal {signal!r} has an unencodable value")
        dt = _ts(rec.get("source_updated_utc"), rec.get("observed_at_utc"), envelope.created_at_utc)
        if dt is None:
            raise InvalidValue(f"S2 signal {signal!r} has no parseable timestamp")
        tags = {**base, "source_id": rec.get("source_id"),
                "asset_id": rec.get("runtime_asset_id") or rec.get("base_asset_id"),
                # SECURE meter dimensions; None on every non-SECURE record -> tag omitted by _point.
                "base_asset_id": rec.get("base_asset_id"),
                "meter_role": rec.get("meter_role")}
        pid = rec.get("point_id")
        if (getattr(settings, "s2_point_id_tag_enabled", True) and isinstance(pid, str)
                and pid.strip() and resolve_point_id is not None):
            # Validate BEFORE writing: the gateway-emitted point_id must resolve UNIQUELY in the
            # approved registry AND belong to THIS record's exact (measurement, signal_key) -- the
            # ONLY identity dimensions the registry defines for a point_id (_by_point_id maps
            # point_id -> (measurement, signal_key); runtime_asset_id/asset_type are NOT part of
            # that mapping, so no asset comparison is invented). Unknown / ambiguous / mismatched
            # -> omit the point_id tag (record + all fields still written). Fail-closed: with no
            # resolver, production never emits an unvalidated point_id tag.
            pid_s = pid.strip()
            try:
                r_meas, r_key, _sig = resolve_point_id(pid_s)
            except (UnknownPointId, AmbiguousPointId):
                r_meas = r_key = None
            if r_meas == meas and r_key == signal:
                tags["point_id"] = pid_s
        # Signal-namespaced metadata (collision-safe): each co-timestamped signal keeps its own.
        # SCOPED to SECURE meter records via meter_role -- a CleanEdge S2 record has no meter_role,
        # so it gets NO new fields and its series stay byte-identical.
        extra = {}
        if rec.get("meter_role") is not None:
            for src_key, suffix in _SECURE_FIELD_SUFFIXES:
                v = rec.get(src_key)
                if v is not None:
                    extra[f"{signal}{suffix}"] = v
        t3 = ENERGY_T3_SIGNALS if t3_signal_keys is None else t3_signal_keys
        bucket = (settings.influx_bucket_3y if signal in t3
                  else settings.influx_bucket_long)
        out.append((bucket, _point(meas, tags, signal, fv, rec.get("quality"), dt, extra)))
    return out


def build_influx_points(envelope, identity, settings, t3_signal_keys=None, resolve_point_id=None):
    """Return [(bucket, Point), ...] for S2 numeric telemetry; [] for any non-S2 stream.

    Raises InvalidValue on any invalid S2 record (see module docstring). It no longer
    swallows shaping errors: the previous blanket `except -> []` allowed malformed records
    to vanish while the message was still ACKed accepted. Callers reject InvalidValue as
    retryable, so no data is lost and nothing is falsely accepted."""
    if getattr(envelope, "stream", None) == "S2":
        return _s2_points(envelope, identity, settings, t3_signal_keys, resolve_point_id)
    return []
