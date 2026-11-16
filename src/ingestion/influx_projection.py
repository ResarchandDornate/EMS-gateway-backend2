"""Project real S2 numeric telemetry records into native-typed InfluxDB points.

Per the backend architecture, ONLY S2 (general_asset_telemetry numeric signals) goes to
InfluxDB. S1 is mixed-type structured and stays in PostgreSQL JSONB; S3..S10 are structured
/ event data and also stay in PostgreSQL. Mixed-type structured records are never forced
into InfluxDB.

S2 -> one point per record, measurement s2_<substream>, asset tag = runtime_asset_id, long
(1y) bucket - except the 12 energy/capacity signals which route to the 3y bucket. Native
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

# 12 energy/capacity signals -> T3 (3-year) bucket (energy_capacity_retention.csv).
ENERGY_T3_SIGNALS = frozenset({
    "system_charge_energy_session", "system_discharge_energy_session",
    "system_full_charge_capacity", "system_full_discharge_capacity",
    "system_accumulated_charge_energy", "system_accumulated_discharge_energy",
    "rack_charge_energy_session", "rack_discharge_energy_session",
    "rack_full_charge_capacity", "rack_full_discharge_capacity",
    "rack_accumulated_charge_energy", "rack_accumulated_discharge_energy",
})


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


def _point(meas, tags: dict, signal_key, fv, quality, dt):
    """Build a Point from an ALREADY-VALIDATED field value `fv` (never None) and `dt`."""
    pt = Point(meas)
    for k, val in tags.items():
        if val:
            pt = pt.tag(k, str(val))
    pt = pt.field(signal_key, fv)
    if isinstance(quality, str) and quality:
        pt = pt.field(f"{signal_key}{QUALITY_SUFFIX}", quality)
    return pt.time(dt)


def _s2_points(envelope, identity, settings):
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
                "asset_id": rec.get("runtime_asset_id") or rec.get("base_asset_id")}
        bucket = (settings.influx_bucket_3y if signal in ENERGY_T3_SIGNALS
                  else settings.influx_bucket_long)
        out.append((bucket, _point(meas, tags, signal, fv, rec.get("quality"), dt)))
    return out


def build_influx_points(envelope, identity, settings):
    """Return [(bucket, Point), ...] for S2 numeric telemetry; [] for any non-S2 stream.

    Raises InvalidValue on any invalid S2 record (see module docstring). It no longer
    swallows shaping errors: the previous blanket `except -> []` allowed malformed records
    to vanish while the message was still ACKed accepted. Callers reject InvalidValue as
    retryable, so no data is lost and nothing is falsely accepted."""
    if getattr(envelope, "stream", None) == "S2":
        return _s2_points(envelope, identity, settings)
    return []
