"""Project real S2 numeric telemetry records into native-typed InfluxDB points.

Per the backend architecture, ONLY S2 (general_asset_telemetry numeric signals) goes to
InfluxDB. S1 is mixed-type structured and stays in PostgreSQL JSONB; S3..S10 are structured
/ event data and also stay in PostgreSQL. Mixed-type structured records are never forced
into InfluxDB.

S2 -> one point per record, measurement s2_<substream>, asset tag = runtime_asset_id, long
(1y) bucket - except the 12 energy/capacity signals which route to the 3y bucket. Native
field types (float / bool / string; nulls skipped; lists/dicts JSON-encoded). Registry is
bypassed (real signal names do not match it). Never raises: a bad signal is skipped.
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


def _point(meas, tags: dict, signal_key, value, quality, dt):
    fv = _field(value)
    if fv is None or dt is None or not isinstance(signal_key, str) or not signal_key:
        return None
    pt = Point(meas)
    for k, val in tags.items():
        if val:
            pt = pt.tag(k, str(val))
    pt = pt.field(signal_key, fv)
    if isinstance(quality, str) and quality:
        pt = pt.field(f"{signal_key}{QUALITY_SUFFIX}", quality)
    return pt.time(dt)


def _s2_points(envelope, identity, settings):
    meas = measurement_for("S2", envelope.substream)
    base = {"organization_id": str(identity["organization_id"]),
            "site_id": str(identity["site_id"]),
            "gateway_id": str(identity["gateway_id"]),
            "substream": envelope.substream}
    out = []
    for rec in envelope.records:
        if not isinstance(rec, dict):
            continue
        signal = rec.get("signal")
        tags = {**base, "source_id": rec.get("source_id"),
                "asset_id": rec.get("runtime_asset_id") or rec.get("base_asset_id")}
        dt = _ts(rec.get("source_updated_utc"), rec.get("observed_at_utc"), envelope.created_at_utc)
        pt = _point(meas, tags, signal, rec.get("value"), rec.get("quality"), dt)
        if pt is None:
            continue
        bucket = (settings.influx_bucket_3y if signal in ENERGY_T3_SIGNALS
                  else settings.influx_bucket_long)
        out.append((bucket, pt))
    return out


def build_influx_points(envelope, identity, settings):
    """Return [(bucket, Point), ...] for S2 numeric telemetry; [] for any other stream
    or on any shaping error (so Postgres paths are never blocked)."""
    try:
        if envelope.stream == "S2":
            return _s2_points(envelope, identity, settings)
    except Exception:
        return []
    return []
