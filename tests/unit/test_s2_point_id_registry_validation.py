"""Patch: S2 point_id tag is written ONLY when the gateway-emitted point_id resolves
UNIQUELY in the approved registry AND matches the record's exact (measurement, signal_key)
-- the only identity dimensions the registry defines for a point_id. Fail-closed: no
resolver -> no tag. Unknown / ambiguous / mismatched -> tag omitted (record + fields kept).
Pure projection test, no Influx I/O."""
from types import SimpleNamespace
from src.ingestion.influx_projection import build_influx_points
from src.influx import RegistryIndex

IDENT = {"organization_id": "org1", "site_id": "site1", "gateway_id": "gw1"}
ON = SimpleNamespace(s2_point_id_tag_enabled=True, influx_bucket_long="telemetry_1y", influx_bucket_3y="telemetry_3y")
OFF = SimpleNamespace(s2_point_id_tag_enabled=False, influx_bucket_long="telemetry_1y", influx_bucket_3y="telemetry_3y")

# approved registry: (primary_stream, substream, signal_key, asset_type, data_type, retention_class, point_id)
ROWS = [
    ("S2", "ems_system",   "battery_system_state", "bms", "int", "T1_1Y", "valid.pid"),
    ("S2", "ems_system",   "soc_percent",          "bms", "int", "T1_1Y", "other.pid"),
    # ambiguous: same point_id on two distinct (measurement, signal_key)
    ("S2", "ems_system",   "sig_a",                "bms", "int", "T1_1Y", "dup.pid"),
    ("S2", "bms_extended", "sig_b",                "bms", "int", "T1_1Y", "dup.pid"),
]
IDX = RegistryIndex.from_rows(ROWS)
RES = IDX.resolve_point_id

def _rec(signal="battery_system_state", value=1, asset="rack_1", pid=None, source="src1"):
    r = {"signal": signal, "value": value, "runtime_asset_id": asset, "source_id": source,
         "source_updated_utc": "2026-11-19T00:00:00Z"}
    if pid is not None: r["point_id"] = pid
    return r

def _env(records, substream="ems_system"):
    return SimpleNamespace(stream="S2", substream=substream, created_at_utc="2026-11-19T00:00:00Z", records=records)

def _lp(bp): return bp[1].to_line_protocol()

# --- registry dimension 1 & 2: measurement AND signal_key must match ---
def test_valid_unique_registry_point_id_is_tagged():
    lp = _lp(build_influx_points(_env([_rec(pid="valid.pid")]), IDENT, ON, resolve_point_id=RES)[0])
    assert "point_id=valid.pid" in lp
    assert "battery_system_state=" in lp              # field still present

def test_unknown_point_id_is_omitted_field_still_written():
    lp = _lp(build_influx_points(_env([_rec(pid="not.in.registry")]), IDENT, ON, resolve_point_id=RES)[0])
    assert "point_id=" not in lp
    assert "battery_system_state=" in lp              # record NOT dropped, just no point_id tag

def test_ambiguous_point_id_is_omitted():
    lp = _lp(build_influx_points(_env([_rec(signal="sig_a", pid="dup.pid")]), IDENT, ON, resolve_point_id=RES)[0])
    assert "point_id=" not in lp
    assert "sig_a=" in lp

def test_signal_key_mismatch_is_omitted():
    # 'valid.pid' maps to battery_system_state; emitting it on soc_percent is a signal_key mismatch
    lp = _lp(build_influx_points(_env([_rec(signal="soc_percent", pid="valid.pid")]), IDENT, ON, resolve_point_id=RES)[0])
    assert "point_id=" not in lp

def test_measurement_mismatch_is_omitted():
    # same signal but different substream/measurement than the registry entry -> omit
    lp = _lp(build_influx_points(_env([_rec(pid="valid.pid")], substream="bms_extended"), IDENT, ON, resolve_point_id=RES)[0])
    assert "point_id=" not in lp

# --- fail-closed: no resolver -> no tag (production never emits an unvalidated point_id) ---
def test_no_resolver_omits_tag_failclosed():
    lp = _lp(build_influx_points(_env([_rec(pid="valid.pid")]), IDENT, ON)[0])
    assert "point_id=" not in lp
    assert "battery_system_state=" in lp

def test_flag_off_suppresses_tag_even_with_resolver():
    lp = _lp(build_influx_points(_env([_rec(pid="valid.pid")]), IDENT, OFF, resolve_point_id=RES)[0])
    assert "point_id=" not in lp

# --- runtime_asset_id is NOT a registry validation dimension (documents the review decision) ---
def test_runtime_asset_id_is_not_a_validation_dimension():
    # same valid (measurement, signal_key, point_id) on a different runtime asset instance -> still tagged
    lp = _lp(build_influx_points(_env([_rec(asset="rack_7", pid="valid.pid")]), IDENT, ON, resolve_point_id=RES)[0])
    assert "point_id=valid.pid" in lp and "asset_id=rack_7" in lp

def test_non_s2_stream_unaffected():
    env = SimpleNamespace(stream="S1", substream="x", created_at_utc="2026-11-19T00:00:00Z", records=[_rec(pid="valid.pid")])
    assert build_influx_points(env, IDENT, ON, resolve_point_id=RES) == []
