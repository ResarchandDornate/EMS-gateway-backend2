"""Phase 2: additive S2 point_id tag + read-path point_id filter.
Pure, no Influx I/O: inspects built Points (line protocol) and the generated Flux."""
import asyncio
from types import SimpleNamespace

from src.ingestion.influx_projection import build_influx_points
from src.influx import InfluxGateway, RegistryIndex

# approved registry for the two tests that assert a tag is written (fail-closed: a
# resolver is now required, and the emitted point_id must match (measurement, signal_key)).
_REG = RegistryIndex.from_rows([
    ('S2','ems_system','battery_system_state','bms','int','T1_1Y','bank.system_data.battery_system_state'),
    ('S2','ems_system','soc_percent','bms','int','T1_1Y','p.B'),
])
_RES = _REG.resolve_point_id

IDENT = {"organization_id": "org1", "site_id": "site1", "gateway_id": "gw1"}
ON = SimpleNamespace(s2_point_id_tag_enabled=True, influx_bucket_long="telemetry_1y", influx_bucket_3y="telemetry_3y")
OFF = SimpleNamespace(s2_point_id_tag_enabled=False, influx_bucket_long="telemetry_1y", influx_bucket_3y="telemetry_3y")

def _rec(signal="battery_system_state", value=1, asset="rack_1", pid=None, source="src1"):
    r = {"signal": signal, "value": value, "runtime_asset_id": asset, "source_id": source,
         "source_updated_utc": "2026-11-19T00:00:00Z"}
    if pid is not None: r["point_id"] = pid
    return r

def _env(records, substream="ems_system"):
    return SimpleNamespace(stream="S2", substream=substream, created_at_utc="2026-11-19T00:00:00Z", records=records)

def _lp(bucket_point): return bucket_point[1].to_line_protocol()

def test_point_id_tag_added_when_present():
    pts = build_influx_points(_env([_rec(pid="bank.system_data.battery_system_state")]), IDENT, ON, resolve_point_id=_RES)
    assert len(pts) == 1
    assert 'point_id=bank.system_data.battery_system_state' in _lp(pts[0])

def test_legacy_record_without_point_id_has_no_tag():
    lp = _lp(build_influx_points(_env([_rec()]), IDENT, ON)[0])
    assert "point_id=" not in lp   # fallback: unchanged behavior

def test_flag_off_suppresses_tag_even_if_present():
    lp = _lp(build_influx_points(_env([_rec(pid="p.1")]), IDENT, OFF)[0])
    assert "point_id=" not in lp

def test_distinct_point_ids_are_distinct_series():
    # distinct SIGNALS each with their own registry point_id (a measurement+signal has exactly
    # one point_id, so two ids on one signal is not a valid registry scenario).
    pts = build_influx_points(_env([_rec(signal="battery_system_state", pid="bank.system_data.battery_system_state"),
                                    _rec(signal="soc_percent", pid="p.B")]), IDENT, ON, resolve_point_id=_RES)
    a, b = _lp(pts[0]), _lp(pts[1])
    assert 'point_id=bank.system_data.battery_system_state' in a and 'point_id=p.B' in b and a != b

def test_gateway_and_asset_isolation():
    pts = build_influx_points(_env([_rec(asset="rack_1", pid="p.1"), _rec(asset="rack_2", pid="p.1")]), IDENT, ON)
    assert 'asset_id=rack_1' in _lp(pts[0]) and 'asset_id=rack_2' in _lp(pts[1])

def test_bucket_routing_preserved():
    e3 = build_influx_points(_env([_rec(signal="system_accumulated_charge_energy", pid="p.e")]), IDENT, ON)
    e1 = build_influx_points(_env([_rec(signal="battery_system_state", pid="p.n")]), IDENT, ON)
    assert e3[0][0] == "telemetry_3y" and e1[0][0] == "telemetry_1y"

def _flux_of(method, **kw):
    gw = InfluxGateway(settings=SimpleNamespace(influx_url="http://x", influx_token="t", influx_org="o", influx_timeout_ms=20000))
    cap = {}
    async def fake_query(flux): cap["flux"] = flux; return []
    gw.query = fake_query
    asyncio.run(getattr(gw, method)("telemetry_1y", "s2_ems_system", "site1", "battery_system_state", **kw))
    return cap["flux"]

def test_read_latest_point_id_filter_added_when_provided():
    assert 'r.point_id == "p.1"' in _flux_of("latest_signal", asset_id="rack_1", point_id="p.1")

def test_read_latest_legacy_has_no_point_id_filter():
    assert "point_id" not in _flux_of("latest_signal", asset_id="rack_1")
