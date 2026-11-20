"""T5b: S2 ENERGY_T3 retention routing driven by the shared registry (not a hardcoded set).
Pure, no Influx I/O: builds points and inspects the chosen bucket + line protocol."""
import csv
from pathlib import Path
from types import SimpleNamespace

from src.ingestion.influx_projection import build_influx_points, ENERGY_T3_SIGNALS
from src.influx import RegistryIndex

ROOT = Path(__file__).resolve().parents[2]
SEED = ROOT / "src" / "signals" / "seed" / "signal_registry.csv"
S = SimpleNamespace(s2_point_id_tag_enabled=True, influx_bucket_long="telemetry_1y", influx_bucket_3y="telemetry_3y")
IDENT = {"organization_id": "o", "site_id": "s", "gateway_id": "g"}

def _index_from_seed():
    rows = []
    for r in csv.DictReader(open(SEED)):
        rows.append((r["primary_stream"], r["substream"], r["signal_key"],
                     r["asset_type"] or None, r["data_type"], r["retention_class"]))
    return RegistryIndex.from_rows(rows)

def _bucket(signal, t3, substream="ems_system"):
    env = SimpleNamespace(stream="S2", substream=substream, created_at_utc="2026-11-19T00:00:00Z",
        records=[{"signal": signal, "value": 1, "runtime_asset_id": "a", "source_id": "src",
                  "source_updated_utc": "2026-11-19T00:00:00Z"}])
    return build_influx_points(env, IDENT, S, t3)[0][0]

def test_t3_signal_keys_returns_only_T3_rows():
    idx = RegistryIndex.from_rows([
        ("S2", "ems_system", "a", "t", "FLOAT32", "T3_3Y"),
        ("S2", "ems_system", "b", "t", "FLOAT32", "T2_1Y"),
        ("S2", "io_module",  "c", "t", "U16",     "T1_RAW_90D")])
    assert idx.t3_signal_keys() == frozenset({"a"})

def test_all_24_registry_T3_signals_route_to_3y():
    t3 = _index_from_seed().t3_signal_keys()
    assert len(t3) == 24
    for sk in t3:
        assert _bucket(sk, t3) == "telemetry_3y", sk

def test_T2_1Y_signal_routes_to_1y():
    t3 = _index_from_seed().t3_signal_keys()
    assert _bucket("battery_system_state", t3) == "telemetry_1y"

def test_T1_RAW_90D_signal_stays_1y_not_raw():
    t3 = _index_from_seed().t3_signal_keys()
    assert _bucket("total_active_power", t3) == "telemetry_1y"   # preserved: never raw

def test_unknown_or_missing_retention_is_safe_1y():
    t3 = _index_from_seed().t3_signal_keys()
    assert _bucket("not_a_registered_signal_at_all", t3) == "telemetry_1y"

def test_non_s2_stream_unchanged():
    env = SimpleNamespace(stream="S1", substream="x", created_at_utc="2026-11-19T00:00:00Z",
                          records=[{"signal": "a", "value": 1}])
    assert build_influx_points(env, IDENT, S, frozenset({"a"})) == []

def test_point_id_tagging_intact_with_t3():
    # self-contained registry: a T3_3Y signal in ems_system WITH a populated point_id, used as
    # both the T3 source and the resolver (fail-closed tag-write requires a resolver).
    idx = RegistryIndex.from_rows([
        ("S2", "ems_system", "system_accumulated_charge_energy", "bms", "int", "T3_3Y", "energy.pid"),
    ])
    t3 = idx.t3_signal_keys()
    env = SimpleNamespace(stream="S2", substream="ems_system", created_at_utc="2026-11-19T00:00:00Z",
        records=[{"signal": "system_accumulated_charge_energy", "value": 1, "runtime_asset_id": "a",
                  "source_id": "src", "point_id": "energy.pid", "source_updated_utc": "2026-11-19T00:00:00Z"}])
    bucket, pt = build_influx_points(env, IDENT, S, t3, resolve_point_id=idx.resolve_point_id)[0]
    assert bucket == "telemetry_3y" and "point_id=energy.pid" in pt.to_line_protocol()

def test_fallback_hardcoded_set_when_no_registry_set():
    assert _bucket(sorted(ENERGY_T3_SIGNALS)[0], None) == "telemetry_3y"   # fallback still 3y
    assert _bucket("not_energy", None) == "telemetry_1y"
