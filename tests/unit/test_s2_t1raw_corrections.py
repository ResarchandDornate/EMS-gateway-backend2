"""T5a: 9 S2 registry rows corrected T1_RAW_90D -> T2_1Y; reader+writer route them to 1y."""
import csv
from pathlib import Path
from types import SimpleNamespace

from src.influx import RegistryIndex, bucket_for, measurement_for
from src.ingestion.influx_projection import build_influx_points

ROOT = Path(__file__).resolve().parents[2]
SEED = ROOT / "src" / "signals" / "seed" / "signal_registry.csv"
CORR = ROOT / "src" / "signals" / "seed" / "corrections_T1RAW90D_to_T2_1Y.csv"
BK = SimpleNamespace(influx_bucket_raw="telemetry_raw_90d", influx_bucket_long="telemetry_1y", influx_bucket_3y="telemetry_3y")
WS = SimpleNamespace(s2_point_id_tag_enabled=True, influx_bucket_long="telemetry_1y", influx_bucket_3y="telemetry_3y")
IDENT = {"organization_id": "o", "site_id": "s", "gateway_id": "g"}

def _rows(): return list(csv.DictReader(open(SEED)))
def _targets(): return [(c["substream"], c["signal_key"]) for c in csv.DictReader(open(CORR))]
def _index():
    rows = [(r["primary_stream"], r["substream"], r["signal_key"], r["asset_type"] or None,
             r["data_type"], r["retention_class"]) for r in _rows()]
    return RegistryIndex.from_rows(rows)

def test_nine_targets_now_T2_1Y():
    by = {(r["substream"], r["signal_key"]): r for r in _rows() if r["primary_stream"] == "S2"}
    for sub, sk in _targets():
        assert by[(sub, sk)]["retention_class"] == "T2_1Y", (sub, sk)

def test_no_S2_T1_RAW_90D_remains():
    assert sum(1 for r in _rows() if r["primary_stream"] == "S2" and r["retention_class"] == "T1_RAW_90D") == 0

def test_reader_routes_nine_to_1y():
    idx = _index()
    for sub, sk in _targets():
        sig = idx.resolve(measurement_for("S2", sub), sk)
        assert sig.retention_class == "T2_1Y"
        assert bucket_for(sig.retention_class, BK) == "telemetry_1y"

def test_writer_routes_nine_to_1y():
    t3 = _index().t3_signal_keys()
    for sub, sk in _targets():
        env = SimpleNamespace(stream="S2", substream=sub, created_at_utc="2026-11-19T00:00:00Z",
            records=[{"signal": sk, "value": 1, "runtime_asset_id": "a", "source_id": "s",
                      "source_updated_utc": "2026-11-19T00:00:00Z"}])
        assert build_influx_points(env, IDENT, WS, t3)[0][0] == "telemetry_1y"

def test_unrelated_rows_unchanged():
    rows = _rows()
    assert sum(1 for r in rows if r["primary_stream"] == "S2") == 3401
    assert sum(1 for r in rows if r["primary_stream"] == "S2" and r["retention_class"] == "T3_3Y") == 24
    by = {(r["substream"], r["signal_key"]): r for r in rows if r["primary_stream"] == "S2"}
    assert by[("ems_system", "battery_system_state")]["retention_class"] == "T2_1Y"
