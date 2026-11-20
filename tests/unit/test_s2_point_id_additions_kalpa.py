"""T3: exact Kalpa S2 point_id additions applied to the shared registry seed.

The Kalpa canonical point_id is already carried per-row in protocol_ref (dotted, e.g.
'bank.system_data.battery_system_state'), with per-row provenance in source_ref. T3 copies
protocol_ref -> point_id for every Kalpa row. Validated independently against the
SHA-256-verified energy_capacity_retention.csv (12/12) from the Kalpa Backend Confirmation Pack.

Proves, from the seed + the frozen manifest alone:
  - exactly 2471 Kalpa point_id cells populated, matching the manifest 1:1;
  - for every Kalpa row, point_id == protocol_ref (exact canonical id, no transform);
  - no duplicate point_id anywhere (Kalpa + CleanEdge global uniqueness);
  - the 793 CleanEdge point_ids are unchanged; the 9 T5a rows remain T2_1Y;
  - no non-Kalpa row's point_id is modified by T3.
Pure file-level checks, no Influx/Postgres I/O.
"""
import csv
import collections
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SEED = ROOT / "src" / "signals" / "seed" / "signal_registry.csv"
KADD = ROOT / "src" / "signals" / "seed" / "point_id_additions_kalpa.csv"
CADD = ROOT / "src" / "signals" / "seed" / "point_id_additions_cleanedge.csv"
CORR = ROOT / "src" / "signals" / "seed" / "corrections_T1RAW90D_to_T2_1Y.csv"


def _rows():
    return list(csv.DictReader(open(SEED)))


def _manifest():
    return list(csv.DictReader(open(KADD)))


def test_manifest_is_2471_kalpa():
    m = _manifest()
    assert len(m) == 2471
    assert all(r["source"] == "kalpa" for r in m)
    assert len({(r["substream"], r["signal_key"]) for r in m}) == 2471
    assert len({r["point_id"] for r in m}) == 2471


def test_exactly_2471_kalpa_cells_populated():
    pop = [r for r in _rows() if r["source"] == "kalpa" and r["point_id"].strip()]
    assert len(pop) == 2471
    # every kalpa row is populated (none left empty)
    assert all(r["point_id"].strip() for r in _rows() if r["source"] == "kalpa")


def test_kalpa_point_id_equals_protocol_ref():
    for r in _rows():
        if r["source"] == "kalpa":
            assert r["point_id"] == r["protocol_ref"], (r["substream"], r["signal_key"])


def test_kalpa_rows_match_manifest_exactly():
    want = {(r["substream"], r["signal_key"]): r["point_id"] for r in _manifest()}
    got = {(r["substream"], r["signal_key"]): r["point_id"]
           for r in _rows() if r["source"] == "kalpa"}
    assert got == want


def test_global_point_id_uniqueness():
    pids = [r["point_id"] for r in _rows() if r["point_id"].strip()]
    dups = [p for p, c in collections.Counter(pids).items() if c > 1]
    assert dups == []
    assert len(pids) == 793 + 2471  # 793 CleanEdge + 2471 Kalpa


def test_cleanedge_793_point_ids_unchanged():
    # the 793 CleanEdge additions must still exactly equal their manifest
    want = {(r["substream"], r["signal_key"]): r["point_id"] for r in csv.DictReader(open(CADD))}
    got = {(r["substream"], r["signal_key"]): r["point_id"]
           for r in _rows() if r["source"] == "cleanhedge" and r["point_id"].strip()}
    assert got == want
    assert len(got) == 793


def test_t5a_nine_rows_still_T2_1Y():
    corr = [(c["substream"], c["signal_key"]) for c in csv.DictReader(open(CORR))]
    by = {(r["substream"], r["signal_key"]): r for r in _rows() if r["primary_stream"] == "S2"}
    for sub, sk in corr:
        assert by[(sub, sk)]["retention_class"] == "T2_1Y", (sub, sk)
    assert sum(1 for r in _rows()
               if r["primary_stream"] == "S2" and r["retention_class"] == "T1_RAW_90D") == 0


def test_non_kalpa_rows_untouched_by_t3():
    # Only Kalpa gained point_ids in T3. Non-Kalpa populated rows are exactly the 793 CleanEdge.
    non_kalpa_pop = [r for r in _rows() if r["source"] != "kalpa" and r["point_id"].strip()]
    assert all(r["source"] == "cleanhedge" for r in non_kalpa_pop)
    assert len(non_kalpa_pop) == 793
