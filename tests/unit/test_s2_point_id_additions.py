"""T2: 793 CleanEdge S2 point_id additions applied to the shared registry seed.

Proves, from the seed + the frozen manifest alone:
  - exactly 793 CleanEdge point_id cells populated, matching the manifest 1:1;
  - every CleanEdge-populated row is S2; no duplicate point_id; no duplicate CleanEdge identity;
  - every CleanEdge row not in the manifest keeps an empty point_id;
  - the 9 T5a rows remain retention_class T2_1Y (T2 did not revert T5a).
All assertions are scoped to source=cleanhedge so they stay correct alongside the T3 Kalpa
additions (Kalpa population is verified separately in test_s2_point_id_additions_kalpa.py).
Pure file-level checks, no Influx/Postgres I/O.
"""
import csv
import collections
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SEED = ROOT / "src" / "signals" / "seed" / "signal_registry.csv"
ADDS = ROOT / "src" / "signals" / "seed" / "point_id_additions_cleanedge.csv"
CORR = ROOT / "src" / "signals" / "seed" / "corrections_T1RAW90D_to_T2_1Y.csv"


def _rows():
    return list(csv.DictReader(open(SEED)))


def _manifest():
    return list(csv.DictReader(open(ADDS)))


def _ce_populated():
    return [r for r in _rows() if r["source"] == "cleanhedge" and r["point_id"].strip()]


def test_manifest_is_793_cleanedge():
    m = _manifest()
    assert len(m) == 793
    assert all(r["source"] == "cleanhedge" for r in m)
    assert len({(r["substream"], r["signal_key"]) for r in m}) == 793  # unique identities
    assert len({r["point_id"] for r in m}) == 793                      # unique point_ids


def test_exactly_793_cleanedge_cells_populated():
    assert len(_ce_populated()) == 793


def test_cleanedge_populated_rows_are_all_s2():
    for r in _ce_populated():
        assert r["primary_stream"] == "S2", (r["substream"], r["signal_key"])


def test_cleanedge_populated_rows_match_manifest_exactly():
    want = {(r["substream"], r["signal_key"]): r["point_id"] for r in _manifest()}
    got = {(r["substream"], r["signal_key"]): r["point_id"] for r in _ce_populated()}
    assert got == want


def test_no_duplicate_point_id_in_seed():
    # global: across every source
    pids = [r["point_id"] for r in _rows() if r["point_id"].strip()]
    dups = [p for p, c in collections.Counter(pids).items() if c > 1]
    assert dups == []


def test_no_duplicate_identity_among_cleanedge_populated():
    keys = [(r["substream"], r["signal_key"]) for r in _ce_populated()]
    dups = [k for k, c in collections.Counter(keys).items() if c > 1]
    assert dups == []


def test_cleanedge_non_target_rows_keep_empty_point_id():
    # every cleanhedge row not in the CleanEdge manifest must stay empty
    want = {(r["substream"], r["signal_key"]) for r in _manifest()}
    for r in _rows():
        if r["source"] == "cleanhedge" and (r["substream"], r["signal_key"]) not in want:
            assert not r["point_id"].strip(), (r["substream"], r["signal_key"])


def test_t5a_nine_rows_still_T2_1Y():
    # T2 must not revert the T5a retention correction
    corr = [(c["substream"], c["signal_key"]) for c in csv.DictReader(open(CORR))]
    by = {(r["substream"], r["signal_key"]): r for r in _rows() if r["primary_stream"] == "S2"}
    for sub, sk in corr:
        assert by[(sub, sk)]["retention_class"] == "T2_1Y", (sub, sk)


def test_row_count_unchanged():
    assert len(_rows()) == 3456  # data rows (3457 lines incl header)
