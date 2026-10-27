"""Phase 9b-ii-a pure tests: record->registry mapping + deterministic point build.
No DB, no Influx I/O. Uses verified-real registry rows and the real seed."""
import types
import uuid
from pathlib import Path

import pytest

from src.influx import (RegistryIndex, TelemetryPoint, build_point, measurement_for,
                        parse_contract_timestamp)
from src.ingestion.mapping import build_message_points
from src.ingestion.schemas import EnvelopeIn
from src.telemetry_errors import (
    InvalidTimestamp, InvalidValue, SignalMismatch,
    UnknownSignal, UnsafeQueryInput,
)

# ---- settings stub: build_point only reads these two attrs via bucket_for ----
SETTINGS = types.SimpleNamespace(
    influx_bucket_raw="telemetry_raw_90d",
    influx_bucket_long="telemetry_1y",
    influx_bucket_3y="telemetry_3y",
)

# ---- verified-real registry rows (from src/signals/seed/signal_registry.csv) ----
# (primary_stream, substream, signal_key, asset_type, data_type, retention_class)
ROWS = [
    ("S1", "bess_fast", "soc", "pcs_1", "Float", "T1_RAW_90D"),
    ("S1", "bess_fast", "charge_discharge_power", "pcs_1", "Float", "T1_RAW_90D"),
    ("S2", "bms_extended", "rack_operating_state", "rack", "U16", "T2_1Y"),
    ("S2", "bms_extended", "mcb_feedback", "rack", "U16", "T2_1Y"),
    ("S2", "bms_extended", "rack_charge_energy_session", "rack", "FLOAT32", "T3_3Y"),
]
IDX = RegistryIndex.from_rows(ROWS)
IDENTITY = {"organization_id": "org1", "site_id": "site1", "gateway_id": "gw1"}
TS = "2026-09-04T07:59:14+00:00"


def _rec(signal_key, value, *, source_id="src1", asset_id="rack1", quality="good", ts=TS):
    return {"source_id": source_id, "asset_id": asset_id, "signal_key": signal_key,
            "value": value, "quality": quality, "timestamp": ts}


def _env(stream, substream, records):
    return EnvelopeIn(
        schema_version="1.0", message_id=str(uuid.uuid4()),
        gateway_id="gw1", site_id="site1", organization_id="org1",
        stream=stream, substream=substream, sequence=1,
        created_at_utc=TS, record_count=len(records), records=records,
    )


def _build(stream, substream, records):
    return build_message_points(_env(stream, substream, records), IDENTITY, IDX, SETTINGS)


# ------------------------- valid mappings + bucket routing -------------------------
def test_valid_s2_two_records_long_bucket():
    out = _build("S2", "bms_extended", [_rec("rack_operating_state", 5), _rec("mcb_feedback", 1)])
    assert len(out) == 2
    assert all(bucket == "telemetry_1y" for bucket, _ in out)


def test_valid_s1_raw_bucket():
    out = _build("S1", "bess_fast", [_rec("soc", 55.0, asset_id="pcs1")])
    assert len(out) == 1 and out[0][0] == "telemetry_raw_90d"


# ------------------------------- validation failures --------------------------------
def test_unknown_signal():
    with pytest.raises(UnknownSignal):
        _build("S2", "bms_extended", [_rec("not_a_signal", 1)])


def test_wrong_substream_is_unknown_measurement():
    # signal exists only under bms_extended; sending it under ems_system -> UnknownSignal
    with pytest.raises(UnknownSignal):
        _build("S2", "ems_system", [_rec("rack_operating_state", 5)])


def test_build_point_substream_mismatch_branch():
    pt = TelemetryPoint("s2_bms_extended", "org1", "site1", "gw1", "src1", "rack1",
                        "WRONG_SUBSTREAM", "rack_operating_state", 5, "good",
                        parse_contract_timestamp(TS))
    with pytest.raises(SignalMismatch):
        build_point(IDX, pt, SETTINGS)


def test_build_point_asset_type_mismatch_branch():
    pt = TelemetryPoint("s2_bms_extended", "org1", "site1", "gw1", "src1", "rack1",
                        "bms_extended", "rack_operating_state", 5, "good",
                        parse_contract_timestamp(TS), asset_type="pack")
    with pytest.raises(SignalMismatch):
        build_point(IDX, pt, SETTINGS)


def test_u16_range_and_fractional():
    with pytest.raises(InvalidValue):
        _build("S2", "bms_extended", [_rec("rack_operating_state", 70000)])     # > 65535
    with pytest.raises(InvalidValue):
        _build("S2", "bms_extended", [_rec("rack_operating_state", 3.5)])       # fractional int


def test_float_rejects_inf():
    with pytest.raises(InvalidValue):
        _build("S1", "bess_fast", [_rec("soc", float("inf"), asset_id="pcs1")])


def test_bad_quality():
    with pytest.raises(InvalidValue):
        _build("S2", "bms_extended", [_rec("rack_operating_state", 5, quality="weird")])


def test_quality_variants_ok():
    for q in ("good", "stale", "offline", "error", None):
        out = _build("S2", "bms_extended", [_rec("rack_operating_state", 5, quality=q)])
        assert len(out) == 1


def test_invalid_timestamp_naive_and_garbage():
    with pytest.raises(InvalidTimestamp):
        _build("S2", "bms_extended", [_rec("rack_operating_state", 5, ts="2026-09-04T07:59:14")])
    with pytest.raises(InvalidTimestamp):
        _build("S2", "bms_extended", [_rec("rack_operating_state", 5, ts="not-a-timestamp")])


def test_identity_validation_rejects_bad_ids():
    with pytest.raises(UnsafeQueryInput):
        _build("S2", "bms_extended", [_rec("rack_operating_state", 5, source_id="bad id")])
    with pytest.raises(UnsafeQueryInput):
        _build("S2", "bms_extended", [_rec("rack_operating_state", 5, asset_id="a/b")])


# --------------------------------- T3 interim routing (-> long) ----------------------------------
def test_t3_interim_routes_to_long():
    # INTERIM 2026-10-26: T3 builds and routes to the long (1-year) bucket.
    built = _build("S2", "bms_extended", [_rec("rack_charge_energy_session", 1.0)])
    assert len(built) == 1 and built[0][0] == SETTINGS.influx_bucket_3y


def test_t3_interim_mixed_message_all_route_to_long():
    # INTERIM 2026-10-26: T2 and T3 records both build; T3 routes to the long bucket.
    built = _build("S2", "bms_extended", [
        _rec("rack_operating_state", 5),            # T2
        _rec("rack_charge_energy_session", 1.0),    # T3 -> 3-year bucket
    ])
    assert len(built) == 2
    assert built[0][0] == SETTINGS.influx_bucket_long
    assert built[1][0] == SETTINGS.influx_bucket_3y


# ------------------- multiple signals sharing a timestamp: no overwrite -------------------
def test_multiple_signals_same_timestamp_distinct_cells():
    out = _build("S2", "bms_extended",
                 [_rec("rack_operating_state", 5), _rec("mcb_feedback", 1)])
    a = out[0][1].to_line_protocol().split(" ")
    b = out[1][1].to_line_protocol().split(" ")
    assert a[0] == b[0]            # identical measurement + tag set (same series)
    assert a[-1] == b[-1]          # identical timestamp
    assert a[1] != b[1]            # different field section -> different field keys -> no overwrite
    assert a[1].startswith("rack_operating_state=")
    assert b[1].startswith("mcb_feedback=")


def test_same_signal_same_series_ts_is_idempotent_identity():
    one = _build("S2", "bms_extended", [_rec("rack_operating_state", 5)])[0][1].to_line_protocol()
    two = _build("S2", "bms_extended", [_rec("rack_operating_state", 5)])[0][1].to_line_protocol()
    assert one == two             # same cell -> last-write-wins on retry


def test_different_asset_is_different_series():
    p1 = _build("S2", "bms_extended", [_rec("rack_operating_state", 5, asset_id="rack1")])[0][1]
    p2 = _build("S2", "bms_extended", [_rec("rack_operating_state", 5, asset_id="rack2")])[0][1]
    assert p1.to_line_protocol().split(" ")[0] != p2.to_line_protocol().split(" ")[0]


# ---------------------------------- all-or-nothing ----------------------------------
def test_all_or_nothing():
    with pytest.raises(UnknownSignal):
        _build("S2", "bms_extended", [
            _rec("rack_operating_state", 5),
            _rec("not_a_signal", 1),          # invalid middle record
            _rec("mcb_feedback", 1),
        ])
    ok = _build("S2", "bms_extended",
                [_rec("rack_operating_state", 5), _rec("mcb_feedback", 1), _rec("rack_operating_state", 6)])
    assert len(ok) == 3


# ------------------------- the ACTUAL full seed builds + routes -------------------------
def test_real_seed_builds_and_routes():
    seed = Path(__file__).resolve().parents[2] / "src" / "signals" / "seed" / "signal_registry.csv"
    import csv
    rows = []
    with open(seed, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append((r["primary_stream"].strip(), r["substream"].strip(),
                         r["signal_key"].strip(), (r["asset_type"] or "").strip() or None,
                         (r["data_type"] or "").strip(), (r["retention_class"] or "").strip()))
    idx = RegistryIndex.from_rows(rows)          # raises RegistryConflict if the seed had dupes
    assert len(idx) == 2534
    # S1 -> raw, S2 T2 -> long, real T3 -> 3y
    from src.ingestion.mapping import build_message_points
    id2 = {"organization_id": "org1", "site_id": "site1", "gateway_id": "gw1"}
    assert build_message_points(_env("S1", "bess_fast", [_rec("soc", 50.0, asset_id="pcs1")]),
                                id2, idx, SETTINGS)[0][0] == "telemetry_raw_90d"
    assert build_message_points(_env("S2", "bms_extended", [_rec("rack_operating_state", 5)]),
                                id2, idx, SETTINGS)[0][0] == "telemetry_1y"
    assert build_message_points(_env("S2", "bms_extended", [_rec("rack_charge_energy_session", 1.0)]),
                                id2, idx, SETTINGS)[0][0] == "telemetry_3y"
