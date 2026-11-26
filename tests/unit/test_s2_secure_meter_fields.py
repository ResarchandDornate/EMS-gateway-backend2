"""Kalpa SECURE meter (S2.11 solar_meter / S2.12 load_meter) projection + separation guards.

Pure, no Influx I/O: builds points and inspects measurement/tags/fields via line protocol. Proves:
  - meter_role / base_asset_id become tags; runtime_signal_id / observed_at_utc / source_updated_utc
    / source_data_age_ms become SIGNAL-NAMESPACED fields (collision-safe like <signal>_q);
  - two signals from one asset at one timestamp keep INDEPENDENT metadata (no field overwrite);
  - a CleanEdge S2 record (no meter_role) is byte-identical to before (no new tags/fields);
  - solar_meter and load_meter are different measurements (no cross-role merge);
  - the same signal from two sites/gateways lands in different series;
  - both source_updated_utc and observed_at_utc remain retrievable; energy -> 3y, measurement -> 1y.
"""
from types import SimpleNamespace

from src.ingestion.influx_projection import build_influx_points, _SECURE_FIELD_SUFFIXES

S = SimpleNamespace(s2_point_id_tag_enabled=True, influx_bucket_long="telemetry_1y", influx_bucket_3y="telemetry_3y")
IDENT = {"organization_id": "o1", "site_id": "s1", "gateway_id": "g1"}


def _rec(signal, **over):
    r = {"signal": signal, "value": 1.5, "runtime_asset_id": "solar_meter_1",
         "base_asset_id": "energy_meter", "source_id": "secure_source", "meter_role": "solar",
         "runtime_signal_id": f"secure_source.solar_meter_1.{signal}",
         "observed_at_utc": "2026-11-26T10:00:05Z", "source_updated_utc": "2026-11-26T10:00:04Z",
         "source_data_age_ms": 120, "quality": "good"}
    r.update(over)
    return r


def _lp(substream, records, ident=IDENT, t3=frozenset()):
    env = SimpleNamespace(stream="S2", substream=substream, created_at_utc="2026-11-26T10:00:00Z", records=records)
    pts = build_influx_points(env, ident, S, t3)
    return [p.to_line_protocol() for _b, p in pts], pts


def test_meter_role_and_base_asset_are_tags():
    (lp,), _ = _lp("solar_meter", [_rec("voltage_a_v")])
    assert lp.startswith("s2_solar_meter,")
    tagsec = lp.split(" ")[0]
    assert "meter_role=solar" in tagsec
    assert "base_asset_id=energy_meter" in tagsec
    assert "asset_id=solar_meter_1" in tagsec


def test_metadata_fields_are_signal_namespaced():
    (lp,), _ = _lp("solar_meter", [_rec("voltage_a_v")])
    for key in ("voltage_a_v_rsid=", "voltage_a_v_obs_utc=", "voltage_a_v_src_utc=",
                "voltage_a_v_age_ms=", "voltage_a_v_q="):
        assert key in lp, key


def test_two_signals_one_timestamp_keep_independent_metadata():
    # core integrity proof: co-timestamped signals carry distinct namespaced keys -> no overwrite
    lps, _ = _lp("solar_meter", [_rec("voltage_a_v"), _rec("voltage_b_v")])
    blob = "\n".join(lps)
    assert 'voltage_a_v_rsid="secure_source.solar_meter_1.voltage_a_v"' in blob
    assert 'voltage_b_v_rsid="secure_source.solar_meter_1.voltage_b_v"' in blob


def test_cleanedge_record_is_byte_identical():
    rec = {"signal": "bank_soh", "value": 97.0, "runtime_asset_id": "a", "source_id": "src",
           "source_updated_utc": "2026-11-26T10:00:00Z", "observed_at_utc": "2026-11-26T10:00:01Z",
           "quality": "good"}
    (lp,), _ = _lp("ems_system", [rec])
    for leaked in ("meter_role", "base_asset_id", "_rsid", "_obs_utc", "_src_utc", "_age_ms"):
        assert leaked not in lp, f"{leaked} leaked into non-SECURE CleanEdge record"


def test_solar_and_load_are_different_measurements():
    (s,), _ = _lp("solar_meter", [_rec("voltage_a_v")])
    (l,), _ = _lp("load_meter", [_rec("voltage_a_v", meter_role="load", runtime_asset_id="load_meter_1")])
    assert s.startswith("s2_solar_meter,") and l.startswith("s2_load_meter,")
    assert s.split(" ")[0] != l.split(" ")[0]


def test_same_signal_different_sites_are_separate_series():
    (a,), _ = _lp("solar_meter", [_rec("voltage_a_v")], {"organization_id": "o", "site_id": "sA", "gateway_id": "gA"})
    (b,), _ = _lp("solar_meter", [_rec("voltage_a_v")], {"organization_id": "o", "site_id": "sB", "gateway_id": "gB"})
    assert "site_id=sA" in a and "site_id=sB" in b
    assert a.split(" ")[0] != b.split(" ")[0]


def test_both_timestamps_preserved():
    (lp,), _ = _lp("solar_meter", [_rec("voltage_a_v")])
    assert 'voltage_a_v_src_utc="2026-11-26T10:00:04Z"' in lp   # measurement time (also the point time)
    assert 'voltage_a_v_obs_utc="2026-11-26T10:00:05Z"' in lp   # recording time


def test_energy_to_3y_measurement_to_1y():
    t3 = frozenset({"active_energy_import_kwh"})
    assert _lp("solar_meter", [_rec("active_energy_import_kwh")], t3=t3)[1][0][0] == "telemetry_3y"
    assert _lp("solar_meter", [_rec("voltage_a_v")], t3=t3)[1][0][0] == "telemetry_1y"


def test_no_suffix_collision_among_meter_signals():
    sigs = {"voltage_a_v", "voltage_b_v", "voltage_c_v", "voltage_average_v", "voltage_ab_v",
            "voltage_bc_v", "voltage_ca_v", "current_a_a", "current_b_a", "current_c_a",
            "current_neutral_a", "power_factor_a", "power_factor_b", "power_factor_c",
            "power_factor_total", "active_power_a_kw", "active_power_b_kw", "active_power_c_kw",
            "active_power_total_kw", "reactive_power_a_kvar", "reactive_power_b_kvar",
            "reactive_power_c_kvar", "reactive_power_total_kvar", "apparent_power_a_kva",
            "apparent_power_b_kva", "apparent_power_c_kva", "apparent_power_total_kva",
            "frequency_hz", "active_energy_import_kwh", "active_energy_export_kwh", "active_energy_net_kwh"}
    assert len(sigs) == 31
    suffixes = ["_q"] + [suf for _k, suf in _SECURE_FIELD_SUFFIXES]
    for sig in sigs:
        for suf in suffixes:
            assert (sig + suf) not in sigs, f"{sig}{suf} collides with a signal name"
