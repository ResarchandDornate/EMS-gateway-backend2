"""T6: S2 point_id-primary registry resolver + additive point_id reads.

Pure unit tests — no DB, no Influx I/O, no app. The telemetry endpoint coroutines are
called directly with an in-memory RegistryIndex and a recording fake gateway, so request
resolution, scoping, conflict handling and response schemas are all exercised deterministically.
"""
import asyncio
import types
import uuid

import pytest
from fastapi import HTTPException

from src.influx import RegistryIndex
from src.telemetry.router import _resolve_target, latest, series, aggregate
from src.telemetry_errors import UnknownPointId, AmbiguousPointId

# (primary_stream, substream, signal_key, asset_type, data_type, retention_class, point_id)
ROWS = [
    ("S2", "ems_system", "battery_system_state", "bank", "U16", "T2_1Y",
     "bank.system_data.battery_system_state"),                                   # Kalpa
    ("S2", "ems_system", "system_accumulated_charge_energy", "bank", "Float", "T3_3Y",
     "bank.system_data.system_accumulated_charge_energy"),                        # Kalpa, T3 bucket
    ("S2", "solar_inverter", "meter_power_factor", "inverter", "Float", "T2_1Y",
     "mfm.average_power_factor"),                                                 # CleanEdge
    ("S2", "io_module", "sig_a", "bank", "U16", "T2_1Y", "dup.pid"),             # ambiguous pair
    ("S2", "io_module", "sig_b", "bank", "U16", "T2_1Y", "dup.pid"),             # ambiguous pair
    ("S2", "pcs_extended", "unmapped_signal", "pcs", "Float", "T2_1Y", ""),      # unresolved: no point_id
]
INDEX = RegistryIndex.from_rows(ROWS)
SETTINGS = types.SimpleNamespace(influx_bucket_raw="telemetry_raw_90d",
                                 influx_bucket_long="telemetry_1y",
                                 influx_bucket_3y="telemetry_3y")
SITE = uuid.uuid4()


class FakeInflux:
    def __init__(self):
        self.settings = SETTINGS
        self.calls = []

    async def _r(self, name, a, k):
        self.calls.append((name, a, k)); return []
    async def latest_signal(self, *a, **k): return await self._r("latest_signal", a, k)
    async def latest_by_asset(self, *a, **k): return await self._r("latest_by_asset", a, k)
    async def series(self, *a, **k): return await self._r("series", a, k)
    async def aggregate(self, *a, **k): return await self._r("aggregate", a, k)


# ----------------------------- resolver (RegistryIndex) -----------------------------

def test_resolve_point_id_kalpa():
    m, sk, sig = INDEX.resolve_point_id("bank.system_data.battery_system_state")
    assert m == "s2_ems_system" and sk == "battery_system_state"
    assert sig.retention_class == "T2_1Y" and sig.substream == "ems_system"

def test_resolve_point_id_cleanedge():
    m, sk, sig = INDEX.resolve_point_id("mfm.average_power_factor")
    assert m == "s2_solar_inverter" and sk == "meter_power_factor"

def test_resolve_point_id_unknown_raises():
    with pytest.raises(UnknownPointId):
        INDEX.resolve_point_id("does.not.exist")

def test_resolve_point_id_unpopulated_is_unknown():
    # an unresolved CleanEdge point (empty point_id cell) is not indexed
    with pytest.raises(UnknownPointId):
        INDEX.resolve_point_id("")

def test_resolve_point_id_ambiguous_raises():
    with pytest.raises(AmbiguousPointId):
        INDEX.resolve_point_id("dup.pid")

def test_signal_key_resolution_unchanged():
    sig = INDEX.resolve("s2_ems_system", "battery_system_state")
    assert sig.data_type == "U16"


# ----------------------------- _resolve_target matrix -----------------------------

def test_target_by_point_id():
    m, b, sk, sig = _resolve_target(INDEX, SETTINGS, "S2", "ems_system", None,
                                    "bank.system_data.battery_system_state")
    assert m == "s2_ems_system" and sk == "battery_system_state" and b == "telemetry_1y"

def test_target_by_point_id_t3_bucket():
    m, b, sk, sig = _resolve_target(INDEX, SETTINGS, "S2", "ems_system", None,
                                    "bank.system_data.system_accumulated_charge_energy")
    assert sk == "system_accumulated_charge_energy" and b == "telemetry_3y"

def test_target_by_signal_key_only_compat():
    m, b, sk, sig = _resolve_target(INDEX, SETTINGS, "S2", "ems_system",
                                    "battery_system_state", None)
    assert m == "s2_ems_system" and sk == "battery_system_state"

def test_target_point_id_and_matching_signal_key_ok():
    m, b, sk, sig = _resolve_target(INDEX, SETTINGS, "S2", "ems_system",
                                    "battery_system_state", "bank.system_data.battery_system_state")
    assert sk == "battery_system_state"

def _status(exc): return exc.value.status_code

def test_target_unknown_point_id_422():
    with pytest.raises(HTTPException) as e:
        _resolve_target(INDEX, SETTINGS, "S2", "ems_system", None, "nope.nope")
    assert _status(e) == 422

def test_target_ambiguous_point_id_409():
    with pytest.raises(HTTPException) as e:
        _resolve_target(INDEX, SETTINGS, "S2", "io_module", None, "dup.pid")
    assert _status(e) == 409

def test_target_conflicting_point_id_and_signal_key_409():
    with pytest.raises(HTTPException) as e:
        _resolve_target(INDEX, SETTINGS, "S2", "ems_system",
                        "battery_system_state", "mfm.average_power_factor")
    assert _status(e) == 409

def test_target_point_id_wrong_substream_409():
    with pytest.raises(HTTPException) as e:
        _resolve_target(INDEX, SETTINGS, "S2", "pcs_extended", None,
                        "bank.system_data.battery_system_state")
    assert _status(e) == 409

def test_target_neither_provided_422():
    with pytest.raises(HTTPException) as e:
        _resolve_target(INDEX, SETTINGS, "S2", "ems_system", None, None)
    assert _status(e) == 422

def test_target_unknown_signal_key_422():
    with pytest.raises(HTTPException) as e:
        _resolve_target(INDEX, SETTINGS, "S2", "ems_system", "not_a_signal", None)
    assert _status(e) == 422

def test_target_unresolved_cleanedge_point_id_fails_safe():
    # an emitted CleanEdge point name with no registry point_id must 422, never a wrong signal
    with pytest.raises(HTTPException) as e:
        _resolve_target(INDEX, SETTINGS, "S2", "pcs_extended", None,
                        "telemetry.pcs.cleanedge_total__some_unmapped")
    assert _status(e) == 422


# ----------------------------- endpoint reads (latest/series/aggregate) -----------------------------

def test_latest_by_point_id_resolves_and_filters():
    fx = FakeInflux()
    resp = asyncio.run(latest(stream="S2", substream="ems_system", signal_key=None,
                              asset_id=None, source_id=None,
                              point_id="bank.system_data.battery_system_state",
                              site_id=SITE, index=INDEX, influx=fx))
    name, a, k = fx.calls[0]
    assert name == "latest_by_asset"
    assert a[1] == "s2_ems_system" and a[3] == "battery_system_state"   # resolved measurement + signal_key
    assert k["point_id"] == "bank.system_data.battery_system_state"      # Influx point_id filter applied
    assert resp.signal_key == "battery_system_state"                     # response schema unchanged

def test_latest_by_point_id_with_asset_scope():
    fx = FakeInflux()
    asyncio.run(latest(stream="S2", substream="ems_system", signal_key=None,
                       asset_id="rack_1", source_id="src1",
                       point_id="bank.system_data.battery_system_state",
                       site_id=SITE, index=INDEX, influx=fx))
    name, a, k = fx.calls[0]
    assert name == "latest_signal"
    assert a[4] == "rack_1"                 # asset scoping preserved
    assert str(a[2]) == str(SITE)           # site isolation preserved

def test_latest_by_signal_key_still_works():
    fx = FakeInflux()
    resp = asyncio.run(latest(stream="S2", substream="ems_system",
                              signal_key="battery_system_state", asset_id=None, source_id=None,
                              point_id=None, site_id=SITE, index=INDEX, influx=fx))
    assert resp.signal_key == "battery_system_state"
    assert fx.calls[0][2].get("point_id") is None

def test_series_by_point_id():
    fx = FakeInflux()
    resp = asyncio.run(series(stream="S2", substream="solar_inverter", signal_key=None,
                              asset_id="inv_1", start="-1h", source_id=None, stop=None,
                              point_id="mfm.average_power_factor", limit=100,
                              site_id=SITE, index=INDEX, influx=fx))
    name, a, k = fx.calls[0]
    assert name == "series" and a[3] == "meter_power_factor"
    assert k["point_id"] == "mfm.average_power_factor"
    assert resp.signal_key == "meter_power_factor" and resp.asset_id == "inv_1"

def test_aggregate_by_point_id():
    fx = FakeInflux()
    resp = asyncio.run(aggregate(stream="S2", substream="ems_system", signal_key=None,
                                 asset_id="bank_1", start="-1d", window="1h", fn="mean",
                                 source_id=None, stop=None,
                                 point_id="bank.system_data.battery_system_state",
                                 site_id=SITE, index=INDEX, influx=fx))
    name, a, k = fx.calls[0]
    assert name == "aggregate" and a[3] == "battery_system_state"
    assert k["point_id"] == "bank.system_data.battery_system_state"
    assert resp.signal_key == "battery_system_state"

def test_unknown_point_id_read_is_422_no_influx_call():
    fx = FakeInflux()
    with pytest.raises(HTTPException) as e:
        asyncio.run(latest(stream="S2", substream="ems_system", signal_key=None,
                           asset_id=None, source_id=None, point_id="ghost.point",
                           site_id=SITE, index=INDEX, influx=fx))
    assert e.value.status_code == 422
    assert fx.calls == []     # never queried Influx -> no silent fallback
