import uuid
from datetime import datetime, timedelta, timezone

import pytest
from influxdb_client import InfluxDBClient

from src.config import get_settings
from src.influx import (InfluxGateway, RegistryIndex, TelemetryPoint, bucket_for,
                        build_point, parse_contract_timestamp, rows)
from src.signals.models import SignalRegistryModel
from src.signals.service import (load_registry_index, load_seed,
                                 quality_field_collisions)
from src.signals.soc import resolve_overall_soc, resolve_rack_soc
from src.signals.types import coerce, field_type, quality_collisions, validate_quality
from src.telemetry_errors import (InvalidTimestamp, InvalidValue, RegistryConflict,
                                  SignalMismatch,
                                  UnknownRetentionClass, UnknownSignal,
                                  UnresolvedMetadata, UnsafeQueryInput, UnsupportedDataType)

# rows: (primary_stream, substream, signal_key, asset_type, data_type, retention_class)
_IDX = RegistryIndex.from_rows([
    ("S2", "bms_extended", "rack_voltage", "rack", "FLOAT32", "T2_1Y"),
    ("S2", "bms_extended", "rack_status", "rack", "U16", "T2_1Y"),
    ("S2", "bms_extended", "rack_energy", "rack", "FLOAT32", "T3_3Y"),
    ("S2", "ems_system", "available_count", "bank", "U16", "T2_1Y"),
])
_IDX_NO_ASSET = RegistryIndex.from_rows([("S2", "x", "sig_no_asset", None, "U16", "T2_1Y")])
_SETTINGS = get_settings()


def _pt(site, asset, source, key, val, ts, meas="s2_bms_extended",
        substream="bms_extended", q="good", asset_type=None):
    return TelemetryPoint(meas, "org_test", site, "gw_test", source, asset,
                          substream, key, val, q, ts, asset_type)


# ===================== UNIT =====================
def test_bucket_t1_t2():
    assert bucket_for("T1_RAW_90D", _SETTINGS) == _SETTINGS.influx_bucket_raw
    assert bucket_for("T2_1Y", _SETTINGS) == _SETTINGS.influx_bucket_long

def test_bucket_t3_routes_to_3y():
    assert bucket_for("T3_3Y", _SETTINGS) == _SETTINGS.influx_bucket_3y

def test_bucket_unknown_rejected():
    with pytest.raises(UnknownRetentionClass): bucket_for("T9_X", _SETTINGS)

def test_field_types_supported():
    assert field_type("FLOAT32") == "float" and field_type("U16") == "int"

def test_field_type_unsupported():
    with pytest.raises(UnsupportedDataType): field_type("STRING")

def test_field_type_rejects_nonnumeric_data_type_strings():
    # "system_alarm" / "system_data" are registry *category* labels, NOT data_types. The signals in
    # those categories carry numeric data_types (U16 / FLOAT32) and ingest as numeric points today;
    # load_registry_index does not even carry `category` into the write path. This test only pins that
    # field_type/coerce reject a *data_type* string that is not a known numeric type -- it does NOT
    # imply those category signals are unsupported.
    for not_a_data_type in ("system_alarm", "system_data"):
        with pytest.raises(UnsupportedDataType):
            field_type(not_a_data_type)
        with pytest.raises(UnsupportedDataType):
            coerce(not_a_data_type, 1)
    # the numeric data_types those categories actually use ARE accepted:
    assert field_type("U16") == "int"
    assert field_type("FLOAT32") == "float"

def test_u16_bounds():
    assert coerce("U16", 0) == 0 and coerce("U16", 65535) == 65535
    with pytest.raises(InvalidValue): coerce("U16", -1)
    with pytest.raises(InvalidValue): coerce("U16", 65536)

def test_s16_bounds():
    assert coerce("S16", -32768) == -32768 and coerce("S16", 32767) == 32767
    with pytest.raises(InvalidValue): coerce("S16", 32768)

def test_u32_bounds():
    assert coerce("U32", 4294967295) == 4294967295
    with pytest.raises(InvalidValue): coerce("U32", 4294967296)

def test_int_rejects_fractional():
    with pytest.raises(InvalidValue): coerce("U16", 3.5)

def test_float_rejects_nan_inf():
    for bad in (float("nan"), float("inf"), float("-inf")):
        with pytest.raises(InvalidValue): coerce("FLOAT32", bad)

def test_quality_validation():
    assert validate_quality("GOOD") == "good" and validate_quality(None) is None
    for bad in ("excellent", 5, ""):
        with pytest.raises(InvalidValue): validate_quality(bad)

def test_quality_collisions_pure():
    assert quality_collisions({"s2_x": {"soc", "soc_q", "v"}}) == [("s2_x", "soc")]

def test_registry_duplicate_detected():
    with pytest.raises(RegistryConflict):
        RegistryIndex.from_rows([
            ("S2", "bms_extended", "rack_voltage", "rack", "FLOAT32", "T2_1Y"),
            ("S2", "bms_extended", "rack_voltage", "rack", "U16", "T2_1Y"),
        ])

def test_identity_validation():
    ts = datetime.now(timezone.utc)
    with pytest.raises(UnsafeQueryInput):
        build_point(_IDX, _pt("", "r1", "s1", "rack_voltage", 700.0, ts), _SETTINGS)
    with pytest.raises(UnsafeQueryInput):
        build_point(_IDX, _pt('bad"site', "r1", "s1", "rack_voltage", 700.0, ts), _SETTINGS)

def test_unknown_signal_and_measurement_mismatch():
    ts = datetime.now(timezone.utc)
    with pytest.raises(UnknownSignal):
        build_point(_IDX, _pt("s", "r1", "src", "not_a_signal", 1, ts), _SETTINGS)
    with pytest.raises(UnknownSignal):
        build_point(_IDX, _pt("s", "r1", "src", "rack_voltage", 700.0, ts,
                              meas="s2_ems_system", substream="ems_system"), _SETTINGS)

def test_substream_mismatch():
    ts = datetime.now(timezone.utc)
    with pytest.raises(SignalMismatch):
        build_point(_IDX, _pt("s", "r1", "src", "rack_voltage", 700.0, ts, substream="pcs_extended"), _SETTINGS)

def test_asset_type_mismatch():
    ts = datetime.now(timezone.utc)
    with pytest.raises(SignalMismatch):
        build_point(_IDX, _pt("s", "r1", "src", "rack_voltage", 700.0, ts, asset_type="pcs"), _SETTINGS)

def test_asset_type_required_from_registry():
    ts = datetime.now(timezone.utc)
    with pytest.raises(UnresolvedMetadata):
        build_point(_IDX_NO_ASSET, _pt("s", "a", "src", "sig_no_asset", 1, ts,
                                       meas="s2_x", substream="x"), _SETTINGS)

def test_retention_t3_builds_to_3y():
    ts = datetime.now(timezone.utc)
    bucket, _built = build_point(_IDX, _pt("s", "r1", "src", "rack_energy", 10.0, ts), _SETTINGS)
    assert bucket == _SETTINGS.influx_bucket_3y

def test_registry_type_enforced():
    ts = datetime.now(timezone.utc)
    with pytest.raises(InvalidValue):
        build_point(_IDX, _pt("s", "ems_system", "src", "available_count", 65536, ts,
                              meas="s2_ems_system", substream="ems_system"), _SETTINGS)

def test_timestamp_naive_none_rejected():
    with pytest.raises(InvalidTimestamp):
        build_point(_IDX, _pt("s", "r1", "src", "rack_voltage", 700.0, datetime(2026, 1, 1)), _SETTINGS)
    with pytest.raises(InvalidTimestamp):
        build_point(_IDX, _pt("s", "r1", "src", "rack_voltage", 700.0, None), _SETTINGS)

def test_parse_contract_timestamp():
    assert parse_contract_timestamp("2026-09-04T07:59:14+05:30") == \
        datetime(2026, 9, 4, 2, 29, 14, tzinfo=timezone.utc)
    with pytest.raises(InvalidTimestamp): parse_contract_timestamp("2026-13-01T00:00:00Z")
    with pytest.raises(InvalidTimestamp): parse_contract_timestamp("2026-09-04T07:59:14+25:00")
    with pytest.raises(InvalidTimestamp): parse_contract_timestamp("2026-09-04T07:59:14")

async def test_unsafe_query_input():
    gw = InfluxGateway()
    with pytest.raises(UnsafeQueryInput):
        await gw.latest_by_asset("telemetry_test", 'evil"]) |> yield(', "site", "soc")
    with pytest.raises(UnsafeQueryInput):
        await gw.latest_signal("telemetry_test", "s2_x", "site", "soc", start="evil")
    with pytest.raises(UnsafeQueryInput):
        await gw.latest_signal("telemetry_test", "s2_x", "site", "soc", start="2026-13-01T00:00:00Z")

def test_soc_unconfigured():
    assert resolve_overall_soc()["status"] == "unconfigured"
    assert resolve_rack_soc()["status"] == "unconfigured"


# ============ POSTGRES INTEGRATION (existing test DB) ============
async def test_quality_suffix_no_collision(session):
    await load_seed(session)
    await session.commit()
    assert await quality_field_collisions(session) == []

async def test_quality_collision_detected(session):
    for key in ("soc", "soc_q"):
        session.add(SignalRegistryModel(source="test", primary_stream="S2",
                                        substream="collide", signal_key=key, data_type="U16"))
    await session.flush()
    assert ("s2_collide", "soc") in await quality_field_collisions(session)

async def test_registry_index_resolves_signal(session):
    await load_seed(session)
    await session.commit()
    index = await load_registry_index(session)
    assert len(index) > 2000
    sig = index.resolve("s2_bms_extended", "rack_operating_state")
    assert sig.data_type == "U16" and sig.substream == "bms_extended" and sig.asset_type


# ============ INFLUX INTEGRATION (dedicated test bucket) ============
@pytest.fixture
def influx_test():
    base = get_settings()
    client = InfluxDBClient(url=base.influx_url, token=base.influx_token, org=base.influx_org)
    try:
        if not client.ping():
            pytest.skip("InfluxDB not reachable")
        buckets = client.buckets_api()
        if buckets.find_bucket_by_name(base.influx_bucket_test) is None:
            buckets.create_bucket(bucket_name=base.influx_bucket_test, org=base.influx_org)
        test_settings = base.model_copy(update={
            "influx_bucket_raw": base.influx_bucket_test,
            "influx_bucket_long": base.influx_bucket_test,
        })
        yield InfluxGateway(settings=test_settings), base.influx_bucket_test
    finally:
        client.close()


async def _cleanup(gw, bucket, site, t0, t1):
    async with gw._client() as c:
        await c.delete_api().delete(t0 - timedelta(seconds=1), t1 + timedelta(seconds=1),
                                    f'site_id="{site}"', bucket=bucket, org=gw._org)


async def test_distinct_rack_identities(influx_test):
    gw, bucket = influx_test
    site = f"pytest-{uuid.uuid4().hex[:8]}"; now = datetime.now(timezone.utc)
    try:
        await gw.write([
            _pt(site, "bms_rack_1", "external_ems_1", "rack_voltage", 700.0, now),
            _pt(site, "bms_rack_2", "external_ems_1", "rack_voltage", 712.0, now),
        ], _IDX)
        res = {r["asset_id"]: r["value"] for r in rows(await gw.latest_by_asset(bucket, "s2_bms_extended", site, "rack_voltage"))}
        assert res == {"bms_rack_1": 700.0, "bms_rack_2": 712.0}
    finally:
        await _cleanup(gw, bucket, site, now, now)


async def test_source_separation(influx_test):
    gw, bucket = influx_test
    site = f"pytest-{uuid.uuid4().hex[:8]}"; now = datetime.now(timezone.utc)
    try:
        await gw.write([
            _pt(site, "ems_system", "external_ems_1", "available_count", 1, now, "s2_ems_system", "ems_system"),
            _pt(site, "ems_system", "external_ems_2", "available_count", 2, now, "s2_ems_system", "ems_system"),
        ], _IDX)
        res = rows(await gw.latest_by_asset(bucket, "s2_ems_system", site, "available_count"))
        assert len(res) == 2
        assert {r["source_id"]: r["value"] for r in res} == {"external_ems_1": 1, "external_ems_2": 2}
    finally:
        await _cleanup(gw, bucket, site, now, now)


async def test_supported_types_roundtrip(influx_test):
    gw, bucket = influx_test
    site = f"pytest-{uuid.uuid4().hex[:8]}"; now = datetime.now(timezone.utc)
    try:
        await gw.write([
            _pt(site, "bms_rack_1", "external_ems_1", "rack_voltage", 700.5, now),
            _pt(site, "bms_rack_1", "external_ems_1", "rack_status", 3, now),
        ], _IDX)
        v = {r["field"]: r["value"]
             for r in rows(await gw.latest_by_asset(bucket, "s2_bms_extended", site, "rack_voltage"))
                    + rows(await gw.latest_by_asset(bucket, "s2_bms_extended", site, "rack_status"))}
        assert isinstance(v["rack_voltage"], float) and v["rack_voltage"] == 700.5
        assert isinstance(v["rack_status"], int) and v["rack_status"] == 3
    finally:
        await _cleanup(gw, bucket, site, now, now)


async def test_duplicate_and_last_write_wins(influx_test):
    gw, bucket = influx_test
    site = f"pytest-{uuid.uuid4().hex[:8]}"; now = datetime.now(timezone.utc)
    try:
        for val in (700.0, 700.0, 715.0):
            await gw.write([_pt(site, "bms_rack_1", "external_ems_1", "rack_voltage", val, now)], _IDX)
        res = rows(await gw.latest_by_asset(bucket, "s2_bms_extended", site, "rack_voltage"))
        assert len(res) == 1 and res[0]["value"] == 715.0
    finally:
        await _cleanup(gw, bucket, site, now, now)


async def test_timestamp_offset_preserved(influx_test):
    gw, bucket = influx_test
    site = f"pytest-{uuid.uuid4().hex[:8]}"
    local = datetime(2026, 9, 4, 13, 29, 14, tzinfo=timezone(timedelta(hours=5, minutes=30)))
    utc = local.astimezone(timezone.utc)
    try:
        await gw.write([_pt(site, "bms_rack_1", "external_ems_1", "rack_voltage", 700.0, local)], _IDX)
        res = rows(await gw.latest_signal(bucket, "s2_bms_extended", site, "rack_voltage",
                                          asset_id="bms_rack_1", start="2026-09-01T00:00:00Z"))
        assert res[0]["time"].replace(microsecond=0) == utc
    finally:
        await _cleanup(gw, bucket, site, utc, utc)


async def test_batch_all_or_nothing(influx_test):
    gw, bucket = influx_test
    site = f"pytest-{uuid.uuid4().hex[:8]}"; now = datetime.now(timezone.utc)
    try:
        with pytest.raises(UnknownSignal):
            await gw.write([
                _pt(site, "bms_rack_1", "external_ems_1", "rack_voltage", 700.0, now),     # valid
                _pt(site, "bms_rack_1", "external_ems_1", "rack_unknown_sig", 5.0, now),   # invalid -> fails closed
            ], _IDX)
        res = rows(await gw.latest_by_asset(bucket, "s2_bms_extended", site, "rack_voltage"))
        assert res == []     # nothing written because build happens before any I/O
    finally:
        await _cleanup(gw, bucket, site, now, now)


def test_bucket_t3_blank_config_fails_closed():
    import types as _t
    from src.telemetry_errors import RetentionNotConfigured
    blank = _t.SimpleNamespace(influx_bucket_raw="r", influx_bucket_long="l", influx_bucket_3y="   ")
    with pytest.raises(RetentionNotConfigured):
        bucket_for("T3_3Y", blank)
    missing = _t.SimpleNamespace(influx_bucket_raw="r", influx_bucket_long="l")  # attr absent
    with pytest.raises(RetentionNotConfigured):
        bucket_for("T3_3Y", missing)


def test_bucket_t1_t2_preserved_with_3y():
    import types as _t
    s = _t.SimpleNamespace(influx_bucket_raw="raw_b", influx_bucket_long="long_b",
                           influx_bucket_3y="three_b")
    assert bucket_for("T1_RAW_90D", s) == "raw_b"
    assert bucket_for("T2_1Y", s) == "long_b"


async def test_writer_accepts_3y_bucket(monkeypatch):
    # The writer's allowed-bucket guard admits the 3-year bucket (reject path is covered in
    # test_ingest_endpoint). _client is stubbed so no real I/O occurs.
    import types as _t, contextlib as _c
    from influxdb_client import Point
    s = _t.SimpleNamespace(influx_url="http://x", influx_token="t", influx_org="o",
                           influx_bucket_raw="raw_b", influx_bucket_long="long_b",
                           influx_bucket_3y="three_b", influx_bucket_test="test_b")
    gw = InfluxGateway(settings=s)
    seen = {}
    class _W:
        async def write(self, bucket=None, record=None):
            seen[bucket] = record
    class _C:
        def write_api(self):
            return _W()
    @_c.asynccontextmanager
    async def _fake():
        yield _C()
    monkeypatch.setattr(gw, "_client", _fake)
    await gw.write_built([("three_b", Point("m"))])
    assert "three_b" in seen
