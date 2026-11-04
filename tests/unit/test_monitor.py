"""Unit tests for the read-only monitoring/dashboard API.

Pure unit tests: the DB session and Influx gateway are stubbed, so no database, no network,
no credentials. They cover response shaping, window validation, scope labelling, the
storage-success derivation, the frequency interval math, and the store-failure fallback.
End-to-end auth/scope against a real DB is exercised on staging (integration), not here.
"""
import datetime
from types import SimpleNamespace

import pytest

from src.monitor import service as svc
from src.monitor import router as r


# ----------------------------------------------------------------- pure service helpers

def test_window_seconds_known_and_default():
    assert svc.window_seconds("24h") == 86400
    assert svc.window_seconds("7d") == 604800
    assert svc.window_seconds("bogus") == svc.WINDOWS[svc.DEFAULT_WINDOW]


def test_sites_list_none_is_unrestricted():
    assert svc._sites_list(None) is None
    assert svc._sites_list(set()) == []
    out = svc._sites_list({"a", "b"})
    assert sorted(out) == ["a", "b"]


def test_flux_site_filter_scoped_vs_unrestricted():
    assert svc._flux_site_filter(None) == ""
    f = svc._flux_site_filter({"11111111-1111-1111-1111-111111111111"})
    assert "contains(value: r.site_id" in f and "11111111-1111-1111-1111-111111111111" in f


def test_scope_ledger_sql_joins_gateways_only_when_scoped():
    assert svc._scope_ledger_sql(None) == ("", False)
    frag, needs = svc._scope_ledger_sql({"s1"})
    assert needs is True and "gateways" in frag and "site_id IN :sites" in frag


def test_count_by_sums_values_per_key():
    class Rec:
        def __init__(self, m, v): self.values = {"_measurement": m}; self._v = v
        def get_value(self): return self._v
    class Tbl:
        def __init__(self, recs): self.records = recs
    tables = [Tbl([Rec("s2_pcs_extended", 10), Rec("s2_pcs_extended", 5)]),
              Tbl([Rec("s2_bms_extended", 7)])]
    out = svc._count_by(tables, "_measurement")
    assert out == {"s2_pcs_extended": 15, "s2_bms_extended": 7}


# ----------------------------------------------------------------- router validation

def test_check_window_rejects_unknown():
    from fastapi import HTTPException
    assert r._check_window("24h") == "24h"
    with pytest.raises(HTTPException):
        r._check_window("99y")


def test_scope_label():
    assert r._scope_label(None) == "all_sites"
    assert r._scope_label({"a", "b"}) == "sites:2"


# ----------------------------------------------------------------- endpoint shaping

async def test_overview_computes_storage_success(monkeypatch):
    async def fake_ledger(session, sites, window=None):
        return [{"stream": "S2", "status": "stored", "n": 18059, "last_stored": None, "last_seen": None},
                {"stream": "S2", "status": "pending", "n": 16372, "last_stored": None, "last_seen": None}]
    async def fake_stuck(session, sites):
        return [{"stream": "S2", "stuck": 16370}]
    monkeypatch.setattr(svc, "ledger_by_stream", fake_ledger)
    monkeypatch.setattr(svc, "stuck_pending", fake_stuck)
    influx = SimpleNamespace(settings=SimpleNamespace(
        influx_bucket_raw="telemetry_raw_90d", influx_bucket_long="telemetry_1y",
        influx_bucket_3y="telemetry_3y"))
    res = await r.overview(scope=(None, object()), influx=influx)
    assert res["totals"]["stored"] == 18059 and res["totals"]["pending"] == 16372
    assert res["totals"]["storage_success_pct"] == 52.4          # 18059/(18059+16372)=52.449→52.4
    assert res["scope"] == "all_sites"
    assert any("DELIVERY" in n for n in res["notes"])            # delivery caveat present
    assert len(res["retention_buckets"]) == 3


async def test_frequency_interval_math_and_quality_flag(monkeypatch):
    async def fake_cadence(session, ss):
        return {"activepower_a": {"min": 15, "max": 15}}
    async def fake_fields(influx, sites, *, substream, window):
        return {"activepower_a": 5760, "activepower_a_q": 5760}
    monkeypatch.setattr(svc, "registry_cadence", fake_cadence)
    monkeypatch.setattr(svc, "s2_field_counts", fake_fields)
    res = await r.frequency(substream="pcs_extended", window="24h",
                            scope=(None, object()), influx=object())
    by = {s["field"]: s for s in res["signals"]}
    assert by["activepower_a"]["observed_interval_s"] == 15.0    # 86400/5760
    assert by["activepower_a"]["expected_cadence_s"] == 15
    assert by["activepower_a_q"]["is_quality"] is True


async def test_frequency_rejects_unknown_substream():
    from fastapi import HTTPException
    with pytest.raises(HTTPException):
        await r.frequency(substream="not_a_substream", window="24h",
                          scope=(None, object()), influx=object())


async def test_s2_store_failure_is_inconclusive_not_crash(monkeypatch):
    async def boom(influx, sites, *, window):
        raise RuntimeError("influx down")
    monkeypatch.setattr(svc, "s2_point_counts", boom)
    res = await r.s2(window="24h", scope=(None, object()), influx=object())
    assert res["evidence"] == "INCONCLUSIVE" and res["measurements"] == []


async def test_live_shapes_rows_and_rejects_unknown(monkeypatch):
    from fastapi import HTTPException
    async def fake_latest(influx, sites, *, substream, window="6h"):
        return {"activepower_a": {"value": 12.5, "time": "2026-11-03T07:00:00+00:00"},
                "activepower_b": {"value": 11.0, "time": "2026-11-03T07:00:01+00:00"}}
    monkeypatch.setattr(svc, "s2_latest", fake_latest)
    res = await r.live(substream="pcs_extended", scope=(None, object()), influx=object())
    assert res["evidence"] == "VERIFIED" and len(res["signals"]) == 2
    assert res["signals"][0]["field"] == "activepower_a" and res["signals"][0]["value"] == 12.5
    assert res["signals"][0]["time"].startswith("2026-11-03")
    with pytest.raises(HTTPException):
        await r.live(substream="nope", scope=(None, object()), influx=object())


async def test_live_store_failure_inconclusive(monkeypatch):
    async def boom(influx, sites, *, substream, window="6h"):
        raise RuntimeError("influx down")
    monkeypatch.setattr(svc, "s2_latest", boom)
    res = await r.live(substream="pcs_extended", scope=(None, object()), influx=object())
    assert res["evidence"] == "INCONCLUSIVE" and res["signals"] == []


async def test_failures_returns_backlog_with_timestamps(monkeypatch):
    async def fake_backlog(session, sites):
        return [{"stream": "S2", "pending": 16372, "stuck": 16370,
                 "first_pending": "2026-11-02T06:13:00+00:00", "last_pending": "2026-11-03T01:05:00+00:00"}]
    monkeypatch.setattr(svc, "backlog_by_stream", fake_backlog)
    res = await r.failures(scope=(None, object()))
    row = res["backlog_by_stream"][0]
    assert row["stream"] == "S2" and row["pending"] == 16372 and row["stuck"] == 16370
    assert row["first_pending"].startswith("2026-11-02") and row["last_pending"].startswith("2026-11-03")


async def test_backlog_items_returns_message_fields(monkeypatch):
    async def fake_items(session, sites, *, limit=100, newest_first=True):
        return [{"message_id": "11111111-1111-1111-1111-111111111111", "stream": "S2",
                 "substream": "pcs_extended", "record_count": 42, "sequence": 7,
                 "created_at": "2026-11-03T01:05:40+00:00", "lease_expires_at": "2026-11-03T01:06:40+00:00"}]
    monkeypatch.setattr(svc, "backlog_items", fake_items)
    res = await r.backlog_items(limit=100, order="newest", scope=(None, object()))
    assert res["count"] == 1 and res["order"] == "newest"
    it = res["items"][0]
    assert it["message_id"].startswith("1111") and it["stream"] == "S2" and it["record_count"] == 42
    assert it["substream"] == "pcs_extended"


async def test_recent_messages_includes_substream_and_status(monkeypatch):
    async def fake_recent(session, sites, *, limit=50):
        return [{"message_id": "aaaa", "stream": "S2", "substream": "pcs_extended",
                 "status": "stored", "record_count": 86, "created_at": "2026-11-03T12:30:00+00:00",
                 "stored_at": "2026-11-03T12:30:00+00:00"}]
    monkeypatch.setattr(svc, "recent_messages", fake_recent)
    res = await r.recent_messages(limit=50, scope=(None, object()))
    assert res["count"] == 1 and res["items"][0]["substream"] == "pcs_extended"
    assert res["items"][0]["status"] == "stored"


def test_monitor_routes_mounted():
    paths = {route.path for route in r.router.routes}
    assert {"/api/v1/monitor/overview", "/api/v1/monitor/ledger", "/api/v1/monitor/streams",
            "/api/v1/monitor/s2", "/api/v1/monitor/frequency", "/api/v1/monitor/failures",
            "/api/v1/monitor/live", "/api/v1/monitor/backlog-items",
            "/api/v1/monitor/recent-messages"} <= paths
