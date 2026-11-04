"""Read-only query layer for the EMS monitoring dashboard.

STRICTLY READ-ONLY: only SELECT / Flux count queries. No INSERT/UPDATE/DELETE, no replay,
no ACK/dequeue, no backfill, no schema change. Every query is bounded (enumerated windows)
and tenant-scoped to the caller's authorized sites. Nothing here duplicates or persists
telemetry; it aggregates what already exists in PostgreSQL + InfluxDB on demand.

Evidence labels returned to the client:
  VERIFIED  = direct from source records
  DERIVED   = computed from verified records (method noted)
  NOT INSTRUMENTED = the event is not recorded by the existing system
A gateway delivery rate is never computed here (no gateway-generated/sent counts exist).
"""
from __future__ import annotations

import datetime as _dt

from sqlalchemy import text, bindparam

# Enumerated, bounded windows (injection-safe: keys only, never interpolated raw input).
WINDOWS = {"1h": 3600, "6h": 21600, "24h": 86400, "7d": 604800, "30d": 2592000}
DEFAULT_WINDOW = "24h"

# PG record tables -> their event-time column. All also carry created_at (storage time)
# and site_id. Used by the stream inventory. (controller/solis/edge_ai are disabled streams.)
PG_TABLES = [
    ("fast_bess_records", "recorded_at", "S1"),
    ("gateway_health_records", "recorded_at", "S3"),
    ("alarm_event_records", "occurred_at", "S4"),
    ("controller_state_records", "sample_time_utc", "S5"),
    ("solis_telemetry_records", "sample_time_utc", "S6"),
    ("edge_ai_inference_records", "sample_time_utc", "S7"),
    ("configuration_audit_records", "occurred_at", "S8"),
    ("command_result_records", "requested_at_utc", "S9"),
    ("stream_records", "recorded_at", "S10"),
]

S2_SUBSTREAMS = {"bms_extended", "pcs_extended", "ems_system", "remote_control",
                 "liquid_cooling", "io_module", "dehumidifier", "fire_protection"}


def _now():
    return _dt.datetime.now(_dt.timezone.utc)


def window_seconds(window: str) -> int:
    return WINDOWS.get(window, WINDOWS[DEFAULT_WINDOW])


# ---------------------------------------------------------------- site-scope helpers

def _sites_list(sites):
    """None -> None (unrestricted). Otherwise a list of str UUIDs (possibly empty)."""
    if sites is None:
        return None
    return [str(s) for s in sites]


def _scope_sql(sites, column="site_id"):
    """Return (sql_fragment, needs_bind). Empty string when unrestricted."""
    if sites is None:
        return "", False
    return f" AND {column} IN :sites", True


def _scope_ledger_sql(sites):
    if sites is None:
        return "", False
    return " AND gateway_id IN (SELECT id FROM gateways WHERE site_id IN :sites)", True


async def _exec(session, sql, params=None, sites_bind=None):
    stmt = text(sql)
    if sites_bind is not None:
        stmt = stmt.bindparams(bindparam("sites", expanding=True))
    res = await session.execute(stmt, params or {})
    return res


# ---------------------------------------------------------------- ledger / overview

async def ledger_by_stream(session, sites, *, window: str | None = None):
    """Stored/pending message counts per stream (optionally within a window), scoped.
    VERIFIED from ingest_messages."""
    lst = _sites_list(sites)
    if lst is not None and not lst:
        return []
    scope, needs = _scope_ledger_sql(sites)
    params = {}
    where = "WHERE 1=1"
    if window:
        params["secs"] = window_seconds(window)
        where += " AND created_at >= now() - make_interval(secs => :secs)"
    sql = (f"SELECT stream, status, count(*) AS n, max(stored_at) AS last_stored, "
           f"max(created_at) AS last_seen FROM ingest_messages {where}{scope} "
           f"GROUP BY stream, status ORDER BY stream, status")
    if needs:
        params["sites"] = lst
    rows = (await _exec(session, sql, params, sites_bind=lst if needs else None)).mappings().all()
    return [dict(r) for r in rows]


async def stuck_pending(session, sites):
    """Lease-expired pending messages per stream — a DERIVED proxy for failed/stuck
    (true failure codes live only in ephemeral logs: NOT INSTRUMENTED)."""
    lst = _sites_list(sites)
    if lst is not None and not lst:
        return []
    scope, needs = _scope_ledger_sql(sites)
    sql = (f"SELECT stream, count(*) AS stuck FROM ingest_messages "
           f"WHERE status = 'pending' AND (lease_expires_at IS NULL OR lease_expires_at < now()){scope} "
           f"GROUP BY stream ORDER BY stream")
    params = {"sites": lst} if needs else {}
    rows = (await _exec(session, sql, params, sites_bind=lst if needs else None)).mappings().all()
    return [dict(r) for r in rows]


async def backlog_by_stream(session, sites):
    """Per-stream pending backlog WITH timestamps: pending count, lease-expired (stuck) count,
    and the first/last receive time (created_at) of pending messages. Shows which stream is
    backlogged and when it accrued. VERIFIED (counts + receive time) / DERIVED (stuck)."""
    lst = _sites_list(sites)
    if lst is not None and not lst:
        return []
    scope, needs = _scope_ledger_sql(sites)
    sql = (f"SELECT stream, count(*) AS pending, "
           f"count(*) FILTER (WHERE lease_expires_at IS NULL OR lease_expires_at < now()) AS stuck, "
           f"min(created_at) AS first_pending, max(created_at) AS last_pending "
           f"FROM ingest_messages WHERE status = 'pending'{scope} GROUP BY stream ORDER BY stream")
    params = {"sites": lst} if needs else {}
    rows = (await _exec(session, sql, params, sites_bind=lst if needs else None)).mappings().all()
    return [dict(r) for r in rows]


async def backlog_items(session, sites, *, limit: int = 100, newest_first: bool = True):
    """Individual pending (backlog) messages: message_id, stream, size (record_count),
    sequence, receive time. Bounded by `limit`. VERIFIED. 'size' is record_count — the
    backend does not store S2 payload bytes, so a byte size is not available."""
    lst = _sites_list(sites)
    if lst is not None and not lst:
        return []
    lim = max(1, min(int(limit), 1000))
    order = "DESC" if newest_first else "ASC"
    scope, needs = _scope_ledger_sql(sites)
    sql = (f"SELECT message_id, stream, substream, record_count, sequence, created_at, lease_expires_at "
           f"FROM ingest_messages WHERE status = 'pending'{scope} "
           f"ORDER BY created_at {order} LIMIT {lim}")
    params = {"sites": lst} if needs else {}
    rows = (await _exec(session, sql, params, sites_bind=lst if needs else None)).mappings().all()
    return [dict(r) for r in rows]


async def recent_messages(session, sites, *, limit: int = 50):
    """Latest messages across all streams (any status) with substream + store status.
    Lets you see substreams populating live. VERIFIED. Rows written before the substream
    column was added show substream null."""
    lst = _sites_list(sites)
    if lst is not None and not lst:
        return []
    lim = max(1, min(int(limit), 500))
    scope, needs = _scope_ledger_sql(sites)
    sql = (f"SELECT message_id, stream, substream, status, record_count, created_at, stored_at "
           f"FROM ingest_messages WHERE 1=1{scope} ORDER BY created_at DESC LIMIT {lim}")
    params = {"sites": lst} if needs else {}
    rows = (await _exec(session, sql, params, sites_bind=lst if needs else None)).mappings().all()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------- PG stream inventory

async def stream_inventory(session, sites):
    """Per PG record table: rows, event-time span, storage-time span. VERIFIED."""
    lst = _sites_list(sites)
    out = []
    for table, evcol, stream in PG_TABLES:
        if lst is not None and not lst:
            out.append({"table": table, "stream": stream, "rows": 0,
                        "first_event": None, "last_event": None,
                        "first_stored": None, "last_stored": None})
            continue
        scope, needs = _scope_sql(sites)
        sql = (f"SELECT count(*) AS rows, min({evcol}) AS first_event, max({evcol}) AS last_event, "
               f"min(created_at) AS first_stored, max(created_at) AS last_stored "
               f"FROM {table} WHERE 1=1{scope}")
        params = {"sites": lst} if needs else {}
        r = (await _exec(session, sql, params, sites_bind=lst if needs else None)).mappings().first()
        row = dict(r) if r else {}
        row.update({"table": table, "stream": stream})
        out.append(row)
    return out


# ---------------------------------------------------------------- signal registry (expected)

async def registry_cadence(session, substream):
    """Expected cadence per signal_key for a substream, from signal_registry. VERIFIED (spec)."""
    sql = ("SELECT signal_key, min(cadence_sec) AS min_cad, max(cadence_sec) AS max_cad "
           "FROM signal_registry WHERE substream = :ss GROUP BY signal_key")
    rows = (await session.execute(text(sql), {"ss": substream})).mappings().all()
    return {r["signal_key"]: {"min": r["min_cad"], "max": r["max_cad"]} for r in rows}


async def registry_summary(session):
    sql = ("SELECT primary_stream, substream, count(*) AS signals, "
           "min(cadence_sec) AS min_cad, max(cadence_sec) AS max_cad "
           "FROM signal_registry GROUP BY primary_stream, substream ORDER BY 1,2")
    return [dict(r) for r in (await session.execute(text(sql))).mappings().all()]


# ---------------------------------------------------------------- InfluxDB (S2 points)

def _flux_site_filter(sites):
    lst = _sites_list(sites)
    if lst is None:
        return ""
    quoted = ",".join('"%s"' % s for s in lst)   # s are DB UUIDs (trusted), re-quoted
    return f' |> filter(fn: (r) => contains(value: r.site_id, set: [{quoted}]))'


def _count_by(tables, key):
    out = {}
    for t in tables:
        for rec in t.records:
            k = rec.values.get(key)
            v = rec.get_value() or 0
            out[k] = out.get(k, 0) + v
    return out


async def s2_point_counts(influx, sites, *, window: str):
    """Per-substream S2 point counts over the window (telemetry_1y). VERIFIED.
    Returns {} on store error; caller labels INCONCLUSIVE."""
    lst = _sites_list(sites)
    if lst is not None and not lst:
        return {}
    dur = "-%ss" % window_seconds(window)
    bucket = influx.settings.influx_bucket_long
    flux = (f'from(bucket: "{bucket}") |> range(start: {dur}) '
            f'|> filter(fn: (r) => r._measurement =~ /^s2_/)'
            f'{_flux_site_filter(sites)} '
            f'|> group(columns: ["_measurement"]) |> count() |> group()')
    tables = await influx.query(flux)
    return _count_by(tables, "_measurement")


async def s2_latest(influx, sites, *, substream: str, window: str = "6h"):
    """Latest value + event-time per signal for one S2 substream (InfluxDB last()).
    Live snapshot only — nothing is stored. VERIFIED. Energy signals (telemetry_3y) are
    not included here (this reads telemetry_1y, the fast operational signals)."""
    lst = _sites_list(sites)
    if lst is not None and not lst:
        return {}
    dur = "-%ss" % window_seconds(window)
    bucket = influx.settings.influx_bucket_long
    meas = "s2_%s" % substream
    flux = (f'from(bucket: "{bucket}") |> range(start: {dur}) '
            f'|> filter(fn: (r) => r._measurement == "{meas}")'
            f'{_flux_site_filter(sites)} '
            f'|> group(columns: ["_field"]) |> last()')
    tables = await influx.query(flux)
    out = {}
    for t in tables:
        for rec in t.records:
            f = rec.values.get("_field")
            ts = rec.values.get("_time")
            out[f] = {"value": rec.get_value(),
                      "time": ts.isoformat() if hasattr(ts, "isoformat") else ts}
    return out


async def s2_field_counts(influx, sites, *, substream: str, window: str):
    """Per-signal (field) point counts for one substream over the window. VERIFIED."""
    lst = _sites_list(sites)
    if lst is not None and not lst:
        return {}
    dur = "-%ss" % window_seconds(window)
    bucket = influx.settings.influx_bucket_long
    meas = "s2_%s" % substream
    flux = (f'from(bucket: "{bucket}") |> range(start: {dur}) '
            f'|> filter(fn: (r) => r._measurement == "{meas}")'
            f'{_flux_site_filter(sites)} '
            f'|> group(columns: ["_field"]) |> count() |> group()')
    tables = await influx.query(flux)
    return _count_by(tables, "_field")
