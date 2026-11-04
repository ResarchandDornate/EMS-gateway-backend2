"""Read-only monitoring/dashboard API.

Mounts /api/v1/monitor/*. Every endpoint is GET, authenticated (dashboard:view),
tenant-scoped to the caller's sites, bounded by an enumerated window, and aggregate.
Nothing writes, replays, ACKs, dequeues, or backfills. Responses carry explicit evidence
labels; no gateway delivery rate is ever produced (no gateway-side counts exist).
"""
from fastapi import APIRouter, Depends, HTTPException, Query, status

from src.monitor import service as svc
from src.monitor.dependencies import require_monitor_scope
from src.telemetry.providers import get_influx_gateway_read

router = APIRouter(prefix="/api/v1/monitor", tags=["monitor"])

_STORE_NOTE = "InfluxDB unavailable or query failed; counts omitted."


def _check_window(window: str) -> str:
    if window not in svc.WINDOWS:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT,
                            f"window must be one of {sorted(svc.WINDOWS)}")
    return window


def _scope_label(sites) -> str:
    return "all_sites" if sites is None else f"sites:{len(sites)}"


@router.get("/overview")
async def overview(scope=Depends(require_monitor_scope), influx=Depends(get_influx_gateway_read)):
    """Pipeline health snapshot for the management view: stored/pending per stream, stuck
    backlog, and configured retention buckets. [VERIFIED ledger; config for buckets]"""
    sites, session = scope
    ledger = await svc.ledger_by_stream(session, sites)
    stuck = await svc.stuck_pending(session, sites)
    pending_total = sum(r["n"] for r in ledger if r["status"] == "pending")
    stored_total = sum(r["n"] for r in ledger if r["status"] == "stored")
    s = influx.settings
    buckets = [
        {"name": s.influx_bucket_raw, "class": "T1_RAW_90D"},
        {"name": s.influx_bucket_long, "class": "T2_1Y"},
        {"name": getattr(s, "influx_bucket_3y", "telemetry_3y"), "class": "T3_3Y"},
    ]
    return {
        "scope": _scope_label(sites),
        "generated_at": svc._now().isoformat(),
        "ledger_by_stream": ledger,
        "totals": {"stored": stored_total, "pending": pending_total,
                   "storage_success_pct": round(100 * stored_total / (stored_total + pending_total), 1)
                   if (stored_total + pending_total) else None},
        "stuck_pending_by_stream": stuck,
        "retention_buckets": buckets,
        "evidence": "VERIFIED (ledger) / DERIVED (storage_success_pct = stored/(stored+pending))",
        "notes": ["storage_success_pct is a backend STORAGE rate, not a gateway DELIVERY rate.",
                  "Gateway generated/sent counts are NOT INSTRUMENTED in this backend."],
    }


@router.get("/ledger")
async def ledger(window: str = Query(svc.DEFAULT_WINDOW), scope=Depends(require_monitor_scope)):
    """Stored/pending message counts per stream within a window. [VERIFIED]"""
    _check_window(window)
    sites, session = scope
    rows = await svc.ledger_by_stream(session, sites, window=window)
    return {"scope": _scope_label(sites), "window": window,
            "generated_at": svc._now().isoformat(), "rows": rows, "evidence": "VERIFIED"}


@router.get("/streams")
async def streams(scope=Depends(require_monitor_scope)):
    """Per PostgreSQL record table: row count + event-time and storage-time spans.
    Includes records-per-message context via the ledger. [VERIFIED]"""
    sites, session = scope
    inv = await svc.stream_inventory(session, sites)
    return {"scope": _scope_label(sites), "generated_at": svc._now().isoformat(),
            "tables": inv, "evidence": "VERIFIED",
            "notes": ["One message can produce many records; compare to ledger message counts."]}


@router.get("/s2")
async def s2(window: str = Query(svc.DEFAULT_WINDOW),
             scope=Depends(require_monitor_scope), influx=Depends(get_influx_gateway_read)):
    """Per-substream S2 point counts over the window (InfluxDB telemetry_1y). [VERIFIED]"""
    _check_window(window)
    sites, session = scope
    try:
        counts = await svc.s2_point_counts(influx, sites, window=window)
        measurements = [{"measurement": k, "points": v} for k, v in sorted(counts.items())]
        evidence, notes = "VERIFIED", ["Counts include companion _q quality fields."]
    except Exception:
        measurements, evidence, notes = [], "INCONCLUSIVE", [_STORE_NOTE]
    return {"scope": _scope_label(sites), "window": window,
            "generated_at": svc._now().isoformat(), "measurements": measurements,
            "evidence": evidence, "notes": notes}


@router.get("/frequency")
async def frequency(substream: str = Query(...), window: str = Query(svc.DEFAULT_WINDOW),
                    scope=Depends(require_monitor_scope), influx=Depends(get_influx_gateway_read)):
    """Observed per-signal interval vs expected registry cadence, for one S2 substream.
    Observed interval = window_seconds / points. [VERIFIED counts; DERIVED interval]"""
    _check_window(window)
    if substream not in svc.S2_SUBSTREAMS:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT,
                            f"substream must be one of {sorted(svc.S2_SUBSTREAMS)}")
    sites, session = scope
    cadence = await svc.registry_cadence(session, substream)
    secs = svc.window_seconds(window)
    try:
        counts = await svc.s2_field_counts(influx, sites, substream=substream, window=window)
        evidence, notes = "VERIFIED (counts) / DERIVED (interval)", []
    except Exception:
        counts, evidence, notes = {}, "INCONCLUSIVE", [_STORE_NOTE]
    out = []
    for field, points in counts.items():
        base = field[:-2] if field.endswith("_q") else field
        exp = cadence.get(base) or cadence.get(field)
        out.append({
            "field": field, "is_quality": field.endswith("_q"), "points": points,
            "observed_interval_s": round(secs / points, 2) if points else None,
            "expected_cadence_s": (None if exp is None else
                                   (exp["min"] if exp["min"] == exp["max"] else [exp["min"], exp["max"]])),
        })
    out.sort(key=lambda r: r["points"], reverse=True)
    return {"scope": _scope_label(sites), "substream": substream, "window": window,
            "generated_at": svc._now().isoformat(), "signals": out, "evidence": evidence, "notes": notes}


@router.get("/backlog-items")
async def backlog_items(limit: int = Query(100, ge=1, le=1000),
                        order: str = Query("newest"),
                        scope=Depends(require_monitor_scope)):
    """Individual backlog (pending) messages with message_id, stream, size (record_count)
    and receive time. Bounded by `limit` (<=1000). [VERIFIED]"""
    sites, session = scope
    rows = await svc.backlog_items(session, sites, limit=limit, newest_first=(order != "oldest"))
    return {"scope": _scope_label(sites), "generated_at": svc._now().isoformat(),
            "count": len(rows), "limit": limit, "order": order, "items": rows,
            "evidence": "VERIFIED (ledger)",
            "notes": ["'size' is record_count (records in the message); the backend does not "
                      "store S2 payload bytes, so a byte size is not available.",
                      "These are pending/unstored messages; at most 'limit' are returned."]}


@router.get("/recent-messages")
async def recent_messages(limit: int = Query(50, ge=1, le=500),
                          scope=Depends(require_monitor_scope)):
    """Latest messages across all streams with substream and store status. [VERIFIED]"""
    sites, session = scope
    rows = await svc.recent_messages(session, sites, limit=limit)
    return {"scope": _scope_label(sites), "generated_at": svc._now().isoformat(),
            "count": len(rows), "items": rows, "evidence": "VERIFIED (ledger)",
            "notes": ["Latest messages across all streams with substream + store status.",
                      "Rows received before the substream column was added show substream null."]}


@router.get("/live")
async def live(substream: str = Query(...),
              scope=Depends(require_monitor_scope), influx=Depends(get_influx_gateway_read)):
    """Live snapshot: latest value + event-timestamp per signal for one S2 substream.
    Read-only, nothing stored. Intended to be polled by the dashboard's Live Data tab.
    [VERIFIED values + event time]"""
    if substream not in svc.S2_SUBSTREAMS:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT,
                            f"substream must be one of {sorted(svc.S2_SUBSTREAMS)}")
    sites, session = scope
    try:
        latest = await svc.s2_latest(influx, sites, substream=substream)
        rows = [{"field": k, "value": v["value"], "time": v["time"]} for k, v in latest.items()]
        rows.sort(key=lambda r: r["field"])
        evidence, notes = "VERIFIED", ["Latest value per signal with gateway event-time; not stored.",
                                       "Energy counters (telemetry_3y) are not shown in this live view."]
    except Exception:
        rows, evidence, notes = [], "INCONCLUSIVE", [_STORE_NOTE]
    return {"scope": _scope_label(sites), "substream": substream,
            "generated_at": svc._now().isoformat(), "signals": rows,
            "evidence": evidence, "notes": notes}


@router.get("/failures")
async def failures(scope=Depends(require_monitor_scope)):
    """Stuck (lease-expired pending) messages per stream — a DERIVED proxy for failed/stuck
    ingestion. True per-failure codes (e.g. HTTP 404) live only in ephemeral container logs:
    NOT INSTRUMENTED as queryable data."""
    sites, session = scope
    rows = await svc.backlog_by_stream(session, sites)
    return {"scope": _scope_label(sites), "generated_at": svc._now().isoformat(),
            "backlog_by_stream": rows,
            "evidence": "VERIFIED (pending count + receive timestamps) / DERIVED (stuck = lease-expired)",
            "notes": ["Shows which stream is backlogged and when it accrued (first/last receive time).",
                      "'stuck' = pending with an expired lease; timestamps are backend receive time (created_at).",
                      "Per-failure error codes are NOT INSTRUMENTED (ephemeral logs only).",
                      "Backlog clears as the gateway resends; it is not confirmed data loss."]}
