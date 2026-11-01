"""Read APIs over the dedicated per-stream tables (S3-S9). Authenticated + site-scoped via
require_site_read (401 unauth / 403 cross-site/cross-org, fails closed); all queries hard-
filtered to the authorized site_id. Pagination is limit/offset + has_more (fetch limit+1).
Time windows use the shared UTC grammar in src.structured_reads.ranges (absolute ISO,
rolling -7d/-4w/-6mo/-1y, or range=this_week|this_month|this_year; stop exclusive). Summary
endpoints aggregate in SQL and zero-fill the bucket axis. No InfluxDB; no org_id/secrets out.
"""
import uuid
from datetime import timezone

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import get_session
from src.telemetry.dependencies import require_site_read
from src.ingestion.health_models import GatewayHealthRecordModel as S3M
from src.ingestion.alarm_models import AlarmEventRecordModel as S4M
from src.ingestion.controller_models import ControllerStateRecordModel as S5M
from src.ingestion.solis_models import SolisTelemetryRecordModel as S6M
from src.ingestion.edge_ai_models import EdgeAiInferenceRecordModel as S7M
from src.ingestion.config_audit_models import ConfigurationAuditRecordModel as S8M
from src.ingestion.command_result_models import CommandResultRecordModel as S9M
from src.structured_reads import ranges, schemas as S
from src.structured_reads.summary import summarize

router = APIRouter(prefix="/api/v1", tags=["structured-reads"])

MAX_LIMIT = 1000
DEF_LATEST = 200
DEF_HIST = 500


def _window(start, stop, range_):
    try:
        return ranges.resolve_range(start, stop, range_)
    except ranges.RangeError as e:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(e))


def _bounded(start, stop, range_):
    try:
        return ranges.resolve_bounded(start, stop, range_)
    except ranges.RangeError as e:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(e))


def _unit(group_by):
    try:
        return ranges.validate_unit(group_by)
    except ranges.RangeError as e:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(e))


def _fmt(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _eqs(stmt, *pairs):
    for col, val in pairs:
        if val is not None:
            stmt = stmt.where(col == val)
    return stmt


async def _page(session, stmt, limit, offset):
    stmt = stmt.offset(offset).limit(limit + 1)
    rows = list((await session.execute(stmt)).scalars().all())
    return rows[:limit], len(rows) > limit


def _resp(site_id, rows, has_more, limit, offset, out):
    return {"site_id": site_id, "limit": limit, "offset": offset, "has_more": has_more,
            "results": [out.model_validate(r) for r in rows]}


def _latest_stmt(M, site_id, order_col, *eqs):
    stmt = _eqs(select(M).where(M.site_id == site_id), *eqs)
    return stmt.distinct(M.gateway_id).order_by(M.gateway_id, order_col.desc())


def _hist_stmt(M, site_id, order_col, start, stop, sort, *eqs):
    stmt = _eqs(select(M).where(M.site_id == site_id), *eqs)
    if start is not None:
        stmt = stmt.where(order_col >= start)
    if stop is not None:
        stmt = stmt.where(order_col < stop)
    return stmt.order_by(order_col.asc() if sort == "asc" else order_col.desc())


def _summary_resp(site_id, unit, start, stop, buckets):
    return {"site_id": site_id, "group_by": unit, "start": _fmt(start), "stop": _fmt(stop),
            "buckets": buckets}


# ---------------- S3 gateway_health ----------------
@router.get("/health/latest")
async def health_latest(gateway_id: uuid.UUID | None = Query(None),
                        limit: int = Query(DEF_LATEST, ge=1, le=MAX_LIMIT),
                        site_id: uuid.UUID = Depends(require_site_read),
                        session: AsyncSession = Depends(get_session)):
    stmt = _latest_stmt(S3M, site_id, S3M.recorded_at, (S3M.gateway_id, gateway_id)).limit(limit)
    rows = list((await session.execute(stmt)).scalars().all())
    return _resp(site_id, rows, False, limit, 0, S.HealthOut)


@router.get("/health/history")
async def health_history(gateway_id: uuid.UUID | None = Query(None),
                         start: str | None = Query(None), stop: str | None = Query(None),
                         range_: str | None = Query(None, alias="range"),
                         limit: int = Query(DEF_HIST, ge=1, le=MAX_LIMIT), offset: int = Query(0, ge=0),
                         site_id: uuid.UUID = Depends(require_site_read),
                         session: AsyncSession = Depends(get_session)):
    start_dt, stop_dt = _window(start, stop, range_)
    stmt = _hist_stmt(S3M, site_id, S3M.recorded_at, start_dt, stop_dt, "desc",
                      (S3M.gateway_id, gateway_id))
    rows, more = await _page(session, stmt, limit, offset)
    return _resp(site_id, rows, more, limit, offset, S.HealthOut)


# ---------------- S4 alarms/events ----------------
@router.get("/alarms")
async def alarms(severity: str | None = Query(None), event_type: str | None = Query(None),
                 state: str | None = Query(None), gateway_id: uuid.UUID | None = Query(None),
                 correlation_id: str | None = Query(None),
                 start: str | None = Query(None), stop: str | None = Query(None),
                 range_: str | None = Query(None, alias="range"),
                 sort: str = Query("desc", pattern="^(asc|desc)$"),
                 limit: int = Query(DEF_HIST, ge=1, le=MAX_LIMIT), offset: int = Query(0, ge=0),
                 site_id: uuid.UUID = Depends(require_site_read),
                 session: AsyncSession = Depends(get_session)):
    start_dt, stop_dt = _window(start, stop, range_)
    stmt = _hist_stmt(S4M, site_id, S4M.occurred_at, start_dt, stop_dt, sort,
                      (S4M.gateway_id, gateway_id), (S4M.severity, severity),
                      (S4M.event_type, event_type), (S4M.state, state),
                      (S4M.correlation_id, correlation_id))
    rows, more = await _page(session, stmt, limit, offset)
    return _resp(site_id, rows, more, limit, offset, S.AlarmOut)


@router.get("/alarms/summary")
async def alarms_summary(group_by: str = Query("day"),
                         severity: str | None = Query(None), event_type: str | None = Query(None),
                         state: str | None = Query(None), gateway_id: uuid.UUID | None = Query(None),
                         start: str | None = Query(None), stop: str | None = Query(None),
                         range_: str | None = Query(None, alias="range"),
                         site_id: uuid.UUID = Depends(require_site_read),
                         session: AsyncSession = Depends(get_session)):
    unit = _unit(group_by)
    start_dt, stop_dt = _bounded(start, stop, range_)
    try:
        buckets = await summarize(session, S4M, site_id=site_id, time_col=S4M.occurred_at, unit=unit,
                                  start=start_dt, stop=stop_dt,
                                  dims={"severity": S4M.severity, "state": S4M.state},
                                  eq_filters=[(S4M.gateway_id, gateway_id), (S4M.event_type, event_type),
                                              (S4M.severity, severity), (S4M.state, state)])
    except ranges.RangeError as e:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(e))
    return _summary_resp(site_id, unit, start_dt, stop_dt, buckets)


# ---------------- S5 soc_controller ----------------
@router.get("/controller/latest")
async def controller_latest(gateway_id: uuid.UUID | None = Query(None),
                            controller_state: str | None = Query(None),
                            limit: int = Query(DEF_LATEST, ge=1, le=MAX_LIMIT),
                            site_id: uuid.UUID = Depends(require_site_read),
                            session: AsyncSession = Depends(get_session)):
    stmt = _latest_stmt(S5M, site_id, S5M.received_at_utc, (S5M.gateway_id, gateway_id),
                        (S5M.controller_state, controller_state)).limit(limit)
    rows = list((await session.execute(stmt)).scalars().all())
    return _resp(site_id, rows, False, limit, 0, S.ControllerOut)


@router.get("/controller/history")
async def controller_history(gateway_id: uuid.UUID | None = Query(None),
                             controller_state: str | None = Query(None),
                             start: str | None = Query(None), stop: str | None = Query(None),
                             range_: str | None = Query(None, alias="range"),
                             limit: int = Query(DEF_HIST, ge=1, le=MAX_LIMIT), offset: int = Query(0, ge=0),
                             site_id: uuid.UUID = Depends(require_site_read),
                             session: AsyncSession = Depends(get_session)):
    start_dt, stop_dt = _window(start, stop, range_)
    stmt = _hist_stmt(S5M, site_id, S5M.received_at_utc, start_dt, stop_dt, "desc",
                      (S5M.gateway_id, gateway_id), (S5M.controller_state, controller_state))
    rows, more = await _page(session, stmt, limit, offset)
    return _resp(site_id, rows, more, limit, offset, S.ControllerOut)


# ---------------- S6 solis / S7 edge_ai (gateway + time only) ----------------
def _simple(prefix, M, Out):
    @router.get(f"/{prefix}/latest", name=f"{prefix}_latest")
    async def _latest(gateway_id: uuid.UUID | None = Query(None),
                      limit: int = Query(DEF_LATEST, ge=1, le=MAX_LIMIT),
                      site_id: uuid.UUID = Depends(require_site_read),
                      session: AsyncSession = Depends(get_session)):
        stmt = _latest_stmt(M, site_id, M.received_at_utc, (M.gateway_id, gateway_id)).limit(limit)
        rows = list((await session.execute(stmt)).scalars().all())
        return _resp(site_id, rows, False, limit, 0, Out)

    @router.get(f"/{prefix}/history", name=f"{prefix}_history")
    async def _history(gateway_id: uuid.UUID | None = Query(None),
                       start: str | None = Query(None), stop: str | None = Query(None),
                       range_: str | None = Query(None, alias="range"),
                       limit: int = Query(DEF_HIST, ge=1, le=MAX_LIMIT), offset: int = Query(0, ge=0),
                       site_id: uuid.UUID = Depends(require_site_read),
                       session: AsyncSession = Depends(get_session)):
        start_dt, stop_dt = _window(start, stop, range_)
        stmt = _hist_stmt(M, site_id, M.received_at_utc, start_dt, stop_dt, "desc",
                          (M.gateway_id, gateway_id))
        rows, more = await _page(session, stmt, limit, offset)
        return _resp(site_id, rows, more, limit, offset, Out)
    return _latest, _history


_simple("solis", S6M, S.SolisOut)
_simple("edge-ai", S7M, S.EdgeAiOut)


# ---------------- S8 configuration_audit ----------------
@router.get("/config-audit")
async def config_audit(actor: str | None = Query(None), setting_path: str | None = Query(None),
                       change_source: str | None = Query(None), gateway_id: uuid.UUID | None = Query(None),
                       start: str | None = Query(None), stop: str | None = Query(None),
                       range_: str | None = Query(None, alias="range"),
                       sort: str = Query("desc", pattern="^(asc|desc)$"),
                       limit: int = Query(DEF_HIST, ge=1, le=MAX_LIMIT), offset: int = Query(0, ge=0),
                       site_id: uuid.UUID = Depends(require_site_read),
                       session: AsyncSession = Depends(get_session)):
    start_dt, stop_dt = _window(start, stop, range_)
    stmt = _hist_stmt(S8M, site_id, S8M.occurred_at, start_dt, stop_dt, sort,
                      (S8M.gateway_id, gateway_id), (S8M.actor, actor),
                      (S8M.setting_path, setting_path), (S8M.change_source, change_source))
    rows, more = await _page(session, stmt, limit, offset)
    return _resp(site_id, rows, more, limit, offset, S.ConfigAuditOut)


@router.get("/config-audit/summary")
async def config_audit_summary(group_by: str = Query("day"),
                               actor: str | None = Query(None), setting_path: str | None = Query(None),
                               change_source: str | None = Query(None),
                               gateway_id: uuid.UUID | None = Query(None),
                               start: str | None = Query(None), stop: str | None = Query(None),
                               range_: str | None = Query(None, alias="range"),
                               site_id: uuid.UUID = Depends(require_site_read),
                               session: AsyncSession = Depends(get_session)):
    unit = _unit(group_by)
    start_dt, stop_dt = _bounded(start, stop, range_)
    try:
        buckets = await summarize(session, S8M, site_id=site_id, time_col=S8M.occurred_at, unit=unit,
                                  start=start_dt, stop=stop_dt,
                                  dims={"change_source": S8M.change_source},
                                  eq_filters=[(S8M.gateway_id, gateway_id), (S8M.actor, actor),
                                              (S8M.setting_path, setting_path),
                                              (S8M.change_source, change_source)])
    except ranges.RangeError as e:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(e))
    return _summary_resp(site_id, unit, start_dt, stop_dt, buckets)


# ---------------- S9 command_results ----------------
@router.get("/commands")
async def commands(command_id: uuid.UUID | None = Query(None), final_status: str | None = Query(None),
                   command_type: str | None = Query(None), gateway_id: uuid.UUID | None = Query(None),
                   start: str | None = Query(None), stop: str | None = Query(None),
                   range_: str | None = Query(None, alias="range"),
                   sort: str = Query("desc", pattern="^(asc|desc)$"),
                   limit: int = Query(DEF_HIST, ge=1, le=MAX_LIMIT), offset: int = Query(0, ge=0),
                   site_id: uuid.UUID = Depends(require_site_read),
                   session: AsyncSession = Depends(get_session)):
    start_dt, stop_dt = _window(start, stop, range_)
    stmt = _hist_stmt(S9M, site_id, S9M.requested_at_utc, start_dt, stop_dt, sort,
                      (S9M.gateway_id, gateway_id), (S9M.command_id, command_id),
                      (S9M.final_status, final_status), (S9M.command_type, command_type))
    rows, more = await _page(session, stmt, limit, offset)
    return _resp(site_id, rows, more, limit, offset, S.CommandOut)


@router.get("/commands/summary")
async def commands_summary(group_by: str = Query("day"),
                           final_status: str | None = Query(None), command_type: str | None = Query(None),
                           gateway_id: uuid.UUID | None = Query(None),
                           start: str | None = Query(None), stop: str | None = Query(None),
                           range_: str | None = Query(None, alias="range"),
                           site_id: uuid.UUID = Depends(require_site_read),
                           session: AsyncSession = Depends(get_session)):
    unit = _unit(group_by)
    start_dt, stop_dt = _bounded(start, stop, range_)
    try:
        buckets = await summarize(session, S9M, site_id=site_id, time_col=S9M.requested_at_utc, unit=unit,
                                  start=start_dt, stop=stop_dt,
                                  dims={"final_status": S9M.final_status},
                                  eq_filters=[(S9M.gateway_id, gateway_id),
                                              (S9M.final_status, final_status),
                                              (S9M.command_type, command_type)])
    except ranges.RangeError as e:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(e))
    return _summary_resp(site_id, unit, start_dt, stop_dt, buckets)
