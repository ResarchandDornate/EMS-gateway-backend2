import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status

from src.influx import _ident, bucket_for, measurement_for, rows
from src.signals.types import field_type
from src.telemetry import service as svc
from src.telemetry.dependencies import require_site_read
from src.telemetry.providers import get_influx_gateway_read, get_registry_index_read
from src.telemetry.schemas import (LatestPoint, LatestResponse, SeriesPoint, SeriesResponse, AggregateResponse)
from src.telemetry_errors import (InfluxTelemetryError, InvalidTimestamp,
                                   RetentionNotConfigured, UnknownRetentionClass,
                                   UnknownSignal, UnsafeQueryInput, UnsupportedDataType)

router = APIRouter(prefix="/api/v1/telemetry", tags=["telemetry"])


def _resolve(index, settings, stream, substream, signal_key):
    """Validate stream + registry signal, return (measurement, readable bucket).
    Unknown signal/stream -> 422; signal with no readable bucket -> 501 (unknown/unconfigured retention; T3 now routes to long)."""
    if stream not in svc.SUPPORTED_STREAMS:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "unsupported stream")
    measurement = measurement_for(stream, substream)
    try:
        sig = index.resolve(measurement, signal_key)
    except UnknownSignal:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "unknown signal")
    try:
        bucket = bucket_for(sig.retention_class, settings)
    except (RetentionNotConfigured, UnknownRetentionClass):
        raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, "signal retention bucket not available")
    return measurement, bucket


@router.get("/latest", response_model=LatestResponse)
async def latest(
    stream: str = Query(...),
    substream: str = Query(...),
    signal_key: str = Query(...),
    asset_id: str | None = Query(None),
    source_id: str | None = Query(None),
    site_id: uuid.UUID = Depends(require_site_read),      # 401/403 + authorized site
    index=Depends(get_registry_index_read),
    influx=Depends(get_influx_gateway_read),
):
    measurement, bucket = _resolve(index, influx.settings, stream, substream, signal_key)
    site = str(site_id)
    try:
        if asset_id is not None:
            _ident("asset_id", asset_id)
        if source_id is not None:
            _ident("source_id", source_id)
        if asset_id is not None:
            tables = await influx.latest_signal(bucket, measurement, site, signal_key,
                                                asset_id, source_id)
        else:
            tables = await influx.latest_by_asset(bucket, measurement, site, signal_key)
        recs = rows(tables)
    except UnsafeQueryInput:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "unsafe identifier")
    except InfluxTelemetryError:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "invalid request")
    except Exception:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "telemetry store unavailable")
    results = [LatestPoint(asset_id=r["asset_id"], source_id=r["source_id"],
                           value=r["value"], time=r["time"]) for r in recs]
    return LatestResponse(site_id=site_id, stream=stream, substream=substream,
                          signal_key=signal_key, results=results)


@router.get("/series", response_model=SeriesResponse)
async def series(
    stream: str = Query(...),
    substream: str = Query(...),
    signal_key: str = Query(...),
    asset_id: str = Query(...),
    start: str = Query(...),
    source_id: str | None = Query(None),
    stop: str | None = Query(None),
    limit: int = Query(svc.DEFAULT_LIMIT, ge=1, le=svc.MAX_LIMIT),
    site_id: uuid.UUID = Depends(require_site_read),
    index=Depends(get_registry_index_read),
    influx=Depends(get_influx_gateway_read),
):
    measurement, bucket = _resolve(index, influx.settings, stream, substream, signal_key)
    try:
        start_dt, stop_dt = svc.resolve_window(start, stop)
    except (InvalidTimestamp, ValueError):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "invalid time range")
    fetch = limit + 1                                   # +1 to detect truncation
    site = str(site_id)
    try:
        _ident("asset_id", asset_id)
        if source_id is not None:
            _ident("source_id", source_id)
        tables = await influx.series(bucket, measurement, site, signal_key,
                                     svc.to_flux_ts(start_dt), svc.to_flux_ts(stop_dt),
                                     asset_id, source_id, fetch)
        recs = rows(tables)
    except UnsafeQueryInput:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "unsafe identifier")
    except InfluxTelemetryError:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "invalid request")
    except Exception:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "telemetry store unavailable")
    truncated = len(recs) > limit
    pts = [SeriesPoint(time=r["time"], value=r["value"]) for r in recs[:limit]]
    return SeriesResponse(site_id=site_id, stream=stream, substream=substream,
                          signal_key=signal_key, asset_id=asset_id, points=pts, truncated=truncated)


@router.get("/aggregate", response_model=AggregateResponse)
async def aggregate(
    stream: str = Query(...),
    substream: str = Query(...),
    signal_key: str = Query(...),
    asset_id: str = Query(...),
    start: str = Query(...),
    window: str = Query(...),
    fn: str = Query(...),
    source_id: str | None = Query(None),
    stop: str | None = Query(None),
    site_id: uuid.UUID = Depends(require_site_read),
    index=Depends(get_registry_index_read),
    influx=Depends(get_influx_gateway_read),
):
    measurement, bucket = _resolve(index, influx.settings, stream, substream, signal_key)
    try:
        ftype = field_type(index.resolve(measurement, signal_key).data_type)
        flux_fn = svc.agg_flux_fn(fn, ftype)
    except UnsupportedDataType:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "signal type not aggregatable")
    except ValueError:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "unsupported aggregate function")
    try:
        start_dt, stop_dt, every = svc.resolve_agg(start, stop, window)
    except (InvalidTimestamp, ValueError):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "invalid aggregation window")
    site = str(site_id)
    try:
        _ident("asset_id", asset_id)
        if source_id is not None:
            _ident("source_id", source_id)
        tables = await influx.aggregate(bucket, measurement, site, signal_key,
                                        svc.to_flux_ts(start_dt), svc.to_flux_ts(stop_dt),
                                        svc.flux_every(every), flux_fn, asset_id, source_id)
        recs = rows(tables)
    except (RetentionNotConfigured, UnknownRetentionClass):
        raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, "signal retention bucket not available")
    except UnsafeQueryInput:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "unsafe identifier")
    except InfluxTelemetryError:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "invalid request")
    except Exception:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "telemetry store unavailable")
    points = [SeriesPoint(time=r["time"], value=r["value"]) for r in recs]
    return AggregateResponse(site_id=site_id, stream=stream, substream=substream,
                             signal_key=signal_key, asset_id=asset_id,
                             window=window, fn=fn, points=points)
