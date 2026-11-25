import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status

from src.influx import _ident, bucket_for, measurement_for, rows
from src.signals.types import QUALITY_SUFFIX
from src.signals.types import field_type
from src.telemetry import service as svc
from src.telemetry.dependencies import require_site_read
from src.telemetry.providers import get_influx_gateway_read, get_registry_index_read
from src.telemetry.schemas import (LatestPoint, LatestResponse, SeriesPoint, SeriesResponse, AggregateResponse)
from src.telemetry_errors import (AmbiguousPointId, InfluxTelemetryError, InvalidTimestamp,
                                   RetentionNotConfigured, UnknownPointId, UnknownRetentionClass,
                                   UnknownSignal, UnsafeQueryInput, UnsupportedDataType)

router = APIRouter(prefix="/api/v1/telemetry", tags=["telemetry"])


def _resolve_target(index, settings, stream, substream, signal_key, point_id):
    """Resolve a read target by signal_key (existing) and/or point_id (additive, T6).
    Returns (measurement, readable bucket, effective signal_key, RegistrySignal).

    - signal_key only: unchanged behavior.
    - point_id given: resolve it to the exact registry signal; it must belong to the named
      stream/substream, and if signal_key is also given the two must agree.
    Unknown/unpopulated point_id and unknown signal -> 422; ambiguous point_id or a
    point_id/signal_key/stream conflict -> 409. Never silently falls back to another signal."""
    if stream not in svc.SUPPORTED_STREAMS:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "unsupported stream")
    if signal_key is None and point_id is None:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "signal_key or point_id required")
    if point_id is not None:
        try:
            p_meas, p_key, sig = index.resolve_point_id(point_id)
        except UnknownPointId:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "unknown point_id")
        except AmbiguousPointId:
            raise HTTPException(status.HTTP_409_CONFLICT, "ambiguous point_id")
        if p_meas != measurement_for(stream, substream):
            raise HTTPException(status.HTTP_409_CONFLICT, "point_id does not belong to this stream/substream")
        if signal_key is not None and signal_key != p_key:
            raise HTTPException(status.HTTP_409_CONFLICT, "point_id and signal_key refer to different signals")
        measurement, signal_key = p_meas, p_key
    else:
        measurement = measurement_for(stream, substream)
        try:
            sig = index.resolve(measurement, signal_key)
        except UnknownSignal:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "unknown signal")
    try:
        bucket = bucket_for(sig.retention_class, settings)
    except (RetentionNotConfigured, UnknownRetentionClass):
        raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, "signal retention bucket not available")
    return measurement, bucket, signal_key, sig


@router.get("/latest", response_model=LatestResponse)
async def latest(
    stream: str = Query(...),
    substream: str = Query(...),
    signal_key: str | None = Query(None),
    asset_id: str | None = Query(None),
    source_id: str | None = Query(None),
    point_id: str | None = Query(None),
    site_id: uuid.UUID = Depends(require_site_read),      # 401/403 + authorized site
    index=Depends(get_registry_index_read),
    influx=Depends(get_influx_gateway_read),
):
    measurement, bucket, signal_key, _sig = _resolve_target(index, influx.settings, stream, substream, signal_key, point_id)
    site = str(site_id)
    try:
        if asset_id is not None:
            _ident("asset_id", asset_id)
        if source_id is not None:
            _ident("source_id", source_id)
        if asset_id is not None:
            tables = await influx.latest_signal(bucket, measurement, site, signal_key,
                                                asset_id, source_id, point_id=point_id)
        else:
            tables = await influx.latest_by_asset(bucket, measurement, site, signal_key, point_id=point_id)
        recs = rows(tables)
    except UnsafeQueryInput:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "unsafe identifier")
    except InfluxTelemetryError:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "invalid request")
    except Exception:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "telemetry store unavailable")
    # CG-08/15/16: pair each value with its companion quality field ("<signal_key>_q")
    # so the UI can tell good / missing / unsupported apart.
    qkey = f"{signal_key}{QUALITY_SUFFIX}"
    quality_by = {(r["asset_id"], r["source_id"]): r["value"]
                  for r in recs if r["field"] == qkey}
    results = [LatestPoint(asset_id=r["asset_id"], source_id=r["source_id"],
                           value=r["value"], time=r["time"],
                           quality=quality_by.get((r["asset_id"], r["source_id"])))
               for r in recs if r["field"] == signal_key]
    return LatestResponse(site_id=site_id, stream=stream, substream=substream,
                          signal_key=signal_key, results=results)


@router.get("/series", response_model=SeriesResponse)
async def series(
    stream: str = Query(...),
    substream: str = Query(...),
    signal_key: str | None = Query(None),
    asset_id: str = Query(...),
    start: str = Query(...),
    source_id: str | None = Query(None),
    stop: str | None = Query(None),
    point_id: str | None = Query(None),
    limit: int = Query(svc.DEFAULT_LIMIT, ge=1, le=svc.MAX_LIMIT),
    site_id: uuid.UUID = Depends(require_site_read),
    index=Depends(get_registry_index_read),
    influx=Depends(get_influx_gateway_read),
):
    measurement, bucket, signal_key, _sig = _resolve_target(index, influx.settings, stream, substream, signal_key, point_id)
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
                                     asset_id, source_id, fetch, point_id=point_id)
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
    signal_key: str | None = Query(None),
    asset_id: str = Query(...),
    start: str = Query(...),
    window: str = Query(...),
    fn: str = Query(...),
    source_id: str | None = Query(None),
    stop: str | None = Query(None),
    point_id: str | None = Query(None),
    site_id: uuid.UUID = Depends(require_site_read),
    index=Depends(get_registry_index_read),
    influx=Depends(get_influx_gateway_read),
):
    measurement, bucket, signal_key, _sig = _resolve_target(index, influx.settings, stream, substream, signal_key, point_id)
    try:
        ftype = field_type(_sig.data_type)
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
                                        svc.flux_every(every), flux_fn, asset_id, source_id, point_id=point_id)
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
