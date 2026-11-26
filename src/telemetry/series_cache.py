"""Best-effort read-through cache for /telemetry/series.

Same contract as snapshot_cache: fail-open and additive. Any Redis/serialization
problem behaves as a MISS, so the endpoint always falls back to its InfluxDB query.
Keyed on the request's own parameters (raw start/stop strings, so a window with no
explicit stop stays a stable key across refetches), site-scoped, short TTL.
"""
from __future__ import annotations
import json
from src.config import get_settings

_client = None
_disabled = False
TTL_SECONDS = 20


def _redis():
    global _client, _disabled
    if _disabled:
        return None
    if _client is None:
        try:
            from redis.asyncio import Redis
            url = getattr(get_settings(), "redis_url", None)
            if not url:
                _disabled = True
                return None
            _client = Redis.from_url(url)
        except Exception:
            _disabled = True
            return None
    return _client


def key(site, stream, substream, signal_key, asset_id, source_id, point_id, start, stop, limit) -> str:
    parts = [site, stream, substream, signal_key or "", asset_id or "",
             source_id or "", point_id or "", start or "", stop or "", str(limit)]
    return "series:" + ":".join(str(p) for p in parts)


async def get_cached(k: str):
    r = _redis()
    if r is None:
        return None
    try:
        raw = await r.get(k)
        return json.loads(raw) if raw else None
    except Exception:
        return None


async def set_cached(k: str, payload: dict) -> None:
    r = _redis()
    if r is None:
        return
    try:
        await r.set(k, json.dumps(payload), ex=TTL_SECONDS)
    except Exception:
        return
