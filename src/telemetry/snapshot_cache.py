"""Best-effort read-through cache for /telemetry/snapshot.

Fail-open and additive: every function swallows errors and behaves as a cache
MISS on any problem (Redis down/misconfigured, serialization, etc.), so the
snapshot endpoint always falls back to its existing InfluxDB query. It never
touches ingestion or stored data and never raises into the request path.
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


def _key(site: str, stream: str, substream: str) -> str:
    return f"snap:{site}:{stream}:{substream}"


async def get_cached(site: str, stream: str, substream: str):
    r = _redis()
    if r is None:
        return None
    try:
        raw = await r.get(_key(site, stream, substream))
        return json.loads(raw) if raw else None
    except Exception:
        return None


async def set_cached(site: str, stream: str, substream: str, points: list) -> None:
    r = _redis()
    if r is None:
        return
    try:
        await r.set(_key(site, stream, substream), json.dumps(points), ex=TTL_SECONDS)
    except Exception:
        return
