"""S2 latest-value cache, written beside the Influx write on ingest.

The snapshot reads every signal's newest value from one Redis hash per
(site, substream), instead of a wide per-field Influx query that does not scale
to the BMS substream. Fail-open: any error is swallowed and ingestion is never
affected -- the Influx write is the source of truth and has already succeeded
when write_latest runs.
"""
from __future__ import annotations

import json

from src.config import get_settings

_client = None
_off = False


def _redis():
    global _client, _off
    if _off:
        return None
    if _client is None:
        try:
            from redis.asyncio import Redis
            url = getattr(get_settings(), "redis_url", None)
            if not url:
                _off = True
                return None
            _client = Redis.from_url(url)
        except Exception:
            _off = True
            return None
    return _client


def _key(site: str, substream: str) -> str:
    return f"s2latest:{site}:{substream}"


async def write_latest(identity: dict, envelope) -> None:
    r = _redis()
    if r is None:
        return
    try:
        site = str(identity["site_id"])
        mapping = {}
        for rec in envelope.records:
            if not isinstance(rec, dict):
                continue
            sig = rec.get("signal")
            val = rec.get("value")
            if not isinstance(sig, str) or not sig or val is None:
                continue
            asset = rec.get("runtime_asset_id") or rec.get("base_asset_id")
            src = rec.get("source_id")
            t = rec.get("source_updated_utc") or rec.get("observed_at_utc") or envelope.created_at_utc
            mapping[f"{asset}|{src}|{sig}"] = json.dumps(
                {"v": val, "q": rec.get("quality"), "t": str(t) if t else None})
        if mapping:
            k = _key(site, envelope.substream)
            await r.hset(k, mapping=mapping)
            await r.expire(k, 3600)
    except Exception:
        return


async def read_latest(site: str, substream: str):
    """[(asset, source, signal, value, quality, time)] from the hash, or None when empty."""
    r = _redis()
    if r is None:
        return None
    try:
        raw = await r.hgetall(_key(site, substream))
        if not raw:
            return None
        out = []
        for f, v in raw.items():
            field = f.decode() if isinstance(f, (bytes, bytearray)) else f
            parts = field.split("|", 2)
            if len(parts) != 3:
                continue
            asset, src, sig = parts
            try:
                d = json.loads(v)
            except Exception:
                continue
            out.append((None if asset == "None" else asset,
                        None if src == "None" else src,
                        sig, d.get("v"), d.get("q"), d.get("t")))
        return out or None
    except Exception:
        return None
