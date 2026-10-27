"""Phase 9b-ii-c dependency providers: the ingestion gate, a per-process registry-index
cache keyed by database identity, and the InfluxGateway provider.

Registry index: IMMUTABLE for the process lifetime once built. Registry seed/schema
changes are NOT observed at runtime and require a process restart before ingestion is
re-enabled. The cache is keyed by the session's database identity (host, port, db) so an
index built from one database can never be served to a session bound to another (prod runs
one registry DB per process; tests override get_registry_index and reset the cache).
"""
from fastapi import Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import Settings, get_settings
from src.database import get_session
from src.gateway.dependencies import get_current_gateway
from src.influx import InfluxGateway, RegistryIndex
from src.signals.service import load_registry_index

_registry_cache: dict = {}


def _db_identity(session: AsyncSession):
    url = session.get_bind().url
    return (url.host, url.port, url.database)


def reset_registry_cache() -> None:
    """Clear the process-global index cache (used by tests; not a runtime refresh)."""
    _registry_cache.clear()


async def require_ingest_enabled(
    gw=Depends(get_current_gateway),                      # auth first: 401/403
    settings: Settings = Depends(get_settings),
):
    if not settings.ingest_enabled:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Telemetry ingestion is not yet enabled",
        )
    return gw


async def load_cached_index(session: AsyncSession) -> RegistryIndex:
    """Shared, UNGATED registry-index cache (keyed by DB identity). Reused by the
    ingestion gate's provider and by the read-only telemetry providers. Failures are
    NOT cached; the index is immutable per process (restart to refresh)."""
    key = _db_identity(session)
    idx = _registry_cache.get(key)
    if idx is None:
        idx = await load_registry_index(session)          # failures are NOT cached
        _registry_cache[key] = idx
    return idx


async def get_registry_index(
    _gate=Depends(require_ingest_enabled),                # not built when the gate is off
    session: AsyncSession = Depends(get_session),
) -> RegistryIndex:
    return await load_cached_index(session)


def get_influx_gateway(_gate=Depends(require_ingest_enabled)) -> InfluxGateway:
    return InfluxGateway(get_settings())
