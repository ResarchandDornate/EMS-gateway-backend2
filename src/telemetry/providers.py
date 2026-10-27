"""Phase 10 read-only providers — UNGATED (work while INGEST_ENABLED=False).

These deliberately do NOT depend on require_ingest_enabled. They reuse the shared
registry-index cache (load_cached_index) and the existing InfluxDB configuration.
"""
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import get_settings
from src.database import get_session
from src.influx import InfluxGateway, RegistryIndex
from src.ingestion.providers import load_cached_index


async def get_registry_index_read(session: AsyncSession = Depends(get_session)) -> RegistryIndex:
    return await load_cached_index(session)


def get_influx_gateway_read() -> InfluxGateway:
    return InfluxGateway(get_settings())
