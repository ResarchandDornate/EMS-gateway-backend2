from datetime import datetime, timezone

from sqlalchemy import select

from src.tenancy.gateways.models import GatewayModel
from src.tenancy.sites.models import SiteModel


async def get_gateway_by_client_id(session, client_id):
    result = await session.execute(
        select(GatewayModel).where(GatewayModel.client_id == client_id)
    )
    return result.scalar_one_or_none()


async def get_gateway_by_id(session, gateway_id):
    result = await session.execute(
        select(GatewayModel).where(GatewayModel.id == gateway_id)
    )
    return result.scalar_one_or_none()


async def resolve_gateway_identity(session, gw):
    """gateway -> site -> organization (DB-resolved, authoritative)."""
    org_id = (await session.execute(
        select(SiteModel.organization_id).where(SiteModel.id == gw.site_id)
    )).scalar_one_or_none()
    return {"gateway_id": gw.id, "site_id": gw.site_id, "organization_id": org_id}


async def mark_gateway_seen(session, gw, *, when=None):
    """Record a SUCCESSFUL gateway token issuance as the interim 'last seen' signal.

    This is the time credentials were last accepted and a token was issued - NOT the
    last telemetry timestamp and NOT a guarantee of live connectivity. Sets the column
    only (timezone-aware UTC) and flushes; the CALLER owns the commit, so the write
    participates in the request transaction and is discarded if that transaction rolls
    back. Call only after credentials are validated and the gateway is enabled.
    """
    gw.last_seen_at = when or datetime.now(timezone.utc)
    await session.flush()
    return gw
