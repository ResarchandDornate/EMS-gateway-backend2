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
