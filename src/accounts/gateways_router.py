import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from src.accounts import service
from src.accounts.schemas import GatewayCreate, GatewayCreated, GatewayOut
from src.database import get_session
from src.rbac.dependencies import require_permission
from src.rbac.service import get_user_sites

router = APIRouter(prefix="/api/v1/admin", tags=["gateways"])


async def _check_site_scope(session, user, site_id):
    site = await service.get_site(session, site_id)
    if site is None:
        raise HTTPException(status_code=404, detail="Site not found")
    sites = await get_user_sites(session, user)         # None = super admin (all sites)
    if sites is not None and site_id not in sites:
        raise HTTPException(status_code=403, detail="Site access denied")
    return site


@router.post("/sites/{site_id}/gateways", response_model=GatewayCreated, status_code=201)
async def register_gateway(
    site_id: uuid.UUID,
    data: GatewayCreate,
    user=Depends(require_permission("site:manage")),
    session: AsyncSession = Depends(get_session),
):
    site = await _check_site_scope(session, user, site_id)
    gateway, secret, error = await service.register_gateway(session, user, site, data)
    if error == "duplicate":
        raise HTTPException(status_code=409, detail="Gateway key already exists")
    return GatewayCreated(
        id=gateway.id, site_id=gateway.site_id, gateway_key=gateway.gateway_key,
        client_id=gateway.client_id, enabled=gateway.enabled, client_secret=secret,
    )


@router.get("/sites/{site_id}/gateways", response_model=list[GatewayOut])
async def list_gateways(
    site_id: uuid.UUID,
    user=Depends(require_permission("site:manage")),
    session: AsyncSession = Depends(get_session),
):
    await _check_site_scope(session, user, site_id)
    gateways = await service.list_gateways(session, site_id)
    return [
        GatewayOut(id=g.id, site_id=g.site_id, gateway_key=g.gateway_key,
                   client_id=g.client_id, enabled=g.enabled, last_seen_at=g.last_seen_at)
        for g in gateways
    ]