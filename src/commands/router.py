"""Pair-control command endpoints. POST is the UI downlink (user auth + role rule);
GET /pending is the gateway's poll (gateway auth). Results come back on S9, not here."""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.router import get_current_user
from src.commands import service
from src.commands.schemas import CommandOut, CreateCommandIn, PendingBatch
from src.database import get_session
from src.gateway.dependencies import get_current_gateway
from src.rbac.service import get_gateway_site, get_user_roles, get_user_sites

router = APIRouter(prefix="/api/v1/commands", tags=["commands"])


@router.post("", response_model=CommandOut, status_code=201)
async def create_command(body: CreateCommandIn, user=Depends(get_current_user),
                         session: AsyncSession = Depends(get_session)):
    gw = await get_gateway_site(session, body.gateway_id)       # (site_id, organization_id)
    if gw is None:
        raise HTTPException(404, "Gateway not found")
    site_id, organization_id = gw

    sites = await get_user_sites(session, user)                 # None = super_admin (all sites)
    if sites is not None and site_id not in sites:
        raise HTTPException(403, "Site access denied")

    role = service.effective_role(await get_user_roles(session, user.id))
    if role is None or not service.may_send(role, body.command_type):
        raise HTTPException(403, "This role may not send this command")

    return await service.create_command(
        session, gateway_id=body.gateway_id, organization_id=organization_id, site_id=site_id,
        command_type=body.command_type, arguments=body.arguments,
        requires_readback=body.requires_readback, requested_by=user.id, requested_role=role,
    )


@router.get("/pending", response_model=PendingBatch)
async def pending(gateway_id: str = Query(...), limit: int = Query(20, ge=1, le=100),
                  gw=Depends(get_current_gateway), session: AsyncSession = Depends(get_session)):
    # The gateway polls by its contract key, not our UUID. The token still decides whose
    # queue it is, so cross-check the asked key against the authenticated gateway.
    if gw.gateway_key != gateway_id:
        raise HTTPException(403, "Gateway mismatch")
    site_key = await service.site_key_of(session, gw.site_id)
    rows = await service.poll_pending(session, gateway_id=gw.id, limit=limit)
    return {"commands": [service.to_pending(r, gw.gateway_key, site_key) for r in rows]}
