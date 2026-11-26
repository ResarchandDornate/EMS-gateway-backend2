import uuid
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.router import get_current_user
from src.automation_config.repository import AutomationConfigRepository
from src.automation_config.schemas import AutomationConfigOut
from src.database import get_session
from src.rbac.service import get_gateway_site, get_user_permissions, get_user_sites

router = APIRouter(prefix="/api/v1/automation-config", tags=["automation-config"])


async def _authorize(session: AsyncSession, user, gateway_id: uuid.UUID) -> None:
    """Same reach as a pair-control command: the gateway must resolve to a site the caller can
    see, and the caller must hold command:write."""
    gw = await get_gateway_site(session, gateway_id)
    if gw is None:
        raise HTTPException(404, "Gateway not found")
    site_id, _ = gw
    sites = await get_user_sites(session, user)                 # None = super_admin (all sites)
    if sites is not None and site_id not in sites:
        raise HTTPException(403, "Site access denied")
    if "command:write" not in await get_user_permissions(session, user):
        raise HTTPException(403, "This role may not set automation")


@router.get("", response_model=AutomationConfigOut)
async def get_automation_config(
    gateway_id: uuid.UUID = Query(...),
    user=Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):
    await _authorize(session, user, gateway_id)
    row = await AutomationConfigRepository(session).get(gateway_id)
    if row is None:
        return AutomationConfigOut(gateway_id=gateway_id, enabled=False, config={})
    return AutomationConfigOut.model_validate(row)


@router.post("", response_model=AutomationConfigOut)
async def save_automation_config(
    gateway_id: uuid.UUID = Query(...),
    config: dict[str, Any] = Body(...),
    user=Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):
    await _authorize(session, user, gateway_id)
    row = await AutomationConfigRepository(session).upsert(gateway_id, config, updated_by=user.email)
    return AutomationConfigOut.model_validate(row)
