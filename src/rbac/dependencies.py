import uuid

from fastapi import Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.router import get_current_user
from src.database import get_session
from src.rbac.service import get_gateway_site, get_user_permissions, get_user_sites


def require_permission(permission):
    async def check(
        user=Depends(get_current_user),
        session: AsyncSession = Depends(get_session),
    ):
        permissions = await get_user_permissions(session, user)
        if permission not in permissions:
            raise HTTPException(status_code=403, detail="Permission denied")
        return user

    return check


async def require_site_access(
    gateway_id: uuid.UUID,
    user=Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):
    gateway = await get_gateway_site(session, gateway_id)
    if gateway is None:
        raise HTTPException(status_code=404, detail="Gateway not found")

    site_id = gateway[0]
    sites = await get_user_sites(session, user)

    if sites is not None and site_id not in sites:
        raise HTTPException(status_code=403, detail="Site access denied")

    return site_id