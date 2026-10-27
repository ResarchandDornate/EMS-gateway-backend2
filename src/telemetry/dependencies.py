"""Phase 10 user-facing, site-ID-keyed read authorization (NOT gateway-token auth)."""
import uuid

from fastapi import Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import get_session
from src.rbac.dependencies import require_permission
from src.rbac.service import get_user_sites


async def require_site_read(
    site_id: uuid.UUID = Query(..., description="Target site; must be within the caller's scope"),
    user=Depends(require_permission("telemetry:read")),   # get_current_user -> 401; perm -> 403
    session: AsyncSession = Depends(get_session),
) -> uuid.UUID:
    """Return the authorized site_id, or 403. Site scope is recomputed from the DB every
    request. super_admin -> unrestricted; org_admin -> org's sites; site_operator -> assigned.
    Returns 403 for both unknown and unauthorized site_ids (no existence leak); fails closed."""
    sites = await get_user_sites(session, user)
    if sites is None:                 # super_admin: unrestricted
        return site_id
    if site_id in sites:
        return site_id
    raise HTTPException(status.HTTP_403_FORBIDDEN, "Site access denied")
