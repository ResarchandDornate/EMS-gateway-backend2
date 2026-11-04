"""Read-only authorization for the monitoring/dashboard endpoints.

Reuses the existing operator auth (get_current_user via require_permission) and the
same DB-derived site scope as the telemetry reads. Returns the caller's authorized
site set (None = super_admin / unrestricted); the service layer scopes every query to
it, so tenant isolation is identical to the rest of the read surface. No new permission
is introduced — the already-seeded 'dashboard:view' permission gates these endpoints.
"""
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import get_session
from src.rbac.dependencies import require_permission
from src.rbac.service import get_user_sites

MONITOR_PERMISSION = "dashboard:view"


async def require_monitor_scope(
    user=Depends(require_permission(MONITOR_PERMISSION)),   # 401 unauth, 403 missing perm
    session: AsyncSession = Depends(get_session),
):
    """Return (sites, session). `sites` is None for super_admin (all sites) or a set of
    authorized site UUIDs otherwise; an empty set means the caller has no site access."""
    sites = await get_user_sites(session, user)
    return sites, session
