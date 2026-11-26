import uuid

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import get_session
from src.rbac.dependencies import require_permission
from src.site_profile.repository import SiteProfileRepository
from src.site_profile.schemas import SiteProfileIn, SiteProfileOut, SiteProfileResponse

# site:manage is held by super_admin only, so every route here is super-admin gated.
_super_admin = require_permission("site:manage")

router = APIRouter(prefix="/api/v1/site-profiles", tags=["site-profiles"])


@router.get("", response_model=list[SiteProfileOut])
async def list_site_profiles(
    _user=Depends(_super_admin),
    session: AsyncSession = Depends(get_session),
):
    """All registered site profiles for the Site Config table."""
    rows = await SiteProfileRepository(session).list_all()
    return [SiteProfileOut.model_validate(r) for r in rows]


@router.get("/{site_id}", response_model=SiteProfileResponse)
async def get_site_profile(
    site_id: uuid.UUID,
    _user=Depends(_super_admin),
    session: AsyncSession = Depends(get_session),
):
    row = await SiteProfileRepository(session).get(site_id)
    return SiteProfileResponse(profile=SiteProfileOut.model_validate(row) if row else None)


@router.post("/{site_id}", response_model=SiteProfileResponse)
async def save_site_profile(
    site_id: uuid.UUID,
    body: SiteProfileIn,
    user=Depends(_super_admin),
    session: AsyncSession = Depends(get_session),
):
    repo = SiteProfileRepository(session)
    row = await repo.upsert(site_id, body.model_dump(), updated_by=user.email)
    return SiteProfileResponse(profile=SiteProfileOut.model_validate(row))
