from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from src.accounts import service
from src.accounts.schemas import (
    OperatorOut,
    OrganizationCreate,
    OrganizationDetail,
    OrganizationOut,
    SiteOut,
)
from src.database import get_session
from src.rbac.dependencies import require_permission

router = APIRouter(prefix="/api/v1/admin", tags=["accounts"])


@router.post("/organizations", response_model=OrganizationOut, status_code=201)
async def create_organization(
    data: OrganizationCreate,
    user=Depends(require_permission("org:manage")),
    session: AsyncSession = Depends(get_session),
):
    if await service.org_key_exists(session, data.org_key):
        raise HTTPException(status_code=409, detail="Organization key already exists")
    if await service.phone_exists(session, data.admin_phone):
        raise HTTPException(status_code=409, detail="Phone number already registered")

    org, admin, sites = await service.create_client(session, user.id, data)

    return OrganizationOut(
        id=org.id,
        org_key=org.org_key,
        name=org.name,
        admin_user_id=admin.id,
        admin_phone=admin.phone,
        sites=[SiteOut(id=s.id, site_key=s.site_key, name=s.name) for s in sites],
    )


@router.get("/organizations", response_model=list[OrganizationDetail])
async def list_organizations(
    user=Depends(require_permission("org:manage")),
    session: AsyncSession = Depends(get_session),
):
    orgs = await service.list_clients(session)
    result = []
    for org in orgs:
        sites = await service.get_sites(session, org.id)
        operators = await service.get_operators(session, org.id)
        result.append(OrganizationDetail(
            id=org.id,
            org_key=org.org_key,
            name=org.name,
            sites=[SiteOut(id=s.id, site_key=s.site_key, name=s.name) for s in sites],
            operators=[OperatorOut(id=o.id, phone=o.phone, email=o.email) for o in operators],
        ))
    return result
