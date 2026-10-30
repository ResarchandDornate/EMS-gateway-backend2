import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from src.accounts import service
from src.accounts.schemas import OperatorCreate, OperatorDetail
from src.database import get_session
from src.rbac.dependencies import require_permission

router = APIRouter(prefix="/api/v1/org", tags=["operators"])


def _require_org(user):
    if user.organization_id is None:
        raise HTTPException(status_code=403, detail="Only an organization admin can manage operators")


@router.post("/operators", response_model=OperatorDetail, status_code=201)
async def create_operator(
    data: OperatorCreate,
    user=Depends(require_permission("user:manage")),
    session: AsyncSession = Depends(get_session),
):
    _require_org(user)
    operator, error = await service.create_operator(session, user, data)
    if error == "limit":
        raise HTTPException(status_code=409, detail="Maximum of 2 active operators reached")
    if error == "bad_site":
        raise HTTPException(status_code=400, detail="One or more sites do not belong to your organization")
    if error == "phone":
        raise HTTPException(status_code=409, detail="Phone number already registered")
    if error == "email":
        raise HTTPException(status_code=409, detail="Email already registered")

    site_ids = await service.operator_site_ids(session, operator.id)
    return OperatorDetail(
        id=operator.id, phone=operator.phone, email=operator.email,
        is_active=operator.is_active, site_ids=site_ids,
    )


@router.get("/operators", response_model=list[OperatorDetail])
async def list_operators(
    user=Depends(require_permission("user:manage")),
    session: AsyncSession = Depends(get_session),
):
    _require_org(user)
    operators = await service.get_operators(session, user.organization_id)
    result = []
    for op in operators:
        site_ids = await service.operator_site_ids(session, op.id)
        result.append(OperatorDetail(
            id=op.id, phone=op.phone, email=op.email,
            is_active=op.is_active, site_ids=site_ids,
        ))
    return result


@router.post("/operators/{operator_id}/deactivate")
async def deactivate_operator(
    operator_id: uuid.UUID,
    user=Depends(require_permission("user:manage")),
    session: AsyncSession = Depends(get_session),
):
    _require_org(user)
    error = await service.deactivate_operator(session, user, operator_id)
    if error == "not_found":
        raise HTTPException(status_code=404, detail="Operator not found")
    if error == "already_inactive":
        raise HTTPException(status_code=409, detail="Operator is already inactive")
    return {"status": "deactivated"}