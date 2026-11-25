import uuid

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.router import get_current_user
from src.database import get_session
from src.financials.repository import RevenueConfigRepository
from src.financials.models import RevenueConfigModel
from src.financials.schemas import RevenueConfig, RevenueConfigResponse, TariffTemplate
from src.financials.templates import STATE_TARIFFS

router = APIRouter(prefix="/api/v1/financials", tags=["financials"])


def _to_schema(row: RevenueConfigModel) -> RevenueConfig:
    return RevenueConfig(
        state=row.state,
        charge_source=row.charge_source,
        charge_cost_rs=row.charge_cost_rs,
        replaced_source=row.replaced_source,
        discharge_cost_rs=row.discharge_cost_rs,
        base_energy_charge_rs=row.base_energy_charge_rs,
        solar_charge_value_rs=row.solar_charge_value_rs,
        slots=row.slots,
        updated_by=row.updated_by,
        updated_at=row.updated_at.isoformat() if row.updated_at else None,
    )


@router.get("/tariff-templates", response_model=list[TariffTemplate])
async def tariff_templates(_user=Depends(get_current_user)):
    """Every state's ToD tariff template — the dropdown's options and the values
    a site auto-fills from. Reference data, read-only."""
    return [TariffTemplate(**t) for t in STATE_TARIFFS]


@router.get("/revenue-config", response_model=RevenueConfigResponse)
async def get_revenue_config(
    gateway_id: uuid.UUID = Query(...),
    _user=Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):
    """The site's saved tariff, or null when nobody has filled it in yet."""
    row = await RevenueConfigRepository(session).get(gateway_id)
    return RevenueConfigResponse(config=_to_schema(row) if row else None)


@router.post("/revenue-config", response_model=RevenueConfigResponse)
async def save_revenue_config(
    body: RevenueConfig,
    gateway_id: uuid.UUID = Query(...),
    user=Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):
    """Save the site's tariff, stamped with who saved it. Shared by everyone on
    the gateway; the flat pair drives today's revenue figure and the ToD fields
    carry the schedule for the time-resolved calc to come."""
    data = body.model_dump(exclude={"updated_by", "updated_at"})
    row = await RevenueConfigRepository(session).upsert(gateway_id, data, updated_by=user.email)
    return RevenueConfigResponse(config=_to_schema(row))
