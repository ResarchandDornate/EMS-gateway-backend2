import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.financials.models import RevenueConfigModel


class RevenueConfigRepository:
    """One row per gateway; the write is an upsert so a site keeps one tariff."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get(self, gateway_id: uuid.UUID) -> RevenueConfigModel | None:
        r = await self.session.execute(
            select(RevenueConfigModel).where(RevenueConfigModel.gateway_id == gateway_id)
        )
        return r.scalar_one_or_none()

    async def upsert(self, gateway_id: uuid.UUID, data: dict, updated_by: str | None) -> RevenueConfigModel:
        row = await self.get(gateway_id)
        if row is None:
            row = RevenueConfigModel(gateway_id=gateway_id, updated_by=updated_by, **data)
            self.session.add(row)
        else:
            for key, value in data.items():
                setattr(row, key, value)
            row.updated_by = updated_by
        await self.session.commit()
        await self.session.refresh(row)
        return row
