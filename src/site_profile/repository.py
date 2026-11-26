import uuid
from collections.abc import Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.site_profile.models import SiteProfileModel

_WRITABLE = (
    "company_name", "address", "latitude", "longitude",
    "solar_capacity_kw", "connection_voltage_kv", "battery_capacity_kwh", "pcs_capacity_kw",
    "state", "pincode",
    "comm_pcs_ems", "comm_pcs_battery", "comm_battery_ems",
    "comm_ems_meter_ems", "comm_solar_inverter_ems",
)


class SiteProfileRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get(self, site_id: uuid.UUID) -> SiteProfileModel | None:
        q = select(SiteProfileModel).where(
            SiteProfileModel.site_id == site_id, SiteProfileModel.is_deleted.is_(False)
        )
        return (await self.session.execute(q)).scalar_one_or_none()

    async def list_all(self) -> Sequence[SiteProfileModel]:
        q = select(SiteProfileModel).where(SiteProfileModel.is_deleted.is_(False))
        return (await self.session.execute(q)).scalars().all()

    async def upsert(
        self, site_id: uuid.UUID, values: dict, updated_by: str | None
    ) -> SiteProfileModel:
        row = await self.get(site_id)
        if row is None:
            row = SiteProfileModel(site_id=site_id)
            self.session.add(row)
        for field in _WRITABLE:
            if field in values:
                setattr(row, field, values[field])
        row.updated_by = updated_by
        await self.session.commit()
        await self.session.refresh(row)
        return row
