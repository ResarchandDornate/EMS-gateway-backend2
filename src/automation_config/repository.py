import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.automation_config.models import AutomationConfigModel


class AutomationConfigRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get(self, gateway_id: uuid.UUID) -> AutomationConfigModel | None:
        q = select(AutomationConfigModel).where(
            AutomationConfigModel.gateway_id == gateway_id,
            AutomationConfigModel.is_deleted.is_(False),
        )
        return (await self.session.execute(q)).scalar_one_or_none()

    async def upsert(
        self, gateway_id: uuid.UUID, config: dict[str, Any], updated_by: str | None
    ) -> AutomationConfigModel:
        row = await self.get(gateway_id)
        if row is None:
            row = AutomationConfigModel(gateway_id=gateway_id)
            self.session.add(row)
        row.config = config
        row.enabled = bool(config.get("enabled", False))
        row.updated_by = updated_by
        await self.session.commit()
        await self.session.refresh(row)
        return row
