import uuid
from typing import Any

from sqlalchemy import Boolean, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.models import BaseEntityModel


class AutomationConfigModel(BaseEntityModel):
    """The scheduled charge/discharge automation blob for one gateway, set from the Strategy →
    Automation editor. Additive: the gateway polls it; no existing flow reads or writes it."""

    __tablename__ = "automation_configs"
    __table_args__ = (UniqueConstraint("gateway_id", name="uq_automation_config_gateway"),)

    gateway_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    config: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, server_default="{}")
    updated_by: Mapped[str | None] = mapped_column(String(128))
