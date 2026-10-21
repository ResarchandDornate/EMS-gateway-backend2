import uuid

from sqlalchemy import Float, ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.models import BaseEntityModel


class BessUnitModel(BaseEntityModel):
    __tablename__ = "bess_units"
    __table_args__ = (UniqueConstraint("gateway_id", "bess_key"),)

    gateway_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("gateways.id", ondelete="CASCADE"), index=True
    )
    bess_key: Mapped[str] = mapped_column(String(64))
    name: Mapped[str] = mapped_column(String(255))
    rated_kw: Mapped[float | None] = mapped_column(Float)
    rated_kwh: Mapped[float | None] = mapped_column(Float)
    chemistry: Mapped[str | None] = mapped_column(String(32))