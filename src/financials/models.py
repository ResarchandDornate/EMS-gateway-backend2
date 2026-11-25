import uuid

from sqlalchemy import Float, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.models import BaseEntityModel


class RevenueConfigModel(BaseEntityModel):
    """One site's revenue tariff, keyed by gateway and shared by everyone on it.

    Replaces the frontend's per-gateway JSON doc (the old /api/revenue-master):
    the flat charge/discharge pair stays for the current revenue figure, and the
    ToD fields (state, base EC, solar value, slots) carry the schedule a
    time-resolved revenue calc will read once it is built.
    """

    __tablename__ = "revenue_config"
    __table_args__ = (UniqueConstraint("gateway_id"),)

    gateway_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    state: Mapped[str | None] = mapped_column(String(64))
    charge_source: Mapped[str] = mapped_column(String(16), default="solar", server_default="solar")
    charge_cost_rs: Mapped[float] = mapped_column(Float)
    replaced_source: Mapped[str] = mapped_column(String(16), default="grid", server_default="grid")
    discharge_cost_rs: Mapped[float] = mapped_column(Float)
    base_energy_charge_rs: Mapped[float | None] = mapped_column(Float)
    solar_charge_value_rs: Mapped[float | None] = mapped_column(Float)
    # The ToD slots as the frontend sends them: [{name,start,end,modifier_pct|modifier_rs}].
    slots: Mapped[list | None] = mapped_column(JSONB)
    updated_by: Mapped[str | None] = mapped_column(String(128))
