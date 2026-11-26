import uuid

from sqlalchemy import Boolean, Float, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.models import BaseEntityModel


class SiteProfileModel(BaseEntityModel):
    """Registration detail captured on the Site Config page. One row per site, written only by
    super admins; additive to the tenancy tables and not read by any existing flow."""

    __tablename__ = "site_profiles"
    __table_args__ = (UniqueConstraint("site_id", name="uq_site_profile_site"),)

    site_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    company_name: Mapped[str | None] = mapped_column(String(255))
    address: Mapped[str | None] = mapped_column(Text)
    latitude: Mapped[float | None] = mapped_column(Float)
    longitude: Mapped[float | None] = mapped_column(Float)
    solar_capacity_kw: Mapped[float | None] = mapped_column(Float)
    connection_voltage_kv: Mapped[float | None] = mapped_column(Float)
    battery_capacity_kwh: Mapped[float | None] = mapped_column(Float)
    pcs_capacity_kw: Mapped[float | None] = mapped_column(Float)
    comm_pcs_ems: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    comm_pcs_battery: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    comm_battery_ems: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    comm_ems_meter_ems: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    comm_solar_inverter_ems: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    updated_by: Mapped[str | None] = mapped_column(String(128))
