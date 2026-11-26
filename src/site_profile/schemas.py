import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict


class SiteProfileIn(BaseModel):
    company_name: str | None = None
    address: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    solar_capacity_kw: float | None = None
    connection_voltage_kv: float | None = None
    battery_capacity_kwh: float | None = None
    pcs_capacity_kw: float | None = None
    comm_pcs_ems: bool = False
    comm_pcs_battery: bool = False
    comm_battery_ems: bool = False
    comm_ems_meter_ems: bool = False
    comm_solar_inverter_ems: bool = False


class SiteProfileOut(SiteProfileIn):
    model_config = ConfigDict(from_attributes=True)

    site_id: uuid.UUID
    updated_by: str | None = None
    updated_at: datetime | None = None


class SiteProfileResponse(BaseModel):
    profile: SiteProfileOut | None = None
