from pydantic import BaseModel, Field


class TariffSlot(BaseModel):
    name: str
    start: str  # "HH:MM", 24 h local
    end: str
    # A slot carries either a percent of the base EC or a flat ₹/kWh, not both.
    modifier_pct: float | None = None
    modifier_rs: float | None = None


class TariffTemplate(BaseModel):
    state: str
    discom: str | None = None
    financial_year: str
    verified: bool
    base_energy_charge_rs: float
    solar_charge_value_rs: float
    slots: list[TariffSlot]
    note: str | None = None


class RevenueConfig(BaseModel):
    """A site's saved tariff. The flat pair is required; the ToD fields are
    optional until the time-resolved calc uses them."""

    state: str | None = None
    charge_source: str = "solar"
    charge_cost_rs: float = Field(ge=0)
    replaced_source: str = "grid"
    discharge_cost_rs: float = Field(ge=0)
    base_energy_charge_rs: float | None = Field(default=None, ge=0)
    solar_charge_value_rs: float | None = Field(default=None, ge=0)
    slots: list[TariffSlot] | None = None
    # Stamped by the server on write; ignored on input.
    updated_by: str | None = None
    updated_at: str | None = None


class RevenueConfigResponse(BaseModel):
    config: RevenueConfig | None
