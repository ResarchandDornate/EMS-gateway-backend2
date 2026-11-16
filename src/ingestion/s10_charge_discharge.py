import uuid

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.ingestion.schemas import (MAX_RECORDS, MAX_SEQUENCE,
                                    SUPPORTED_SCHEMA_VERSIONS)

S10_STREAM = "S10"


class S10PairStatusRecord(BaseModel):
    """One pair charge/discharge-control status record. Promoted leaves are typed and nullable
    (the contract marks the per-pair values as nullable JSON); every other field is preserved
    verbatim via extra='allow'. No strict required-field list is imposed."""

    model_config = ConfigDict(extra="allow")

    timestamp_utc: str | None = None
    pair_id: int | str | None = None
    stage: str | None = None
    run_status: str | None = None
    direction: str | None = None
    requested_power_kw: float | None = None
    commanded_power_kw: float | None = None
    actual_power_kw: float | None = None
    rack_voltage_v: float | None = None
    rack_soc_percent: float | None = None
    available: bool | None = None
    battery_ready: bool | None = None
    pcs_running: bool | None = None
    hard_blocked: bool | None = None
    platform_ready: bool | None = None


class S10ChargeDischargeEnvelopeIn(BaseModel):
    """S10 ingest envelope (battery_charge_discharge_control / pair_controller). Same identity
    wrapper as the other streams; records are per-pair status snapshots."""

    schema_version: str = Field(max_length=16)
    message_id: uuid.UUID
    gateway_id: str = Field(max_length=64)
    site_id: str = Field(max_length=64)
    organization_id: str = Field(max_length=64)
    stream: str = Field(max_length=8)
    substream: str = Field(max_length=64)
    sequence: int = Field(ge=0, le=MAX_SEQUENCE)
    created_at_utc: str = Field(max_length=64)
    record_count: int = Field(ge=0, le=MAX_RECORDS)
    records: list[S10PairStatusRecord]
    compression: str | None = Field(default=None, max_length=32)
    gateway_software_version: str | None = Field(default=None, max_length=64)

    @field_validator("schema_version")
    @classmethod
    def _supported_version(cls, v):
        if v not in SUPPORTED_SCHEMA_VERSIONS:
            raise ValueError(f"unsupported schema_version: {v!r}")
        return v

    @field_validator("stream")
    @classmethod
    def _is_s10(cls, v):
        if v != S10_STREAM:
            raise ValueError(f"S10 envelope requires stream=='S10', got {v!r}")
        return v

    @model_validator(mode="after")
    def _counts_match(self):
        if self.record_count != len(self.records):
            raise ValueError("record_count does not match number of records")
        return self
