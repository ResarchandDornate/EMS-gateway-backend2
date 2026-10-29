import uuid

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.ingestion.schemas import (MAX_RECORDS, MAX_SEQUENCE,
                                    SUPPORTED_SCHEMA_VERSIONS)

S5_STREAM = "S5"


class ControllerBlock(BaseModel):
    """Nested `controller` object. Only the promoted leaves are typed (str, forward-compatible
    per the contract); all other controller.* fields are preserved verbatim via extra='allow'."""

    model_config = ConfigDict(extra="allow")

    state: str | None = None
    previous_state: str | None = None
    decision: str | None = None


class S5ControllerStateRecord(BaseModel):
    """One SOC-controller state snapshot (GET /api/controller/status shape). The contract marks
    these fields High priority but nullable ('Any of the string fields and healthy may be null'),
    and controller state/decision are explicitly strings for forward compatibility. Validation is
    therefore permissive: promoted-source fields are typed+nullable, every other field (thresholds,
    bess[X/Y], solis, action_plan, last_actions, ...) is preserved verbatim via extra='allow'. No
    strict required-field list is imposed beyond what the contract supports."""

    model_config = ConfigDict(extra="allow")

    timestamp_utc: str | None = None
    mode: str | None = None
    version: str | None = None
    healthy: bool | None = None
    controller: ControllerBlock | None = None


class S5ControllerEnvelopeIn(BaseModel):
    """S5 ingest envelope. Same identity wrapper as the other streams; records are controller
    state snapshots. (S5 remains deferred at the endpoint; this is used by the persistence slice
    and its tests, not yet wired into routing.)"""

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
    records: list[S5ControllerStateRecord]
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
    def _is_s5(cls, v):
        if v != S5_STREAM:
            raise ValueError(f"S5 envelope requires stream=='S5', got {v!r}")
        return v

    @model_validator(mode="after")
    def _counts_match(self):
        if self.record_count != len(self.records):
            raise ValueError("record_count does not match number of records")
        return self
