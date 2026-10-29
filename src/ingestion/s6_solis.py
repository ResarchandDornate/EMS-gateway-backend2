import uuid

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.ingestion.schemas import (MAX_RECORDS, MAX_SEQUENCE,
                                    SUPPORTED_SCHEMA_VERSIONS)

S6_STREAM = "S6"


class S6SolisTelemetryRecord(BaseModel):
    """One Solis operational-telemetry snapshot (GET /api/solis/status shape). Stored verbatim.

    The frozen contract marks fields High priority but nullable, and the promoted-field model is
    disputed between the frozen sheet and S6_SOLIS_API.md, so this schema imposes NO required
    fields and NO shape beyond the one timestamp path the frozen contract defines (top-level
    timestamp_utc, str, optional here). Every other field (solis.*, controller_state, decision,
    power-limit chain, nested objects, nulls) is preserved verbatim via extra='allow'."""

    model_config = ConfigDict(extra="allow")

    timestamp_utc: str | None = None


class S6SolisEnvelopeIn(BaseModel):
    """S6 ingest envelope. Same identity wrapper as the other streams; records are Solis telemetry
    snapshots. S6 remains deferred at the endpoint; this is used by the persistence slice and its
    tests, not wired into routing."""

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
    records: list[S6SolisTelemetryRecord]
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
    def _is_s6(cls, v):
        if v != S6_STREAM:
            raise ValueError(f"S6 envelope requires stream=='S6', got {v!r}")
        return v

    @model_validator(mode="after")
    def _counts_match(self):
        if self.record_count != len(self.records):
            raise ValueError("record_count does not match number of records")
        return self
