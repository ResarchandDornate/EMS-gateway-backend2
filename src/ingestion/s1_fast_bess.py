import uuid

from pydantic import BaseModel, Field, field_validator, model_validator

from src.ingestion.schemas import (MAX_RECORDS, MAX_SEQUENCE,
                                    SUPPORTED_SCHEMA_VERSIONS)

S1_STREAM = "S1"


class S1FastBessEnvelopeIn(BaseModel):
    """S1 fast_bess ingest envelope (structured JSONB path).

    Records are preserved VERBATIM as JSON objects (list[dict]). The real producer emits
    nested per-signal objects with mixed value types; nothing is coerced or dropped at
    ingest. Identity fields mirror the common envelope; `priority` is accepted as an
    additive transport hint (the Northbound outbox includes it)."""

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
    records: list[dict]
    compression: str | None = Field(default=None, max_length=32)
    gateway_software_version: str | None = Field(default=None, max_length=64)
    priority: str | None = Field(default=None, max_length=8)

    @field_validator("schema_version")
    @classmethod
    def _supported_version(cls, v):
        if v not in SUPPORTED_SCHEMA_VERSIONS:
            raise ValueError(f"unsupported schema_version: {v!r}")
        return v

    @field_validator("stream")
    @classmethod
    def _is_s1(cls, v):
        if v != S1_STREAM:
            raise ValueError(f"S1 envelope requires stream=='S1', got {v!r}")
        return v

    @model_validator(mode="after")
    def _counts_match(self):
        if self.record_count != len(self.records):
            raise ValueError("record_count does not match number of records")
        return self
