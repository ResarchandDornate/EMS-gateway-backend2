import uuid

from pydantic import BaseModel, Field, field_validator, model_validator

from src.ingestion.schemas import (MAX_RECORDS, MAX_SEQUENCE,
                                    SUPPORTED_SCHEMA_VERSIONS)

# Northbound streams stored verbatim through the generic path (S1 has its own slice).
GENERIC_STREAMS = frozenset({"S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9", "S10"})


class GenericStreamEnvelopeIn(BaseModel):
    """Envelope for the verbatim structured streams S2..S10. Records are preserved as
    list[dict]; nothing is coerced or validated per-signal at ingest."""

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
    def _known(cls, v):
        if v not in GENERIC_STREAMS:
            raise ValueError(f"stream {v!r} not in generic store set")
        return v

    @model_validator(mode="after")
    def _counts_match(self):
        if self.record_count != len(self.records):
            raise ValueError("record_count does not match number of records")
        return self
