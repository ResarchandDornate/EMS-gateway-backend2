import uuid

from pydantic import BaseModel, Field, field_validator, model_validator

from src.streams import KNOWN_STREAMS

SUPPORTED_SCHEMA_VERSIONS = {"1.0"}
MAX_RECORDS = 10_000
MAX_SEQUENCE = 2**63 - 1          # fits BigInteger column


class RecordIn(BaseModel):
    # PROVISIONAL keys - pending the gateway team's literal JSON contract. UNCHANGED.
    source_id: str = Field(max_length=128)
    asset_id: str = Field(max_length=128)
    signal_key: str = Field(max_length=128)
    value: float | int | str | bool | None
    quality: str | None = Field(default=None, max_length=32)
    timestamp: str = Field(max_length=64)   # ISO-8601; SEMANTIC validation deferred to 9b-ii


class EnvelopeIn(BaseModel):
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
    records: list[RecordIn]
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
    def _known_stream(cls, v):
        # unknown -> validation error (422). Deferred streams parse OK and are
        # routed to 501 by service.route_stream in 9b-ii.
        if v not in KNOWN_STREAMS:
            raise ValueError(f"unknown stream: {v!r}")
        return v

    @model_validator(mode="after")
    def _counts_match(self):
        if self.record_count != len(self.records):
            raise ValueError("record_count does not match number of records")
        return self


class IngestAck(BaseModel):
    status: str            # "stored" | "duplicate"
    message_id: uuid.UUID
    record_count: int
