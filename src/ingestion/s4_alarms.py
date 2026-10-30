import uuid
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.ingestion.schemas import (MAX_RECORDS, MAX_SEQUENCE,
                                    SUPPORTED_SCHEMA_VERSIONS)

S4_STREAM = "S4"


class Severity(str, Enum):
    CRITICAL = "Critical"
    HIGH = "High"
    WARNING = "Warning"
    INFO = "Info"


class EventState(str, Enum):
    ACTIVE = "ACTIVE"
    CLEARED = "CLEARED"
    INFO = "INFO"


class S4AlarmEventRecord(BaseModel):
    """One alarm/event record from the frozen Northbound v1.4 S4 contract. Required fields
    (Requirement='Required') are present + strictly typed; severity/state validated against
    the contract's enums. source_id is Required but nullable (key present, value may be null).
    asset_id/signal_name are 'Required where device/signal' -> optional at schema level.
    Any extra context key (payload, previous_value, current_value, etc.) preserved verbatim."""

    # Strict by default so contract scalar types (str/int) are never silently coerced.
    # event_id (UUID) and severity/state (enums) arrive as JSON strings on the wire, so those
    # three fields opt out of strict via Field(strict=False): str->UUID parses and enum
    # membership is still enforced (invalid values rejected). All other fields stay strict.
    # extra="allow" preserves every unlisted context key (payload, previous/current_value...).
    model_config = ConfigDict(extra="allow", strict=True)

    event_id: uuid.UUID = Field(strict=False)
    timestamp_utc: str
    gateway_id: str
    site_id: str
    source_id: str | None
    event_type: str
    severity: Severity = Field(strict=False)
    state: EventState = Field(strict=False)
    message: str
    dedupe_key: str
    # recommended / optional (strictly typed where provided)
    correlation_id: str | None = None
    asset_id: str | None = None
    signal_name: str | None = None
    occurrence_count: int | None = None
    cleared_at_utc: str | None = None


class S4AlarmEnvelopeIn(BaseModel):
    """S4 ingest envelope. Same identity wrapper as the other streams; records are events."""

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
    records: list[S4AlarmEventRecord]
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
    def _is_s4(cls, v):
        if v != S4_STREAM:
            raise ValueError(f"S4 envelope requires stream=='S4', got {v!r}")
        return v

    @model_validator(mode="after")
    def _counts_match(self):
        if self.record_count != len(self.records):
            raise ValueError("record_count does not match number of records")
        return self
