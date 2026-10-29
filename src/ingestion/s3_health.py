import uuid
from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.ingestion.schemas import (MAX_RECORDS, MAX_SEQUENCE,
                                    SUPPORTED_SCHEMA_VERSIONS)
from src.telemetry_errors import InvalidValue

S3_STREAM = "S3"


class S3GatewayHealthRecord(BaseModel):
    """One gateway_health record. The 11 'Current' (deployed) gateway-scope scalars
    from the frozen Northbound v1.4 S3 contract are required and strictly typed;
    every other key (nested storage/sources/fast_bess_logger/legacy_server_upload
    objects, arrays, nulls, and 'Required by Central Sync patch' fields) is preserved
    verbatim via extra='allow'."""

    model_config = ConfigDict(extra="allow", strict=True)

    status: str
    timestamp_utc: str
    gateway_mode: str
    source_count: int
    asset_count: int
    online_asset_count: int
    total_signal_count: int
    bad_signal_count: int
    commands_enabled: bool
    control_enabled: bool
    storage_can_write: bool


class S3HealthEnvelopeIn(BaseModel):
    """S3 ingest envelope. Mirrors the common envelope identity fields but carries
    structured health records instead of numeric telemetry records."""

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
    records: list[S3GatewayHealthRecord]
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
    def _is_s3(cls, v):
        if v != S3_STREAM:
            raise ValueError(f"S3 envelope requires stream=='S3', got {v!r}")
        return v

    @model_validator(mode="after")
    def _counts_match(self):
        if self.record_count != len(self.records):
            raise ValueError("record_count does not match number of records")
        return self


def parse_record_timestamp(ts: str) -> datetime:
    """Parse the record's ISO-8601 timestamp_utc to a tz-aware UTC datetime.
    Raises InvalidValue on anything unparseable (validate-before-claim)."""
    s = (ts or "").strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except (TypeError, ValueError) as e:
        raise InvalidValue(f"unparseable timestamp_utc {ts!r}") from e
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
