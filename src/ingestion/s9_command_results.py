import uuid
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.ingestion.schemas import (MAX_RECORDS, MAX_SEQUENCE,
                                    SUPPORTED_SCHEMA_VERSIONS)

S9_STREAM = "S9"


class FinalStatus(str, Enum):
    """The six contract-defined lifecycle states (S9 sheet, final_status)."""

    ACCEPTED = "accepted"
    REJECTED = "rejected"
    EXECUTING = "executing"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"


class S9CommandResultRecord(BaseModel):
    """One command-result lifecycle record. Strict on the contract's scalar types; command_id (UUID)
    and final_status (enum) opt out of strict to accept JSON strings (UUID parse / enum membership
    still enforced). command_type is an OPEN string (S9 does not enumerate its vocabulary -> D1
    owns it). requested_value and readback_value are ANY JSON (incl. null). Only the contract's
    UNCONDITIONAL required fields are enforced; conditional ones ('Required if executed/write/on
    failure/where source') are optional here (no conditional-required matrix is invented). Any
    extra key is preserved verbatim via extra='allow'."""

    model_config = ConfigDict(extra="allow", strict=True)

    # unconditional Required
    command_id: uuid.UUID = Field(strict=False)
    gateway_id: str
    site_id: str
    requested_at_utc: str
    received_at_utc: str                       # contract: gateway receipt time
    requested_by: str
    requested_role: str
    command_type: str                          # open string (not enumerated in S9)
    requested_value: Any                       # required key; any JSON
    gateway_validation_ok: bool
    readback_requested: bool
    verified: bool | None                      # required key; value may be null
    final_status: FinalStatus = Field(strict=False)
    # Recommended / Optional / conditionally-required -> optional here
    correlation_id: str | None = None
    source_id: str | None = None
    asset_id: str | None = None
    signal_name: str | None = None
    validation_reason: str | None = None
    execution_started_at_utc: str | None = None
    execution_finished_at_utc: str | None = None
    write_ok: bool | None = None
    readback_value: Any = None                 # any JSON incl. null
    failure_reason: str | None = None
    gateway_error_code: str | None = None
    duration_ms: float | int | None = None
    note: str | None = None


class S9CommandEnvelopeIn(BaseModel):
    """S9 ingest envelope. Same identity wrapper as the other streams; records are command results.
    S9 remains deferred at the endpoint; used by the persistence slice and its tests only."""

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
    records: list[S9CommandResultRecord]
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
    def _is_s9(cls, v):
        if v != S9_STREAM:
            raise ValueError(f"S9 envelope requires stream=='S9', got {v!r}")
        return v

    @model_validator(mode="after")
    def _counts_match(self):
        if self.record_count != len(self.records):
            raise ValueError("record_count does not match number of records")
        return self
