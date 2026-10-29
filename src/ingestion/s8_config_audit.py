import uuid
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.ingestion.schemas import (MAX_RECORDS, MAX_SEQUENCE,
                                    SUPPORTED_SCHEMA_VERSIONS)

S8_STREAM = "S8"


class ChangeSource(str, Enum):
    """Authoritative four-value enum (S8 Snapshot Example + S8_CONFIG_AUDIT_API.md). The Notes
    cell's 'software_update/model_update' is an unresolved clarification; model_update is NOT a
    distinct accepted value (contract decision: Option A)."""

    LOCAL_API = "local_api"
    CENTRAL_COMMAND = "central_command"
    CONFIG_FILE = "config_file"
    SOFTWARE_UPDATE = "software_update"


class S8ConfigAuditRecord(BaseModel):
    """One configuration-audit event. Strict on the contract's scalar types; audit_id (UUID) and
    change_source (enum) opt out of strict to accept JSON strings while still enforcing UUID
    parse / enum membership. old_value and new_value are REQUIRED keys that may hold ANY JSON
    value, including null. Every extra context key (e.g. `data`) is preserved verbatim."""

    model_config = ConfigDict(extra="allow", strict=True)

    audit_id: uuid.UUID = Field(strict=False)
    timestamp_utc: str
    gateway_id: str
    site_id: str
    actor: str
    actor_role: str | None
    change_source: ChangeSource = Field(strict=False)
    setting_path: str
    old_value: Any                       # required key; any JSON incl. null
    new_value: Any                       # required key; any JSON incl. null
    revision: int | str
    reason: str | None
    correlation_id: str | None


class S8ConfigAuditEnvelopeIn(BaseModel):
    """S8 ingest envelope. Same identity wrapper as the other streams; records are audit events."""

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
    records: list[S8ConfigAuditRecord]
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
    def _is_s8(cls, v):
        if v != S8_STREAM:
            raise ValueError(f"S8 envelope requires stream=='S8', got {v!r}")
        return v

    @model_validator(mode="after")
    def _counts_match(self):
        if self.record_count != len(self.records):
            raise ValueError("record_count does not match number of records")
        return self
