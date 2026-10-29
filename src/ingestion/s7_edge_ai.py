import uuid

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.ingestion.schemas import (MAX_RECORDS, MAX_SEQUENCE,
                                    SUPPORTED_SCHEMA_VERSIONS)

S7_STREAM = "S7"


class S7EdgeAiInferenceRecord(BaseModel):
    """One Edge-AI inference snapshot (edge_ai_inference family). Stored verbatim.

    The frozen contract marks fields High priority but nullable, and the promoted-field model is
    disputed between the frozen sheet and S7_EDGE_AI_API.md, so this schema imposes NO required
    fields and NO shape beyond the one time path the frozen contract defines: top-level cycle_time
    (optional here; S7 has no timestamp_utc). Every other field (phase, cycle_index, model_info,
    sources[], key_features, edge_ai_status, ...) is preserved verbatim via extra='allow'."""

    model_config = ConfigDict(extra="allow")

    cycle_time: str | None = None


class S7EdgeAiEnvelopeIn(BaseModel):
    """S7 ingest envelope. Same identity wrapper as the other streams; records are inference
    snapshots. S7 remains deferred at the endpoint; used by the persistence slice and its tests,
    not wired into routing."""

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
    records: list[S7EdgeAiInferenceRecord]
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
    def _is_s7(cls, v):
        if v != S7_STREAM:
            raise ValueError(f"S7 envelope requires stream=='S7', got {v!r}")
        return v

    @model_validator(mode="after")
    def _counts_match(self):
        if self.record_count != len(self.records):
            raise ValueError("record_count does not match number of records")
        return self
