"""Response item models for the dedicated-stream read APIs. Typed query columns are exposed
plus the verbatim JSONB `payload`; organization_id is intentionally never exposed."""
import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict

_C = ConfigDict(from_attributes=True)


class HealthOut(BaseModel):              # S3
    model_config = _C
    gateway_id: uuid.UUID
    message_id: uuid.UUID
    substream: str
    record_index: int
    recorded_at: datetime
    payload: Any


class AlarmOut(BaseModel):               # S4
    model_config = _C
    gateway_id: uuid.UUID
    message_id: uuid.UUID
    event_id: uuid.UUID
    event_type: str
    severity: str
    state: str
    dedupe_key: str
    correlation_id: str | None
    occurred_at: datetime
    payload: Any


class ControllerOut(BaseModel):          # S5
    model_config = _C
    gateway_id: uuid.UUID
    message_id: uuid.UUID
    record_index: int
    controller_state: str | None
    previous_state: str | None
    decision: str | None
    mode: str | None
    version: str | None
    healthy: bool | None
    sample_time_utc: datetime | None
    received_at_utc: datetime
    payload: Any


class SolisOut(BaseModel):               # S6
    model_config = _C
    gateway_id: uuid.UUID
    message_id: uuid.UUID
    record_index: int
    sample_time_utc: datetime | None
    received_at_utc: datetime
    payload: Any


class EdgeAiOut(BaseModel):              # S7
    model_config = _C
    gateway_id: uuid.UUID
    message_id: uuid.UUID
    record_index: int
    sample_time_utc: datetime | None
    received_at_utc: datetime
    payload: Any


class ConfigAuditOut(BaseModel):         # S8
    model_config = _C
    gateway_id: uuid.UUID
    message_id: uuid.UUID
    audit_id: uuid.UUID
    occurred_at: datetime
    received_at_utc: datetime
    actor: str
    actor_role: str | None
    change_source: str
    setting_path: str
    revision: str | None
    correlation_id: str | None
    old_value: Any | None
    new_value: Any | None
    payload: Any


class CommandOut(BaseModel):             # S9
    model_config = _C
    gateway_id: uuid.UUID
    message_id: uuid.UUID
    record_index: int
    command_id: uuid.UUID
    correlation_id: str | None
    command_type: str
    final_status: str
    requested_at_utc: datetime
    gateway_received_at_utc: datetime
    stored_at: datetime
    requested_value: Any | None
    readback_value: Any | None
    payload: Any
