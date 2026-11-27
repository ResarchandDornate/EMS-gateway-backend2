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


class AlarmCatalogOut(BaseModel):         # CG-33 S4 catalogue (resolved from signal_name)
    model_config = _C
    code: str
    english_name: str
    severity: str | None = None
    category: str | None = None
    device_type: str | None = None
    description: str | None = None
    operator_action: str | None = None


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
    # CG-33: catalogue definition for this event's signal_name (None if unmatched).
    catalog: AlarmCatalogOut | None = None


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


class AutomationStatusOut(BaseModel):     # S10 automation_status (CG-28)
    model_config = _C
    gateway_id: uuid.UUID
    site_id: uuid.UUID
    automation_enabled: bool | None
    config_revision: str | None
    supervisor_alive: bool | None
    phase: str | None
    direction: str | None
    fault_latched: bool | None
    safe_stop_requested: bool | None
    sample_time_utc: datetime | None
    received_at_utc: datetime
    payload: Any


class AutomationEventOut(BaseModel):      # S10 automation_events (CG-28)
    model_config = _C
    gateway_id: uuid.UUID
    site_id: uuid.UUID
    event_id: str
    event_type: str | None
    phase: str | None
    direction: str | None
    timestamp_utc: datetime | None
    received_at_utc: datetime
    payload: Any


class ChargeDischargeOut(BaseModel):     # S10
    model_config = _C
    gateway_id: uuid.UUID
    message_id: uuid.UUID
    record_index: int
    pair_id: str | None
    stage: str | None
    run_status: str | None
    direction: str | None
    requested_power_kw: float | None
    commanded_power_kw: float | None
    actual_power_kw: float | None
    rack_voltage_v: float | None
    rack_soc_percent: float | None
    available: bool | None
    battery_ready: bool | None
    pcs_running: bool | None
    hard_blocked: bool | None
    platform_ready: bool | None
    sample_time_utc: datetime | None
    received_at_utc: datetime
    payload: Any
