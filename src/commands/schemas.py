"""Request/response models for the command endpoints. arguments stays an open bag — the
gateway owns D1 argument validation; the backend only gates who may send which type."""
import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

_C = ConfigDict(from_attributes=True)


class CreateCommandIn(BaseModel):
    gateway_id: uuid.UUID
    command_type: str = Field(max_length=64)
    arguments: dict[str, Any] = Field(default_factory=dict)
    requires_readback: bool = True


class CommandOut(BaseModel):              # POST /commands response + the UI lifecycle read
    model_config = _C
    command_id: uuid.UUID
    gateway_id: uuid.UUID
    command_type: str
    arguments: Any
    requires_readback: bool
    requested_role: str
    status: str
    issued_at_utc: datetime
    expires_at_utc: datetime
    final_status: str | None = None
    validation_reason: str | None = None
    gateway_error_code: str | None = None


class PendingCommand(BaseModel):          # one item the gateway receives from the poll
    model_config = _C
    command_id: uuid.UUID
    gateway_id: str                       # contract key (gateway_key), not our UUID
    site_id: str                          # contract key (site_key)
    command_type: str
    requested_role: str
    arguments: Any
    requires_readback: bool
    issued_at_utc: datetime
    expires_at_utc: datetime


class PendingBatch(BaseModel):
    commands: list[PendingCommand]
