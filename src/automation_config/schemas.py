import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict


class AutomationConfigOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    gateway_id: uuid.UUID
    enabled: bool
    config: dict[str, Any]
    updated_by: str | None = None
    updated_at: datetime | None = None
