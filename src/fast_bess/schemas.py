"""S1 Fast BESS read schemas. payload is returned verbatim (nested + mixed types preserved)."""
import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel


class FastBessRecord(BaseModel):
    gateway_id: uuid.UUID
    message_id: uuid.UUID
    substream: str
    record_index: int
    source_id: str | None
    bess_id: str | None
    recorded_at: datetime
    payload: Any            # original nested JSONB record, mixed types untouched


class FastBessLatestResponse(BaseModel):
    site_id: uuid.UUID
    results: list[FastBessRecord]   # newest record per bess_id at the site


class FastBessHistoryResponse(BaseModel):
    site_id: uuid.UUID
    limit: int
    offset: int
    has_more: bool
    results: list[FastBessRecord]
