import uuid
from datetime import datetime

from pydantic import BaseModel


class LatestPoint(BaseModel):
    asset_id: str | None
    source_id: str | None
    value: float | int | None
    time: datetime | None


class LatestResponse(BaseModel):
    site_id: uuid.UUID
    stream: str
    substream: str
    signal_key: str
    results: list[LatestPoint]


class SeriesPoint(BaseModel):
    time: datetime
    value: float | int | None


class SeriesResponse(BaseModel):
    site_id: uuid.UUID
    stream: str
    substream: str
    signal_key: str
    asset_id: str
    points: list[SeriesPoint]
    truncated: bool


class AggregateResponse(BaseModel):
    site_id: uuid.UUID
    stream: str
    substream: str
    signal_key: str
    asset_id: str
    window: str
    fn: str
    points: list[SeriesPoint]
