import uuid
from datetime import datetime

from pydantic import BaseModel, field_validator


def _num_or_none(v):
    # A stored register that is not a number (e.g. a meter model string like
    # "abb_acdb_mfm") nulls the value instead of failing the whole response.
    if v is None or isinstance(v, (int, float)):
        return v
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


class LatestPoint(BaseModel):
    asset_id: str | None
    source_id: str | None
    value: float | int | None
    time: datetime | None
    quality: str | None = None

    _coerce_value = field_validator("value", mode="before")(_num_or_none)


class LatestResponse(BaseModel):
    site_id: uuid.UUID
    stream: str
    substream: str
    signal_key: str
    results: list[LatestPoint]


class SnapshotPoint(BaseModel):
    asset_id: str | None
    source_id: str | None
    signal_key: str
    value: float | int | None
    time: datetime | None
    quality: str | None = None

    _coerce_value = field_validator("value", mode="before")(_num_or_none)


class SnapshotResponse(BaseModel):
    site_id: uuid.UUID
    stream: str
    substream: str
    results: list[SnapshotPoint]


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
