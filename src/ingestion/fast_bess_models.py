import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.models import BaseEntityModel


class FastBessRecordModel(BaseEntityModel):
    """S1 fast_bess_telemetry structured records.

    Real S1 records are mixed-type nested objects (each signal is a
    {value, quality, unit, category, updated_utc, ...} object, with booleans, strings,
    nulls and arrays) under a ~44-signal set that does NOT match the numeric registry.
    They are stored VERBATIM as JSONB here instead of the numeric S1/S2 Influx path.
    Idempotency is provided by the shared ingest_messages ledger; this table upserts on
    (gateway_id, message_id, record_index) so a retried message overwrites rather than
    duplicates.
    """

    __tablename__ = "fast_bess_records"
    __table_args__ = (
        UniqueConstraint("gateway_id", "message_id", "record_index",
                         name="uq_fastbess_msg_record"),
    )

    gateway_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("gateways.id", ondelete="CASCADE"), index=True
    )
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    site_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    message_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    substream: Mapped[str] = mapped_column(String(64))
    record_index: Mapped[int] = mapped_column(Integer)
    source_id: Mapped[str | None] = mapped_column(String(128))
    bess_id: Mapped[str | None] = mapped_column(String(128))
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    payload: Mapped[dict] = mapped_column(JSONB)   # full record, verbatim
