import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.models import BaseEntityModel


class GatewayHealthRecordModel(BaseEntityModel):
    """S3 gateway_health structured records (Northbound frozen contract v1.4).

    S3 is NOT numeric telemetry: it is one structured health record per message
    (nested objects, booleans, strings, arrays, nulls). Stored verbatim as JSONB
    here instead of the numeric S1/S2 Influx path. Idempotency is provided by the
    shared ingest_messages ledger; this table upserts on (gateway_id, message_id,
    record_index) so a retried message overwrites rather than duplicates.
    """

    __tablename__ = "gateway_health_records"
    __table_args__ = (
        UniqueConstraint("gateway_id", "message_id", "record_index",
                         name="uq_gwhealth_msg_record"),
    )

    gateway_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("gateways.id", ondelete="CASCADE"), index=True
    )
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    site_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    message_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    substream: Mapped[str] = mapped_column(String(32))
    record_index: Mapped[int] = mapped_column(Integer)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    payload: Mapped[dict] = mapped_column(JSONB)   # full validated record, verbatim
