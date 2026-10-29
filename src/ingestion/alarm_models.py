import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.models import BaseEntityModel


class AlarmEventRecordModel(BaseEntityModel):
    """S4 alarms/events records (Northbound frozen contract v1.4).

    S4 is a derived transition stream, NOT numeric telemetry: discrete alarm/state/fault
    events, each with its own unique event_id. Stored verbatim as JSONB. Idempotency is
    event-level: upsert on (gateway_id, event_id) so the same event redelivered in a
    different message (outbox resend) overwrites in place rather than duplicating. ACTIVE
    and CLEARED are distinct events (own event_id), optionally linked by correlation_id;
    this table is append-by-event, no backend lifecycle merge is performed.
    """

    __tablename__ = "alarm_event_records"
    __table_args__ = (
        UniqueConstraint("gateway_id", "event_id", name="uq_alarm_gateway_event"),
    )

    gateway_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("gateways.id", ondelete="CASCADE"), index=True
    )
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    site_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    message_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    event_type: Mapped[str] = mapped_column(String(128), index=True)
    severity: Mapped[str] = mapped_column(String(16))
    state: Mapped[str] = mapped_column(String(16))
    dedupe_key: Mapped[str] = mapped_column(String(255), index=True)
    correlation_id: Mapped[str | None] = mapped_column(String(128))
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    payload: Mapped[dict] = mapped_column(JSONB)   # full validated event, verbatim
