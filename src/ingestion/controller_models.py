import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.models import BaseEntityModel


class ControllerStateRecordModel(BaseEntityModel):
    """S5 SOC-controller STATE snapshots (Northbound frozen contract v1.4 / S5_CONTROLLER_API).

    Scope: controller_state ONLY (controller_events is NOT implemented). S5 is a state/decision
    stream, not periodic telemetry. The full raw controller status record is stored verbatim in
    `payload` (JSONB); a defined set of fields is promoted to typed, nullable columns for query.
    Idempotency is message-level (shared ingest_messages ledger + upsert on
    (gateway_id, message_id, record_index)); NO per-snapshot id is invented. received_at_utc is
    backend-assigned (store time); sample_time_utc is promoted from the record's timestamp_utc.
    """

    __tablename__ = "controller_state_records"
    __table_args__ = (
        UniqueConstraint("gateway_id", "message_id", "record_index",
                         name="uq_controller_state_msg_record"),
    )

    gateway_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("gateways.id", ondelete="CASCADE"), index=True
    )
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    site_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    message_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    record_index: Mapped[int] = mapped_column(Integer)
    # promoted (all nullable: the contract/API notes state any string field and healthy may be null)
    controller_state: Mapped[str | None] = mapped_column(String(64), index=True)
    previous_state: Mapped[str | None] = mapped_column(String(64))
    decision: Mapped[str | None] = mapped_column(String(128))
    mode: Mapped[str | None] = mapped_column(String(32))
    version: Mapped[str | None] = mapped_column(String(64))
    healthy: Mapped[bool | None] = mapped_column(Boolean)
    sample_time_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    received_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    payload: Mapped[dict] = mapped_column(JSONB)   # full controller state record, verbatim
