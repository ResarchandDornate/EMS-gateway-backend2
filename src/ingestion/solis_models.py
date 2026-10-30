import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.models import BaseEntityModel


class SolisTelemetryRecordModel(BaseEntityModel):
    """S6 Solis operational telemetry snapshots (frozen Northbound contract v1.4).

    Scope: solis_telemetry ONLY (solis_events is NOT implemented). The full incoming S6 record
    is stored verbatim in `payload` (JSONB), NOT reshaped to the generic multi-inverter API read
    model. No disputed promoted columns (inverter_id / solis.state / quality / data.* measurements)
    and no inverter identity are committed — the sheet vs S6_SOLIS_API.md field model is unresolved.
    Only two time anchors are promoted: sample_time_utc (from the contract's top-level timestamp_utc,
    nullable) and received_at_utc (backend-assigned store time). Idempotency is message-level only
    (shared ingest_messages ledger + upsert on (gateway_id, message_id, record_index)); no
    per-snapshot id or cross-message dedup rule is invented.
    """

    __tablename__ = "solis_telemetry_records"
    __table_args__ = (
        UniqueConstraint("gateway_id", "message_id", "record_index",
                         name="uq_solis_telemetry_msg_record"),
    )

    gateway_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("gateways.id", ondelete="CASCADE"), index=True
    )
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    site_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    message_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    record_index: Mapped[int] = mapped_column(Integer)
    sample_time_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    received_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    payload: Mapped[dict] = mapped_column(JSONB)   # full Solis telemetry record, verbatim
