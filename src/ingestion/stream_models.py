import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.models import BaseEntityModel


class StreamRecordModel(BaseEntityModel):
    """Generic VERBATIM structured store for the Kalpa northbound streams S2..S10
    (general_asset_telemetry and the gateway_health / alarms / soc_controller / solis /
    edge_ai / configuration_audit / command_results / charge_discharge streams).

    Records are stored verbatim as JSONB - mixed-type nested objects preserved, no
    per-stream schema or registry validation at ingest (same approach as S1 fast_bess).
    Idempotency via the shared ingest_messages ledger; upsert on
    (gateway_id, message_id, record_index) so a retried message overwrites in place.
    """

    __tablename__ = "stream_records"
    __table_args__ = (
        UniqueConstraint("gateway_id", "message_id", "record_index",
                         name="uq_streamrec_msg_record"),
    )

    gateway_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("gateways.id", ondelete="CASCADE"), index=True
    )
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    site_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    message_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    stream: Mapped[str] = mapped_column(String(8), index=True)
    substream: Mapped[str] = mapped_column(String(64), index=True)
    record_index: Mapped[int] = mapped_column(Integer)
    source_id: Mapped[str | None] = mapped_column(String(128))
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    payload: Mapped[dict] = mapped_column(JSONB)   # full record, verbatim
