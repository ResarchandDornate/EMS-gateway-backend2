import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.models import BaseEntityModel


class EdgeAiInferenceRecordModel(BaseEntityModel):
    """S7 Edge-AI inference snapshots (frozen Northbound contract v1.4).

    Scope: edge_ai_inference ONLY (edge_ai_anomaly_context is NOT implemented). The full incoming
    inference record is stored verbatim in `payload` (JSONB), preserving nested model_info,
    sources[] and key_features. No disputed promoted columns from S7_EDGE_AI_API.md (model_name,
    result, score, inference_type, status) are committed. Only time anchors are promoted:
    sample_time_utc (from the contract's cycle_time; S7 has no timestamp_utc) and received_at_utc
    (backend-assigned). Idempotency is message-level only (shared ingest_messages ledger + upsert
    on (gateway_id, message_id, record_index)); the contract's cycle_index is NOT used for
    cross-message dedup in this slice, and no key is invented.
    """

    __tablename__ = "edge_ai_inference_records"
    __table_args__ = (
        UniqueConstraint("gateway_id", "message_id", "record_index",
                         name="uq_edge_ai_inference_msg_record"),
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
    payload: Mapped[dict] = mapped_column(JSONB)   # full inference record, verbatim
