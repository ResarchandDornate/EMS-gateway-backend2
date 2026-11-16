"""Durable dead-letter table for permanently-failed ingestion (audit finding D-13).

When an ingest write fails PERMANENTLY (a deterministic error that a retry cannot fix -
e.g. a 4xx from InfluxDB, a bad-bucket/config error), the message is captured here with its
full payload, the matching ledger row is transitioned pending -> failed, and the gateway is
told non-retryable so it stops resending. Nothing is lost (the payload is stored verbatim for
investigation and manual replay) and no infinite replay loop can form. Transient failures are
NOT dead-lettered - they stay 'pending' and the gateway keeps retrying as before.

One row per (gateway_id, message_id): a repeated permanent failure of the same message
increments `attempts` instead of inserting a duplicate.
"""
import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.models import BaseEntityModel


class IngestDeadLetterModel(BaseEntityModel):
    __tablename__ = "ingest_dead_letter"
    __table_args__ = (
        UniqueConstraint("gateway_id", "message_id", name="uq_dlq_gateway_message"),
    )

    gateway_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("gateways.id", ondelete="CASCADE"), index=True
    )
    message_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    stream: Mapped[str] = mapped_column(String(8))
    substream: Mapped[str | None] = mapped_column(String(32))
    error_code: Mapped[str] = mapped_column(String(64))
    detail: Mapped[str | None] = mapped_column(Text)
    attempts: Mapped[int] = mapped_column(Integer, default=1, server_default="1")
    payload: Mapped[dict] = mapped_column(JSONB)            # verbatim message, for replay
    status: Mapped[str] = mapped_column(String(16), default="parked", server_default="parked")
    first_failed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_failed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    replayed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
