import uuid
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.models import BaseEntityModel


class IngestMessageModel(BaseEntityModel):
    """Message-level ingestion ledger (idempotency + processing lease).

    One row per (gateway_id, message_id). 9b-i writes only 'pending' rows with a
    time-bounded lease; the 'stored' transition and the Influx write belong to 9b-ii.
    """

    __tablename__ = "ingest_messages"
    __table_args__ = (
        UniqueConstraint("gateway_id", "message_id", name="uq_ingest_gateway_message"),
    )

    gateway_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("gateways.id", ondelete="CASCADE"), index=True
    )
    message_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    stream: Mapped[str] = mapped_column(String(8))
    sequence: Mapped[int] = mapped_column(BigInteger)
    record_count: Mapped[int] = mapped_column(Integer)
    payload_hash: Mapped[str] = mapped_column(String(64))         # sha256 hex of canonical form
    status: Mapped[str] = mapped_column(String(16))               # pending | stored
    lease_owner: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    stored_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
