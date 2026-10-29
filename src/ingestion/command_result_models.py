import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.models import BaseEntityModel


class CommandResultRecordModel(BaseEntityModel):
    """S9 command_results records (frozen Northbound contract v1.4).

    Remote-command execution ledger, stored verbatim in PostgreSQL JSONB. The contract does NOT
    define whether lifecycle updates are evolving records or separate transitions, and gives no
    per-update identity/order key -- so this model does NOT collapse by command_id. Idempotency is
    MESSAGE-scoped only: unique (gateway_id, message_id, record_index) with the shared ingest_messages
    ledger. The same command_id arriving in different messages is retained as distinct rows
    (transition history preserved); retries of one message stay idempotent.

    received_at_utc is the CONTRACT field (gateway receipt time), promoted to gateway_received_at_utc;
    stored_at is the backend store time (separate). T5_5Y+ retention is metadata only.
    """

    __tablename__ = "command_result_records"
    __table_args__ = (
        UniqueConstraint("gateway_id", "message_id", "record_index",
                         name="uq_command_result_msg_record"),
    )

    gateway_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("gateways.id", ondelete="CASCADE"), index=True
    )
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    site_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    message_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    record_index: Mapped[int] = mapped_column(Integer)
    command_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)   # NOT unique
    correlation_id: Mapped[str | None] = mapped_column(String(128), index=True)
    command_type: Mapped[str] = mapped_column(String(64), index=True)
    final_status: Mapped[str] = mapped_column(String(16), index=True)
    requested_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    gateway_received_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True))  # contract received_at_utc
    stored_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))                # backend store time
    requested_value: Mapped[dict | None] = mapped_column(JSONB)   # any JSON (scalar/object)
    readback_value: Mapped[dict | None] = mapped_column(JSONB)    # any JSON (scalar/object/null)
    payload: Mapped[dict] = mapped_column(JSONB)                  # full command-result record, verbatim
