"""Pair-control command downlink — the pending-command queue (CG-26/25/27).

The UI raises a command here (POST /commands); the gateway polls for pending ones
(GET /commands/pending) and, after it runs them, reports the outcome on the S9
command_results stream, which closes the matching row (command_result_service).
One row per command_id; status walks pending -> served -> a final S9 state (or
expired on TTL). The gateway de-duplicates on command_id, so the poll may re-serve
a row until a result arrives or it expires.
"""
import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.models import BaseEntityModel


class PendingCommandModel(BaseEntityModel):
    """One issued pair-control command awaiting delivery and execution. command_id is the
    lifecycle key the S9 result joins back on; it is unique."""

    __tablename__ = "pending_commands"
    __table_args__ = (UniqueConstraint("command_id"),)

    command_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    gateway_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    site_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)

    command_type: Mapped[str] = mapped_column(String(64), index=True)
    arguments: Mapped[dict] = mapped_column(JSONB)
    requires_readback: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")

    requested_by: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    requested_role: Mapped[str] = mapped_column(String(32))
    correlation_id: Mapped[str | None] = mapped_column(String(64), index=True)

    issued_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    expires_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

    # pending|served|accepted|executing|completed|failed|timeout|rejected|expired
    status: Mapped[str] = mapped_column(String(16), index=True, default="pending", server_default="pending")
    served_at_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    closed_at_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # Filled from the S9 result when the command closes.
    final_status: Mapped[str | None] = mapped_column(String(16))
    validation_reason: Mapped[str | None] = mapped_column(Text)
    gateway_error_code: Mapped[str | None] = mapped_column(String(64))
