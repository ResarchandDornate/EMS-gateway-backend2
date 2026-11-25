"""S10 automation substreams — site automation supervisor status and events (CG-28).

These arrive on battery_charge_discharge_control alongside pair_controller, as the
substreams automation_status and automation_events. automation_status is ONE record
per site (stored as history; latest = newest by received_at); automation_events is an
append-only log de-duplicated on event_id. Both keep the full gateway record verbatim
in `payload`, with a few promoted columns for querying.
"""
import uuid
from datetime import datetime

from sqlalchemy import BigInteger, Boolean, DateTime, Integer, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.models import BaseEntityModel


class AutomationStatusRecordModel(BaseEntityModel):
    """One site automation_status snapshot. History is kept; the read endpoint returns the
    newest by received_at. Idempotency key mirrors the other S10 records."""

    __tablename__ = "automation_status_records"
    __table_args__ = (UniqueConstraint("gateway_id", "message_id", "record_index"),)

    gateway_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    site_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    message_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    record_index: Mapped[int] = mapped_column(Integer)

    automation_enabled: Mapped[bool | None] = mapped_column(Boolean)
    config_revision: Mapped[int | None] = mapped_column(BigInteger)
    supervisor_alive: Mapped[bool | None] = mapped_column(Boolean)
    phase: Mapped[str | None] = mapped_column(String(32))
    direction: Mapped[str | None] = mapped_column(String(16))
    fault_latched: Mapped[bool | None] = mapped_column(Boolean)
    safe_stop_requested: Mapped[bool | None] = mapped_column(Boolean)
    sample_time_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    received_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    # Full record verbatim: schedule, active_pairs, prepared_pairs, hardware_engaged, solar_kw, …
    payload: Mapped[dict] = mapped_column(JSONB)


class AutomationEventRecordModel(BaseEntityModel):
    """One automation event, de-duplicated on event_id (each event is sent once, but a message
    may be retried). The message ledger gives message-level idempotency; this unique key gives
    event-level de-duplication."""

    __tablename__ = "automation_event_records"
    __table_args__ = (UniqueConstraint("gateway_id", "event_id"),)

    gateway_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    site_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    message_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))

    event_id: Mapped[str] = mapped_column(String(128), index=True)
    event_type: Mapped[str | None] = mapped_column(String(64), index=True)
    phase: Mapped[str | None] = mapped_column(String(32))
    direction: Mapped[str | None] = mapped_column(String(16))
    timestamp_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    received_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    payload: Mapped[dict] = mapped_column(JSONB)
