import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.models import BaseEntityModel


class ConfigurationAuditRecordModel(BaseEntityModel):
    """S8 configuration-audit records (frozen Northbound contract v1.4).

    Single-family structured-event stream (configuration_audit), stored in PostgreSQL JSONB. The
    full incoming audit record is kept verbatim in `payload`; contract-defined fields are promoted
    to typed columns (old_value/new_value are arbitrary JSON -> JSONB). Idempotency is event-level:
    upsert on (gateway_id, audit_id) so the same change redelivered in another message overwrites
    rather than duplicating. received_at_utc is backend-assigned; occurred_at is from the
    contract's timestamp_utc. Baseline suppression is producer-side; the backend fabricates/suppresses
    nothing. T5_5Y+ retention is metadata only (no bucket/worker here).
    """

    __tablename__ = "configuration_audit_records"
    __table_args__ = (
        UniqueConstraint("gateway_id", "audit_id", name="uq_config_audit_gateway_audit"),
    )

    gateway_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("gateways.id", ondelete="CASCADE"), index=True
    )
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    site_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    message_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    audit_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    received_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    actor: Mapped[str] = mapped_column(String(255))
    actor_role: Mapped[str | None] = mapped_column(String(64))
    change_source: Mapped[str] = mapped_column(String(32), index=True)
    setting_path: Mapped[str] = mapped_column(String(255), index=True)
    revision: Mapped[str | None] = mapped_column(String(64))
    correlation_id: Mapped[str | None] = mapped_column(String(128))
    old_value: Mapped[dict | None] = mapped_column(JSONB)   # any JSON (scalar/object/null)
    new_value: Mapped[dict | None] = mapped_column(JSONB)   # any JSON (scalar/object/null)
    payload: Mapped[dict] = mapped_column(JSONB)            # full audit record, verbatim
