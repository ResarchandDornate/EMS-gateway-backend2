import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.models import BaseEntityModel


class ChargeDischargeRecordModel(BaseEntityModel):
    """S10 battery_charge_discharge_control / pair_controller status records.

    One record per pair (contract: 'four pair status records'; Change + 5 s heartbeat). The full
    raw record is stored verbatim in `payload` (JSONB); a defined set of per-pair operational fields
    is promoted to typed, nullable columns for query. Message-level idempotency via the shared
    ingest_messages ledger + an upsert on (gateway_id, message_id, record_index). received_at_utc is
    backend-assigned (store time); sample_time_utc is promoted from the record's timestamp_utc.
    """

    __tablename__ = "charge_discharge_records"
    __table_args__ = (
        UniqueConstraint("gateway_id", "message_id", "record_index",
                         name="uq_chargedischarge_msg_record"),
    )

    gateway_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("gateways.id", ondelete="CASCADE"), index=True
    )
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    site_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    message_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    record_index: Mapped[int] = mapped_column(Integer)
    # promoted per-pair fields (all nullable per contract)
    pair_id: Mapped[str | None] = mapped_column(String(32), index=True)
    stage: Mapped[str | None] = mapped_column(String(32))
    run_status: Mapped[str | None] = mapped_column(String(32), index=True)
    direction: Mapped[str | None] = mapped_column(String(16))
    requested_power_kw: Mapped[float | None] = mapped_column(Float)
    commanded_power_kw: Mapped[float | None] = mapped_column(Float)
    actual_power_kw: Mapped[float | None] = mapped_column(Float)
    rack_voltage_v: Mapped[float | None] = mapped_column(Float)
    rack_soc_percent: Mapped[float | None] = mapped_column(Float)
    available: Mapped[bool | None] = mapped_column(Boolean)
    battery_ready: Mapped[bool | None] = mapped_column(Boolean)
    pcs_running: Mapped[bool | None] = mapped_column(Boolean)
    hard_blocked: Mapped[bool | None] = mapped_column(Boolean)
    platform_ready: Mapped[bool | None] = mapped_column(Boolean)
    sample_time_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    received_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    payload: Mapped[dict] = mapped_column(JSONB)   # full pair-status record, verbatim
