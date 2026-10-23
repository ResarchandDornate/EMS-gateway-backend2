from sqlalchemy import Boolean, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from src.models import BaseEntityModel


class SignalRegistryModel(BaseEntityModel):
    """Canonical telemetry point catalog for S1 and S2.

    S1 points come from the Ornate Canonical Signal Matrix (Primary Stream = S1).
    S2 points come from the Kalpa workbook only, deduplicated to one canonical
    row per (substream, signal_key); rack/PCS multiplicity is handled at ingest
    via the asset_id tag, not by storing duplicate registry rows.
    """

    __tablename__ = "signal_registry"
    __table_args__ = (
        UniqueConstraint("primary_stream", "substream", "signal_key",
                         name="uq_signal_registry_point"),
    )

    point_id: Mapped[str | None] = mapped_column(String(64), index=True)   # Ornate p-id; null for Kalpa
    source: Mapped[str] = mapped_column(String(16))                         # ornate | kalpa
    primary_stream: Mapped[str] = mapped_column(String(8), index=True)      # S1 | S2
    substream: Mapped[str] = mapped_column(String(32), index=True)
    asset_type: Mapped[str | None] = mapped_column(String(32))
    signal_key: Mapped[str] = mapped_column(String(160), index=True)
    display_name: Mapped[str | None] = mapped_column(String(255))
    unit: Mapped[str | None] = mapped_column(String(32))
    category: Mapped[str | None] = mapped_column(String(64))
    data_type: Mapped[str | None] = mapped_column(String(32))
    policy_mode: Mapped[str | None] = mapped_column(String(32))
    cadence_sec: Mapped[int | None] = mapped_column(Integer)
    deadband: Mapped[str | None] = mapped_column(String(32))
    policy_group: Mapped[str | None] = mapped_column(String(64))
    retention_class: Mapped[str | None] = mapped_column(String(16), index=True)
    event_eligible: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    rw: Mapped[str | None] = mapped_column(String(8))
    protocol_ref: Mapped[str | None] = mapped_column(String(200))           # vendor protocol path / register
    source_ref: Mapped[str | None] = mapped_column(String(128))             # workbook sheet + row
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")
