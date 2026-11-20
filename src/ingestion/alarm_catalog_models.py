"""S4 alarm catalogue lookup (CG-33).

Two reference/lookup tables, seeded from the CleanEdge Master Contract
(CH ALARM CATALOGUE / CH ALARM EXCLUDED). Reference data only — no tenancy,
no foreign keys in, and NOT written by the ingestion path. They let an S4
alarm event's runtime key / (register, bit) be resolved to a human name,
severity, category, description and operator action.

Populated by d9run/cg33_alarm_catalog_seed.sql (364 + 326 rows), idempotently.
"""
from sqlalchemy import Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from src.models import BaseEntityModel


class AlarmCatalogModel(BaseEntityModel):
    """One catalogue alarm definition (CH ALARM CATALOGUE, 364 rows)."""

    __tablename__ = "alarm_catalog"
    __table_args__ = (UniqueConstraint("code", name="uq_alarm_catalog_code"),)

    code: Mapped[str] = mapped_column(String(128))
    english_name: Mapped[str] = mapped_column(String(255))
    severity: Mapped[str | None] = mapped_column(String(16))
    category: Mapped[str | None] = mapped_column(String(64), index=True)
    device: Mapped[str | None] = mapped_column(String(64))
    assets: Mapped[str | None] = mapped_column(String(128))
    register: Mapped[str | None] = mapped_column(String(32))
    bit: Mapped[int | None] = mapped_column(Integer)
    runtime_key: Mapped[str | None] = mapped_column(String(128), index=True)
    basis: Mapped[str | None] = mapped_column(Text)
    note: Mapped[str | None] = mapped_column(Text)
    device_type: Mapped[str | None] = mapped_column(String(64))
    description: Mapped[str | None] = mapped_column(Text)
    operator_action: Mapped[str | None] = mapped_column(Text)


class AlarmCatalogExcludedModel(BaseEntityModel):
    """A bit the gateway sends but that is NOT an alarm (CH ALARM EXCLUDED, 326 rows)."""

    __tablename__ = "alarm_catalog_excluded"
    __table_args__ = (
        UniqueConstraint("runtime_key", name="uq_alarm_catalog_excluded_runtime_key"),
    )

    english_name: Mapped[str | None] = mapped_column(String(255))
    device: Mapped[str | None] = mapped_column(String(64))
    register: Mapped[str | None] = mapped_column(String(32))
    bit: Mapped[int | None] = mapped_column(Integer)
    runtime_key: Mapped[str] = mapped_column(String(128))
    reason: Mapped[str | None] = mapped_column(Text)
    still_sent_in: Mapped[str | None] = mapped_column(String(64))
