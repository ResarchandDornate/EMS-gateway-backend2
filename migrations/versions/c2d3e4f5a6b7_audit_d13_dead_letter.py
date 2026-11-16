"""Audit D-13: durable dead-letter table for permanently-failed ingestion.

Additive only. Creates ingest_dead_letter (one row per gateway_id+message_id) that captures
a message whose write failed PERMANENTLY, with its verbatim payload for investigation/manual
replay. No existing table, column, routing, ACK, or data behaviour changes; no historical
row is touched. Transient failures are NOT dead-lettered.

Revision ID: c2d3e4f5a6b7
Revises: b1c2d3e4f5a6
Create Date: 2026-11-11

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "c2d3e4f5a6b7"
down_revision = "b1c2d3e4f5a6"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "ingest_dead_letter",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("is_deleted", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("gateway_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("message_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("stream", sa.String(8), nullable=False),
        sa.Column("substream", sa.String(32), nullable=True),
        sa.Column("error_code", sa.String(64), nullable=False),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("attempts", sa.Integer(), server_default="1", nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("status", sa.String(16), server_default="parked", nullable=False),
        sa.Column("first_failed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_failed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("replayed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["gateway_id"], ["gateways.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("gateway_id", "message_id", name="uq_dlq_gateway_message"),
    )
    op.create_index(op.f("ix_ingest_dead_letter_gateway_id"), "ingest_dead_letter",
                    ["gateway_id"], unique=False)
    op.create_index("ix_ingest_dead_letter_status", "ingest_dead_letter", ["status"],
                    unique=False)


def downgrade():
    op.drop_table("ingest_dead_letter")
