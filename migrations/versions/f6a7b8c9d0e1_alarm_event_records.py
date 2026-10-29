"""alarm_event_records (S4 structured JSONB)

Revision ID: f6a7b8c9d0e1
Revises: e5f6a7b8c9d0
Create Date: 2026-10-29

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "f6a7b8c9d0e1"
down_revision = "e5f6a7b8c9d0"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "alarm_event_records",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("is_deleted", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("gateway_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("site_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("message_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(128), nullable=False),
        sa.Column("severity", sa.String(16), nullable=False),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("dedupe_key", sa.String(255), nullable=False),
        sa.Column("correlation_id", sa.String(128), nullable=True),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(["gateway_id"], ["gateways.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("gateway_id", "event_id", name="uq_alarm_gateway_event"),
    )
    op.create_index(op.f("ix_alarm_event_records_gateway_id"), "alarm_event_records", ["gateway_id"])
    op.create_index(op.f("ix_alarm_event_records_organization_id"), "alarm_event_records", ["organization_id"])
    op.create_index(op.f("ix_alarm_event_records_site_id"), "alarm_event_records", ["site_id"])
    op.create_index(op.f("ix_alarm_event_records_event_type"), "alarm_event_records", ["event_type"])
    op.create_index(op.f("ix_alarm_event_records_dedupe_key"), "alarm_event_records", ["dedupe_key"])
    op.create_index(op.f("ix_alarm_event_records_occurred_at"), "alarm_event_records", ["occurred_at"])


def downgrade():
    op.drop_table("alarm_event_records")
