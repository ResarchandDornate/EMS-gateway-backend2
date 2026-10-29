"""solis_telemetry_records (S6 solis_telemetry structured JSONB)

Revision ID: b8c9d0e1f2a3
Revises: a7b8c9d0e1f2
Create Date: 2026-10-29

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "b8c9d0e1f2a3"
down_revision = "a7b8c9d0e1f2"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "solis_telemetry_records",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("is_deleted", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("gateway_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("site_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("message_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("record_index", sa.Integer(), nullable=False),
        sa.Column("sample_time_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column("received_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(["gateway_id"], ["gateways.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("gateway_id", "message_id", "record_index", name="uq_solis_telemetry_msg_record"),
    )
    op.create_index(op.f("ix_solis_telemetry_records_gateway_id"), "solis_telemetry_records", ["gateway_id"])
    op.create_index(op.f("ix_solis_telemetry_records_organization_id"), "solis_telemetry_records", ["organization_id"])
    op.create_index(op.f("ix_solis_telemetry_records_site_id"), "solis_telemetry_records", ["site_id"])
    op.create_index(op.f("ix_solis_telemetry_records_sample_time_utc"), "solis_telemetry_records", ["sample_time_utc"])


def downgrade():
    op.drop_table("solis_telemetry_records")
