"""command_result_records (S9 command_results structured JSONB)

Revision ID: e1f2a3b4c5d6
Revises: d0e1f2a3b4c5
Create Date: 2026-10-29

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "e1f2a3b4c5d6"
down_revision = "d0e1f2a3b4c5"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "command_result_records",
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
        sa.Column("command_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("correlation_id", sa.String(128), nullable=True),
        sa.Column("command_type", sa.String(64), nullable=False),
        sa.Column("final_status", sa.String(16), nullable=False),
        sa.Column("requested_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("gateway_received_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("stored_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("requested_value", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("readback_value", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(["gateway_id"], ["gateways.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("gateway_id", "message_id", "record_index", name="uq_command_result_msg_record"),
    )
    op.create_index(op.f("ix_command_result_records_gateway_id"), "command_result_records", ["gateway_id"])
    op.create_index(op.f("ix_command_result_records_organization_id"), "command_result_records", ["organization_id"])
    op.create_index(op.f("ix_command_result_records_site_id"), "command_result_records", ["site_id"])
    op.create_index(op.f("ix_command_result_records_command_id"), "command_result_records", ["command_id"])
    op.create_index(op.f("ix_command_result_records_correlation_id"), "command_result_records", ["correlation_id"])
    op.create_index(op.f("ix_command_result_records_command_type"), "command_result_records", ["command_type"])
    op.create_index(op.f("ix_command_result_records_final_status"), "command_result_records", ["final_status"])
    op.create_index(op.f("ix_command_result_records_requested_at_utc"), "command_result_records", ["requested_at_utc"])


def downgrade():
    op.drop_table("command_result_records")
