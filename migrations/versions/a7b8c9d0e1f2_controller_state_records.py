"""controller_state_records (S5 controller_state structured JSONB)

Revision ID: a7b8c9d0e1f2
Revises: f6a7b8c9d0e1
Create Date: 2026-10-29

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "a7b8c9d0e1f2"
down_revision = "f6a7b8c9d0e1"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "controller_state_records",
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
        sa.Column("controller_state", sa.String(64), nullable=True),
        sa.Column("previous_state", sa.String(64), nullable=True),
        sa.Column("decision", sa.String(128), nullable=True),
        sa.Column("mode", sa.String(32), nullable=True),
        sa.Column("version", sa.String(64), nullable=True),
        sa.Column("healthy", sa.Boolean(), nullable=True),
        sa.Column("sample_time_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column("received_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(["gateway_id"], ["gateways.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("gateway_id", "message_id", "record_index", name="uq_controller_state_msg_record"),
    )
    op.create_index(op.f("ix_controller_state_records_gateway_id"), "controller_state_records", ["gateway_id"])
    op.create_index(op.f("ix_controller_state_records_organization_id"), "controller_state_records", ["organization_id"])
    op.create_index(op.f("ix_controller_state_records_site_id"), "controller_state_records", ["site_id"])
    op.create_index(op.f("ix_controller_state_records_controller_state"), "controller_state_records", ["controller_state"])
    op.create_index(op.f("ix_controller_state_records_sample_time_utc"), "controller_state_records", ["sample_time_utc"])


def downgrade():
    op.drop_table("controller_state_records")
