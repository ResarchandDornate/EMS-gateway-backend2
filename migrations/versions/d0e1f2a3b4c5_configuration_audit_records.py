"""configuration_audit_records (S8 configuration_audit structured JSONB)

Revision ID: d0e1f2a3b4c5
Revises: c9d0e1f2a3b4
Create Date: 2026-10-29

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "d0e1f2a3b4c5"
down_revision = "c9d0e1f2a3b4"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "configuration_audit_records",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("is_deleted", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("gateway_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("site_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("message_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("audit_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("received_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("actor", sa.String(255), nullable=False),
        sa.Column("actor_role", sa.String(64), nullable=True),
        sa.Column("change_source", sa.String(32), nullable=False),
        sa.Column("setting_path", sa.String(255), nullable=False),
        sa.Column("revision", sa.String(64), nullable=True),
        sa.Column("correlation_id", sa.String(128), nullable=True),
        sa.Column("old_value", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("new_value", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(["gateway_id"], ["gateways.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("gateway_id", "audit_id", name="uq_config_audit_gateway_audit"),
    )
    op.create_index(op.f("ix_configuration_audit_records_gateway_id"), "configuration_audit_records", ["gateway_id"])
    op.create_index(op.f("ix_configuration_audit_records_organization_id"), "configuration_audit_records", ["organization_id"])
    op.create_index(op.f("ix_configuration_audit_records_site_id"), "configuration_audit_records", ["site_id"])
    op.create_index(op.f("ix_configuration_audit_records_occurred_at"), "configuration_audit_records", ["occurred_at"])
    op.create_index(op.f("ix_configuration_audit_records_change_source"), "configuration_audit_records", ["change_source"])
    op.create_index(op.f("ix_configuration_audit_records_setting_path"), "configuration_audit_records", ["setting_path"])


def downgrade():
    op.drop_table("configuration_audit_records")
