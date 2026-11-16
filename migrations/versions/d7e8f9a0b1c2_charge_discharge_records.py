"""charge_discharge_records (S10 structured JSONB)

Revision ID: d7e8f9a0b1c2
Revises: c2d3e4f5a6b7
Create Date: 2026-11-16

NOTE: down_revision is set to c2d3e4f5a6b7 (the audit DLQ migration). If `alembic heads` on your
tarun-DB shows a different head, set down_revision to that value before running.
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "d7e8f9a0b1c2"
down_revision = "c2d3e4f5a6b7"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "charge_discharge_records",
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
        sa.Column("pair_id", sa.String(32), nullable=True),
        sa.Column("stage", sa.String(32), nullable=True),
        sa.Column("run_status", sa.String(32), nullable=True),
        sa.Column("direction", sa.String(16), nullable=True),
        sa.Column("requested_power_kw", sa.Float(), nullable=True),
        sa.Column("commanded_power_kw", sa.Float(), nullable=True),
        sa.Column("actual_power_kw", sa.Float(), nullable=True),
        sa.Column("rack_voltage_v", sa.Float(), nullable=True),
        sa.Column("rack_soc_percent", sa.Float(), nullable=True),
        sa.Column("available", sa.Boolean(), nullable=True),
        sa.Column("battery_ready", sa.Boolean(), nullable=True),
        sa.Column("pcs_running", sa.Boolean(), nullable=True),
        sa.Column("hard_blocked", sa.Boolean(), nullable=True),
        sa.Column("platform_ready", sa.Boolean(), nullable=True),
        sa.Column("sample_time_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column("received_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(["gateway_id"], ["gateways.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("gateway_id", "message_id", "record_index", name="uq_chargedischarge_msg_record"),
    )
    op.create_index(op.f("ix_charge_discharge_records_gateway_id"), "charge_discharge_records", ["gateway_id"])
    op.create_index(op.f("ix_charge_discharge_records_organization_id"), "charge_discharge_records", ["organization_id"])
    op.create_index(op.f("ix_charge_discharge_records_site_id"), "charge_discharge_records", ["site_id"])
    op.create_index(op.f("ix_charge_discharge_records_pair_id"), "charge_discharge_records", ["pair_id"])
    op.create_index(op.f("ix_charge_discharge_records_run_status"), "charge_discharge_records", ["run_status"])
    op.create_index(op.f("ix_charge_discharge_records_sample_time_utc"), "charge_discharge_records", ["sample_time_utc"])


def downgrade():
    op.drop_table("charge_discharge_records")
