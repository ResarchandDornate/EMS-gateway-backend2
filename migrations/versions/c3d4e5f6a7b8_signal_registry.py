"""signal_registry

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-10-22

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "c3d4e5f6a7b8"
down_revision = "b2c3d4e5f6a7"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "signal_registry",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("is_deleted", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("point_id", sa.String(64), nullable=True),
        sa.Column("source", sa.String(16), nullable=False),
        sa.Column("primary_stream", sa.String(8), nullable=False),
        sa.Column("substream", sa.String(32), nullable=False),
        sa.Column("asset_type", sa.String(32), nullable=True),
        sa.Column("signal_key", sa.String(160), nullable=False),
        sa.Column("display_name", sa.String(255), nullable=True),
        sa.Column("unit", sa.String(32), nullable=True),
        sa.Column("category", sa.String(64), nullable=True),
        sa.Column("data_type", sa.String(32), nullable=True),
        sa.Column("policy_mode", sa.String(32), nullable=True),
        sa.Column("cadence_sec", sa.Integer(), nullable=True),
        sa.Column("deadband", sa.String(32), nullable=True),
        sa.Column("policy_group", sa.String(64), nullable=True),
        sa.Column("retention_class", sa.String(16), nullable=True),
        sa.Column("event_eligible", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("rw", sa.String(8), nullable=True),
        sa.Column("protocol_ref", sa.String(200), nullable=True),
        sa.Column("source_ref", sa.String(128), nullable=True),
        sa.Column("enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("primary_stream", "substream", "signal_key",
                            name="uq_signal_registry_point"),
    )
    op.create_index("ix_signal_registry_point_id", "signal_registry", ["point_id"])
    op.create_index("ix_signal_registry_primary_stream", "signal_registry", ["primary_stream"])
    op.create_index("ix_signal_registry_substream", "signal_registry", ["substream"])
    op.create_index("ix_signal_registry_signal_key", "signal_registry", ["signal_key"])
    op.create_index("ix_signal_registry_retention_class", "signal_registry", ["retention_class"])


def downgrade():
    op.drop_table("signal_registry")
