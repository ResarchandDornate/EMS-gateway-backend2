"""alarm_catalog + alarm_catalog_excluded (CG-33 S4 alarm catalogue lookup)

Revision ID: e8f9a0b1c2d3
Revises: d7e8f9a0b1c2
Create Date: 2026-11-20

Additive only. Two reference/lookup tables for the S4 alarm catalogue:
  * alarm_catalog           — 364 catalogue codes/names (unique `code`)
  * alarm_catalog_excluded  — 326 excluded bits         (unique `runtime_key`)
No change to alarm_event_records or any existing table/endpoint. Populated by
d9run/cg33_alarm_catalog_seed.sql after `alembic upgrade head`.
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "e8f9a0b1c2d3"
down_revision = "d7e8f9a0b1c2"
branch_labels = None
depends_on = None


def _base_cols():
    return [
        sa.Column("id", postgresql.UUID(as_uuid=True),
                  server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("is_deleted", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    ]


def upgrade():
    op.create_table(
        "alarm_catalog",
        *_base_cols(),
        sa.Column("code", sa.String(128), nullable=False),
        sa.Column("english_name", sa.String(255), nullable=False),
        sa.Column("severity", sa.String(16), nullable=True),
        sa.Column("category", sa.String(64), nullable=True),
        sa.Column("device", sa.String(64), nullable=True),
        sa.Column("assets", sa.String(128), nullable=True),
        sa.Column("register", sa.String(32), nullable=True),
        sa.Column("bit", sa.Integer(), nullable=True),
        sa.Column("runtime_key", sa.String(128), nullable=True),
        sa.Column("basis", sa.Text(), nullable=True),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("device_type", sa.String(64), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("operator_action", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("code", name="uq_alarm_catalog_code"),
    )
    op.create_index(op.f("ix_alarm_catalog_runtime_key"), "alarm_catalog", ["runtime_key"])
    op.create_index(op.f("ix_alarm_catalog_category"), "alarm_catalog", ["category"])
    op.create_index(op.f("ix_alarm_catalog_severity"), "alarm_catalog", ["severity"])
    op.create_index("ix_alarm_catalog_register_bit", "alarm_catalog", ["register", "bit"])

    op.create_table(
        "alarm_catalog_excluded",
        *_base_cols(),
        sa.Column("english_name", sa.String(255), nullable=True),
        sa.Column("device", sa.String(64), nullable=True),
        sa.Column("register", sa.String(32), nullable=True),
        sa.Column("bit", sa.Integer(), nullable=True),
        sa.Column("runtime_key", sa.String(128), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("still_sent_in", sa.String(64), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("runtime_key", name="uq_alarm_catalog_excluded_runtime_key"),
    )
    op.create_index("ix_alarm_catalog_excluded_register_bit", "alarm_catalog_excluded", ["register", "bit"])


def downgrade():
    op.drop_table("alarm_catalog_excluded")
    op.drop_table("alarm_catalog")
