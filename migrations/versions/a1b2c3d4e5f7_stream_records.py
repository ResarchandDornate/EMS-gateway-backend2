"""stream_records (S2..S10 generic verbatim JSONB)

Revision ID: a1b2c3d4e5f7
Revises: f1a2b3c4d5e6
Create Date: 2026-10-30

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "a1b2c3d4e5f7"
down_revision = "f1a2b3c4d5e6"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "stream_records",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("is_deleted", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("gateway_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("site_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("message_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("stream", sa.String(8), nullable=False),
        sa.Column("substream", sa.String(64), nullable=False),
        sa.Column("record_index", sa.Integer(), nullable=False),
        sa.Column("source_id", sa.String(128), nullable=True),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(["gateway_id"], ["gateways.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("gateway_id", "message_id", "record_index", name="uq_streamrec_msg_record"),
    )
    op.create_index(op.f("ix_stream_records_gateway_id"), "stream_records", ["gateway_id"])
    op.create_index(op.f("ix_stream_records_organization_id"), "stream_records", ["organization_id"])
    op.create_index(op.f("ix_stream_records_site_id"), "stream_records", ["site_id"])
    op.create_index(op.f("ix_stream_records_stream"), "stream_records", ["stream"])
    op.create_index(op.f("ix_stream_records_substream"), "stream_records", ["substream"])
    op.create_index(op.f("ix_stream_records_recorded_at"), "stream_records", ["recorded_at"])


def downgrade():
    op.drop_table("stream_records")
