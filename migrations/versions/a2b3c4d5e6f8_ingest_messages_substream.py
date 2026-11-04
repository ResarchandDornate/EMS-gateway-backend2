"""ingest_messages.substream (nullable) — record the S2 substream on the ledger row

Additive only: adds a nullable column so backlog/pending messages can be attributed to
their substream going forward. Does NOT backfill existing rows (their payload is not
retained) and does NOT change ingestion, ACK, retry, or storage behaviour.

Revision ID: a2b3c4d5e6f8
Revises: a1b2c3d4e5f7
Create Date: 2026-11-03

"""
import sqlalchemy as sa
from alembic import op

revision = "a2b3c4d5e6f8"
down_revision = "a1b2c3d4e5f7"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("ingest_messages", sa.Column("substream", sa.String(32), nullable=True))


def downgrade():
    op.drop_column("ingest_messages", "substream")
