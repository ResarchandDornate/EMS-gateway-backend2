"""automation_status_records.config_revision -> text (gateway sends a hex hash)

Revision ID: a1c2e3f4b5d6
Revises: a6b7c8d9e0f1
Create Date: 2026-11-27

The gateway reports config_revision as a 16-char hex hash (e.g. bf2d28614f02d16e),
not an integer. The column was BigInteger, so the promoted-column insert failed and
automation_status records did not store. Widen it to text. Additive to behaviour;
the verbatim payload column already held the value.
"""
import sqlalchemy as sa
from alembic import op

revision = "a1c2e3f4b5d6"
down_revision = "a6b7c8d9e0f1"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        "automation_status_records", "config_revision",
        existing_type=sa.BigInteger(), type_=sa.String(64), existing_nullable=True,
        postgresql_using="config_revision::text",
    )


def downgrade():
    op.alter_column(
        "automation_status_records", "config_revision",
        existing_type=sa.String(64), type_=sa.BigInteger(), existing_nullable=True,
        postgresql_using="config_revision::bigint",
    )
