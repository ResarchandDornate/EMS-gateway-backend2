"""site profile: state + pincode (revenue auto-fill)

Revision ID: a6b7c8d9e0f1
Revises: f5a6b7c8d9e0
Create Date: 2026-11-26 13:30:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = 'a6b7c8d9e0f1'
down_revision: Union[str, None] = 'f5a6b7c8d9e0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('site_profiles', sa.Column('state', sa.String(length=64), nullable=True))
    op.add_column('site_profiles', sa.Column('pincode', sa.String(length=16), nullable=True))


def downgrade() -> None:
    op.drop_column('site_profiles', 'pincode')
    op.drop_column('site_profiles', 'state')
