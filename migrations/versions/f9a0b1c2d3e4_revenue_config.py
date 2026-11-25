"""revenue config

Revision ID: f9a0b1c2d3e4
Revises: e8f9a0b1c2d3
Create Date: 2026-11-25 16:30:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = 'f9a0b1c2d3e4'
down_revision: Union[str, None] = 'e8f9a0b1c2d3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'revenue_config',
        sa.Column('gateway_id', sa.UUID(), nullable=False),
        sa.Column('state', sa.String(length=64), nullable=True),
        sa.Column('charge_source', sa.String(length=16), server_default='solar', nullable=False),
        sa.Column('charge_cost_rs', sa.Float(), nullable=False),
        sa.Column('replaced_source', sa.String(length=16), server_default='grid', nullable=False),
        sa.Column('discharge_cost_rs', sa.Float(), nullable=False),
        sa.Column('base_energy_charge_rs', sa.Float(), nullable=True),
        sa.Column('solar_charge_value_rs', sa.Float(), nullable=True),
        sa.Column('slots', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('updated_by', sa.String(length=128), nullable=True),
        sa.Column('id', sa.UUID(), nullable=False),
        sa.Column('is_active', sa.Boolean(), server_default='true', nullable=False),
        sa.Column('is_deleted', sa.Boolean(), server_default='false', nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('gateway_id'),
    )
    op.create_index('ix_revenue_config_gateway_id', 'revenue_config', ['gateway_id'])


def downgrade() -> None:
    op.drop_index('ix_revenue_config_gateway_id', table_name='revenue_config')
    op.drop_table('revenue_config')
