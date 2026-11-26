"""site registration detail (Site Config page)

Revision ID: e4f5a6b7c8d9
Revises: d3e4f5a6b7c8
Create Date: 2026-11-26 11:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = 'e4f5a6b7c8d9'
down_revision: Union[str, None] = 'd3e4f5a6b7c8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _base_cols():
    return [
        sa.Column('id', sa.UUID(), nullable=False),
        sa.Column('is_active', sa.Boolean(), server_default='true', nullable=False),
        sa.Column('is_deleted', sa.Boolean(), server_default='false', nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    ]


def upgrade() -> None:
    op.create_table(
        'site_profiles',
        sa.Column('site_id', sa.UUID(), nullable=False),
        sa.Column('company_name', sa.String(length=255), nullable=True),
        sa.Column('address', sa.Text(), nullable=True),
        sa.Column('latitude', sa.Float(), nullable=True),
        sa.Column('longitude', sa.Float(), nullable=True),
        sa.Column('solar_capacity_kw', sa.Float(), nullable=True),
        sa.Column('connection_voltage_kv', sa.Float(), nullable=True),
        sa.Column('battery_capacity_kwh', sa.Float(), nullable=True),
        sa.Column('pcs_capacity_kw', sa.Float(), nullable=True),
        sa.Column('comm_pcs_ems', sa.Boolean(), server_default='false', nullable=False),
        sa.Column('comm_pcs_battery', sa.Boolean(), server_default='false', nullable=False),
        sa.Column('comm_battery_ems', sa.Boolean(), server_default='false', nullable=False),
        sa.Column('comm_ems_meter_ems', sa.Boolean(), server_default='false', nullable=False),
        sa.Column('comm_solar_inverter_ems', sa.Boolean(), server_default='false', nullable=False),
        sa.Column('updated_by', sa.String(length=128), nullable=True),
        *_base_cols(),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('site_id', name='uq_site_profile_site'),
    )
    op.create_index('ix_site_profiles_site_id', 'site_profiles', ['site_id'])


def downgrade() -> None:
    op.drop_table('site_profiles')
