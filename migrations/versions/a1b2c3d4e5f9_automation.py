"""automation status and events (CG-28)

Revision ID: a1b2c3d4e5f9
Revises: f9a0b1c2d3e4
Create Date: 2026-11-25 17:30:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = 'a1b2c3d4e5f9'
down_revision: Union[str, None] = 'f9a0b1c2d3e4'
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
        'automation_status_records',
        sa.Column('gateway_id', sa.UUID(), nullable=False),
        sa.Column('organization_id', sa.UUID(), nullable=False),
        sa.Column('site_id', sa.UUID(), nullable=False),
        sa.Column('message_id', sa.UUID(), nullable=False),
        sa.Column('record_index', sa.Integer(), nullable=False),
        sa.Column('automation_enabled', sa.Boolean(), nullable=True),
        sa.Column('config_revision', sa.BigInteger(), nullable=True),
        sa.Column('supervisor_alive', sa.Boolean(), nullable=True),
        sa.Column('phase', sa.String(length=32), nullable=True),
        sa.Column('direction', sa.String(length=16), nullable=True),
        sa.Column('fault_latched', sa.Boolean(), nullable=True),
        sa.Column('safe_stop_requested', sa.Boolean(), nullable=True),
        sa.Column('sample_time_utc', sa.DateTime(timezone=True), nullable=True),
        sa.Column('received_at_utc', sa.DateTime(timezone=True), nullable=False),
        sa.Column('payload', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        *_base_cols(),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('gateway_id', 'message_id', 'record_index'),
    )
    op.create_index('ix_automation_status_gateway_id', 'automation_status_records', ['gateway_id'])
    op.create_index('ix_automation_status_site_id', 'automation_status_records', ['site_id'])
    op.create_index('ix_automation_status_received_at', 'automation_status_records', ['received_at_utc'])

    op.create_table(
        'automation_event_records',
        sa.Column('gateway_id', sa.UUID(), nullable=False),
        sa.Column('organization_id', sa.UUID(), nullable=False),
        sa.Column('site_id', sa.UUID(), nullable=False),
        sa.Column('message_id', sa.UUID(), nullable=False),
        sa.Column('event_id', sa.String(length=128), nullable=False),
        sa.Column('event_type', sa.String(length=64), nullable=True),
        sa.Column('phase', sa.String(length=32), nullable=True),
        sa.Column('direction', sa.String(length=16), nullable=True),
        sa.Column('timestamp_utc', sa.DateTime(timezone=True), nullable=True),
        sa.Column('received_at_utc', sa.DateTime(timezone=True), nullable=False),
        sa.Column('payload', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        *_base_cols(),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('gateway_id', 'event_id'),
    )
    op.create_index('ix_automation_event_gateway_id', 'automation_event_records', ['gateway_id'])
    op.create_index('ix_automation_event_site_id', 'automation_event_records', ['site_id'])
    op.create_index('ix_automation_event_event_id', 'automation_event_records', ['event_id'])
    op.create_index('ix_automation_event_received_at', 'automation_event_records', ['received_at_utc'])


def downgrade() -> None:
    op.drop_table('automation_event_records')
    op.drop_table('automation_status_records')
