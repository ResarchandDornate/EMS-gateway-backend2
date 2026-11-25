"""pending command queue (CG-26/25/27)

Revision ID: d3e4f5a6b7c8
Revises: a1b2c3d4e5f9
Create Date: 2026-11-25 18:30:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = 'd3e4f5a6b7c8'
down_revision: Union[str, None] = 'a1b2c3d4e5f9'
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
        'pending_commands',
        sa.Column('command_id', sa.UUID(), nullable=False),
        sa.Column('gateway_id', sa.UUID(), nullable=False),
        sa.Column('organization_id', sa.UUID(), nullable=False),
        sa.Column('site_id', sa.UUID(), nullable=False),
        sa.Column('command_type', sa.String(length=64), nullable=False),
        sa.Column('arguments', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('requires_readback', sa.Boolean(), server_default='true', nullable=False),
        sa.Column('requested_by', sa.UUID(), nullable=False),
        sa.Column('requested_role', sa.String(length=32), nullable=False),
        sa.Column('correlation_id', sa.String(length=64), nullable=True),
        sa.Column('issued_at_utc', sa.DateTime(timezone=True), nullable=False),
        sa.Column('expires_at_utc', sa.DateTime(timezone=True), nullable=False),
        sa.Column('status', sa.String(length=16), server_default='pending', nullable=False),
        sa.Column('served_at_utc', sa.DateTime(timezone=True), nullable=True),
        sa.Column('closed_at_utc', sa.DateTime(timezone=True), nullable=True),
        sa.Column('final_status', sa.String(length=16), nullable=True),
        sa.Column('validation_reason', sa.Text(), nullable=True),
        sa.Column('gateway_error_code', sa.String(length=64), nullable=True),
        *_base_cols(),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('command_id'),
    )
    op.create_index('ix_pending_commands_command_id', 'pending_commands', ['command_id'])
    op.create_index('ix_pending_commands_gateway_id', 'pending_commands', ['gateway_id'])
    op.create_index('ix_pending_commands_organization_id', 'pending_commands', ['organization_id'])
    op.create_index('ix_pending_commands_site_id', 'pending_commands', ['site_id'])
    op.create_index('ix_pending_commands_command_type', 'pending_commands', ['command_type'])
    op.create_index('ix_pending_commands_correlation_id', 'pending_commands', ['correlation_id'])
    op.create_index('ix_pending_commands_issued_at', 'pending_commands', ['issued_at_utc'])
    op.create_index('ix_pending_commands_expires_at', 'pending_commands', ['expires_at_utc'])
    op.create_index('ix_pending_commands_status', 'pending_commands', ['status'])


def downgrade() -> None:
    op.drop_table('pending_commands')
