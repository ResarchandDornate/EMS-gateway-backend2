"""unique gateway client_id

Revision ID: b2c3d4e5f6a7
Revises: 6817c26f2b7e
Create Date: 2026-10-22

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "b2c3d4e5f6a7"
down_revision = "6817c26f2b7e"
branch_labels = None
depends_on = None


def upgrade():
    op.create_unique_constraint("uq_gateways_client_id", "gateways", ["client_id"])


def downgrade():
    op.drop_constraint("uq_gateways_client_id", "gateways", type_="unique")
