# UnityESS Central EMS — Backend: Code & Architecture Reference

_Generated 2026-10-28 from the live `new-ems/backend` repository. Secrets and `.env` are never included; `config.py` shows field declarations and non-secret defaults only._

## 1. Architecture overview

**Stack.** FastAPI (async) + SQLAlchemy async (asyncpg) over PostgreSQL for the relational/control plane; InfluxDB 2.7 for time-series telemetry; Redis (redis.asyncio) for the rate-limiter token bucket and pub/sub. Alembic manages the Postgres schema.

**Shape.** A per-domain module layout repeated throughout `src/`: each domain has `models.py` (ORM), `repository.py` (data access), `service.py` (domain logic), `schemas.py` (Pydantic I/O), and `router.py` (HTTP). Cross-cutting concerns live at the top level (`config.py`, `database.py`, `middleware.py`, `logging.py`, `ratelimit.py`, `influx.py`).

**Two layers.** (a) A built, tested *foundation*: identity/JWT (`auth`), multi-tenancy models (`tenancy`), onboarding API (`accounts`), RBAC (`rbac`), gateway identity (`gateway`), telemetry ingestion entry + idempotency ledger (`ingestion`), telemetry read API (`telemetry`), the signal registry (`signals`), and a shadow-first rate limiter (`ratelimit`). (b) A scaffolded *feature layer* (empty 0-byte stubs, not mounted): `analytics`, `financials`, `fleet`, `strategy`, `genie`, `notifications`, `realtime`, `events/*`, the `streams/s1..s9` decoders, and the async `workers/*` pipeline.

**Request/data flow.** Gateways authenticate at `/api/v1/gateway/token` (JWT aud=`gateway`), then POST telemetry to `/api/v1/gateway/ingest` (gated by `INGEST_ENABLED`, idempotency ledger prevents double-apply). Users authenticate via `/api/v1/auth/login` (super-admin password) or `/api/v1/auth/otp/{request,verify}` (org_admin / site_operator phone OTP; JWT aud=`user`, 15-min). Admin onboarding (orgs, operators, gateways) is under `/api/v1/admin/*` behind RBAC. Telemetry is read back (read-only) at `/api/v1/telemetry/{latest,series,aggregate}`.

**Rollout gates (all OFF by default).** `INGEST_ENABLED=False`, `rate_limit_enabled=False`, `rate_limit_enforce=False` — the live ingestion path and rate limiting are inert until explicitly enabled.

## 2. Repository structure

```
src/
  __init__.py
  config.py
  database.py
  influx.py
  influx_provision.py
  logging.py
  main.py
  middleware.py
  models.py
  ratelimit.py
  redis_client.py
  telemetry_errors.py
  accounts/
    __init__.py
    gateways_router.py
    models.py
    operators_router.py
    repository.py
    router.py
    schemas.py
    service.py
  analytics/
    __init__.py
    calculations.py
    models.py
    repository.py
    router.py
    schemas.py
    service.py
  auth/
    __init__.py
    models.py
    otp.py
    otp_provider.py
    rate_limit.py
    repository.py
    router.py
    schemas.py
    security.py
    service.py
    tokens.py
  events/
    __init__.py
    alarms/
      __init__.py
      models.py
      repository.py
      router.py
      schemas.py
      service.py
    command_results/
      __init__.py
      models.py
      repository.py
      router.py
      schemas.py
      service.py
    config_audit/
      __init__.py
      models.py
      repository.py
      router.py
      schemas.py
      service.py
  financials/
    __init__.py
    models.py
    repository.py
    router.py
    schemas.py
    service.py
  fleet/
    __init__.py
    models.py
    repository.py
    router.py
    schemas.py
    service.py
  gateway/
    __init__.py
    dependencies.py
    router.py
    schemas.py
    service.py
  genie/
    __init__.py
    exports.py
    models.py
    repository.py
    router.py
    schemas.py
    service.py
  ingestion/
    __init__.py
    body.py
    gateway_auth.py
    idempotency.py
    mapping.py
    models.py
    providers.py
    queue.py
    repository.py
    router.py
    schemas.py
    service.py
  notifications/
    __init__.py
    models.py
    push.py
    repository.py
    router.py
    schemas.py
    service.py
  rbac/
    __init__.py
    dependencies.py
    models.py
    permissions.py
    repository.py
    router.py
    schemas.py
    service.py
  realtime/
    __init__.py
    pubsub.py
    router.py
  signals/
    __init__.py
    models.py
    repository.py
    router.py
    schemas.py
    service.py
    soc.py
    types.py
    seed/
      signal_registry.csv
  strategy/
    __init__.py
    models.py
    repository.py
    router.py
    schemas.py
    service.py
  streams/
    __init__.py
    d1_commands.py
    envelope.py
    s1_fast_bess.py
    s2_general.py
    s3_gateway_health.py
    s4_alarms.py
    s5_controller.py
    s6_solis.py
    s7_edge_ai.py
    s8_config_audit.py
    s9_command_results.py
  telemetry/
    __init__.py
    dependencies.py
    models.py
    providers.py
    queries.py
    repository.py
    router.py
    schemas.py
    service.py
  tenancy/
    __init__.py
    bess_units/
      __init__.py
      models.py
      repository.py
      router.py
      schemas.py
      service.py
    gateways/
      __init__.py
      models.py
      repository.py
      router.py
      schemas.py
      service.py
    organizations/
      __init__.py
      models.py
      repository.py
      router.py
      schemas.py
      service.py
    sites/
      __init__.py
      models.py
      repository.py
      router.py
      schemas.py
      service.py
  workers/
    __init__.py
    __main__.py
    consumer.py
    dlq.py
    writers/
      __init__.py
      influx_writer.py
      postgres_writer.py
```

### Top-level files

```
.env
.env.example
.gitignore
README.md
alembic.ini
docker-compose.yml
preflight_check.py
pyproject.toml
requirements.lock.txt
```

## 3. Alembic migrations

### `migrations/versions/2e8049c791ba_auth_and_rbac_tables.py`

```python
"""auth and rbac tables

Revision ID: 2e8049c791ba
Revises: 723d4c5ce073
Create Date: 2026-10-21 12:05:50.156842
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '2e8049c791ba'
down_revision: Union[str, None] = '723d4c5ce073'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('permissions',
    sa.Column('name', sa.String(length=64), nullable=False),
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('is_active', sa.Boolean(), server_default='true', nullable=False),
    sa.Column('is_deleted', sa.Boolean(), server_default='false', nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('name')
    )
    op.create_table('roles',
    sa.Column('name', sa.String(length=32), nullable=False),
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('is_active', sa.Boolean(), server_default='true', nullable=False),
    sa.Column('is_deleted', sa.Boolean(), server_default='false', nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('name')
    )
    op.create_table('organization_permissions',
    sa.Column('organization_id', sa.UUID(), nullable=False),
    sa.Column('permission', sa.String(length=64), nullable=False),
    sa.Column('allowed', sa.Boolean(), server_default='false', nullable=False),
    sa.Column('updated_by', sa.UUID(), nullable=True),
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('is_active', sa.Boolean(), server_default='true', nullable=False),
    sa.Column('is_deleted', sa.Boolean(), server_default='false', nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('organization_id', 'permission')
    )
    op.create_index(op.f('ix_organization_permissions_organization_id'), 'organization_permissions', ['organization_id'], unique=False)
    op.create_table('role_permissions',
    sa.Column('role_id', sa.UUID(), nullable=False),
    sa.Column('permission_id', sa.UUID(), nullable=False),
    sa.ForeignKeyConstraint(['permission_id'], ['permissions.id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['role_id'], ['roles.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('role_id', 'permission_id')
    )
    op.create_table('users',
    sa.Column('email', sa.String(length=255), nullable=True),
    sa.Column('phone', sa.String(length=20), nullable=True),
    sa.Column('password_hash', sa.String(length=255), nullable=True),
    sa.Column('organization_id', sa.UUID(), nullable=True),
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('is_active', sa.Boolean(), server_default='true', nullable=False),
    sa.Column('is_deleted', sa.Boolean(), server_default='false', nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('email'),
    sa.UniqueConstraint('phone')
    )
    op.create_index(op.f('ix_users_organization_id'), 'users', ['organization_id'], unique=False)
    op.create_table('admin_audit',
    sa.Column('actor_user_id', sa.UUID(), nullable=True),
    sa.Column('action', sa.String(length=64), nullable=False),
    sa.Column('target_type', sa.String(length=64), nullable=True),
    sa.Column('target_id', sa.UUID(), nullable=True),
    sa.Column('old_value', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('new_value', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('is_active', sa.Boolean(), server_default='true', nullable=False),
    sa.Column('is_deleted', sa.Boolean(), server_default='false', nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['actor_user_id'], ['users.id'], ondelete='SET NULL'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_admin_audit_actor_user_id'), 'admin_audit', ['actor_user_id'], unique=False)
    op.create_table('auth_tokens',
    sa.Column('user_id', sa.UUID(), nullable=False),
    sa.Column('kind', sa.String(length=16), nullable=False),
    sa.Column('token_hash', sa.String(length=255), nullable=False),
    sa.Column('expires_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('used_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('is_active', sa.Boolean(), server_default='true', nullable=False),
    sa.Column('is_deleted', sa.Boolean(), server_default='false', nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_auth_tokens_user_id'), 'auth_tokens', ['user_id'], unique=False)
    op.create_table('user_roles',
    sa.Column('user_id', sa.UUID(), nullable=False),
    sa.Column('role_id', sa.UUID(), nullable=False),
    sa.ForeignKeyConstraint(['role_id'], ['roles.id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('user_id', 'role_id')
    )
    op.create_table('user_site_access',
    sa.Column('user_id', sa.UUID(), nullable=False),
    sa.Column('site_id', sa.UUID(), nullable=False),
    sa.ForeignKeyConstraint(['site_id'], ['sites.id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('user_id', 'site_id')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('user_site_access')
    op.drop_table('user_roles')
    op.drop_index(op.f('ix_auth_tokens_user_id'), table_name='auth_tokens')
    op.drop_table('auth_tokens')
    op.drop_index(op.f('ix_admin_audit_actor_user_id'), table_name='admin_audit')
    op.drop_table('admin_audit')
    op.drop_index(op.f('ix_users_organization_id'), table_name='users')
    op.drop_table('users')
    op.drop_table('role_permissions')
    op.drop_index(op.f('ix_organization_permissions_organization_id'), table_name='organization_permissions')
    op.drop_table('organization_permissions')
    op.drop_table('roles')
    op.drop_table('permissions')
    # ### end Alembic commands ###
```

### `migrations/versions/6817c26f2b7e_otp_challenges.py`

```python
"""otp challenges

Revision ID: 6817c26f2b7e
Revises: 2e8049c791ba
Create Date: 2026-10-21 16:53:47.815492
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '6817c26f2b7e'
down_revision: Union[str, None] = '2e8049c791ba'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('otp_challenges',
    sa.Column('user_id', sa.UUID(), nullable=False),
    sa.Column('channel', sa.String(length=8), nullable=False),
    sa.Column('destination', sa.String(length=255), nullable=False),
    sa.Column('otp_hash', sa.String(length=255), nullable=False),
    sa.Column('expires_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('attempts', sa.Integer(), server_default='0', nullable=False),
    sa.Column('consumed_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('is_active', sa.Boolean(), server_default='true', nullable=False),
    sa.Column('is_deleted', sa.Boolean(), server_default='false', nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_otp_challenges_expires_at'), 'otp_challenges', ['expires_at'], unique=False)
    op.create_index(op.f('ix_otp_challenges_user_id'), 'otp_challenges', ['user_id'], unique=False)
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_otp_challenges_user_id'), table_name='otp_challenges')
    op.drop_index(op.f('ix_otp_challenges_expires_at'), table_name='otp_challenges')
    op.drop_table('otp_challenges')
    # ### end Alembic commands ###
```

### `migrations/versions/723d4c5ce073_tenancy_tables.py`

```python
"""tenancy tables

Revision ID: 723d4c5ce073
Revises: 
Create Date: 2026-10-21 11:54:21.119269
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '723d4c5ce073'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('organizations',
    sa.Column('org_key', sa.String(length=64), nullable=False),
    sa.Column('name', sa.String(length=255), nullable=False),
    sa.Column('status', sa.String(length=32), server_default='active', nullable=False),
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('is_active', sa.Boolean(), server_default='true', nullable=False),
    sa.Column('is_deleted', sa.Boolean(), server_default='false', nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('org_key')
    )
    op.create_table('sites',
    sa.Column('organization_id', sa.UUID(), nullable=False),
    sa.Column('site_key', sa.String(length=64), nullable=False),
    sa.Column('name', sa.String(length=255), nullable=False),
    sa.Column('timezone', sa.String(length=64), server_default='UTC', nullable=False),
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('is_active', sa.Boolean(), server_default='true', nullable=False),
    sa.Column('is_deleted', sa.Boolean(), server_default='false', nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('organization_id', 'site_key')
    )
    op.create_index(op.f('ix_sites_organization_id'), 'sites', ['organization_id'], unique=False)
    op.create_table('gateways',
    sa.Column('site_id', sa.UUID(), nullable=False),
    sa.Column('gateway_key', sa.String(length=64), nullable=False),
    sa.Column('client_id', sa.String(length=128), nullable=False),
    sa.Column('client_secret_hash', sa.String(length=255), nullable=False),
    sa.Column('enabled', sa.Boolean(), server_default='true', nullable=False),
    sa.Column('software_version', sa.String(length=64), nullable=True),
    sa.Column('last_seen_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('is_active', sa.Boolean(), server_default='true', nullable=False),
    sa.Column('is_deleted', sa.Boolean(), server_default='false', nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['site_id'], ['sites.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('gateway_key')
    )
    op.create_index(op.f('ix_gateways_site_id'), 'gateways', ['site_id'], unique=False)
    op.create_table('bess_units',
    sa.Column('gateway_id', sa.UUID(), nullable=False),
    sa.Column('bess_key', sa.String(length=64), nullable=False),
    sa.Column('name', sa.String(length=255), nullable=False),
    sa.Column('rated_kw', sa.Float(), nullable=True),
    sa.Column('rated_kwh', sa.Float(), nullable=True),
    sa.Column('chemistry', sa.String(length=32), nullable=True),
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('is_active', sa.Boolean(), server_default='true', nullable=False),
    sa.Column('is_deleted', sa.Boolean(), server_default='false', nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['gateway_id'], ['gateways.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('gateway_id', 'bess_key')
    )
    op.create_index(op.f('ix_bess_units_gateway_id'), 'bess_units', ['gateway_id'], unique=False)
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_bess_units_gateway_id'), table_name='bess_units')
    op.drop_table('bess_units')
    op.drop_index(op.f('ix_gateways_site_id'), table_name='gateways')
    op.drop_table('gateways')
    op.drop_index(op.f('ix_sites_organization_id'), table_name='sites')
    op.drop_table('sites')
    op.drop_table('organizations')
    # ### end Alembic commands ###
```

### `migrations/versions/b2c3d4e5f6a7_unique_gateway_client_id.py`

```python
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

```

### `migrations/versions/c3d4e5f6a7b8_signal_registry.py`

```python
"""signal_registry

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-10-22

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "c3d4e5f6a7b8"
down_revision = "b2c3d4e5f6a7"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "signal_registry",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("is_deleted", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("point_id", sa.String(64), nullable=True),
        sa.Column("source", sa.String(16), nullable=False),
        sa.Column("primary_stream", sa.String(8), nullable=False),
        sa.Column("substream", sa.String(32), nullable=False),
        sa.Column("asset_type", sa.String(32), nullable=True),
        sa.Column("signal_key", sa.String(160), nullable=False),
        sa.Column("display_name", sa.String(255), nullable=True),
        sa.Column("unit", sa.String(32), nullable=True),
        sa.Column("category", sa.String(64), nullable=True),
        sa.Column("data_type", sa.String(32), nullable=True),
        sa.Column("policy_mode", sa.String(32), nullable=True),
        sa.Column("cadence_sec", sa.Integer(), nullable=True),
        sa.Column("deadband", sa.String(32), nullable=True),
        sa.Column("policy_group", sa.String(64), nullable=True),
        sa.Column("retention_class", sa.String(16), nullable=True),
        sa.Column("event_eligible", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("rw", sa.String(8), nullable=True),
        sa.Column("protocol_ref", sa.String(200), nullable=True),
        sa.Column("source_ref", sa.String(128), nullable=True),
        sa.Column("enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("primary_stream", "substream", "signal_key",
                            name="uq_signal_registry_point"),
    )
    op.create_index("ix_signal_registry_point_id", "signal_registry", ["point_id"])
    op.create_index("ix_signal_registry_primary_stream", "signal_registry", ["primary_stream"])
    op.create_index("ix_signal_registry_substream", "signal_registry", ["substream"])
    op.create_index("ix_signal_registry_signal_key", "signal_registry", ["signal_key"])
    op.create_index("ix_signal_registry_retention_class", "signal_registry", ["retention_class"])


def downgrade():
    op.drop_table("signal_registry")

```

### `migrations/versions/d4e5f6a7b8c9_ingest_messages.py`

```python
"""ingest_messages

Revision ID: d4e5f6a7b8c9
Revises: c3d4e5f6a7b8
Create Date: 2026-10-23

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "d4e5f6a7b8c9"
down_revision = "c3d4e5f6a7b8"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "ingest_messages",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("is_deleted", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("gateway_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("message_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("stream", sa.String(8), nullable=False),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("record_count", sa.Integer(), nullable=False),
        sa.Column("payload_hash", sa.String(64), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("lease_owner", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("stored_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["gateway_id"], ["gateways.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("gateway_id", "message_id", name="uq_ingest_gateway_message"),
    )
    op.create_index(op.f("ix_ingest_messages_gateway_id"), "ingest_messages", ["gateway_id"], unique=False)


def downgrade():
    op.drop_table("ingest_messages")

```

## 4. Implemented source (`src/`)

_52 non-empty files below; 126 empty scaffold files listed in §5._

### `src/accounts/gateways_router.py`

```python
import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from src.accounts import service
from src.accounts.schemas import GatewayCreate, GatewayCreated, GatewayOut
from src.database import get_session
from src.rbac.dependencies import require_permission
from src.rbac.service import get_user_sites

router = APIRouter(prefix="/api/v1/admin", tags=["gateways"])


async def _check_site_scope(session, user, site_id):
    site = await service.get_site(session, site_id)
    if site is None:
        raise HTTPException(status_code=404, detail="Site not found")
    sites = await get_user_sites(session, user)         # None = super admin (all sites)
    if sites is not None and site_id not in sites:
        raise HTTPException(status_code=403, detail="Site access denied")
    return site


@router.post("/sites/{site_id}/gateways", response_model=GatewayCreated, status_code=201)
async def register_gateway(
    site_id: uuid.UUID,
    data: GatewayCreate,
    user=Depends(require_permission("site:manage")),
    session: AsyncSession = Depends(get_session),
):
    site = await _check_site_scope(session, user, site_id)
    gateway, secret, error = await service.register_gateway(session, user, site, data)
    if error == "duplicate":
        raise HTTPException(status_code=409, detail="Gateway key already exists")
    return GatewayCreated(
        id=gateway.id, site_id=gateway.site_id, gateway_key=gateway.gateway_key,
        client_id=gateway.client_id, enabled=gateway.enabled, client_secret=secret,
    )


@router.get("/sites/{site_id}/gateways", response_model=list[GatewayOut])
async def list_gateways(
    site_id: uuid.UUID,
    user=Depends(require_permission("site:manage")),
    session: AsyncSession = Depends(get_session),
):
    await _check_site_scope(session, user, site_id)
    gateways = await service.list_gateways(session, site_id)
    return [
        GatewayOut(id=g.id, site_id=g.site_id, gateway_key=g.gateway_key,
                   client_id=g.client_id, enabled=g.enabled, last_seen_at=g.last_seen_at)
        for g in gateways
    ]
```

### `src/accounts/operators_router.py`

```python
import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from src.accounts import service
from src.accounts.schemas import OperatorCreate, OperatorDetail
from src.database import get_session
from src.rbac.dependencies import require_permission

router = APIRouter(prefix="/api/v1/org", tags=["operators"])


def _require_org(user):
    if user.organization_id is None:
        raise HTTPException(status_code=403, detail="Only an organization admin can manage operators")


@router.post("/operators", response_model=OperatorDetail, status_code=201)
async def create_operator(
    data: OperatorCreate,
    user=Depends(require_permission("user:manage")),
    session: AsyncSession = Depends(get_session),
):
    _require_org(user)
    operator, error = await service.create_operator(session, user, data)
    if error == "limit":
        raise HTTPException(status_code=409, detail="Maximum of 2 active operators reached")
    if error == "bad_site":
        raise HTTPException(status_code=400, detail="One or more sites do not belong to your organization")
    if error == "phone":
        raise HTTPException(status_code=409, detail="Phone number already registered")
    if error == "email":
        raise HTTPException(status_code=409, detail="Email already registered")

    site_ids = await service.operator_site_ids(session, operator.id)
    return OperatorDetail(
        id=operator.id, phone=operator.phone, email=operator.email,
        is_active=operator.is_active, site_ids=site_ids,
    )


@router.get("/operators", response_model=list[OperatorDetail])
async def list_operators(
    user=Depends(require_permission("user:manage")),
    session: AsyncSession = Depends(get_session),
):
    _require_org(user)
    operators = await service.get_operators(session, user.organization_id)
    result = []
    for op in operators:
        site_ids = await service.operator_site_ids(session, op.id)
        result.append(OperatorDetail(
            id=op.id, phone=op.phone, email=op.email,
            is_active=op.is_active, site_ids=site_ids,
        ))
    return result


@router.post("/operators/{operator_id}/deactivate")
async def deactivate_operator(
    operator_id: uuid.UUID,
    user=Depends(require_permission("user:manage")),
    session: AsyncSession = Depends(get_session),
):
    _require_org(user)
    error = await service.deactivate_operator(session, user, operator_id)
    if error == "not_found":
        raise HTTPException(status_code=404, detail="Operator not found")
    if error == "already_inactive":
        raise HTTPException(status_code=409, detail="Operator is already inactive")
    return {"status": "deactivated"}
```

### `src/accounts/router.py`

```python
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from src.accounts import service
from src.accounts.schemas import (
    OperatorOut,
    OrganizationCreate,
    OrganizationDetail,
    OrganizationOut,
    SiteOut,
)
from src.database import get_session
from src.rbac.dependencies import require_permission

router = APIRouter(prefix="/api/v1/admin", tags=["accounts"])


@router.post("/organizations", response_model=OrganizationOut, status_code=201)
async def create_organization(
    data: OrganizationCreate,
    user=Depends(require_permission("org:manage")),
    session: AsyncSession = Depends(get_session),
):
    # intra-request duplicate site keys (already normalized by the schema) -> 409,
    # rejected BEFORE any write (no partial rows). We reject rather than drop a site.
    site_keys = [s.site_key for s in data.sites]
    if len(site_keys) != len(set(site_keys)):
        raise HTTPException(status_code=409, detail="Duplicate site key in request")

    if await service.org_key_exists(session, data.org_key):
        raise HTTPException(status_code=409, detail="Organization key already exists")
    if await service.phone_exists(session, data.admin_phone):
        raise HTTPException(status_code=409, detail="Phone number already registered")

    try:
        org, admin, sites = await service.create_client(session, user.id, data)
    except service.ConflictError as e:               # unique violation that raced the pre-check
        raise HTTPException(status_code=409, detail=e.detail)

    return OrganizationOut(
        id=org.id,
        org_key=org.org_key,
        name=org.name,
        admin_user_id=admin.id,
        admin_phone=admin.phone,
        sites=[SiteOut(id=s.id, site_key=s.site_key, name=s.name) for s in sites],
    )


@router.get("/organizations", response_model=list[OrganizationDetail])
async def list_organizations(
    user=Depends(require_permission("org:manage")),
    session: AsyncSession = Depends(get_session),
):
    orgs = await service.list_clients(session)
    result = []
    for org in orgs:
        sites = await service.get_sites(session, org.id)
        operators = await service.get_operators(session, org.id)
        result.append(OrganizationDetail(
            id=org.id,
            org_key=org.org_key,
            name=org.name,
            sites=[SiteOut(id=s.id, site_key=s.site_key, name=s.name) for s in sites],
            operators=[OperatorOut(id=o.id, phone=o.phone, email=o.email) for o in operators],
        ))
    return result

```

### `src/accounts/schemas.py`

```python
import uuid
from datetime import datetime

from pydantic import BaseModel, EmailStr, field_validator


class SiteIn(BaseModel):
    site_key: str
    name: str

    @field_validator("site_key")
    @classmethod
    def _norm_site_key(cls, v: str) -> str:
        return v.strip().lower()          # trim + lowercase (identity)

    @field_validator("name")
    @classmethod
    def _norm_site_name(cls, v: str) -> str:
        return v.strip()                  # display label: trim only


class OrganizationCreate(BaseModel):
    org_key: str
    name: str
    admin_phone: str
    admin_email: EmailStr | None = None
    sites: list[SiteIn] = []

    @field_validator("org_key")
    @classmethod
    def _norm_org_key(cls, v: str) -> str:
        return v.strip().lower()          # trim + lowercase (identity)

    @field_validator("name")
    @classmethod
    def _norm_org_name(cls, v: str) -> str:
        return v.strip()                  # display label: trim only


class SiteOut(BaseModel):
    id: uuid.UUID
    site_key: str
    name: str


class OperatorOut(BaseModel):
    id: uuid.UUID
    phone: str | None
    email: str | None


class OrganizationOut(BaseModel):
    id: uuid.UUID
    org_key: str
    name: str
    admin_user_id: uuid.UUID
    admin_phone: str
    sites: list[SiteOut]


class OrganizationDetail(BaseModel):
    id: uuid.UUID
    org_key: str
    name: str
    sites: list[SiteOut]
    operators: list[OperatorOut]


class OperatorCreate(BaseModel):
    phone: str
    email: EmailStr | None = None
    site_ids: list[uuid.UUID] = []


class OperatorDetail(BaseModel):
    id: uuid.UUID
    phone: str | None
    email: str | None
    is_active: bool
    site_ids: list[uuid.UUID]


class GatewayCreate(BaseModel):
    gateway_key: str
    software_version: str | None = None

    @field_validator("gateway_key")
    @classmethod
    def _norm_gateway_key(cls, v: str) -> str:
        return v.strip()                  # trim ONLY - case preserved (external id)


class GatewayOut(BaseModel):
    id: uuid.UUID
    site_id: uuid.UUID
    gateway_key: str
    client_id: str
    enabled: bool
    # Last SUCCESSFUL token issuance (interim liveness); NULL until the gateway first
    # obtains a token. Not telemetry time; not a live-connectivity guarantee.
    last_seen_at: datetime | None = None


class GatewayCreated(GatewayOut):
    client_secret: str   # returned ONCE, at registration

```

### `src/accounts/service.py`

```python
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
import secrets

from src.auth.security import hash_password
from src.tenancy.gateways.models import GatewayModel

from src.auth.models import AdminAuditModel, UserModel
from src.rbac.models import RoleModel, UserRoleModel, UserSiteAccessModel
from src.rbac.service import seed_org_permissions
from src.tenancy.organizations.models import OrganizationModel
from src.tenancy.sites.models import SiteModel


class ConflictError(Exception):
    """A duplicate-key conflict that must surface as HTTP 409."""
    def __init__(self, detail: str):
        self.detail = detail
        super().__init__(detail)


def _is_unique_violation(exc: IntegrityError) -> bool:
    """True iff exc is a Postgres UNIQUE violation (SQLSTATE 23505). Uses only the
    SQLSTATE the asyncpg adapter copies onto exc.orig (.sqlstate/.pgcode)."""
    orig = getattr(exc, "orig", None)
    code = getattr(orig, "sqlstate", None) or getattr(orig, "pgcode", None)
    return code == "23505"


def _unique_constraint_name(exc: IntegrityError) -> str | None:
    """The exact violated UNIQUE constraint name (lowercased) for a 23505 error, or None.

    The SQLAlchemy asyncpg dialect raises its wrapper with `raise translated_error from
    error`, so the ORIGINAL asyncpg error - which carries `.constraint_name` - is
    reachable through the cause/context chain. Returns None if it is not a unique
    violation OR the name cannot be recovered; callers then degrade safely (generic 409
    or re-raise), never guessing which key collided."""
    if not _is_unique_violation(exc):
        return None
    orig = getattr(exc, "orig", None)
    for obj in (orig,
                getattr(orig, "__cause__", None),
                getattr(orig, "__context__", None),
                getattr(exc, "__cause__", None)):
        name = getattr(obj, "constraint_name", None)
        if name:
            return name.lower()
    return None


async def get_role(session, name):
    result = await session.execute(select(RoleModel).where(RoleModel.name == name))
    return result.scalar_one()


async def org_key_exists(session, org_key):
    result = await session.execute(
        select(OrganizationModel).where(OrganizationModel.org_key == org_key)
    )
    return result.scalar_one_or_none() is not None


async def phone_exists(session, phone):
    result = await session.execute(select(UserModel).where(UserModel.phone == phone))
    return result.scalar_one_or_none() is not None


async def create_client(session, actor_id, data):
    try:
        # 1. organization
        org = OrganizationModel(org_key=data.org_key, name=data.name)
        session.add(org)
        await session.flush()

        # 2. give the org its permission rows at safe defaults
        await seed_org_permissions(session, org.id)

        # 3. org admin user (phone login, no password)
        admin = UserModel(
            phone=data.admin_phone,
            email=data.admin_email,
            organization_id=org.id,
        )
        session.add(admin)
        await session.flush()

        # 4. assign org_admin role
        role = await get_role(session, "org_admin")
        session.add(UserRoleModel(user_id=admin.id, role_id=role.id))

        # 5. sites for this org (keys normalized by the schema; intra-request dups are
        #    rejected in the router before this runs)
        sites = []
        for s in data.sites:
            site = SiteModel(organization_id=org.id, site_key=s.site_key, name=s.name)
            session.add(site)
            sites.append(site)
        await session.flush()

        # 6. audit
        session.add(AdminAuditModel(
            actor_user_id=actor_id,
            action="client_onboarded",
            target_type="organization",
            target_id=org.id,
            new_value={
                "org_key": org.org_key,
                "name": org.name,
                "admin_phone": admin.phone,
                "sites": [s.site_key for s in sites],
            },
        ))

        await session.commit()
        return org, admin, sites
    except IntegrityError as e:
        await session.rollback()                     # clean the session BEFORE reuse/return
        if _is_unique_violation(e):
            name = _unique_constraint_name(e) or ""
            if "org_key" in name:
                raise ConflictError("Organization key already exists")
            if "site_key" in name:
                raise ConflictError("Site key already exists in this organization")
            if "phone" in name:
                raise ConflictError("Phone number already registered")
            if "email" in name:
                raise ConflictError("Email already registered")
            raise ConflictError("Duplicate key conflict")   # unique but unnamed -> safe generic 409
        raise                                                # not a unique violation -> surface


async def list_clients(session):
    result = await session.execute(select(OrganizationModel))
    return result.scalars().all()


async def get_sites(session, org_id):
    result = await session.execute(
        select(SiteModel).where(SiteModel.organization_id == org_id)
    )
    return result.scalars().all()


async def get_operators(session, org_id):
    result = await session.execute(
        select(UserModel)
        .join(UserRoleModel, UserRoleModel.user_id == UserModel.id)
        .join(RoleModel, RoleModel.id == UserRoleModel.role_id)
        .where(UserModel.organization_id == org_id)
        .where(RoleModel.name == "site_operator")
    )
    return result.scalars().all()



MAX_ACTIVE_OPERATORS = 2


async def org_site_ids(session, org_id):
    result = await session.execute(
        select(SiteModel.id).where(SiteModel.organization_id == org_id)
    )
    return set(result.scalars())


async def count_active_operators(session, org_id):
    result = await session.execute(
        select(func.count())
        .select_from(UserModel)
        .join(UserRoleModel, UserRoleModel.user_id == UserModel.id)
        .join(RoleModel, RoleModel.id == UserRoleModel.role_id)
        .where(UserModel.organization_id == org_id)
        .where(RoleModel.name == "site_operator")
        .where(UserModel.is_active.is_(True))
    )
    return result.scalar_one()


async def operator_site_ids(session, user_id):
    result = await session.execute(
        select(UserSiteAccessModel.site_id).where(UserSiteAccessModel.user_id == user_id)
    )
    return list(result.scalars())


async def get_operator_in_org(session, org_id, operator_id):
    result = await session.execute(
        select(UserModel)
        .join(UserRoleModel, UserRoleModel.user_id == UserModel.id)
        .join(RoleModel, RoleModel.id == UserRoleModel.role_id)
        .where(UserModel.id == operator_id)
        .where(UserModel.organization_id == org_id)
        .where(RoleModel.name == "site_operator")
    )
    return result.scalar_one_or_none()


async def create_operator(session, org_admin, data):
    org_id = org_admin.organization_id

    # lock the org row so two concurrent creates can't both pass the cap
    await session.execute(
        select(OrganizationModel).where(OrganizationModel.id == org_id).with_for_update()
    )

    if await count_active_operators(session, org_id) >= MAX_ACTIVE_OPERATORS:
        return None, "limit"

    if not set(data.site_ids).issubset(await org_site_ids(session, org_id)):
        return None, "bad_site"

    if data.phone and await phone_exists(session, data.phone):
        return None, "phone"

    try:
        operator = UserModel(
            phone=data.phone,
            email=data.email,
            organization_id=org_id,
            is_active=True,
        )
        session.add(operator)
        await session.flush()

        role = await get_role(session, "site_operator")
        session.add(UserRoleModel(user_id=operator.id, role_id=role.id))

        for site_id in data.site_ids:
            session.add(UserSiteAccessModel(user_id=operator.id, site_id=site_id))

        session.add(AdminAuditModel(
            actor_user_id=org_admin.id,
            action="operator_created",
            target_type="user",
            target_id=operator.id,
            new_value={"phone": data.phone, "sites": [str(s) for s in data.site_ids]},
        ))

        await session.commit()
        return operator, None
    except IntegrityError as e:
        await session.rollback()
        name = _unique_constraint_name(e) or ""
        if "email" in name:                              # exact: never mislabels email as phone
            return None, "email"
        if "phone" in name:
            return None, "phone"
        raise                                            # unnamed / unrelated -> surface


async def deactivate_operator(session, org_admin, operator_id):
    operator = await get_operator_in_org(session, org_admin.organization_id, operator_id)
    if operator is None:
        return "not_found"
    if not operator.is_active:
        return "already_inactive"

    operator.is_active = False
    session.add(AdminAuditModel(
        actor_user_id=org_admin.id,
        action="operator_deactivated",
        target_type="user",
        target_id=operator.id,
        old_value={"is_active": True},
        new_value={"is_active": False},
    ))
    await session.commit()
    return None



async def get_site(session, site_id):
    result = await session.execute(select(SiteModel).where(SiteModel.id == site_id))
    return result.scalar_one_or_none()


async def gateway_key_exists(session, gateway_key):
    result = await session.execute(
        select(GatewayModel).where(GatewayModel.gateway_key == gateway_key)
    )
    return result.scalar_one_or_none() is not None


async def _unique_client_id(session):
    while True:
        client_id = "gw_" + secrets.token_hex(16)
        exists = await session.execute(
            select(GatewayModel).where(GatewayModel.client_id == client_id)
        )
        if exists.scalar_one_or_none() is None:
            return client_id


async def register_gateway(session, actor, site, data):
    if await gateway_key_exists(session, data.gateway_key):
        return None, None, "duplicate"

    client_id = await _unique_client_id(session)         # UNCHANGED: server-generated
    client_secret = secrets.token_urlsafe(32)

    try:
        gateway = GatewayModel(
            site_id=site.id,
            gateway_key=data.gateway_key,
            client_id=client_id,
            client_secret_hash=hash_password(client_secret),
            software_version=data.software_version,
            enabled=True,
        )
        session.add(gateway)
        await session.flush()

        session.add(AdminAuditModel(
            actor_user_id=actor.id,
            action="gateway_registered",
            target_type="gateway",
            target_id=gateway.id,
            new_value={
                "gateway_key": gateway.gateway_key,
                "site_id": str(site.id),
                "client_id": client_id,
            },
        ))

        await session.commit()
        return gateway, client_secret, None
    except IntegrityError as e:
        await session.rollback()
        name = _unique_constraint_name(e) or ""
        if "gateway_key" in name:                        # ONLY the expected key -> duplicate
            return None, None, "duplicate"
        raise                                            # client_id collision / unnamed / other -> surface


async def list_gateways(session, site_id):
    result = await session.execute(
        select(GatewayModel).where(GatewayModel.site_id == site_id)
    )
    return result.scalars().all()    
```

### `src/auth/models.py`

```python
import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.models import BaseEntityModel


class UserModel(BaseEntityModel):
    __tablename__ = "users"
    email: Mapped[str | None] = mapped_column(String(255), unique=True)
    phone: Mapped[str | None] = mapped_column(String(20), unique=True)
    password_hash: Mapped[str | None] = mapped_column(String(255))
    organization_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), index=True)


class AuthTokenModel(BaseEntityModel):
    __tablename__ = "auth_tokens"
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), index=True)
    kind: Mapped[str] = mapped_column(String(16))
    token_hash: Mapped[str] = mapped_column(String(255))
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AdminAuditModel(BaseEntityModel):
    __tablename__ = "admin_audit"
    actor_user_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), index=True)
    action: Mapped[str] = mapped_column(String(64))
    target_type: Mapped[str | None] = mapped_column(String(64))
    target_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    old_value: Mapped[dict | None] = mapped_column(JSONB)
    new_value: Mapped[dict | None] = mapped_column(JSONB)


class OtpChallengeModel(BaseEntityModel):
    __tablename__ = "otp_challenges"
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), index=True)
    channel: Mapped[str] = mapped_column(String(8))             # "sms" | "email"
    destination: Mapped[str] = mapped_column(String(255))
    otp_hash: Mapped[str] = mapped_column(String(255))          # hash only
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
```

### `src/auth/otp.py`

```python
import secrets
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.models import OtpChallengeModel
from src.auth.security import hash_password, verify_password

OTP_LENGTH = 6
OTP_TTL_MINUTES = 5
OTP_MAX_ATTEMPTS = 5


def generate_code(length: int = OTP_LENGTH) -> str:
    return "".join(secrets.choice("0123456789") for _ in range(length))


async def create_challenge(session: AsyncSession, *, user_id, channel: str, destination: str) -> str:
    code = generate_code()
    session.add(OtpChallengeModel(
        user_id=user_id, channel=channel, destination=destination,
        otp_hash=hash_password(code),
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=OTP_TTL_MINUTES)))
    await session.flush()
    return code


async def verify_code(session: AsyncSession, *, user_id, code: str) -> bool:
    now = datetime.now(timezone.utc)
    stmt = (select(OtpChallengeModel)
            .where(OtpChallengeModel.user_id == user_id,
                   OtpChallengeModel.consumed_at.is_(None),
                   OtpChallengeModel.expires_at > now)
            .order_by(OtpChallengeModel.created_at.desc())
            .with_for_update().limit(1))
    ch = (await session.execute(stmt)).scalar_one_or_none()
    if ch is None or ch.attempts >= OTP_MAX_ATTEMPTS:
        return False
    ch.attempts += 1
    if not verify_password(code, ch.otp_hash):
        await session.flush()
        return False
    ch.consumed_at = now          # single-use
    await session.flush()
    return True
```

### `src/auth/otp_provider.py`

```python
from abc import ABC, abstractmethod

from src.config import get_settings


class OtpDeliveryProvider(ABC):
    @abstractmethod
    async def send_sms(self, mobile: str, code: str) -> None: ...
    @abstractmethod
    async def send_email(self, email: str, code: str) -> None: ...


class MockOtpProvider(OtpDeliveryProvider):
    def __init__(self) -> None:
        self.sent: list[tuple[str, str, str]] = []

    async def send_sms(self, mobile: str, code: str) -> None:
        self.sent.append(("sms", mobile, code))

    async def send_email(self, email: str, code: str) -> None:
        self.sent.append(("email", email, code))


class Msg91OtpProvider(OtpDeliveryProvider):
    """PLACEHOLDER — complete after MSG91 is live (Auth Key, DLT template/sender,
    exact endpoints/params). Will use httpx + get_settings().msg91_*."""
    async def send_sms(self, mobile: str, code: str) -> None:
        raise NotImplementedError("MSG91 SMS pending account setup")
    async def send_email(self, email: str, code: str) -> None:
        raise NotImplementedError("MSG91 email pending account setup")


def get_otp_provider() -> OtpDeliveryProvider:
    if get_settings().otp_provider == "msg91":
        return Msg91OtpProvider()
    return MockOtpProvider()
```

### `src/auth/repository.py`

```python
import uuid

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.models import UserModel


class UserRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_identifier(self, identifier: str) -> UserModel | None:
        stmt = select(UserModel).where(
            or_(UserModel.email == identifier, UserModel.phone == identifier))
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def get_by_id(self, user_id: uuid.UUID) -> UserModel | None:
        return await self.session.get(UserModel, user_id)
```

### `src/auth/router.py`

```python
import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.otp import create_challenge, verify_code
from src.auth.otp_provider import OtpDeliveryProvider, get_otp_provider
from src.auth.repository import UserRepository
from src.auth.schemas import (LoginRequest, OtpRequestIn, OtpRequestOut,
                              OtpVerifyIn, TokenResponse, UserMe)
from src.auth.security import create_access_token, decode_access_token
from src.auth.service import (OTP_ROLES, authenticate, get_role_names,
                             resolve_channel)
from src.database import get_session

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])
_bearer = HTTPBearer(auto_error=True)


# --- Super Admin: password ---
@router.post("/login", response_model=TokenResponse)
async def login(body: LoginRequest, session: AsyncSession = Depends(get_session)):
    user = await authenticate(session, body.identifier, body.password)
    if user is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid credentials")
    return TokenResponse(access_token=create_access_token(user.id))


# --- Org Admin / Operator: phone OTP ---
@router.post("/otp/request", response_model=OtpRequestOut)
async def otp_request(
    body: OtpRequestIn,
    session: AsyncSession = Depends(get_session),
    provider: OtpDeliveryProvider = Depends(get_otp_provider),
):
    user = await UserRepository(session).get_by_identifier(body.identifier)
    if user is None or not user.is_active:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not registered")
    if not (await get_role_names(session, user.id) & OTP_ROLES):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "OTP login is not allowed for this account")
    resolved = resolve_channel(body.identifier, user)
    if resolved is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "No phone number registered for this account")
    channel, destination = resolved
    if channel == "sms":                      # phone only for now; email later
        code = await create_challenge(
            session, user_id=user.id, channel=channel, destination=destination)
        await session.commit()                       # persist challenge BEFORE any SMS I/O
        await provider.send_sms(destination, code)   # network I/O runs with no open transaction
    return OtpRequestOut()


@router.post("/otp/verify", response_model=TokenResponse)
async def otp_verify(body: OtpVerifyIn, session: AsyncSession = Depends(get_session)):
    user = await UserRepository(session).get_by_identifier(body.identifier)
    if user is None or not user.is_active:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not registered")
    if not (await get_role_names(session, user.id) & OTP_ROLES):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "OTP login is not allowed for this account")
    ok = await verify_code(session, user_id=user.id, code=body.otp)
    await session.commit()                    # persist attempts (wrong code) or consumption (success)
    if not ok:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid or expired OTP")
    return TokenResponse(access_token=create_access_token(user.id))


# --- shared: current user ---
async def get_current_user(
    creds: HTTPAuthorizationCredentials = Depends(_bearer),
    session: AsyncSession = Depends(get_session),
):
    sub = decode_access_token(creds.credentials)
    if sub is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid or expired token")
    user = await UserRepository(session).get_by_id(uuid.UUID(sub))
    if user is None or not user.is_active:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "User not found or inactive")
    return user


@router.get("/me", response_model=UserMe)
async def me(user=Depends(get_current_user)):
    return UserMe(id=str(user.id), email=user.email, phone=user.phone,
                  organization_id=str(user.organization_id) if user.organization_id else None)
```

### `src/auth/schemas.py`

```python
from pydantic import BaseModel


class LoginRequest(BaseModel):
    identifier: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class UserMe(BaseModel):
    id: str
    email: str | None = None
    phone: str | None = None
    organization_id: str | None = None


class OtpRequestIn(BaseModel):
    identifier: str          # registered phone (email later)


class OtpRequestOut(BaseModel):
    status: str = "otp_sent"


class OtpVerifyIn(BaseModel):
    identifier: str
    otp: str
```

### `src/auth/security.py`

```python
from datetime import datetime, timedelta, timezone

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import VerificationError, VerifyMismatchError

try:  # argon2-cffi >= 23.1.0
    from argon2.exceptions import InvalidHashError
except ImportError:  # older argon2-cffi
    from argon2.exceptions import InvalidHash as InvalidHashError

from src.config import get_settings

_ph = PasswordHasher()        # Argon2id
_ALGO = "HS256"
ACCESS_TOKEN_MINUTES = 15
GATEWAY_TOKEN_MINUTES = 30
USER_AUDIENCE = "user"
GATEWAY_AUDIENCE = "gateway"


def hash_password(password: str) -> str:
    return _ph.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return _ph.verify(password_hash, password)
    except (VerifyMismatchError, VerificationError, InvalidHashError):
        # wrong or malformed stored hash -> non-match. Unexpected (non-Argon2)
        # errors are NOT caught here; they propagate (generic 500, no internals leaked).
        return False


def create_access_token(user_id) -> str:
    now = datetime.now(timezone.utc)
    payload = {"sub": str(user_id), "aud": USER_AUDIENCE, "iat": now,
               "exp": now + timedelta(minutes=ACCESS_TOKEN_MINUTES)}
    return jwt.encode(payload, get_settings().jwt_secret, algorithm=_ALGO)


def decode_access_token(token: str) -> str | None:
    try:
        payload = jwt.decode(token, get_settings().jwt_secret, algorithms=[_ALGO],
                             audience=USER_AUDIENCE)
    except jwt.PyJWTError:
        return None
    sub = payload.get("sub")
    return sub if sub else None


def create_gateway_token(gateway_id) -> str:
    now = datetime.now(timezone.utc)
    payload = {"sub": str(gateway_id), "aud": GATEWAY_AUDIENCE, "iat": now,
               "exp": now + timedelta(minutes=GATEWAY_TOKEN_MINUTES)}
    return jwt.encode(payload, get_settings().jwt_secret, algorithm=_ALGO)


def decode_gateway_token(token: str) -> str | None:
    try:
        payload = jwt.decode(token, get_settings().jwt_secret, algorithms=[_ALGO],
                             audience=GATEWAY_AUDIENCE)
    except jwt.PyJWTError:
        return None
    sub = payload.get("sub")
    return sub if sub else None

```

### `src/auth/service.py`

```python
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.models import UserModel
from src.auth.repository import UserRepository
from src.auth.security import verify_password
from src.rbac.models import RoleModel, UserRoleModel

OTP_ROLES = {"org_admin", "site_operator"}


async def authenticate(session: AsyncSession, identifier: str, password: str) -> UserModel | None:
    user = await UserRepository(session).get_by_identifier(identifier)
    if user is None or not user.is_active or user.password_hash is None:
        return None
    if not verify_password(password, user.password_hash):
        return None
    return user


async def get_role_names(session: AsyncSession, user_id) -> set[str]:
    stmt = (select(RoleModel.name)
            .join(UserRoleModel, UserRoleModel.role_id == RoleModel.id)
            .where(UserRoleModel.user_id == user_id))
    return set((await session.execute(stmt)).scalars())


def resolve_channel(identifier: str, user):
    ident = identifier.strip()
    if "@" in ident:
        if user.email and user.email.lower() == ident.lower():
            return ("email", user.email)
        return None
    if user.phone and user.phone == ident:
        return ("sms", user.phone)
    return None

```

### `src/config.py`

```python
from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    env: str = "dev"
    database_url: str
    influx_url: str
    influx_token: str
    influx_org: str
    influx_bucket_raw: str
    influx_bucket_long: str
    influx_bucket_test: str = "telemetry_test"
    influx_bucket_3y: str = "telemetry_3y"
    influx_bucket_3y_retention_seconds: int = Field(default=94_608_000, gt=0)  # 1095 d (PROPOSED, not prod-approved)
    redis_url: str
    jwt_secret: str

    # 9b-ii-c live-ingestion gate (default OFF; record contract still PROVISIONAL)
    ingest_enabled: bool = False

    # --- Rate limiting (MECHANISM only; enforcement OFF by default). ---
    # NONE of the numbers below are approved operational policy; they are unset
    # placeholders pending gateway/ops confirmation. The limiter is a hard no-op
    # unless rate_limit_enabled=True AND a policy's capacity+refill are configured.
    rate_limit_enabled: bool = False          # master switch (default OFF)
    rate_limit_enforce: bool = False          # False => SHADOW (never blocks / no 429)
    rate_limit_redis_timeout_seconds: float = 0.05        # bounded per-check (fail-open)
    rate_limit_redis_connect_timeout_seconds: float = 0.25
    rate_limit_redis_socket_timeout_seconds: float = 0.25
    token_rate_capacity: int | None = None                # UNAPPROVED placeholder
    token_rate_refill_per_sec: float | None = None        # UNAPPROVED placeholder
    ingest_rate_capacity: int | None = None               # UNAPPROVED placeholder
    ingest_rate_refill_per_sec: float | None = None       # UNAPPROVED placeholder
    # Fallback-bucket MEMORY bounds (mechanism sizing, NOT a rate/burst/replay policy):
    rate_limit_local_fallback_max_keys: int = 10_000
    rate_limit_local_fallback_idle_seconds: float = 300.0

    # auth / OTP
    otp_provider: str = "mock"            # "mock" now; "msg91" once the account is live
    msg91_auth_key: str | None = None
    msg91_base_url: str | None = None     # confirm from MSG91 docs — do not guess
    msg91_sms_template_id: str | None = None
    msg91_sender_id: str | None = None
    msg91_email_from: str | None = None

    @field_validator("influx_bucket_3y")
    @classmethod
    def _influx_bucket_3y_nonblank(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("influx_bucket_3y must not be blank")
        return v

    @field_validator("token_rate_capacity", "ingest_rate_capacity")
    @classmethod
    def _rl_capacity_nonneg(cls, v):
        if v is not None and v < 0:
            raise ValueError("rate capacity must be >= 0 or null")
        return v

    @field_validator("token_rate_refill_per_sec", "ingest_rate_refill_per_sec")
    @classmethod
    def _rl_refill_nonneg(cls, v):
        if v is not None and v < 0:
            raise ValueError("rate refill must be >= 0 or null")
        return v

    @field_validator("rate_limit_redis_timeout_seconds",
                     "rate_limit_redis_connect_timeout_seconds",
                     "rate_limit_redis_socket_timeout_seconds")
    @classmethod
    def _rl_timeout_positive(cls, v):
        if v is None or v <= 0:
            raise ValueError("rate-limit timeout seconds must be > 0")
        return v

    @field_validator("rate_limit_local_fallback_max_keys")
    @classmethod
    def _rl_maxkeys_positive(cls, v):
        if v < 1:
            raise ValueError("rate_limit_local_fallback_max_keys must be >= 1")
        return v

    @field_validator("rate_limit_local_fallback_idle_seconds")
    @classmethod
    def _rl_idle_nonneg(cls, v):
        if v < 0:
            raise ValueError("rate_limit_local_fallback_idle_seconds must be >= 0")
        return v


@lru_cache
def get_settings() -> Settings:
    return Settings()
```

### `src/database.py`

```python
from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.config import get_settings

engine = create_async_engine(get_settings().database_url, pool_pre_ping=True)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)


async def get_session() -> AsyncIterator[AsyncSession]:
    async with SessionLocal() as session:
        yield session
```

### `src/gateway/dependencies.py`

```python
import uuid

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.security import decode_gateway_token
from src.database import get_session
from src.gateway import service

_bearer = HTTPBearer(auto_error=False)


async def get_current_gateway(
    creds: HTTPAuthorizationCredentials | None = Depends(_bearer),
    session: AsyncSession = Depends(get_session),
):
    if creds is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Missing bearer token")
    sub = decode_gateway_token(creds.credentials)      # requires aud="gateway" + not expired
    if sub is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid or expired gateway token")
    try:
        gid = uuid.UUID(sub)
    except ValueError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid gateway token subject")
    gw = await service.get_gateway_by_id(session, gid)
    if gw is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Gateway not found")
    if not gw.enabled:                                 # DB check on every request
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Gateway disabled")
    return gw

```

### `src/gateway/router.py`

```python
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.security import GATEWAY_TOKEN_MINUTES, create_gateway_token, verify_password
from src.database import get_session
from src.gateway import service
from src.gateway.dependencies import get_current_gateway
from src.gateway.schemas import GatewayIdentity, GatewayTokenRequest, GatewayTokenResponse
from src.ratelimit import token_rate_limit

router = APIRouter(prefix="/api/v1/gateway", tags=["gateway"])


@router.post("/token", response_model=GatewayTokenResponse)
async def gateway_token(body: GatewayTokenRequest, session: AsyncSession = Depends(get_session),
                        _rl=Depends(token_rate_limit)):
    gw = await service.get_gateway_by_client_id(session, body.client_id)
    # generic 401 for unknown client OR bad secret (do not reveal which)
    if gw is None or not verify_password(body.client_secret, gw.client_secret_hash):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid credentials")
    if not gw.enabled:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Gateway disabled")
    # Generate the token FIRST so a token-generation failure cannot persist a heartbeat.
    token = create_gateway_token(gw.id)
    # interim liveness: record this SUCCESSFUL token issuance (not telemetry, not a
    # live-connectivity guarantee). Commit the heartbeat in this request's own transaction.
    await service.mark_gateway_seen(session, gw)
    await session.commit()
    return GatewayTokenResponse(access_token=token, expires_in=GATEWAY_TOKEN_MINUTES * 60)


@router.get("/me", response_model=GatewayIdentity)
async def gateway_me(gw=Depends(get_current_gateway), session: AsyncSession = Depends(get_session)):
    ident = await service.resolve_gateway_identity(session, gw)
    return GatewayIdentity(
        gateway_id=ident["gateway_id"], site_id=ident["site_id"],
        organization_id=ident["organization_id"], enabled=gw.enabled,
    )

```

### `src/gateway/schemas.py`

```python
import uuid

from pydantic import BaseModel


class GatewayTokenRequest(BaseModel):
    client_id: str
    client_secret: str


class GatewayTokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int          # seconds


class GatewayIdentity(BaseModel):
    gateway_id: uuid.UUID
    site_id: uuid.UUID
    organization_id: uuid.UUID | None
    enabled: bool

```

### `src/gateway/service.py`

```python
from datetime import datetime, timezone

from sqlalchemy import select

from src.tenancy.gateways.models import GatewayModel
from src.tenancy.sites.models import SiteModel


async def get_gateway_by_client_id(session, client_id):
    result = await session.execute(
        select(GatewayModel).where(GatewayModel.client_id == client_id)
    )
    return result.scalar_one_or_none()


async def get_gateway_by_id(session, gateway_id):
    result = await session.execute(
        select(GatewayModel).where(GatewayModel.id == gateway_id)
    )
    return result.scalar_one_or_none()


async def resolve_gateway_identity(session, gw):
    """gateway -> site -> organization (DB-resolved, authoritative)."""
    org_id = (await session.execute(
        select(SiteModel.organization_id).where(SiteModel.id == gw.site_id)
    )).scalar_one_or_none()
    return {"gateway_id": gw.id, "site_id": gw.site_id, "organization_id": org_id}


async def mark_gateway_seen(session, gw, *, when=None):
    """Record a SUCCESSFUL gateway token issuance as the interim 'last seen' signal.

    This is the time credentials were last accepted and a token was issued - NOT the
    last telemetry timestamp and NOT a guarantee of live connectivity. Sets the column
    only (timezone-aware UTC) and flushes; the CALLER owns the commit, so the write
    participates in the request transaction and is discarded if that transaction rolls
    back. Call only after credentials are validated and the gateway is enabled.
    """
    gw.last_seen_at = when or datetime.now(timezone.utc)
    await session.flush()
    return gw

```

### `src/influx.py`

```python
from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone

from influxdb_client import Point
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

from src.config import get_settings
from src.signals.types import QUALITY_SUFFIX, coerce, validate_quality
from src.telemetry_errors import (
    InvalidTimestamp,
    RegistryConflict,
    RetentionNotConfigured,
    SignalMismatch,
    UnknownRetentionClass,
    UnknownSignal,
    UnresolvedMetadata,
    UnsafeQueryInput,
)

# Tags, verbatim from the frozen contract. asset_id = "Runtime Asset",
# source_id = "Source ID". Both are TAGS -> distinct series, never merged.
TAG_KEYS = ("organization_id", "site_id", "gateway_id", "source_id", "asset_id", "substream")
_REQUIRED = ("measurement",) + TAG_KEYS + ("signal_key",)

_IDENT = re.compile(r"^[A-Za-z0-9_.:\-]+$")
_RANGE = re.compile(r"^-?\d+(ns|us|ms|s|m|h|d|w|mo|y)$")
_AGG_FLUX_FNS = frozenset({"mean", "min", "max", "first", "last", "count"})


def measurement_for(stream: str, substream: str) -> str:
    return f"{stream.lower()}_{substream}"


def bucket_for(retention_class: str, settings) -> str:
    """Code-level routing only (NOT the actual Influx retention policy, which must be
    verified in InfluxDB; see src/influx_provision.py). T1->raw, T2->long, T3->the
    dedicated 3-year bucket. Unknown classes and a missing/blank T3 bucket fail closed.

    The T3 bucket's enforced retention (proposed 1095 d) is pending production
    confirmation; it is verified against the configured seconds at provisioning time.
    The registry keeps the T3_3Y classification unchanged."""
    if retention_class == "T1_RAW_90D":
        return settings.influx_bucket_raw
    if retention_class == "T2_1Y":
        return settings.influx_bucket_long
    if retention_class == "T3_3Y":
        bucket = getattr(settings, "influx_bucket_3y", "") or ""
        if not str(bucket).strip():
            raise RetentionNotConfigured("T3_3Y bucket not configured")
        return bucket
    raise UnknownRetentionClass(f"Unknown retention class: {retention_class!r}")


def parse_contract_timestamp(value) -> datetime:
    """Parse ISO-8601 offset-aware timestamp. Semantic validation (invalid calendar
    dates and offsets are rejected), preserve instant, normalize to UTC, reject naive."""
    if not isinstance(value, str):
        raise InvalidTimestamp("timestamp must be an ISO-8601 string")
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as e:
        raise InvalidTimestamp(f"invalid ISO-8601 timestamp: {value!r}") from e
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise InvalidTimestamp("timestamp must include a UTC offset (no naive datetimes)")
    return dt.astimezone(timezone.utc)


def _require_time(ts) -> datetime:
    if not isinstance(ts, datetime):
        raise InvalidTimestamp("timestamp must be a datetime (record Source Updated UTC)")
    if ts.tzinfo is None or ts.utcoffset() is None:
        raise InvalidTimestamp("timestamp must be timezone-aware")
    return ts.astimezone(timezone.utc)


def _ident(name: str, value) -> str:
    if not isinstance(value, str) or not value.strip() or not _IDENT.match(value):
        raise UnsafeQueryInput(f"invalid {name}: {value!r}")
    return value


def _range(value) -> str:
    if isinstance(value, str) and _RANGE.match(value):
        return value
    try:
        parse_contract_timestamp(value)        # semantic RFC3339 validation
    except InvalidTimestamp:
        raise UnsafeQueryInput(f"unsafe range: {value!r}")
    return value


@dataclass(frozen=True)
class RegistrySignal:
    primary_stream: str
    substream: str
    asset_type: str | None
    data_type: str
    retention_class: str


@dataclass
class RegistryIndex:
    """Trusted signal metadata from signal_registry, keyed by (measurement, signal_key)."""
    _by_key: dict

    @classmethod
    def from_rows(cls, rows) -> "RegistryIndex":
        # rows: (primary_stream, substream, signal_key, asset_type, data_type, retention_class)
        d = {}
        for ps, sub, key, asset_type, dt, rc in rows:
            k = (measurement_for(ps, sub), key)
            if k in d:
                raise RegistryConflict(f"duplicate (measurement, signal_key): {k}")
            d[k] = RegistrySignal(ps, sub, asset_type, dt, rc)
        return cls(d)

    def resolve(self, measurement: str, signal_key: str) -> RegistrySignal:
        try:
            return self._by_key[(measurement, signal_key)]
        except KeyError:
            raise UnknownSignal(f"signal not in registry: {measurement}/{signal_key}")

    def __len__(self) -> int:
        return len(self._by_key)


@dataclass
class TelemetryPoint:
    measurement: str
    organization_id: str
    site_id: str
    gateway_id: str
    source_id: str
    asset_id: str
    substream: str
    signal_key: str
    value: object
    quality: str | None
    timestamp: datetime
    asset_type: str | None = None     # optional cross-check; registry asset_type is authoritative


def _validate_identities(p: TelemetryPoint) -> None:
    for name in _REQUIRED:
        v = getattr(p, name)
        if not isinstance(v, str) or not v.strip():
            raise UnsafeQueryInput(f"{name} is required and must be a non-empty string")
        if not _IDENT.match(v):
            raise UnsafeQueryInput(f"invalid {name}: {v!r}")


def build_point(index: RegistryIndex, p: TelemetryPoint, settings):
    """Validate + resolve trusted metadata, build (bucket, Point). Raises before any I/O.
    asset_type is always resolved from the registry (cannot be bypassed); a point-supplied
    asset_type must match it."""
    _validate_identities(p)
    sig = index.resolve(p.measurement, p.signal_key)                 # UnknownSignal
    if p.substream != sig.substream:
        raise SignalMismatch(f"substream mismatch for {p.signal_key}: "
                             f"point={p.substream!r} registry={sig.substream!r}")
    if sig.asset_type is None or not str(sig.asset_type).strip():
        raise UnresolvedMetadata(f"asset_type missing in registry for {p.measurement}/{p.signal_key}")
    if p.asset_type is not None and p.asset_type != sig.asset_type:
        raise SignalMismatch(f"asset_type mismatch for {p.signal_key}: "
                             f"point={p.asset_type!r} registry={sig.asset_type!r}")
    ts = _require_time(p.timestamp)
    val = coerce(sig.data_type, p.value)                             # trusted type + range
    q = validate_quality(p.quality)
    bucket = bucket_for(sig.retention_class, settings)               # trusted retention routing
    pt = Point(p.measurement)
    for k in TAG_KEYS:
        pt = pt.tag(k, getattr(p, k))
    pt = pt.field(p.signal_key, val)
    if q is not None:
        pt = pt.field(f"{p.signal_key}{QUALITY_SUFFIX}", q)
    return bucket, pt.time(ts)


class InfluxGateway:
    """Low-level telemetry store. NOT an authorization boundary — Phase 11 read APIs
    must apply trusted, DB-derived organization/site scope before calling this.

    Conflict behavior: point identity is (measurement + full tag set + timestamp +
    field key); a repeat/identical write overwrites in place (last-write-wins). POINT-level
    overwrite only, NOT message-level idempotency (Phase 9, Postgres).
    """

    def __init__(self, settings=None):
        self._settings = settings or get_settings()
        self._url = self._settings.influx_url
        self._token = self._settings.influx_token
        self._org = self._settings.influx_org

    def _client(self) -> InfluxDBClientAsync:
        return InfluxDBClientAsync(url=self._url, token=self._token, org=self._org)

    @property
    def settings(self):
        return self._settings

    async def health(self) -> bool:
        async with self._client() as c:
            return await c.ping()

    async def write(self, points: list[TelemetryPoint], index: RegistryIndex) -> None:
        """Route each point to its registry-derived bucket. No caller-supplied bucket.
        ALL points are validated and built before any I/O (reject-before-write), so a
        mixed-retention batch with one bad point writes nothing."""
        by_bucket: dict[str, list] = defaultdict(list)
        for p in points:
            bucket, pt = build_point(index, p, self._settings)
            by_bucket[bucket].append(pt)
        async with self._client() as c:
            wapi = c.write_api()
            for bucket, recs in by_bucket.items():
                await wapi.write(bucket=bucket, record=recs)

    async def write_built(self, built) -> None:
        """Write pre-built (bucket, Point) tuples (from build_message_points) without
        rebuilding. Destinations are restricted to THIS gateway's configured retention
        buckets; a tuple naming any other bucket is refused (never writes to an arbitrary
        non-configured bucket)."""
        allowed = {self._settings.influx_bucket_raw, self._settings.influx_bucket_long,
                   self._settings.influx_bucket_3y}
        by_bucket: dict[str, list] = defaultdict(list)
        for bucket, pt in built:
            if bucket not in allowed:
                raise UnsafeQueryInput(f"refusing write to non-configured bucket: {bucket!r}")
            by_bucket[bucket].append(pt)
        async with self._client() as c:
            wapi = c.write_api()
            for bucket, recs in by_bucket.items():
                await wapi.write(bucket=bucket, record=recs)

    async def query(self, flux: str):
        async with self._client() as c:
            return await c.query_api().query(flux)

    async def latest_signal(self, bucket, measurement, site_id, signal_key,
                            asset_id=None, source_id=None, start="-1h"):
        _ident("bucket", bucket); _ident("measurement", measurement); _ident("site_id", site_id)
        _ident("signal_key", signal_key); _range(start)
        extra = ""
        if asset_id:
            extra += f' and r.asset_id == "{_ident("asset_id", asset_id)}"'
        if source_id:
            extra += f' and r.source_id == "{_ident("source_id", source_id)}"'
        flux = (
            f'from(bucket: "{bucket}")\n'
            f'  |> range(start: {start})\n'
            f'  |> filter(fn: (r) => r._measurement == "{measurement}" '
            f'and r.site_id == "{site_id}" and r._field == "{signal_key}"{extra})\n'
            f'  |> last()'
        )
        return await self.query(flux)

    async def latest_by_asset(self, bucket, measurement, site_id, signal_key, start="-1h"):
        _ident("bucket", bucket); _ident("measurement", measurement); _ident("site_id", site_id)
        _ident("signal_key", signal_key); _range(start)
        flux = (
            f'from(bucket: "{bucket}")\n'
            f'  |> range(start: {start})\n'
            f'  |> filter(fn: (r) => r._measurement == "{measurement}" '
            f'and r.site_id == "{site_id}" and r._field == "{signal_key}")\n'
            f'  |> group(columns: ["asset_id", "source_id"])\n'
            f'  |> last()'
        )
        return await self.query(flux)

    async def series(self, bucket, measurement, site_id, signal_key, start, stop,
                     asset_id=None, source_id=None, limit=1000):
        """Bounded, site-scoped time-series read. All identifiers/ranges are guarded;
        never queries without site_id. The caller enforces window + limit caps; this
        method also refuses a non-positive limit. Points are time-sorted ascending."""
        _ident("bucket", bucket); _ident("measurement", measurement); _ident("site_id", site_id)
        _ident("signal_key", signal_key); _range(start); _range(stop)
        if not isinstance(limit, int) or limit < 1:
            raise UnsafeQueryInput(f"invalid limit: {limit!r}")
        extra = ""
        if asset_id:
            extra += f' and r.asset_id == "{_ident("asset_id", asset_id)}"'
        if source_id:
            extra += f' and r.source_id == "{_ident("source_id", source_id)}"'
        flux = (
            f'from(bucket: "{bucket}")\n'
            f'  |> range(start: {start}, stop: {stop})\n'
            f'  |> filter(fn: (r) => r._measurement == "{measurement}" '
            f'and r.site_id == "{site_id}" and r._field == "{signal_key}"{extra})\n'
            f'  |> sort(columns: ["_time"])\n'
            f'  |> limit(n: {int(limit)})'
        )
        return await self.query(flux)

    async def aggregate(self, bucket, measurement, site_id, signal_key,
                        start, stop, every, flux_fn, asset_id, source_id=None):
        """Bounded, site-scoped, time-bucketed read. SPARSE: createEmpty is false, so
        only windows containing data are returned (missing timestamps = gaps, not zero).
        The caller validates window/fn/bucket-count and maps fn to a fixed Flux name; this
        method guards every identifier and never queries without site_id. Bucket timestamp
        = window start."""
        _ident("bucket", bucket); _ident("measurement", measurement); _ident("site_id", site_id)
        _ident("signal_key", signal_key); _range(start); _range(stop); _range(every)
        if flux_fn not in _AGG_FLUX_FNS:
            raise UnsafeQueryInput(f"unsupported aggregate function: {flux_fn!r}")
        extra = f' and r.asset_id == "{_ident("asset_id", asset_id)}"'
        if source_id:
            extra += f' and r.source_id == "{_ident("source_id", source_id)}"'
        flux = (
            f'from(bucket: "{bucket}")\n'
            f'  |> range(start: {start}, stop: {stop})\n'
            f'  |> filter(fn: (r) => r._measurement == "{measurement}" '
            f'and r.site_id == "{site_id}" and r._field == "{signal_key}"{extra})\n'
            f'  |> aggregateWindow(every: {every}, fn: {flux_fn}, createEmpty: false, timeSrc: "_start")\n'
            f'  |> sort(columns: ["_time"])'
        )
        return await self.query(flux)


def rows(tables):
    out = []
    for t in tables:
        for rec in t.records:
            out.append({
                "asset_id": rec.values.get("asset_id"),
                "source_id": rec.values.get("source_id"),
                "field": rec.get_field(),
                "value": rec.get_value(),
                "time": rec.get_time(),
            })
    return out
```

### `src/influx_provision.py`

```python
"""Stage A-1: provision & verify the dedicated 3-year telemetry bucket.

One-time OPERATIONAL step, run explicitly (NOT wired into app startup):
    python -m src.influx_provision

Creates the configured bucket only if absent, then re-fetches and verifies its
enforced retention equals the configured seconds. Never updates, patches, or deletes
an existing bucket; fails visibly (non-zero exit) on any config/API/verification
error. The enforced duration (proposed 1095 d) is a configured value, not a
contract-confirmed production requirement.
"""
from influxdb_client import InfluxDBClient
from influxdb_client.domain.bucket_retention_rules import BucketRetentionRules

from src.config import get_settings


class BucketProvisionError(RuntimeError):
    """Provisioning / retention-verification failure (safe, self-authored message)."""


def ensure_t3_bucket(client, *, org: str, name: str, expect_seconds: int) -> None:
    """Create-if-absent then verify. Reads Bucket.retention_rules (list of
    BucketRetentionRules); the sole 'expire' rule's every_seconds must equal
    expect_seconds (unit: seconds). Never mutates an existing bucket."""
    api = client.buckets_api()
    bucket = api.find_bucket_by_name(name)              # raises on auth/network
    if bucket is None:
        api.create_bucket(
            bucket_name=name, org=org,
            retention_rules=[BucketRetentionRules(type="expire", every_seconds=expect_seconds)],
        )
        bucket = api.find_bucket_by_name(name)          # re-fetch; trust the store
        if bucket is None:
            raise BucketProvisionError(f"bucket {name!r} not found after creation")
    rules = [r for r in (bucket.retention_rules or []) if getattr(r, "type", None) == "expire"]
    if len(rules) != 1 or rules[0].every_seconds != expect_seconds:
        found = [getattr(r, "every_seconds", None) for r in rules]
        raise BucketProvisionError(
            f"bucket {name!r} retention {found}s != {expect_seconds}s; not modifying")


def main() -> int:
    s = get_settings()
    missing = [n for n in ("influx_url", "influx_token", "influx_org", "influx_bucket_3y")
               if not getattr(s, n, None)]
    if missing:
        print(f"FATAL: missing/empty configuration: {missing}")       # names only, never values
        return 2
    secs = getattr(s, "influx_bucket_3y_retention_seconds", None)
    if not isinstance(secs, int) or secs <= 0:
        print("FATAL: influx_bucket_3y_retention_seconds must be a positive integer")
        return 2
    client = InfluxDBClient(url=s.influx_url, token=s.influx_token, org=s.influx_org)
    try:
        ensure_t3_bucket(client, org=s.influx_org, name=s.influx_bucket_3y, expect_seconds=secs)
        print(f"OK: bucket '{s.influx_bucket_3y}' verified at {secs}s retention")
        return 0
    except BucketProvisionError as e:
        print(f"FATAL: {e}")                                          # safe message, no secrets
        return 1
    except Exception as e:
        print(f"FATAL: provisioning failed ({type(e).__name__})")     # type only; no token/body/url
        return 1
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())

```

### `src/ingestion/body.py`

```python
import zlib

MAX_COMPRESSED_BYTES = 5 * 1024 * 1024       # 5 MiB
MAX_DECOMPRESSED_BYTES = 50 * 1024 * 1024    # 50 MiB


class BodyError(Exception):
    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail)


def _gunzip_limited(raw: bytes, cap: int) -> bytes:
    """Decompress a single gzip member with a HARD output cap.

    Never produces or buffers more than cap+1 bytes: input is fed in bounded
    steps and the output is checked after each step, so a zip bomb is stopped
    at cap+1. No unbounded flush() is used. Rejects output over cap (413),
    truncated streams (400), and trailing/concatenated data (400)."""
    d = zlib.decompressobj(16 + zlib.MAX_WBITS)      # gzip header
    out = bytearray()
    data = raw
    while True:
        want = cap + 1 - len(out)
        if want <= 0:                                # already at cap, more remains
            raise BodyError(413, "decompressed body too large")
        chunk = d.decompress(data, want)
        out += chunk
        if len(out) > cap:
            raise BodyError(413, "decompressed body too large")
        if d.eof:
            break
        data = d.unconsumed_tail
        if not chunk and not data:                   # no progress, stream not ended
            raise BodyError(400, "truncated gzip body")
    if d.unused_data:                                # bytes beyond the first member
        raise BodyError(400, "trailing data after gzip stream")
    return bytes(out)


def decode_body(raw: bytes, content_encoding: str | None) -> bytes:
    """Validate Content-Encoding and size; return decompressed bytes.
    Safe (no bypass) on truncated, concatenated, or malformed gzip."""
    if len(raw) > MAX_COMPRESSED_BYTES:
        raise BodyError(413, "request body too large")
    enc = (content_encoding or "identity").strip().lower()
    if enc in ("", "identity"):
        if len(raw) > MAX_DECOMPRESSED_BYTES:
            raise BodyError(413, "body too large")
        return raw
    if enc != "gzip":
        raise BodyError(415, f"unsupported Content-Encoding: {enc!r}")
    try:
        return _gunzip_limited(raw, MAX_DECOMPRESSED_BYTES)
    except BodyError:
        raise
    except (OSError, EOFError, zlib.error):
        raise BodyError(400, "malformed gzip body")

```

### `src/ingestion/mapping.py`

```python
"""Phase 9b-ii-a: pure record -> registry -> deterministic Influx point mapping.

NO I/O. No Influx writes, no ledger, no endpoint changes. This module only turns a
validated envelope plus the authenticated gateway's DB-derived identity into a list of
(bucket, influxdb_client.Point) using the existing registry validators in src.influx.

Record keys are PROVISIONAL (source_id, asset_id, signal_key, value, quality,
timestamp) and their timestamp semantics are NOT gateway-confirmed.
"""
from src.influx import TelemetryPoint, build_point, measurement_for, parse_contract_timestamp


def build_message_points(envelope, identity, index, settings):
    """Validate EVERY record and build EVERY (bucket, Point) before returning.

    A single invalid record raises (UnknownSignal / SignalMismatch / InvalidValue /
    InvalidTimestamp / UnsafeQueryInput / RetentionNotConfigured / UnresolvedMetadata /
    UnknownRetentionClass), so the whole message is rejected and nothing is produced.
    The function performs NO I/O; the caller (9b-ii-c) writes only if this returns.

    identity: DB-derived {"organization_id","site_id","gateway_id"} from the
    authenticated gateway. Client-supplied envelope IDs are NEVER used for tags.
    """
    measurement = measurement_for(envelope.stream, envelope.substream)
    org = str(identity["organization_id"])
    site = str(identity["site_id"])
    gw = str(identity["gateway_id"])
    built = []
    for rec in envelope.records:
        point = TelemetryPoint(
            measurement=measurement,
            organization_id=org,
            site_id=site,
            gateway_id=gw,
            source_id=rec.source_id,
            asset_id=rec.asset_id,
            substream=envelope.substream,
            signal_key=rec.signal_key,
            value=rec.value,
            quality=rec.quality,
            timestamp=parse_contract_timestamp(rec.timestamp),
            asset_type=None,     # registry asset_type is authoritative (build_point)
        )
        built.append(build_point(index, point, settings))
    return built

```

### `src/ingestion/models.py`

```python
import uuid
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.models import BaseEntityModel


class IngestMessageModel(BaseEntityModel):
    """Message-level ingestion ledger (idempotency + processing lease).

    One row per (gateway_id, message_id). 9b-i writes only 'pending' rows with a
    time-bounded lease; the 'stored' transition and the Influx write belong to 9b-ii.
    """

    __tablename__ = "ingest_messages"
    __table_args__ = (
        UniqueConstraint("gateway_id", "message_id", name="uq_ingest_gateway_message"),
    )

    gateway_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("gateways.id", ondelete="CASCADE"), index=True
    )
    message_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    stream: Mapped[str] = mapped_column(String(8))
    sequence: Mapped[int] = mapped_column(BigInteger)
    record_count: Mapped[int] = mapped_column(Integer)
    payload_hash: Mapped[str] = mapped_column(String(64))         # sha256 hex of canonical form
    status: Mapped[str] = mapped_column(String(16))               # pending | stored
    lease_owner: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    stored_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

```

### `src/ingestion/providers.py`

```python
"""Phase 9b-ii-c dependency providers: the ingestion gate, a per-process registry-index
cache keyed by database identity, and the InfluxGateway provider.

Registry index: IMMUTABLE for the process lifetime once built. Registry seed/schema
changes are NOT observed at runtime and require a process restart before ingestion is
re-enabled. The cache is keyed by the session's database identity (host, port, db) so an
index built from one database can never be served to a session bound to another (prod runs
one registry DB per process; tests override get_registry_index and reset the cache).
"""
from fastapi import Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import Settings, get_settings
from src.database import get_session
from src.gateway.dependencies import get_current_gateway
from src.influx import InfluxGateway, RegistryIndex
from src.signals.service import load_registry_index

_registry_cache: dict = {}


def _db_identity(session: AsyncSession):
    url = session.get_bind().url
    return (url.host, url.port, url.database)


def reset_registry_cache() -> None:
    """Clear the process-global index cache (used by tests; not a runtime refresh)."""
    _registry_cache.clear()


async def require_ingest_enabled(
    gw=Depends(get_current_gateway),                      # auth first: 401/403
    settings: Settings = Depends(get_settings),
):
    if not settings.ingest_enabled:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Telemetry ingestion is not yet enabled",
        )
    return gw


async def load_cached_index(session: AsyncSession) -> RegistryIndex:
    """Shared, UNGATED registry-index cache (keyed by DB identity). Reused by the
    ingestion gate's provider and by the read-only telemetry providers. Failures are
    NOT cached; the index is immutable per process (restart to refresh)."""
    key = _db_identity(session)
    idx = _registry_cache.get(key)
    if idx is None:
        idx = await load_registry_index(session)          # failures are NOT cached
        _registry_cache[key] = idx
    return idx


async def get_registry_index(
    _gate=Depends(require_ingest_enabled),                # not built when the gate is off
    session: AsyncSession = Depends(get_session),
) -> RegistryIndex:
    return await load_cached_index(session)


def get_influx_gateway(_gate=Depends(require_ingest_enabled)) -> InfluxGateway:
    return InfluxGateway(get_settings())

```

### `src/ingestion/router.py`

```python
import json

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import get_session
from src.gateway.service import resolve_gateway_identity
from src.ingestion.body import BodyError, decode_body
from src.ingestion.mapping import build_message_points
from src.ingestion.providers import (get_influx_gateway, get_registry_index,
                                      require_ingest_enabled)
from src.ingestion.schemas import EnvelopeIn, IngestAck
from src.ratelimit import ingest_rate_limit
from src.ingestion.service import (IngestDecision, canonical_hash, claim_or_resolve,
                                   mark_stored, route_stream, verify_scope)
from src.telemetry_errors import (InfluxTelemetryError, RetentionNotConfigured,
                                   UnknownRetentionClass, UnresolvedMetadata)

router = APIRouter(prefix="/api/v1/gateway", tags=["ingestion"])


@router.post("/ingest", response_model=IngestAck)
async def ingest(
    request: Request,
    _rl=Depends(ingest_rate_limit),          # rate limiter BEFORE gate + ledger claim (shadow by default)
    gw=Depends(require_ingest_enabled),          # auth (401/403) -> gate (503) -> here
    session: AsyncSession = Depends(get_session),
    index=Depends(get_registry_index),
    influx=Depends(get_influx_gateway),
):
    """Phase 9b-ii-c live ingestion, behind the default-disabled gate.

    PROVISIONAL: record keys (source_id, asset_id, signal_key, value, quality, timestamp)
    and per-record timestamp semantics are NOT gateway-contract-confirmed; the
    default-disabled gate is the mitigation. Order is validate-before-claim.
    """
    # ---- Phase A: body + envelope (no ledger, no write) ----
    try:
        raw = await request.body()
        decoded = decode_body(raw, request.headers.get("content-encoding"))
    except BodyError as e:
        raise HTTPException(e.status_code, "request body rejected")
    try:
        envelope = EnvelopeIn(**json.loads(decoded))
    except json.JSONDecodeError:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "malformed JSON body")
    except (ValidationError, TypeError, ValueError):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "invalid envelope")

    # trusted DB-derived scope; client IDs are never trusted for authorization
    identity = await resolve_gateway_identity(session, gw)
    if not verify_scope(identity, gateway_id=envelope.gateway_id,
                        site_id=envelope.site_id, organization_id=envelope.organization_id):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "envelope scope does not match gateway")

    route = route_stream(envelope.stream)
    if route == "deferred":
        raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, f"stream {envelope.stream} not supported yet")
    if route != "ingest":
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "unsupported stream")

    # ---- Phase B: validate + build all points (no ledger, no write) ----
    try:
        built = build_message_points(envelope, identity, index, influx.settings)
    except (RetentionNotConfigured, UnknownRetentionClass):
        raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, "retention class not configured")
    except UnresolvedMetadata:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "registry metadata incomplete")
    except InfluxTelemetryError:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "invalid telemetry record")

    payload_hash = canonical_hash(envelope.model_dump())

    # ---- Phase C: ledger claim (DB infra errors -> 503, never 422) ----
    try:
        decision, token = await claim_or_resolve(
            session, gateway_id=gw.id, stream=envelope.stream, message_id=envelope.message_id,
            sequence=envelope.sequence, record_count=envelope.record_count, payload_hash=payload_hash)
    except Exception:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "ingestion temporarily unavailable")

    if decision == IngestDecision.DUPLICATE_STORED:
        return IngestAck(status="duplicate", message_id=envelope.message_id,
                         record_count=envelope.record_count)
    if decision == IngestDecision.HASH_MISMATCH:
        raise HTTPException(status.HTTP_409_CONFLICT, "message_id reused with a different payload")
    if decision == IngestDecision.IN_PROGRESS:
        raise HTTPException(status.HTTP_409_CONFLICT, "message is already being processed")

    # decision is CLAIMED or RECLAIMED
    # ---- Phase D: write to Influx, then confirm stored via lease-owned CAS ----
    try:
        await influx.write_built(built)
    except Exception:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "telemetry store temporarily unavailable")
    try:
        stored = await mark_stored(session, gateway_id=gw.id,
                                   message_id=envelope.message_id, token=token)
    except Exception:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "ingestion temporarily unavailable")
    if not stored:                                  # lease lost -> do NOT acknowledge stored
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "store not confirmed; retry")
    return IngestAck(status="stored", message_id=envelope.message_id,
                     record_count=envelope.record_count)

```

### `src/ingestion/schemas.py`

```python
import uuid

from pydantic import BaseModel, Field, field_validator, model_validator

from src.streams import KNOWN_STREAMS

SUPPORTED_SCHEMA_VERSIONS = {"1.0"}
MAX_RECORDS = 10_000
MAX_SEQUENCE = 2**63 - 1          # fits BigInteger column


class RecordIn(BaseModel):
    # PROVISIONAL keys - pending the gateway team's literal JSON contract. UNCHANGED.
    source_id: str = Field(max_length=128)
    asset_id: str = Field(max_length=128)
    signal_key: str = Field(max_length=128)
    value: float | int | str | bool | None
    quality: str | None = Field(default=None, max_length=32)
    timestamp: str = Field(max_length=64)   # ISO-8601; SEMANTIC validation deferred to 9b-ii


class EnvelopeIn(BaseModel):
    schema_version: str = Field(max_length=16)
    message_id: uuid.UUID
    gateway_id: str = Field(max_length=64)
    site_id: str = Field(max_length=64)
    organization_id: str = Field(max_length=64)
    stream: str = Field(max_length=8)
    substream: str = Field(max_length=64)
    sequence: int = Field(ge=0, le=MAX_SEQUENCE)
    created_at_utc: str = Field(max_length=64)
    record_count: int = Field(ge=0, le=MAX_RECORDS)
    records: list[RecordIn]
    compression: str | None = Field(default=None, max_length=32)
    gateway_software_version: str | None = Field(default=None, max_length=64)

    @field_validator("schema_version")
    @classmethod
    def _supported_version(cls, v):
        if v not in SUPPORTED_SCHEMA_VERSIONS:
            raise ValueError(f"unsupported schema_version: {v!r}")
        return v

    @field_validator("stream")
    @classmethod
    def _known_stream(cls, v):
        # unknown -> validation error (422). Deferred streams parse OK and are
        # routed to 501 by service.route_stream in 9b-ii.
        if v not in KNOWN_STREAMS:
            raise ValueError(f"unknown stream: {v!r}")
        return v

    @model_validator(mode="after")
    def _counts_match(self):
        if self.record_count != len(self.records):
            raise ValueError("record_count does not match number of records")
        return self


class IngestAck(BaseModel):
    status: str            # "stored" | "duplicate"
    message_id: uuid.UUID
    record_count: int

```

### `src/ingestion/service.py`

```python
"""Telemetry ingestion ledger (Phase 9b-i).

Lease model
-----------
Each accepted message row carries a time-bounded processing lease
(lease_owner token + lease_expires_at). A claim grants LEASE_TTL_SECONDS of
exclusive ownership. Crash recovery: an expired lease is stolen via an atomic
compare-and-swap on (lease_owner, lease_expires_at).

9b-ii - ownership protocol for the write path (NOT implemented here)
  1. mark_stored is a CAS:
         UPDATE ... SET status='stored', stored_at=now()
         WHERE id=? AND lease_owner=<my token> AND status='pending'
     If the lease was stolen mid-write, the CAS matches 0 rows and the worker
     MUST NOT acknowledge 'stored' - it treats its write as superseded.
  2. Lease renewal / over-run: LEASE_TTL_SECONDS is set above p99 single-message
     write latency; large batches renew (heartbeat) the lease by extending
     lease_expires_at under the same CAS guard.
  3. Deterministic Influx point identity on retries: a record always maps to the
     same series+timestamp - tags (organization_id, site_id, gateway_id,
     source_id, asset_id, substream), field (signal_key), time (record
     timestamp). Influx is last-write-wins per point, so a re-write overwrites
     with identical data rather than duplicating.
No DB transaction is held open across the Influx write.
"""
import hashlib
import json
import uuid
from datetime import datetime, timedelta, timezone
from enum import Enum

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.ingestion.models import IngestMessageModel
from src.streams import DEFERRED_STREAMS, INGEST_STREAMS

LEASE_TTL_SECONDS = 60     # must exceed p99 end-to-end single-message write latency


class IngestDecision(str, Enum):
    CLAIMED = "claimed"
    RECLAIMED = "reclaimed"
    DUPLICATE_STORED = "duplicate_stored"
    IN_PROGRESS = "in_progress"
    HASH_MISMATCH = "hash_mismatch"       # -> 409 at HTTP layer (9b-ii)


def route_stream(stream: str) -> str:
    if stream in INGEST_STREAMS:
        return "ingest"
    if stream in DEFERRED_STREAMS:
        return "deferred"
    return "unknown"


def verify_scope(identity: dict, *, gateway_id, site_id, organization_id) -> bool:
    """Envelope scope must equal the authenticated gateway's DB-resolved identity."""
    return (
        str(identity["gateway_id"]) == str(gateway_id)
        and str(identity["site_id"]) == str(site_id)
        and str(identity["organization_id"]) == str(organization_id)
    )


def canonical_hash(envelope: dict) -> str:
    """Deterministic SHA-256 over telemetry-bearing content only (sorted keys,
    no whitespace). Excludes identity (validated separately), compression and
    gateway_software_version."""
    canon = {
        "schema_version": envelope["schema_version"],
        "stream": envelope["stream"],
        "substream": envelope["substream"],
        "sequence": envelope["sequence"],
        "created_at_utc": envelope["created_at_utc"],
        "record_count": envelope["record_count"],
        "records": envelope["records"],
    }
    blob = json.dumps(canon, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


async def claim_or_resolve(session, *, gateway_id, stream, message_id,
                           sequence, record_count, payload_hash,
                           lease_ttl_seconds=LEASE_TTL_SECONDS):
    """Ledger state machine. Returns (IngestDecision, lease_token | None).

    9b-i scope: performs only the claim/lease decision. It NEVER marks 'stored'
    and NEVER triggers an Influx write - those belong to 9b-ii.

    Transaction contract: the function owns its transaction boundaries. It
    COMMITS on the write paths (CLAIMED; and the steal yielding RECLAIMED or a
    lost-race IN_PROGRESS) and ROLLS BACK on the read-only paths (HASH_MISMATCH,
    DUPLICATE_STORED, active-lease IN_PROGRESS). On any exception it ATTEMPTS a
    rollback and re-raises the ORIGINAL exception; if the rollback itself fails,
    that secondary error is suppressed so the original database error
    propagates. Rollback is attempted, not guaranteed. The function never
    swallows a database error or returns a successful decision after one. On
    normal return the session has no open transaction.
    """
    now = datetime.now(timezone.utc)
    token = uuid.uuid4()
    expires = now + timedelta(seconds=lease_ttl_seconds)

    try:
        claim = (
            pg_insert(IngestMessageModel)
            .values(id=uuid.uuid4(), gateway_id=gateway_id, message_id=message_id,
                    stream=stream, sequence=sequence, record_count=record_count,
                    payload_hash=payload_hash, status="pending",
                    lease_owner=token, lease_expires_at=expires)
            .on_conflict_do_nothing(index_elements=["gateway_id", "message_id"])
            .returning(IngestMessageModel.id)
        )
        inserted = (await session.execute(claim)).scalar_one_or_none()
        if inserted is not None:
            await session.commit()
            return IngestDecision.CLAIMED, token

        row = (await session.execute(
            select(IngestMessageModel)
            .where(IngestMessageModel.gateway_id == gateway_id,
                   IngestMessageModel.message_id == message_id)
        )).scalar_one()

        if row.payload_hash != payload_hash:
            await session.rollback()
            return IngestDecision.HASH_MISMATCH, None
        if row.status == "stored":
            await session.rollback()
            return IngestDecision.DUPLICATE_STORED, None
        if row.lease_expires_at is not None and row.lease_expires_at > now:
            await session.rollback()
            return IngestDecision.IN_PROGRESS, None

        steal = (await session.execute(
            update(IngestMessageModel)
            .where(IngestMessageModel.id == row.id,
                   IngestMessageModel.status == "pending",
                   IngestMessageModel.lease_owner == row.lease_owner,
                   IngestMessageModel.lease_expires_at == row.lease_expires_at)
            .values(lease_owner=token, lease_expires_at=expires)
            .returning(IngestMessageModel.id)
        )).scalar_one_or_none()
        await session.commit()
        return (IngestDecision.RECLAIMED, token) if steal is not None else (IngestDecision.IN_PROGRESS, None)
    except Exception:
        try:
            await session.rollback()
        except Exception:
            pass          # keep the original error; don't let a failed rollback mask it
        raise


async def mark_stored(session, *, gateway_id, message_id, token) -> bool:
    """Phase 9b-ii CAS: flip the ledger row to 'stored' ONLY if this worker still owns
    the lease. Matches (gateway_id, message_id, status='pending', lease_owner=token) and
    sets status='stored' + stored_at. Returns True iff exactly one row changed.

    A lost lease / already-stored / wrong message -> 0 rows -> False, row left unchanged;
    the caller MUST NOT acknowledge 'stored' on False. 9b-ii callers invoke this only AFTER
    a successful Influx write: an Influx failure means this is never called and the row
    stays 'pending' (retryable). A successful Influx write then this returning False (lease
    stolen) or raising (DB down) is NOT acked stored -> the lease expires, a retry reclaims
    and re-writes the same deterministic points (last-write-wins, no duplication) and then
    stores. At-least-once with idempotent points, NOT distributed exactly-once.
    """
    try:
        row = (await session.execute(
            update(IngestMessageModel)
            .where(IngestMessageModel.gateway_id == gateway_id,
                   IngestMessageModel.message_id == message_id,
                   IngestMessageModel.status == "pending",
                   IngestMessageModel.lease_owner == token)
            .values(status="stored", stored_at=datetime.now(timezone.utc))
            .returning(IngestMessageModel.id)
        )).scalar_one_or_none()
        await session.commit()
        return row is not None
    except Exception:
        try:
            await session.rollback()
        except Exception:
            pass          # keep the original error; a failed rollback must not mask it
        raise

```

### `src/logging.py`

```python
import logging
import sys

import structlog

from src.config import get_settings


def configure_logging() -> None:
    dev = get_settings().env == "dev"
    renderer = structlog.dev.ConsoleRenderer() if dev else structlog.processors.JSONRenderer()
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        logger_factory=structlog.PrintLoggerFactory(sys.stdout),
        cache_logger_on_first_use=True,
    )
```

### `src/main.py`

```python
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from sqlalchemy import text

from src.accounts.gateways_router import router as gateways_router
from src.accounts.router import router as accounts_router
from src.auth.router import router as auth_router
from src.accounts.operators_router import router as operators_router
from src.gateway.router import router as gateway_router
from src.ingestion.router import router as ingest_router
from src.telemetry.router import router as telemetry_router
from src.config import get_settings
from src.database import engine
from src.influx import InfluxGateway
from src.logging import configure_logging
from src.middleware import RequestContextMiddleware
from src.ratelimit import init_rate_limiter, shutdown_rate_limiter

# Bound the Influx AND Redis readiness probes so a slow/hung dependency can't stall
# /ready; the Postgres probe is unchanged in this slice.
READY_PROBE_TIMEOUT_SECONDS = 3


@asynccontextmanager
async def _lifespan(app: FastAPI):
    # Build the shared rate limiter (no-op unless rate_limit_enabled). Never fails startup.
    await init_rate_limiter(app)
    try:
        yield
    finally:
        await shutdown_rate_limiter(app)


def create_app() -> FastAPI:
    configure_logging()
    app = FastAPI(title="UnityESS Central EMS", lifespan=_lifespan)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "service": "ems-backend"}

    @app.get("/ready")
    async def ready() -> JSONResponse:
        checks: dict[str, str] = {}
        try:
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            checks["postgres"] = "ok"
        except Exception as exc:  # noqa: BLE001 - readiness probe reports any failure
            checks["postgres"] = f"error: {type(exc).__name__}"
        try:
            client = Redis.from_url(get_settings().redis_url)
            try:
                await asyncio.wait_for(client.ping(), timeout=READY_PROBE_TIMEOUT_SECONDS)
            finally:
                await client.aclose()
            checks["redis"] = "ok"
        except Exception as exc:  # noqa: BLE001 - readiness probe reports any failure
            checks["redis"] = f"error: {type(exc).__name__}"
        try:
            healthy = await asyncio.wait_for(
                InfluxGateway(get_settings()).health(),
                timeout=READY_PROBE_TIMEOUT_SECONDS,
            )
            checks["influx"] = "ok" if healthy else "error: unhealthy"
        except Exception as exc:  # noqa: BLE001 - reports any failure, incl. TimeoutError
            checks["influx"] = f"error: {type(exc).__name__}"
        ok = all(v == "ok" for v in checks.values())
        return JSONResponse(
            {"status": "ok" if ok else "unavailable", **checks}, status_code=200 if ok else 503
        )

    app.include_router(auth_router)      # <-- add (mounts /api/v1/auth/*)
    app.include_router(accounts_router) 
    app.include_router(operators_router) 
    app.include_router(gateways_router)  # mounts /api/v1/admin/sites/{site_id}/gateways
    app.include_router(gateway_router)   # mounts /api/v1/gateway/*
    app.include_router(ingest_router)    # mounts /api/v1/gateway/ingest (503 until 9b-ii)
    app.include_router(telemetry_router) # mounts /api/v1/telemetry/* (read-only; ungated)
    return app


app = create_app()

# Served ASGI entrypoint: request-correlation middleware mounted OUTERMOST so it
# wraps Starlette's ServerErrorMiddleware -> framework-generated 500s still carry
# X-Request-ID and normal framework exception handling runs. Serve src.main:asgi_app.
asgi_app = RequestContextMiddleware(app)

```

### `src/middleware.py`

```python
"""Request-correlation + access-logging ASGI middleware (slice O-1).

Pure ASGI middleware mounted OUTERMOST (wrapping Starlette's ServerErrorMiddleware, via
`asgi_app` in src/main.py) so:
  * X-Request-ID is injected on EVERY response, including framework-generated 500s;
  * exactly one access event is emitted per HTTP request (even when the app raises);
  * unhandled exceptions are NOT swallowed - a single structured `unhandled_exception`
    event (request_id/method/path/exc_type only) is logged, then the exception is
    re-raised so Starlette/FastAPI's ServerErrorMiddleware and any registered exception
    handlers run normally and the server still renders the 500.
Both the access event and the error event carry only request_id + method/path (+status/
duration or exc_type); never headers, cookies, bodies, query strings, exception messages,
args, or tracebacks.
"""
import re
import time
import uuid

import structlog
from starlette.datastructures import MutableHeaders

# Safe correlation id: printable, bounded, no CR/LF / control chars / spaces, so it cannot
# inject a header. Validated AS RECEIVED (no trimming); anything else -> generated uuid.
_VALID_ID = re.compile(r"^[A-Za-z0-9._-]{1,128}$")


def _resolve_request_id(scope) -> str:
    found = [v for (k, v) in scope.get("headers", []) if k == b"x-request-id"]
    if len(found) == 1:                       # exactly one header; 0 or ambiguous duplicates -> generate
        try:
            candidate = found[0].decode("latin-1")
        except Exception:
            candidate = ""
        if _VALID_ID.fullmatch(candidate):    # validated as received, not stripped
            return candidate
    return uuid.uuid4().hex


class RequestContextMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)      # forward lifespan/websocket untouched
            return

        request_id = _resolve_request_id(scope)
        # 500 until a response.start is observed (covers the app raising before responding;
        # the real framework 500 flows through send_wrapper and overwrites this).
        state = {"status": 500}

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                state["status"] = message["status"]
                message.setdefault("headers", [])
                MutableHeaders(raw=message["headers"])["X-Request-ID"] = request_id
            await send(message)

        start = time.perf_counter()
        # bound_contextvars resets request_id on exit (normal OR exception) -> no leak.
        with structlog.contextvars.bound_contextvars(request_id=request_id):
            try:
                # Let exceptions propagate so ServerErrorMiddleware and any registered
                # handlers run; its 500 response.start passes through send_wrapper above,
                # so the header is injected before it re-raises.
                await self.app(scope, receive, send_wrapper)
            except Exception as exc:
                # One structured, correlated line for a genuinely-unhandled exception.
                # Fields ONLY: no message/args/traceback/exc_info/headers/cookies/body/query.
                try:
                    structlog.get_logger("error").error(
                        "unhandled_exception",
                        request_id=request_id,
                        method=scope.get("method"),
                        path=scope.get("path"),
                        exc_type=type(exc).__name__,
                    )
                except Exception:
                    pass  # logging must never replace or suppress the application exception
                raise
            finally:
                duration_ms = round((time.perf_counter() - start) * 1000, 2)
                structlog.get_logger("access").info(
                    "access",
                    request_id=request_id,
                    method=scope.get("method"),
                    path=scope.get("path"),
                    status=state["status"],
                    duration_ms=duration_ms,
                )

```

### `src/models.py`

```python
import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class BaseEntityModel(Base):
    __abstract__ = True

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
```

### `src/ratelimit.py`

```python
"""Rate limiting (shadow-first; enforcement OFF by default).

Implements the reviewed rate-limiting MECHANISM only. It changes no authentication,
scope-validation, idempotency, or ingestion semantics. It is inert unless explicitly
configured:

  * rate_limit_enabled = False  -> dependencies are a hard no-op (no Redis, no log).
  * rate_limit_enforce = False  -> SHADOW mode: evaluate + count + log would-be
                                   denials, but NEVER block and NEVER return 429.
  * a policy capacity/refill is None or capacity <= 0 -> that policy is inert even when
    enabled. NO numeric value here is approved operational policy; all are placeholders
    pending gateway/ops confirmation.

Keys:
  * /token  -> source IP only (direct peer). Request-body identity is untrusted and is
    never used as a key. X-Forwarded-For is NOT trusted (this repo establishes no
    trusted-proxy handling); behind a proxy the key is the proxy address - a documented
    limitation - with a safe constant fallback when the peer address is unavailable.
  * /ingest -> keyed on the AUTHENTICATED gateway id. The dependency runs AFTER
    authentication (it depends on get_current_gateway), so a missing/invalid/disabled
    credential returns the existing 401/403 BEFORE any limiting - unauthenticated
    requests are never rate-limited and auth semantics are unchanged.

Global-limit note: only the Redis token bucket (atomic single-EVAL Lua) enforces a
GLOBAL limit. The process-local fallback used when Redis is unavailable is PER-WORKER
and therefore DOES NOT enforce a global limit (see LocalTokenBucket) - it is a
degradation-mode circuit breaker only, with bounded memory.

Operational prerequisite (NOT assumed or provisioned here): a reachable, correctly
sized and - for multi-instance deployments - HIGHLY AVAILABLE Redis is required for the
global limit to hold. This module introduces no Redis HA assumption.

Privacy: logs carry only route, a coarse key-kind, and a truncated SHA-256 of the key.
Never raw IPs, client secrets, connection URLs, tokens, Authorization headers, or bodies.
"""
from __future__ import annotations

import asyncio
import hashlib
import time
from collections import OrderedDict
from dataclasses import dataclass

import structlog
from fastapi import Depends, HTTPException, Request, status

from src.config import get_settings
from src.gateway.dependencies import get_current_gateway

_log = structlog.get_logger("ratelimit")

# Atomic token bucket in a single EVAL -> race-free across workers/instances.
_LUA_TOKEN_BUCKET = """
local cap    = tonumber(ARGV[1])
local refill = tonumber(ARGV[2])
local now    = tonumber(ARGV[3])
local ttl    = tonumber(ARGV[4])
local d = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(d[1])
local ts = tonumber(d[2])
if tokens == nil then tokens = cap; ts = now end
local delta = now - ts
if delta < 0 then delta = 0 end
tokens = math.min(cap, tokens + delta * refill)
local allowed = 0
local retry = 0.0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  if refill > 0 then retry = (1 - tokens) / refill else retry = ttl end
end
redis.call('HMSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('EXPIRE', KEYS[1], ttl)
return {allowed, tostring(tokens), tostring(retry)}
"""


@dataclass(frozen=True)
class Policy:
    capacity: float
    refill_per_sec: float

    @property
    def ttl(self) -> int:
        if self.refill_per_sec > 0:
            return max(1, int(self.capacity / self.refill_per_sec) + 1)
        return 60


@dataclass(frozen=True)
class Decision:
    allowed: bool
    remaining: float
    retry_after: float


class RedisTokenBucket:
    """Global, atomic token bucket backed by Redis (one EVAL per check)."""

    def __init__(self, client):
        self._c = client

    async def consume(self, key: str, policy: Policy, now: float) -> Decision:
        res = await self._c.eval(
            _LUA_TOKEN_BUCKET, 1, key,
            policy.capacity, policy.refill_per_sec, now, policy.ttl,
        )
        return Decision(bool(int(res[0])), float(res[1]), float(res[2]))

    async def aclose(self) -> None:
        try:
            await self._c.aclose()
        except Exception:
            pass


class LocalTokenBucket:
    """PER-PROCESS fallback bucket - DEGRADATION MODE ONLY, with bounded memory.

    Lets one worker bound its own load when Redis is unreachable. It is NOT shared
    across workers/instances, so with W processes the aggregate allowance is ~ W x
    capacity: it DOES NOT enforce a global limit and must never be presented as one.

    Memory is bounded two ways (concurrency-safe under a single asyncio.Lock):
      * idle expiry - entries untouched for >= idle_seconds are dropped (0 disables);
      * hard cap   - at most max_keys entries; the least-recently-used are evicted.
    """

    def __init__(self, max_keys: int = 10_000, idle_seconds: float = 300.0) -> None:
        self._state: "OrderedDict[str, tuple[float, float]]" = OrderedDict()
        self._lock = asyncio.Lock()
        self._max_keys = max(1, int(max_keys))
        self._idle_seconds = float(idle_seconds)

    def set_bounds(self, max_keys: int, idle_seconds: float) -> None:
        self._max_keys = max(1, int(max_keys))
        self._idle_seconds = float(idle_seconds)

    def _evict_idle(self, now: float) -> None:
        if self._idle_seconds <= 0:
            return
        cutoff = now - self._idle_seconds
        stale = []
        for k, (_tok, ts) in self._state.items():   # oldest-access first (LRU order)
            if ts < cutoff:
                stale.append(k)
            else:
                break
        for k in stale:
            del self._state[k]

    async def consume(self, key: str, policy: Policy, now: float) -> Decision:
        async with self._lock:
            self._evict_idle(now)
            tokens, ts = self._state.get(key, (policy.capacity, now))
            delta = max(0.0, now - ts)
            tokens = min(policy.capacity, tokens + delta * policy.refill_per_sec)
            if tokens >= 1:
                tokens -= 1
                allowed, retry = True, 0.0
            else:
                allowed = False
                retry = (1 - tokens) / policy.refill_per_sec if policy.refill_per_sec > 0 else float(policy.ttl)
            self._state[key] = (tokens, now)
            self._state.move_to_end(key)               # mark most-recently-used
            while len(self._state) > self._max_keys:
                self._state.popitem(last=False)        # evict least-recently-used
            return Decision(allowed, tokens, retry)


# One per-worker fallback instance (bounds refreshed from settings at use time).
_local_fallback = LocalTokenBucket()


async def init_rate_limiter(app) -> None:
    """Lifespan startup: build the shared Redis limiter iff enabled. When disabled
    (default) sets app.state.rate_limiter = None and touches no network. Never raises:
    a limiter-setup failure is logged (exc_type only - no URL/secret) and the app starts
    fail-open (per-process fallback only)."""
    s = get_settings()
    app.state.rate_limiter = None
    if not s.rate_limit_enabled:
        return
    try:
        from redis.asyncio import Redis
        client = Redis.from_url(
            s.redis_url,
            socket_connect_timeout=s.rate_limit_redis_connect_timeout_seconds,
            socket_timeout=s.rate_limit_redis_socket_timeout_seconds,
            encoding="utf-8",
            decode_responses=True,
        )
        app.state.rate_limiter = RedisTokenBucket(client)
    except Exception as exc:
        # Observable, but never leak the URL/credentials: exc_type only.
        _log.warning("ratelimit_redis_init_failed", exc_type=type(exc).__name__)
        app.state.rate_limiter = None


async def shutdown_rate_limiter(app) -> None:
    rl = getattr(app.state, "rate_limiter", None)
    if rl is not None:
        await rl.aclose()


def _hash_key(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8", "replace")).hexdigest()[:16]


def _client_ip(request: Request) -> str:
    # X-Forwarded-For is deliberately NOT trusted (no established trusted-proxy config);
    # trusting it would let clients spoof the key. Direct peer only; behind a proxy this
    # is the proxy IP (documented). Safe constant fallback when peer is unavailable.
    client = request.client
    if client and client.host:
        return client.host
    return "unknown"


def _policy_for(route: str, s) -> Policy | None:
    if route == "token":
        cap, refill = s.token_rate_capacity, s.token_rate_refill_per_sec
    else:
        cap, refill = s.ingest_rate_capacity, s.ingest_rate_refill_per_sec
    # None or non-positive capacity, or (defensively) negative refill -> inert no-op.
    if cap is None or refill is None or cap <= 0 or refill < 0:
        return None
    return Policy(float(cap), float(refill))


async def _limit(request: Request, policy: Policy, key: str) -> Decision:
    """Bounded check: a slow/failed Redis must NOT hang the request. On any Redis error
    or timeout, degrade to the per-process fallback (which is NOT a global limit)."""
    s = get_settings()
    rl = getattr(request.app.state, "rate_limiter", None)
    now = time.time()
    if rl is not None:
        try:
            return await asyncio.wait_for(
                rl.consume(key, policy, now),
                timeout=s.rate_limit_redis_timeout_seconds,
            )
        except Exception:
            _log.warning("ratelimit_degraded", reason="redis_unavailable")
    _local_fallback.set_bounds(
        s.rate_limit_local_fallback_max_keys, s.rate_limit_local_fallback_idle_seconds)
    return await _local_fallback.consume(key, policy, now)


async def _evaluate(request: Request, *, route: str, key: str, key_kind: str) -> None:
    s = get_settings()
    if not s.rate_limit_enabled:
        return                               # hard no-op (default)
    policy = _policy_for(route, s)
    if policy is None:
        return                               # unconfigured / inert policy -> no-op
    dec = await _limit(request, policy, key)
    if dec.allowed:
        return
    if s.rate_limit_enforce:                  # OFF by default; not enabled in this task
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS,
            "rate limit exceeded",
            headers={"Retry-After": str(max(1, int(dec.retry_after + 0.999)))},
        )
    # SHADOW: never block. Privacy-conscious, low-cardinality event.
    _log.info("ratelimit_shadow_block", route=route, key_kind=key_kind,
              key_hash=_hash_key(key), retry_after=round(dec.retry_after, 3))


async def token_rate_limit(request: Request) -> None:
    """/token dependency: key on source IP (never on untrusted request-body identity)."""
    await _evaluate(request, route="token",
                    key=f"rl:token:ip:{_client_ip(request)}", key_kind="ip")


async def ingest_rate_limit(request: Request, gw=Depends(get_current_gateway)) -> None:
    """/ingest dependency: runs AFTER authentication (depends on get_current_gateway), so
    missing/invalid/disabled credentials return the existing 401/403 BEFORE any limiting;
    unauthenticated requests are never rate-limited and auth semantics are unchanged. Keyed
    on the authenticated gateway id (never a client-supplied value). Still runs before the
    ingest gate (503) and the ledger claim."""
    await _evaluate(request, route="ingest",
                    key=f"rl:ingest:gw:{gw.id}", key_kind="gateway")

```

### `src/rbac/dependencies.py`

```python
import uuid

from fastapi import Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.router import get_current_user
from src.database import get_session
from src.rbac.service import get_gateway_site, get_user_permissions, get_user_sites


def require_permission(permission):
    async def check(
        user=Depends(get_current_user),
        session: AsyncSession = Depends(get_session),
    ):
        permissions = await get_user_permissions(session, user)
        if permission not in permissions:
            raise HTTPException(status_code=403, detail="Permission denied")
        return user

    return check


async def require_site_access(
    gateway_id: uuid.UUID,
    user=Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
):
    gateway = await get_gateway_site(session, gateway_id)
    if gateway is None:
        raise HTTPException(status_code=404, detail="Gateway not found")

    site_id = gateway[0]
    sites = await get_user_sites(session, user)

    if sites is not None and site_id not in sites:
        raise HTTPException(status_code=403, detail="Site access denied")

    return site_id
```

### `src/rbac/models.py`

```python
import uuid

from sqlalchemy import Boolean, ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.models import Base, BaseEntityModel


class RoleModel(BaseEntityModel):
    __tablename__ = "roles"
    name: Mapped[str] = mapped_column(String(32), unique=True)   # super_admin / org_admin / site_operator


class PermissionModel(BaseEntityModel):
    __tablename__ = "permissions"
    name: Mapped[str] = mapped_column(String(64), unique=True)   # telemetry:read, command:write, ...


class OrganizationPermissionModel(BaseEntityModel):
    __tablename__ = "organization_permissions"
    __table_args__ = (UniqueConstraint("organization_id", "permission"),)

    organization_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        index=True,
    )
    permission: Mapped[str] = mapped_column(String(64))
    allowed: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    updated_by: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))


# --- junction tables: inherit Base, composite primary key (no id column) ---

class UserRoleModel(Base):
    __tablename__ = "user_roles"
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    role_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("roles.id", ondelete="CASCADE"), primary_key=True
    )


class RolePermissionModel(Base):
    __tablename__ = "role_permissions"
    role_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("roles.id", ondelete="CASCADE"), primary_key=True
    )
    permission_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("permissions.id", ondelete="CASCADE"), primary_key=True
    )


class UserSiteAccessModel(Base):
    __tablename__ = "user_site_access"
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    site_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("sites.id", ondelete="CASCADE"), primary_key=True
    )
```

### `src/rbac/permissions.py`

```python
# The full permission catalog
PERMISSIONS = [
    "dashboard:view", "telemetry:read", "soc:read", "solis:read",
    "alarms:read", "alarms:ack", "anomaly:read", "history:read",
    "config:read", "config:write", "command:write", "data:export",
    "user:manage", "site:manage", "org:manage",
]

ROLES = ["super_admin", "org_admin", "site_operator"]

# Which permissions each role holds by default
ROLE_PERMISSIONS = {
    "super_admin": PERMISSIONS,  # everything
    "org_admin": [
        "dashboard:view", "telemetry:read", "soc:read", "solis:read",
        "alarms:read", "alarms:ack", "anomaly:read", "history:read",
        "config:read", "config:write", "command:write", "data:export",
        "user:manage",
    ],
    "site_operator": [
        "dashboard:view", "telemetry:read", "soc:read", "solis:read",
        "alarms:read", "alarms:ack", "anomaly:read", "history:read",
        "config:read",
    ],
}

# Default allowed state when seeding an org: reads ON, sensitive writes OFF
DEFAULT_ORG_PERMISSION_ALLOWED = {p: True for p in PERMISSIONS}
for _p in ("config:write", "command:write", "data:export",
           "site:manage", "org:manage"):
    DEFAULT_ORG_PERMISSION_ALLOWED[_p] = False
```

### `src/rbac/service.py`

```python
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.rbac.models import (OrganizationPermissionModel, PermissionModel,RoleModel, RolePermissionModel)
from src.rbac.permissions import (DEFAULT_ORG_PERMISSION_ALLOWED, PERMISSIONS, ROLE_PERMISSIONS, ROLES)
from src.tenancy.organizations.models import OrganizationModel
from src.rbac.models import UserRoleModel, UserSiteAccessModel
from src.tenancy.gateways.models import GatewayModel
from src.tenancy.sites.models import SiteModel

async def seed_roles_permissions(session: AsyncSession) -> None:
    """Idempotent: creates roles, permissions, and role_permissions if missing."""
    roles = {r.name: r for r in (await session.execute(select(RoleModel))).scalars()}
    for name in ROLES:
        if name not in roles:
            roles[name] = RoleModel(name=name)
            session.add(roles[name])

    perms = {p.name: p for p in (await session.execute(select(PermissionModel))).scalars()}
    for name in PERMISSIONS:
        if name not in perms:
            perms[name] = PermissionModel(name=name)
            session.add(perms[name])

    await session.flush()  # assign ids

    existing = {(rp.role_id, rp.permission_id)
                for rp in (await session.execute(select(RolePermissionModel))).scalars()}
    for role_name, perm_names in ROLE_PERMISSIONS.items():
        for pname in perm_names:
            key = (roles[role_name].id, perms[pname].id)
            if key not in existing:
                session.add(RolePermissionModel(role_id=key[0], permission_id=key[1]))


async def seed_org_permissions(session: AsyncSession, organization_id) -> int:
    """Fill missing organization_permissions rows for one org at safe defaults."""
    existing = {op.permission for op in (await session.execute(
        select(OrganizationPermissionModel).where(
            OrganizationPermissionModel.organization_id == organization_id))).scalars()}
    added = 0
    for perm in PERMISSIONS:
        if perm not in existing:
            session.add(OrganizationPermissionModel(
                organization_id=organization_id,
                permission=perm,
                allowed=DEFAULT_ORG_PERMISSION_ALLOWED.get(perm, False)))
            added += 1
    return added


async def backfill_org_permissions(session: AsyncSession) -> int:
    """Run after adding a new permission, or over all orgs. Never overwrites existing rows."""
    orgs = (await session.execute(select(OrganizationModel))).scalars().all()
    total = 0
    for org in orgs:
        total += await seed_org_permissions(session, org.id)
    return total


async def get_user_roles(session, user_id):
    result = await session.execute(
        select(RoleModel.name)
        .join(UserRoleModel, UserRoleModel.role_id == RoleModel.id)
        .where(UserRoleModel.user_id == user_id)
    )
    return set(result.scalars())


async def get_user_permissions(session, user):
    roles = await get_user_roles(session, user.id)
    if "super_admin" in roles:
        return set(PERMISSIONS)

    result = await session.execute(
        select(PermissionModel.name)
        .join(RolePermissionModel, RolePermissionModel.permission_id == PermissionModel.id)
        .join(UserRoleModel, UserRoleModel.role_id == RolePermissionModel.role_id)
        .where(UserRoleModel.user_id == user.id)
    )
    role_permissions = set(result.scalars())

    if user.organization_id is None:
        return set()

    result = await session.execute(
        select(OrganizationPermissionModel.permission)
        .where(OrganizationPermissionModel.organization_id == user.organization_id)
        .where(OrganizationPermissionModel.allowed.is_(True))
    )
    org_permissions = set(result.scalars())

    return role_permissions & org_permissions


async def get_gateway_site(session, gateway_id):
    result = await session.execute(
        select(GatewayModel.site_id, SiteModel.organization_id)
        .join(SiteModel, SiteModel.id == GatewayModel.site_id)
        .where(GatewayModel.id == gateway_id)
    )
    return result.first()


async def get_user_sites(session, user):
    roles = await get_user_roles(session, user.id)

    if "super_admin" in roles:
        return None

    if "org_admin" in roles and user.organization_id is not None:
        result = await session.execute(
            select(SiteModel.id).where(SiteModel.organization_id == user.organization_id)
        )
        return set(result.scalars())

    result = await session.execute(
        select(UserSiteAccessModel.site_id).where(UserSiteAccessModel.user_id == user.id)
    )
    return set(result.scalars())
```

### `src/signals/models.py`

```python
from sqlalchemy import Boolean, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from src.models import BaseEntityModel


class SignalRegistryModel(BaseEntityModel):
    """Canonical telemetry point catalog for S1 and S2.

    S1 points come from the Ornate Canonical Signal Matrix (Primary Stream = S1).
    S2 points come from the Kalpa workbook only, deduplicated to one canonical
    row per (substream, signal_key); rack/PCS multiplicity is handled at ingest
    via the asset_id tag, not by storing duplicate registry rows.
    """

    __tablename__ = "signal_registry"
    __table_args__ = (
        UniqueConstraint("primary_stream", "substream", "signal_key",
                         name="uq_signal_registry_point"),
    )

    point_id: Mapped[str | None] = mapped_column(String(64), index=True)   # Ornate p-id; null for Kalpa
    source: Mapped[str] = mapped_column(String(16))                         # ornate | kalpa
    primary_stream: Mapped[str] = mapped_column(String(8), index=True)      # S1 | S2
    substream: Mapped[str] = mapped_column(String(32), index=True)
    asset_type: Mapped[str | None] = mapped_column(String(32))
    signal_key: Mapped[str] = mapped_column(String(160), index=True)
    display_name: Mapped[str | None] = mapped_column(String(255))
    unit: Mapped[str | None] = mapped_column(String(32))
    category: Mapped[str | None] = mapped_column(String(64))
    data_type: Mapped[str | None] = mapped_column(String(32))
    policy_mode: Mapped[str | None] = mapped_column(String(32))
    cadence_sec: Mapped[int | None] = mapped_column(Integer)
    deadband: Mapped[str | None] = mapped_column(String(32))
    policy_group: Mapped[str | None] = mapped_column(String(64))
    retention_class: Mapped[str | None] = mapped_column(String(16), index=True)
    event_eligible: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    rw: Mapped[str | None] = mapped_column(String(8))
    protocol_ref: Mapped[str | None] = mapped_column(String(200))           # vendor protocol path / register
    source_ref: Mapped[str | None] = mapped_column(String(128))             # workbook sheet + row
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")

```

### `src/signals/service.py`

```python
# import csv
# from pathlib import Path

# from sqlalchemy import func, select

# from src.signals.models import SignalRegistryModel

# SEED_PATH = Path(__file__).parent / "seed" / "signal_registry.csv"


# def _s(v):
#     v = (v or "").strip()
#     return v or None


# def _int(v):
#     v = (v or "").strip()
#     return int(v) if v else None


# def _bool(v):
#     return (v or "").strip().lower() in ("true", "1", "yes")


# async def load_seed(session, csv_path=SEED_PATH):
#     """Idempotent load. Skips rows already present by (primary_stream, substream, signal_key)."""
#     existing = {
#         (r.primary_stream, r.substream, r.signal_key)
#         for r in (await session.execute(select(SignalRegistryModel))).scalars()
#     }
#     new_rows = []
#     with open(csv_path, newline="", encoding="utf-8") as f:
#         for row in csv.DictReader(f):
#             key = (row["primary_stream"], row["substream"], row["signal_key"])
#             if key in existing:
#                 continue
#             existing.add(key)
#             new_rows.append(SignalRegistryModel(
#                 point_id=_s(row["point_id"]),
#                 source=row["source"],
#                 primary_stream=row["primary_stream"],
#                 substream=row["substream"],
#                 asset_type=_s(row["asset_type"]),
#                 signal_key=row["signal_key"],
#                 display_name=_s(row["display_name"]),
#                 unit=_s(row["unit"]),
#                 category=_s(row["category"]),
#                 data_type=_s(row["data_type"]),
#                 policy_mode=_s(row["policy_mode"]),
#                 cadence_sec=_int(row["cadence_sec"]),
#                 deadband=_s(row["deadband"]),
#                 policy_group=_s(row["policy_group"]),
#                 retention_class=_s(row["retention_class"]),
#                 event_eligible=_bool(row["event_eligible"]),
#                 rw=_s(row["rw"]),
#                 protocol_ref=_s(row["protocol_ref"]),
#                 source_ref=_s(row["source_ref"]),
#             ))
#     session.add_all(new_rows)
#     await session.flush()
#     return len(new_rows)


# async def count_by_stream(session):
#     rows = await session.execute(
#         select(SignalRegistryModel.primary_stream, func.count())
#         .group_by(SignalRegistryModel.primary_stream)
#     )
#     return dict(rows.all())


# async def count_s2_by_substream(session):
#     rows = await session.execute(
#         select(SignalRegistryModel.substream, func.count())
#         .where(SignalRegistryModel.primary_stream == "S2")
#         .group_by(SignalRegistryModel.substream)
#     )
#     return dict(rows.all())


import csv
from collections import defaultdict
from pathlib import Path

from sqlalchemy import func, select

from src.influx import RegistryIndex, measurement_for
from src.signals.models import SignalRegistryModel
from src.signals.types import quality_collisions

SEED_PATH = Path(__file__).parent / "seed" / "signal_registry.csv"


def _s(v):
    v = (v or "").strip()
    return v or None


def _int(v):
    v = (v or "").strip()
    return int(v) if v else None


def _bool(v):
    return (v or "").strip().lower() in ("true", "1", "yes")


async def load_seed(session, csv_path=SEED_PATH):
    existing = {
        (r.primary_stream, r.substream, r.signal_key)
        for r in (await session.execute(select(SignalRegistryModel))).scalars()
    }
    new_rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            key = (row["primary_stream"], row["substream"], row["signal_key"])
            if key in existing:
                continue
            existing.add(key)
            new_rows.append(SignalRegistryModel(
                point_id=_s(row["point_id"]), source=row["source"],
                primary_stream=row["primary_stream"], substream=row["substream"],
                asset_type=_s(row["asset_type"]), signal_key=row["signal_key"],
                display_name=_s(row["display_name"]), unit=_s(row["unit"]),
                category=_s(row["category"]), data_type=_s(row["data_type"]),
                policy_mode=_s(row["policy_mode"]), cadence_sec=_int(row["cadence_sec"]),
                deadband=_s(row["deadband"]), policy_group=_s(row["policy_group"]),
                retention_class=_s(row["retention_class"]),
                event_eligible=_bool(row["event_eligible"]), rw=_s(row["rw"]),
                protocol_ref=_s(row["protocol_ref"]), source_ref=_s(row["source_ref"]),
            ))
    session.add_all(new_rows)
    await session.flush()
    return len(new_rows)


async def count_by_stream(session):
    rows = await session.execute(
        select(SignalRegistryModel.primary_stream, func.count())
        .group_by(SignalRegistryModel.primary_stream)
    )
    return dict(rows.all())


async def count_s2_by_substream(session):
    rows = await session.execute(
        select(SignalRegistryModel.substream, func.count())
        .where(SignalRegistryModel.primary_stream == "S2")
        .group_by(SignalRegistryModel.substream)
    )
    return dict(rows.all())


async def load_registry_index(session) -> RegistryIndex:
    """Trusted (measurement, signal_key) -> RegistrySignal index for the write path."""
    rows = (await session.execute(
        select(SignalRegistryModel.primary_stream, SignalRegistryModel.substream,
               SignalRegistryModel.signal_key, SignalRegistryModel.asset_type,
               SignalRegistryModel.data_type, SignalRegistryModel.retention_class)
    )).all()
    return RegistryIndex.from_rows(rows)


async def quality_field_collisions(session):
    rows = (await session.execute(
        select(SignalRegistryModel.primary_stream, SignalRegistryModel.substream,
               SignalRegistryModel.signal_key)
    )).all()
    by_meas = defaultdict(set)
    for ps, sub, key in rows:
        by_meas[measurement_for(ps, sub)].add(key)
    return quality_collisions(by_meas)
```

### `src/signals/soc.py`

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class SoCMapping:
    measurement: str
    signal_key: str
    scope: str          # "per_asset" | "single"


# NOT production-confirmed. Do NOT populate from display_soc / PCS soc / ems_system soc
# until the gateway/EMS team confirms the authoritative signal and its asset semantics.
OVERALL_SOC: SoCMapping | None = None
RACK_SOC: SoCMapping | None = None

_UNCONFIGURED = {
    "status": "unconfigured",
    "detail": "SoC source not confirmed by gateway/EMS team; no value is guessed, "
              "duplicated across racks, or averaged.",
}


def resolve_overall_soc() -> dict:
    if OVERALL_SOC is None:
        return dict(_UNCONFIGURED, which="overall_soc")
    return {"status": "configured", "mapping": OVERALL_SOC}


def resolve_rack_soc() -> dict:
    if RACK_SOC is None:
        return dict(_UNCONFIGURED, which="rack_soc")
    return {"status": "configured", "mapping": RACK_SOC}
```

### `src/signals/types.py`

```python
import math

from src.telemetry_errors import InvalidValue, UnsupportedDataType

QUALITY_SUFFIX = "_q"

# Only the data types actually present in signal_registry + the frozen contract.
_FLOAT = {"FLOAT32", "Float"}
_INT_RANGE = {
    "U16": (0, 65535),
    "S16": (-32768, 32767),
    "U32": (0, 4294967295),
}

# Documented quality states: telemetry shows "good"; the S5-S6 delta defines
# GOOD/STALE/OFFLINE/ERROR. Case-insensitive on input, stored lowercase.
_QUALITY = {"good", "stale", "offline", "error"}


def field_type(data_type: str) -> str:
    if data_type in _FLOAT:
        return "float"
    if data_type in _INT_RANGE:
        return "int"
    raise UnsupportedDataType(f"Unsupported registry data_type: {data_type!r}")


def coerce(data_type: str, raw):
    """Typed value (float|int) or raise. Never clamps/rounds/substitutes.
    floats reject NaN/±Inf; ints reject fractional values and enforce documented range."""
    ft = field_type(data_type)
    if raw is None or isinstance(raw, bool):
        raise InvalidValue(f"invalid value {raw!r} for {data_type}")
    try:
        f = float(raw)
    except (TypeError, ValueError) as e:
        raise InvalidValue(f"cannot parse {raw!r} as a number ({data_type})") from e
    if math.isnan(f) or math.isinf(f):
        raise InvalidValue(f"NaN/Inf not allowed ({data_type})")
    if ft == "float":
        return f
    if not f.is_integer():
        raise InvalidValue(f"non-integer value {raw!r} for integer type {data_type}")
    v = int(f)
    lo, hi = _INT_RANGE[data_type]
    if not (lo <= v <= hi):
        raise InvalidValue(f"{v} out of range for {data_type} [{lo}, {hi}]")
    return v


def validate_quality(q):
    """Validate against the documented quality set; return normalized lowercase or None."""
    if q is None:
        return None
    if not isinstance(q, str):
        raise InvalidValue(f"quality must be a string, got {type(q).__name__}")
    ql = q.strip().lower()
    if ql not in _QUALITY:
        raise InvalidValue(f"unsupported quality {q!r}; allowed {sorted(_QUALITY)}")
    return ql


def quality_collisions(by_measurement: dict) -> list:
    """[(measurement, signal_key)] where <signal_key>+QUALITY_SUFFIX would overwrite
    a real signal in the same measurement."""
    out = []
    for meas, keys in by_measurement.items():
        ks = set(keys)
        for k in ks:
            if k + QUALITY_SUFFIX in ks:
                out.append((meas, k))
    return out
```

### `src/streams/__init__.py`

```python
"""Canonical stream classification - the single source of truth for which telemetry
streams are ingest-eligible, recognized-but-deferred, or known at all.

Defined on this package's __init__ so `from src.streams import ...` yields the shared,
immutable classification. Dependency-free (no imports from other src modules) to avoid
any circular import. These are CURRENT, source-verified classifications; the Gateway v2
stream/substream vocabulary is NOT finalized, so this is not a contract and holds no
per-substream vocabulary - substreams remain validated at runtime against the signal
registry, not a hardcoded list here.

The placeholder submodules (s1_fast_bess, ... d1_commands, envelope) are unused scaffolding
and are left untouched.
"""

# Streams accepted by the ingestion write path. Telemetry read support mirrors this set
# (see SUPPORTED_STREAMS in src/telemetry/service.py).
INGEST_STREAMS = frozenset({"S1", "S2"})

# Recognized by the contract but not yet implemented -> HTTP 501 at ingest.
DEFERRED_STREAMS = frozenset({"S3", "S4", "S5", "S6", "S7", "S8", "S9", "D1"})

# Every stream the envelope validator accepts; anything else -> HTTP 422.
KNOWN_STREAMS = INGEST_STREAMS | DEFERRED_STREAMS   # frozenset | frozenset -> frozenset

```

### `src/telemetry/dependencies.py`

```python
"""Phase 10 user-facing, site-ID-keyed read authorization (NOT gateway-token auth)."""
import uuid

from fastapi import Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import get_session
from src.rbac.dependencies import require_permission
from src.rbac.service import get_user_sites


async def require_site_read(
    site_id: uuid.UUID = Query(..., description="Target site; must be within the caller's scope"),
    user=Depends(require_permission("telemetry:read")),   # get_current_user -> 401; perm -> 403
    session: AsyncSession = Depends(get_session),
) -> uuid.UUID:
    """Return the authorized site_id, or 403. Site scope is recomputed from the DB every
    request. super_admin -> unrestricted; org_admin -> org's sites; site_operator -> assigned.
    Returns 403 for both unknown and unauthorized site_ids (no existence leak); fails closed."""
    sites = await get_user_sites(session, user)
    if sites is None:                 # super_admin: unrestricted
        return site_id
    if site_id in sites:
        return site_id
    raise HTTPException(status.HTTP_403_FORBIDDEN, "Site access denied")

```

### `src/telemetry/providers.py`

```python
"""Phase 10 read-only providers — UNGATED (work while INGEST_ENABLED=False).

These deliberately do NOT depend on require_ingest_enabled. They reuse the shared
registry-index cache (load_cached_index) and the existing InfluxDB configuration.
"""
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import get_settings
from src.database import get_session
from src.influx import InfluxGateway, RegistryIndex
from src.ingestion.providers import load_cached_index


async def get_registry_index_read(session: AsyncSession = Depends(get_session)) -> RegistryIndex:
    return await load_cached_index(session)


def get_influx_gateway_read() -> InfluxGateway:
    return InfluxGateway(get_settings())

```

### `src/telemetry/router.py`

```python
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status

from src.influx import _ident, bucket_for, measurement_for, rows
from src.signals.types import field_type
from src.telemetry import service as svc
from src.telemetry.dependencies import require_site_read
from src.telemetry.providers import get_influx_gateway_read, get_registry_index_read
from src.telemetry.schemas import (LatestPoint, LatestResponse, SeriesPoint, SeriesResponse, AggregateResponse)
from src.telemetry_errors import (InfluxTelemetryError, InvalidTimestamp,
                                   RetentionNotConfigured, UnknownRetentionClass,
                                   UnknownSignal, UnsafeQueryInput, UnsupportedDataType)

router = APIRouter(prefix="/api/v1/telemetry", tags=["telemetry"])


def _resolve(index, settings, stream, substream, signal_key):
    """Validate stream + registry signal, return (measurement, readable bucket).
    Unknown signal/stream -> 422; signal with no readable bucket -> 501 (unknown/unconfigured retention; T3 now routes to long)."""
    if stream not in svc.SUPPORTED_STREAMS:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "unsupported stream")
    measurement = measurement_for(stream, substream)
    try:
        sig = index.resolve(measurement, signal_key)
    except UnknownSignal:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "unknown signal")
    try:
        bucket = bucket_for(sig.retention_class, settings)
    except (RetentionNotConfigured, UnknownRetentionClass):
        raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, "signal retention bucket not available")
    return measurement, bucket


@router.get("/latest", response_model=LatestResponse)
async def latest(
    stream: str = Query(...),
    substream: str = Query(...),
    signal_key: str = Query(...),
    asset_id: str | None = Query(None),
    source_id: str | None = Query(None),
    site_id: uuid.UUID = Depends(require_site_read),      # 401/403 + authorized site
    index=Depends(get_registry_index_read),
    influx=Depends(get_influx_gateway_read),
):
    measurement, bucket = _resolve(index, influx.settings, stream, substream, signal_key)
    site = str(site_id)
    try:
        if asset_id is not None:
            _ident("asset_id", asset_id)
        if source_id is not None:
            _ident("source_id", source_id)
        if asset_id is not None:
            tables = await influx.latest_signal(bucket, measurement, site, signal_key,
                                                asset_id, source_id)
        else:
            tables = await influx.latest_by_asset(bucket, measurement, site, signal_key)
        recs = rows(tables)
    except UnsafeQueryInput:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "unsafe identifier")
    except InfluxTelemetryError:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "invalid request")
    except Exception:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "telemetry store unavailable")
    results = [LatestPoint(asset_id=r["asset_id"], source_id=r["source_id"],
                           value=r["value"], time=r["time"]) for r in recs]
    return LatestResponse(site_id=site_id, stream=stream, substream=substream,
                          signal_key=signal_key, results=results)


@router.get("/series", response_model=SeriesResponse)
async def series(
    stream: str = Query(...),
    substream: str = Query(...),
    signal_key: str = Query(...),
    asset_id: str = Query(...),
    start: str = Query(...),
    source_id: str | None = Query(None),
    stop: str | None = Query(None),
    limit: int = Query(svc.DEFAULT_LIMIT, ge=1, le=svc.MAX_LIMIT),
    site_id: uuid.UUID = Depends(require_site_read),
    index=Depends(get_registry_index_read),
    influx=Depends(get_influx_gateway_read),
):
    measurement, bucket = _resolve(index, influx.settings, stream, substream, signal_key)
    try:
        start_dt, stop_dt = svc.resolve_window(start, stop)
    except (InvalidTimestamp, ValueError):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "invalid time range")
    fetch = limit + 1                                   # +1 to detect truncation
    site = str(site_id)
    try:
        _ident("asset_id", asset_id)
        if source_id is not None:
            _ident("source_id", source_id)
        tables = await influx.series(bucket, measurement, site, signal_key,
                                     svc.to_flux_ts(start_dt), svc.to_flux_ts(stop_dt),
                                     asset_id, source_id, fetch)
        recs = rows(tables)
    except UnsafeQueryInput:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "unsafe identifier")
    except InfluxTelemetryError:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "invalid request")
    except Exception:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "telemetry store unavailable")
    truncated = len(recs) > limit
    pts = [SeriesPoint(time=r["time"], value=r["value"]) for r in recs[:limit]]
    return SeriesResponse(site_id=site_id, stream=stream, substream=substream,
                          signal_key=signal_key, asset_id=asset_id, points=pts, truncated=truncated)


@router.get("/aggregate", response_model=AggregateResponse)
async def aggregate(
    stream: str = Query(...),
    substream: str = Query(...),
    signal_key: str = Query(...),
    asset_id: str = Query(...),
    start: str = Query(...),
    window: str = Query(...),
    fn: str = Query(...),
    source_id: str | None = Query(None),
    stop: str | None = Query(None),
    site_id: uuid.UUID = Depends(require_site_read),
    index=Depends(get_registry_index_read),
    influx=Depends(get_influx_gateway_read),
):
    measurement, bucket = _resolve(index, influx.settings, stream, substream, signal_key)
    try:
        ftype = field_type(index.resolve(measurement, signal_key).data_type)
        flux_fn = svc.agg_flux_fn(fn, ftype)
    except UnsupportedDataType:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "signal type not aggregatable")
    except ValueError:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "unsupported aggregate function")
    try:
        start_dt, stop_dt, every = svc.resolve_agg(start, stop, window)
    except (InvalidTimestamp, ValueError):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "invalid aggregation window")
    site = str(site_id)
    try:
        _ident("asset_id", asset_id)
        if source_id is not None:
            _ident("source_id", source_id)
        tables = await influx.aggregate(bucket, measurement, site, signal_key,
                                        svc.to_flux_ts(start_dt), svc.to_flux_ts(stop_dt),
                                        svc.flux_every(every), flux_fn, asset_id, source_id)
        recs = rows(tables)
    except (RetentionNotConfigured, UnknownRetentionClass):
        raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, "signal retention bucket not available")
    except UnsafeQueryInput:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "unsafe identifier")
    except InfluxTelemetryError:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "invalid request")
    except Exception:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "telemetry store unavailable")
    points = [SeriesPoint(time=r["time"], value=r["value"]) for r in recs]
    return AggregateResponse(site_id=site_id, stream=stream, substream=substream,
                             signal_key=signal_key, asset_id=asset_id,
                             window=window, fn=fn, points=points)

```

### `src/telemetry/schemas.py`

```python
import uuid
from datetime import datetime

from pydantic import BaseModel


class LatestPoint(BaseModel):
    asset_id: str | None
    source_id: str | None
    value: float | int | None
    time: datetime | None


class LatestResponse(BaseModel):
    site_id: uuid.UUID
    stream: str
    substream: str
    signal_key: str
    results: list[LatestPoint]


class SeriesPoint(BaseModel):
    time: datetime
    value: float | int | None


class SeriesResponse(BaseModel):
    site_id: uuid.UUID
    stream: str
    substream: str
    signal_key: str
    asset_id: str
    points: list[SeriesPoint]
    truncated: bool


class AggregateResponse(BaseModel):
    site_id: uuid.UUID
    stream: str
    substream: str
    signal_key: str
    asset_id: str
    window: str
    fn: str
    points: list[SeriesPoint]

```

### `src/telemetry/service.py`

```python
"""Phase 10 read helpers: stream support, bounded-window parsing, Flux-time formatting."""
import re
from datetime import datetime, timedelta, timezone

from src.influx import parse_contract_timestamp
from src.streams import INGEST_STREAMS

# Telemetry read supports exactly the ingest-eligible streams (single source: src/streams).
SUPPORTED_STREAMS = INGEST_STREAMS
MAX_WINDOW = timedelta(days=7)
DEFAULT_LIMIT = 1000
MAX_LIMIT = 5000

_REL = re.compile(r"^-(\d+)(s|m|h|d|w)$")
_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


def resolve_start(start: str, now: datetime) -> datetime:
    """A supported relative range (e.g. -6h, -7d) or an RFC3339 timestamp (raises
    InvalidTimestamp on anything else)."""
    m = _REL.match(start)
    if m:
        return now - timedelta(seconds=int(m.group(1)) * _UNIT_SECONDS[m.group(2)])
    return parse_contract_timestamp(start)


def resolve_window(start: str, stop: str | None):
    """Return (start_dt, stop_dt) in UTC or raise ValueError/InvalidTimestamp for an
    invalid, reversed, or over-7-day window."""
    now = datetime.now(timezone.utc)
    start_dt = resolve_start(start, now)
    stop_dt = parse_contract_timestamp(stop) if stop else now
    if start_dt >= stop_dt:
        raise ValueError("start must be before stop")
    if stop_dt - start_dt > MAX_WINDOW:
        raise ValueError("time window exceeds 7 days")
    return start_dt, stop_dt


def to_flux_ts(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


# ---- Phase 10 Stage 2: time-bucketed aggregation ----
MAX_AGG_WINDOW = timedelta(days=366)
MAX_BUCKETS = 2000
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_MAX_WINDOW_DIGITS = 9   # the 366-day cap is 31_622_400s (8 digits); >9 digits is always invalid

# public fn name -> fixed Flux function (NEVER interpolate client input)
AGG_FUNCS = {
    "mean": "mean", "min": "min", "max": "max",
    "first": "first", "last": "last", "count": "count",
}
# fn -> the field_type() values it is valid for. field_type() returns exactly
# "float" or "int" (src/signals/types.py); every current registry signal is numeric,
# and the gate still rejects any future non-numeric type with 422.
_AGG_FN_TYPES = {
    "mean":  {"float", "int"},
    "min":   {"float", "int"},
    "max":   {"float", "int"},
    "first": {"float", "int"},
    "last":  {"float", "int"},
    "count": {"float", "int"},
}

_WINDOW = re.compile(r"^(\d+)(s|m|h|d|w)$")


def parse_window(window: str) -> timedelta:
    """Positive bucket interval, capped at MAX_AGG_WINDOW. Raises ValueError on a bad
    format, non-positive value, or an interval too large -- including an extremely long
    digit string, rejected by length BEFORE int() so a pathological input maps to 422."""
    m = _WINDOW.match(window or "")
    if not m:
        raise ValueError(f"invalid window: {window!r}")
    digits = m.group(1)
    if len(digits) > _MAX_WINDOW_DIGITS:
        raise ValueError(f"window value too large: {window!r}")
    n = int(digits)
    if n <= 0:
        raise ValueError(f"window must be positive: {window!r}")
    secs = n * _UNIT_SECONDS[m.group(2)]
    if secs > int(MAX_AGG_WINDOW.total_seconds()):
        raise ValueError(f"window exceeds maximum of 366 days: {window!r}")
    return timedelta(seconds=secs)


def flux_every(every: timedelta) -> str:
    """Whole-second Flux duration literal for aggregateWindow."""
    return f"{int(every.total_seconds())}s"


def agg_flux_fn(fn: str, field_type_name: str) -> str:
    """Map a public fn to its fixed Flux function, rejecting (ValueError) an unknown fn
    or an fn not valid for the resolved signal's field type."""
    flux = AGG_FUNCS.get(fn)
    if flux is None:
        raise ValueError(f"unsupported aggregate function: {fn!r}")
    if field_type_name not in _AGG_FN_TYPES[fn]:
        raise ValueError(f"function {fn!r} not valid for field type {field_type_name!r}")
    return flux


def _epoch_us(dt: datetime) -> int:
    """Exact integer microseconds since the UTC epoch (no float rounding)."""
    return (dt.astimezone(timezone.utc) - _EPOCH) // timedelta(microseconds=1)


def bucket_count(start_dt: datetime, stop_dt: datetime, every: timedelta) -> int:
    """Exact count of epoch/UTC-aligned windows of width `every` overlapping the half-open
    interval [start, stop), via integer microsecond arithmetic. Exact (not merely
    conservative): an exact stop boundary is excluded; a partial boundary window at either
    end is counted once; fractional-second timestamps honored at us resolution."""
    w_us = int(every.total_seconds()) * 1_000_000
    s = _epoch_us(start_dt)
    e = _epoch_us(stop_dt)
    first = s // w_us
    first_empty = -(-e // w_us)
    return first_empty - first


def resolve_agg(start: str, stop: str | None, window: str):
    """Validate an aggregation request BEFORE any InfluxDB call. Returns
    (start_dt, stop_dt, every) in UTC, or raises ValueError/InvalidTimestamp for a bad
    format, reversed/empty range, over-366-day horizon, bad/oversized window, or more than
    MAX_BUCKETS aligned output buckets."""
    now = datetime.now(timezone.utc)
    start_dt = resolve_start(start, now)
    stop_dt = parse_contract_timestamp(stop) if stop else now
    if start_dt >= stop_dt:
        raise ValueError("start must be before stop")
    if stop_dt - start_dt > MAX_AGG_WINDOW:
        raise ValueError("time window exceeds 366 days")
    every = parse_window(window)
    if bucket_count(start_dt, stop_dt, every) > MAX_BUCKETS:
        raise ValueError(f"aggregation exceeds {MAX_BUCKETS} buckets")
    return start_dt, stop_dt, every

```

### `src/telemetry_errors.py`

```python
class InfluxTelemetryError(Exception):
    """Base for telemetry write/query errors."""


class RetentionNotConfigured(InfluxTelemetryError): ...
class UnknownRetentionClass(InfluxTelemetryError): ...
class UnsupportedDataType(InfluxTelemetryError): ...
class InvalidValue(InfluxTelemetryError): ...
class InvalidTimestamp(InfluxTelemetryError): ...
class UnsafeQueryInput(InfluxTelemetryError): ...
class UnknownSignal(InfluxTelemetryError): ...
class SignalMismatch(InfluxTelemetryError): ...
class RegistryConflict(InfluxTelemetryError): ...
class UnresolvedMetadata(InfluxTelemetryError): ...
```

### `src/tenancy/bess_units/models.py`

```python
import uuid

from sqlalchemy import Float, ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.models import BaseEntityModel


class BessUnitModel(BaseEntityModel):
    __tablename__ = "bess_units"
    __table_args__ = (UniqueConstraint("gateway_id", "bess_key"),)

    gateway_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("gateways.id", ondelete="CASCADE"), index=True
    )
    bess_key: Mapped[str] = mapped_column(String(64))
    name: Mapped[str] = mapped_column(String(255))
    rated_kw: Mapped[float | None] = mapped_column(Float)
    rated_kwh: Mapped[float | None] = mapped_column(Float)
    chemistry: Mapped[str | None] = mapped_column(String(32))
```

### `src/tenancy/gateways/models.py`

```python
import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.models import BaseEntityModel


class GatewayModel(BaseEntityModel):
    __tablename__ = "gateways"

    site_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("sites.id", ondelete="CASCADE"), index=True
    )
    gateway_key: Mapped[str] = mapped_column(String(64), unique=True)
    # client_id: Mapped[str] = mapped_column(String(128))
    client_id: Mapped[str] = mapped_column(String(128), unique=True)
    client_secret_hash: Mapped[str] = mapped_column(String(255))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")
    software_version: Mapped[str | None] = mapped_column(String(64))
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
```

### `src/tenancy/organizations/models.py`

```python
from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from src.models import BaseEntityModel


class OrganizationModel(BaseEntityModel):
    __tablename__ = "organizations"

    org_key: Mapped[str] = mapped_column(String(64), unique=True)
    name: Mapped[str] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(32), default="active", server_default="active")
```

### `src/tenancy/sites/models.py`

```python
import uuid

from sqlalchemy import ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.models import BaseEntityModel


class SiteModel(BaseEntityModel):
    __tablename__ = "sites"
    __table_args__ = (UniqueConstraint("organization_id", "site_key"),)

    organization_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), index=True
    )
    site_key: Mapped[str] = mapped_column(String(64))
    name: Mapped[str] = mapped_column(String(255))
    timezone: Mapped[str] = mapped_column(String(64), default="UTC", server_default="UTC")
```

## 5. Empty scaffold files (not yet implemented)

- `src/__init__.py`
- `src/accounts/__init__.py`
- `src/accounts/models.py`
- `src/accounts/repository.py`
- `src/analytics/__init__.py`
- `src/analytics/calculations.py`
- `src/analytics/models.py`
- `src/analytics/repository.py`
- `src/analytics/router.py`
- `src/analytics/schemas.py`
- `src/analytics/service.py`
- `src/auth/__init__.py`
- `src/auth/rate_limit.py`
- `src/auth/tokens.py`
- `src/events/__init__.py`
- `src/events/alarms/__init__.py`
- `src/events/alarms/models.py`
- `src/events/alarms/repository.py`
- `src/events/alarms/router.py`
- `src/events/alarms/schemas.py`
- `src/events/alarms/service.py`
- `src/events/command_results/__init__.py`
- `src/events/command_results/models.py`
- `src/events/command_results/repository.py`
- `src/events/command_results/router.py`
- `src/events/command_results/schemas.py`
- `src/events/command_results/service.py`
- `src/events/config_audit/__init__.py`
- `src/events/config_audit/models.py`
- `src/events/config_audit/repository.py`
- `src/events/config_audit/router.py`
- `src/events/config_audit/schemas.py`
- `src/events/config_audit/service.py`
- `src/financials/__init__.py`
- `src/financials/models.py`
- `src/financials/repository.py`
- `src/financials/router.py`
- `src/financials/schemas.py`
- `src/financials/service.py`
- `src/fleet/__init__.py`
- `src/fleet/models.py`
- `src/fleet/repository.py`
- `src/fleet/router.py`
- `src/fleet/schemas.py`
- `src/fleet/service.py`
- `src/gateway/__init__.py`
- `src/genie/__init__.py`
- `src/genie/exports.py`
- `src/genie/models.py`
- `src/genie/repository.py`
- `src/genie/router.py`
- `src/genie/schemas.py`
- `src/genie/service.py`
- `src/ingestion/__init__.py`
- `src/ingestion/gateway_auth.py`
- `src/ingestion/idempotency.py`
- `src/ingestion/queue.py`
- `src/ingestion/repository.py`
- `src/notifications/__init__.py`
- `src/notifications/models.py`
- `src/notifications/push.py`
- `src/notifications/repository.py`
- `src/notifications/router.py`
- `src/notifications/schemas.py`
- `src/notifications/service.py`
- `src/rbac/__init__.py`
- `src/rbac/repository.py`
- `src/rbac/router.py`
- `src/rbac/schemas.py`
- `src/realtime/__init__.py`
- `src/realtime/pubsub.py`
- `src/realtime/router.py`
- `src/redis_client.py`
- `src/signals/__init__.py`
- `src/signals/repository.py`
- `src/signals/router.py`
- `src/signals/schemas.py`
- `src/strategy/__init__.py`
- `src/strategy/models.py`
- `src/strategy/repository.py`
- `src/strategy/router.py`
- `src/strategy/schemas.py`
- `src/strategy/service.py`
- `src/streams/d1_commands.py`
- `src/streams/envelope.py`
- `src/streams/s1_fast_bess.py`
- `src/streams/s2_general.py`
- `src/streams/s3_gateway_health.py`
- `src/streams/s4_alarms.py`
- `src/streams/s5_controller.py`
- `src/streams/s6_solis.py`
- `src/streams/s7_edge_ai.py`
- `src/streams/s8_config_audit.py`
- `src/streams/s9_command_results.py`
- `src/telemetry/__init__.py`
- `src/telemetry/models.py`
- `src/telemetry/queries.py`
- `src/telemetry/repository.py`
- `src/tenancy/__init__.py`
- `src/tenancy/bess_units/__init__.py`
- `src/tenancy/bess_units/repository.py`
- `src/tenancy/bess_units/router.py`
- `src/tenancy/bess_units/schemas.py`
- `src/tenancy/bess_units/service.py`
- `src/tenancy/gateways/__init__.py`
- `src/tenancy/gateways/repository.py`
- `src/tenancy/gateways/router.py`
- `src/tenancy/gateways/schemas.py`
- `src/tenancy/gateways/service.py`
- `src/tenancy/organizations/__init__.py`
- `src/tenancy/organizations/repository.py`
- `src/tenancy/organizations/router.py`
- `src/tenancy/organizations/schemas.py`
- `src/tenancy/organizations/service.py`
- `src/tenancy/sites/__init__.py`
- `src/tenancy/sites/repository.py`
- `src/tenancy/sites/router.py`
- `src/tenancy/sites/schemas.py`
- `src/tenancy/sites/service.py`
- `src/workers/__init__.py`
- `src/workers/__main__.py`
- `src/workers/consumer.py`
- `src/workers/dlq.py`
- `src/workers/writers/__init__.py`
- `src/workers/writers/influx_writer.py`
- `src/workers/writers/postgres_writer.py`
