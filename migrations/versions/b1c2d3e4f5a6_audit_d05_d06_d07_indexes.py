"""Audit D-05/D-06/D-07: add missing hot-path indexes.

Addresses three confirmed read/monitor hotspots found in the audit:

* D-05 ingest_messages: the monitor dashboard filters/sorts on status, created_at and
  stream, but only (gateway_id) and the uq(gateway_id,message_id) existed -> seq-scan+sort
  of the fastest-growing table on every dashboard call. Adds (status, created_at),
  (stream, status) and a PARTIAL index on created_at WHERE status='pending' (the stuck/
  pending backlog scan).
* D-06 S5/S6/S7 (controller/solis/edge_ai): the history/latest reads ORDER BY
  received_at_utc, but only sample_time_utc was indexed. Adds (site_id, received_at_utc).
* D-07 fast_bess_records: the live S1 snapshot does DISTINCT ON (bess_id) ORDER BY bess_id,
  recorded_at DESC with no supporting index. Adds (site_id, bess_id, recorded_at DESC).

PRODUCTION SAFETY: every index is created CONCURRENTLY inside an autocommit block and with
IF NOT EXISTS, so applying this on the live populated tables does NOT take a long write lock
and is idempotent/re-runnable. It is additive only — no column, constraint, routing, or data
change. CONCURRENTLY cannot run inside a transaction, hence the autocommit_block.

Verify after apply (see deployment runbook):
    EXPLAIN (ANALYZE, BUFFERS) <the monitor pending query>;      -- expect Index Scan
    EXPLAIN (ANALYZE, BUFFERS) <S5 received_at_utc history query>;
    EXPLAIN (ANALYZE, BUFFERS) <fast_bess latest DISTINCT ON query>;

Revision ID: b1c2d3e4f5a6
Revises: a2b3c4d5e6f8
Create Date: 2026-11-11

"""
import sqlalchemy as sa
from alembic import op

revision = "b1c2d3e4f5a6"
down_revision = "a2b3c4d5e6f8"
branch_labels = None
depends_on = None


def upgrade():
    with op.get_context().autocommit_block():
        # D-05 ingest_messages monitoring
        op.create_index("ix_ingest_messages_status_created_at", "ingest_messages",
                        ["status", "created_at"], unique=False,
                        postgresql_concurrently=True, if_not_exists=True)
        op.create_index("ix_ingest_messages_stream_status", "ingest_messages",
                        ["stream", "status"], unique=False,
                        postgresql_concurrently=True, if_not_exists=True)
        op.create_index("ix_ingest_messages_pending_created_at", "ingest_messages",
                        ["created_at"], unique=False,
                        postgresql_where=sa.text("status = 'pending'"),
                        postgresql_concurrently=True, if_not_exists=True)
        # D-06 S5/S6/S7 reads order/filter on received_at_utc
        op.create_index("ix_controller_state_site_received", "controller_state_records",
                        ["site_id", "received_at_utc"], unique=False,
                        postgresql_concurrently=True, if_not_exists=True)
        op.create_index("ix_solis_telemetry_site_received", "solis_telemetry_records",
                        ["site_id", "received_at_utc"], unique=False,
                        postgresql_concurrently=True, if_not_exists=True)
        op.create_index("ix_edge_ai_inference_site_received", "edge_ai_inference_records",
                        ["site_id", "received_at_utc"], unique=False,
                        postgresql_concurrently=True, if_not_exists=True)
        # D-07 fast_bess latest-per-bess snapshot
        op.create_index("ix_fast_bess_site_bess_recorded", "fast_bess_records",
                        ["site_id", "bess_id", op.f("recorded_at")], unique=False,
                        postgresql_concurrently=True, if_not_exists=True,
                        postgresql_ops={"recorded_at": "DESC"})


def downgrade():
    with op.get_context().autocommit_block():
        for name, table in [
            ("ix_fast_bess_site_bess_recorded", "fast_bess_records"),
            ("ix_edge_ai_inference_site_received", "edge_ai_inference_records"),
            ("ix_solis_telemetry_site_received", "solis_telemetry_records"),
            ("ix_controller_state_site_received", "controller_state_records"),
            ("ix_ingest_messages_pending_created_at", "ingest_messages"),
            ("ix_ingest_messages_stream_status", "ingest_messages"),
            ("ix_ingest_messages_status_created_at", "ingest_messages"),
        ]:
            op.drop_index(name, table_name=table, postgresql_concurrently=True,
                          if_exists=True)
