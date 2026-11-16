# PostgreSQL Retention & Partitioning Plan (audit D-08)

**Status: PREPARATORY / APPROVAL-GATED.** This defines the proposed retention policy and a
safe partitioning design. It contains **no destructive migration**: no existing row is
deleted, truncated, archived, or moved by this branch. Partition conversion and retention
activation are explicitly gated on approval and executed by an operator against a validated
staging copy first.

## 1. Tables with unbounded growth (confirmed)

| Table | Source | Event-time column | Grows with |
|---|---|---|---|
| `fast_bess_records` | S1 (PG JSONB) | `recorded_at` | every S1 fast sample |
| `stream_records` | S10 / generic (PG JSONB) | `recorded_at` | generic verbatim records |
| `gateway_health_records` | S3 | `recorded_at` | health snapshots |
| `controller_state_records` | S5 | `received_at_utc` | controller states |
| `solis_telemetry_records` | S6 | `received_at_utc` | solis telemetry |
| `edge_ai_inference_records` | S7 | `received_at_utc` | inferences |
| `alarm_event_records` | S4 | `occurred_at` | events |
| `configuration_audit_records` | S8 | `occurred_at` | config changes |
| `command_result_records` | S9 | `requested_at_utc` | commands |
| `ingest_messages` | ledger | `created_at` | every message |

S2 telemetry is in **InfluxDB** (already retention-bounded at 90d / 365d / 3y) and is out of
scope here.

## 2. Proposed retention policy (to confirm against the frozen contract)

These are **proposals**, not approved policy. Several stream periods are not yet fixed by the
Northbound contract and are marked *TBC*.

| Table | Proposed retention | Basis |
|---|---|---|
| `fast_bess_records` (S1) | 90 days hot in PG | high-rate fast data; long-term trend lives in Influx S2 |
| `stream_records` (S10/generic) | 180 days *TBC* | no authoritative contract period |
| `gateway_health_records` (S3) | 365 days *TBC* | operational history |
| `controller_state_records` (S5) | 365 days *TBC* | operational history |
| `solis_telemetry_records` (S6) | 365 days *TBC* | operational history |
| `edge_ai_inference_records` (S7) | 365 days *TBC* | operational history |
| `alarm_event_records` (S4) | 3 years | events are audit-relevant |
| `configuration_audit_records` (S8) | ≥ 5 years | audit / compliance (contract T5_5Y+) |
| `command_result_records` (S9) | ≥ 5 years | audit / compliance |
| `ingest_messages` (ledger) | 90 days for `stored`/`failed`; keep `pending` indefinitely | idempotency window; never drop unresolved |

**Hard rule:** retention must NEVER remove a `pending` or `failed` ledger row or a DLQ
`parked` row — those are unresolved/under-investigation. The **95 existing pending messages
are preserved** under every policy here.

## 3. Partitioning design (justified where it earns its keep)

Range-partition the high-volume, time-series-like tables **by month** on their event-time
column: `fast_bess_records`, `stream_records`, and S3/S5/S6/S7. This gives:

- **Partition pruning** — the site+time reads (now correctly indexed per D-05/06/07) only
  touch relevant months.
- **O(1) retention** — dropping an expired month is an instant `DROP TABLE partition`, not a
  mass `DELETE` + autovacuum storm.

Do **not** partition the low-volume audit tables (S4/S8/S9) or `ingest_messages` yet: their
unique keys include non-time columns (so the partition key can't be part of the PK without a
redesign), and their growth is modest. Revisit only if volume proves otherwise.

> Note: a partitioned table's PRIMARY KEY / UNIQUE constraint must include the partition key.
> The current per-stream uniqueness is `(gateway_id, message_id, record_index)`; converting to
> partitioned requires adding the month/event-time to that unique key. This is a schema
> decision to confirm before conversion (it changes the idempotency key shape).

## 4. Safe conversion procedure (approval-gated; non-destructive to data)

PostgreSQL cannot convert a populated table to partitioned in place. Use the **parallel-table
swap**, validated on staging first, with the old table retained as an immediate rollback:

1. **Backup**: `pg_dump --format=custom` of the target table (and a full-DB base backup).
2. Create `â€¦_partitioned` as a `PARTITION BY RANGE (<event_time>)` parent with the same
   columns + the revised unique key; pre-create the month partitions covering existing data +
   a few ahead.
3. **Copy** existing rows in batches (`INSERT â€¦ SELECT` by month) — source table untouched.
4. **Validate**: row counts per month match; spot-check newest/oldest rows; run the read
   APIs against the new table in staging.
5. **Swap** inside one transaction: `ALTER TABLE <t> RENAME TO <t>_old; ALTER TABLE
   <t>_partitioned RENAME TO <t>;` Keep `<t>_old` until confidence is high.
6. **Rollback**: reverse the rename; `<t>_old` is the untouched original.
7. Only after the swap is proven: attach a retention job that `DROP`s partitions older than
   the approved period (this is the destructive activation — separately approved).

Run the whole sequence on `docker-compose.staging.yml` (project `ems-gateway-staging`) with a
production-like data volume before touching production.

## 5. Safe first step (non-destructive, do now)

Before any conversion, size the impact with a **read-only** retention-candidate report — it
only counts, it never deletes:

```sql
-- rows older than a proposed cutoff, per table (example: fast_bess_records at 90d)
SELECT count(*) AS would_expire, min(recorded_at) AS oldest
FROM fast_bess_records
WHERE recorded_at < now() - interval '90 days';
```

Run the equivalent per table/period to quantify what retention would reclaim and to confirm
the policy before committing to partitioning.

## 6. What is intentionally NOT done here

- No retention `DELETE`/`DROP` is scheduled or executed.
- No table is partitioned in place.
- No existing data is deleted, truncated, archived, or migrated.
- No production or shared database is touched.
