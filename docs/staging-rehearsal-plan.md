# UnityESS Central EMS — Staging LIVE-Cutover Rehearsal Plan

> Preparation companion to `docs/go-live-checklist.md` (FROZEN — this file does not change it).
> STAGING ONLY. Nothing here has been executed. Production safety restrictions (from the runbook)
> remain in force: do not connect to production, do not enable ingestion in production, do not run
> production migrations/backfill, do not deploy/restart production, do not replay/ack gateway
> messages. Enabling ingestion ON STAGING is an explicit, logged operator decision for the rehearsal.
> Serve `src.main:asgi_app` (the Dockerfile CMD has been corrected to this).

## 1. Infrastructure inventory (verified from the repository)
| Item | Finding | Reference |
|---|---|---|
| Platform | Docker Compose only (no k8s/helm/systemd in repo) | `docker-compose.yml`, `Dockerfile` |
| Services | postgres:16 (user `ems`, db `ems`), influxdb:2.7 (org `unityess`), redis:7, `api` (build ., :8040->8000) | `docker-compose.yml` |
| API runtime | `alembic upgrade head && uvicorn src.main:asgi_app --workers 2 --proxy-headers` (migrations auto-apply at start; 2 workers/container; single `api` service, no replicas) | `Dockerfile` (CMD corrected this stage) |
| Background writers | NONE — ingestion runs in the HTTP request path; no Celery/RQ/cron/scheduler; "Central Sync" is the gateway-side pusher (client), not a backend process | grep of `src/` |
| Pooling | No PgBouncer in repo; direct asyncpg pool (`create_async_engine(..., pool_pre_ping=True)`); no `application_name` set | `src/database.py` |
| Migrations | Alembic single head `a1b2c3d4e5f7`, auto-applied by the `api` container at startup | `migrations/versions`, `Dockerfile` |

NOTE: `docker-compose.yml` ships DEV defaults (local passwords/tokens). Staging must override every
secret/host via staging env; the dev values are not staging configuration.

## 2. Entrypoint fix (done this stage)
`Dockerfile` CMD changed from `uvicorn src.main:app` to `uvicorn src.main:asgi_app` so the container
runs `RequestContextMiddleware` + CORS. `README.md` corrected (it had claimed there was no Dockerfile).
Regression guard added: `tests/unit/test_entrypoint.py`.

## 3. Staging LIVE-style cutover rehearsal (Docker Compose, staging-only)
Plain `docker compose up -d` recreate is STOP-THEN-START (a brief ingestion gap; gateway retries — no
loss), NOT a concurrent old/new overlap. To rehearse the true concurrent-overlap race, front two `api`
containers (old-tag + new-tag) with a reverse proxy — that proxy is OPERATOR-SUPPLIED (not in repo).

1. Bring up an ISOLATED staging stack (staging env/secrets, never prod): `docker compose up -d`.
   Confirm `docker compose ps` healthy; `alembic current` == head (auto-applied at start).
2. Build two tagged images: current deployed build as `ems-api:old`, this build as `ems-api:new`.
3. Staging PG backup (runbook 0.3 procedure) BEFORE any migrate/backfill.
4. [OPERATOR DECISION] enable staging ingestion; drive SYNTHETIC S1-S9 batches (pack
   `gateway_simulator.py`). Do not replay real production traffic.
5. Snapshot backfill (runbook 2.B step 2).
6. Cut over: `docker compose up -d` with `ems-api:new` (recreate), or start new alongside + drain old
   for the overlap variant.
7. Establish the boundary (runbook 2.B step 4): `docker compose ps` shows only new-tag; compare image
   digests with `docker inspect --format '{{.Image}}' <container>` / `docker image inspect --format
   '{{.Id}}' ems-api:new`; `pg_stat_activity` shows no old-container sessions; two identical S3-S9
   `stream_records` samples over an interval > the measured gateway cadence.
8. Catch-up backfill (runbook 2.B step 5) then the Section 4 SQL (runbook 2.B step 6).

## 4. Verification SQL (corrected — safe for NULLs and malformed UUIDs)
Destination identity per stream:
| Stream | Dedicated table | Match key |
|---|---|---|
| S3 | gateway_health_records | gateway_id + message_id + record_index |
| S4 | alarm_event_records | gateway_id + event_id (from payload) |
| S5 | controller_state_records | gateway_id + message_id + record_index |
| S6 | solis_telemetry_records | gateway_id + message_id + record_index |
| S7 | edge_ai_inference_records | gateway_id + message_id + record_index |
| S8 | configuration_audit_records | gateway_id + audit_id (from payload) |
| S9 | command_result_records | gateway_id + message_id + record_index |

Comparisons use `IS DISTINCT FROM` (NULL-safe) instead of `<>`. Event-keyed casts are guarded by a
UUID regex in a CASE so a malformed value yields NULL (never a cast error) and surfaces as unmatched.

```sql
-- UUID shape used below:
--   ^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$

-- (0) PRE-VALIDATION of event-keyed identities (run FIRST). Expect 0 rows; any row = malformed.
SELECT s.gateway_id, s.message_id, s.payload->>'event_id' AS bad_event_id
FROM stream_records s
WHERE s.stream='S4'
  AND (s.payload->>'event_id') IS NOT NULL
  AND (s.payload->>'event_id') !~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';
SELECT s.gateway_id, s.message_id, s.payload->>'audit_id' AS bad_audit_id
FROM stream_records s
WHERE s.stream='S8'
  AND (s.payload->>'audit_id') IS NOT NULL
  AND (s.payload->>'audit_id') !~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';

-- (i) ANTI-JOIN — expect 0 per stream.
-- record-keyed (run for S3/gateway_health_records, S5/controller_state_records,
--   S6/solis_telemetry_records, S7/edge_ai_inference_records, S9/command_result_records):
SELECT count(*) FROM stream_records s
LEFT JOIN gateway_health_records d
  ON d.gateway_id=s.gateway_id AND d.message_id=s.message_id AND d.record_index=s.record_index
WHERE s.stream='S3' AND d.id IS NULL;                                   -- expect 0
-- event-keyed S4 (guarded cast):
SELECT count(*) FROM stream_records s
LEFT JOIN alarm_event_records d
  ON d.gateway_id=s.gateway_id
 AND d.event_id = CASE WHEN s.payload->>'event_id' ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
                       THEN (s.payload->>'event_id')::uuid END
WHERE s.stream='S4' AND d.id IS NULL;                                   -- expect 0
-- event-keyed S8:
SELECT count(*) FROM stream_records s
LEFT JOIN configuration_audit_records d
  ON d.gateway_id=s.gateway_id
 AND d.audit_id = CASE WHEN s.payload->>'audit_id' ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
                       THEN (s.payload->>'audit_id')::uuid END
WHERE s.stream='S8' AND d.id IS NULL;                                   -- expect 0

-- (ii) PAYLOAD INTEGRITY — expect 0 (NULL-safe JSONB compare).
-- record-keyed template (per stream+table as in (i)):
SELECT count(*) FROM stream_records s
JOIN gateway_health_records d
  ON d.gateway_id=s.gateway_id AND d.message_id=s.message_id AND d.record_index=s.record_index
WHERE s.stream='S3' AND d.payload IS DISTINCT FROM s.payload;           -- expect 0
-- event-keyed S4 / S8: same guarded-cast join as (i), with
--   AND d.payload IS DISTINCT FROM s.payload

-- (iii) TENANT/SITE IDENTITY — expect 0 for every dedicated table (NULL-safe).
-- dedicated.site_id must equal the gateway's own site_id:
SELECT count(*) FROM gateway_health_records d JOIN gateways g ON g.id=d.gateway_id
WHERE d.site_id IS DISTINCT FROM g.site_id;                             -- expect 0  (repeat per table)
-- dedicated.site_id must equal the source row's site_id (record-keyed join shown; event-keyed uses
-- the guarded-cast join from (i)):
SELECT count(*) FROM stream_records s
JOIN gateway_health_records d
  ON d.gateway_id=s.gateway_id AND d.message_id=s.message_id AND d.record_index=s.record_index
WHERE s.stream='S3' AND d.site_id IS DISTINCT FROM s.site_id;           -- expect 0
```

## 5. Evidence requirements (record before declaring the rehearsal a pass)
- 2.B/4b: written inventory of old writers + evidence each stopped (here: the `api` container(s)).
- 2.B/4c: `docker compose ps` + image digests proving only the new tag runs (old-build count 0).
- 2.B/4d: `pg_stat_activity` output showing no old-deployment session `active`/`idle in transaction`.
- 2.B/4e: the TWO identical S3-S9 `stream_records` samples + the recorded boundary max(created_at).
- 2.B/6: Section-4 SQL outputs — pre-validation 0 rows, and all anti-join / payload / tenant counts 0;
  plus the corroborating `migrated:0` re-run (not sufficient alone).
- Read-API spot checks (runbook 1.6) against the rehearsed data.

## 6. Operator-supplied (cannot be established from the repo — do not invent)
- Actual STAGING topology: is it this compose, a managed PG, replicas, a front proxy, extra writers?
- Gateway BATCH push cadence for the 4e interval (repo cadences are per-signal and UNAPPROVED; the
  batch-push interval is unknown — measure on staging or obtain from the gateway team).
- PgBouncer / managed-pooler presence in staging (none in repo).
- The "old" (currently deployed) image tag/digest to compare against.
- Staging CORS origins, staging gateway client credentials, a staging user JWT for read checks.

## 7. Pre-rehearsal safety checklist
- Isolated staging DB/Influx/Redis; never point at production.
- Rehearsal image serves `src.main:asgi_app` (fixed) — confirm via `tests/unit/test_entrypoint.py`.
- Staging PG backup taken + restore-verified before migrate/backfill.
- `INGEST_ENABLED` stays OFF in production; enabling on staging is a logged operator decision.
- Synthetic test batches only; no replay of real production gateway traffic.
- Record all Section-5 evidence at each gate.
