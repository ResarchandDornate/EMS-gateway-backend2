# UnityESS Central EMS — Go-Live Execution Checklist (First Pilot)

> EXECUTION CHECKLIST. Only Section 0.1 (code tests) has recorded evidence. Everything else is
> an unexecuted operator step. Production ingestion state is NOT assumed — it is classified from
> evidence in Step 0.5, which then selects COLD (2.A) or LIVE (2.B).
> Serve the app as `src.main:asgi_app` (NOT `src.main:app`) — middleware + CORS live only on asgi_app.

## Status legend
- [VERIFIED] evidence exists (command output recorded).
- [TO RUN]   action defined; not yet executed.
- [GATE]     requires recorded approval / evidence before proceeding.

---

## SECTION 0 — Shared prerequisites (complete before staging OR production)

### 0.1 Code verification  [VERIFIED]
- Evidence: `pytest -q` on the dev host, 2026-11-01 -> **501 passed, 2 skipped**. Focused suites also
  green (batch_ingest, backfill_structured, fast_bess_reads, structured_reads, structured_summaries,
  ranges, cors, streams).
- Action: `pytest -q`
- Expected: all pass (2 pre-existing skips).
- Pass/Fail: 0 failed.
- Recovery: fix before proceeding; never start Section 1/2 on a red suite.
- NOTE: this is the ONLY layer with evidence. All steps below are unexecuted.

### 0.2 Migration-state pre-check + Alembic upgrade  [GATE]
The dedicated tables (gateway_health_records, alarm_event_records, controller_state_records,
solis_telemetry_records, edge_ai_inference_records, configuration_audit_records,
command_result_records) and fast_bess_records/stream_records are created by Alembic; the target
head is `a1b2c3d4e5f7`. Backfill and the new routing both FAIL if the DB is behind head.
- Action (per environment, staging first):
  `alembic current`                      # show the DB's current revision
  `alembic heads`                        # expect a1b2c3d4e5f7 as the single head
  If behind: take a backup first (Step 0.3), then `alembic upgrade head`.
- Expected: `alembic current` == `a1b2c3d4e5f7` (head). The upgrade only CREATEs new tables
  (additive; no change to existing tables), so it is online-safe on a running DB.
- Pass/Fail: current == head after the step. Fail = mismatch, or upgrade error.
- Recovery: if `alembic upgrade head` errors, do NOT proceed; restore from the Step 0.3 backup
  (`alembic downgrade` is a fallback only if a specific new table must be removed) and investigate.

### 0.3 PostgreSQL backup + restore verification  [GATE]  (shared prerequisite)
Required before 0.2's upgrade, before the staging dry run (1.5), and before the production
backfill (2.A/2.B). This backup is the production rollback point.
- SCOPE LIMITATION: this backs up PostgreSQL ONLY. S2 telemetry lives in **InfluxDB and is NOT
  included** in this restore point. A full S2 restore point requires a separate InfluxDB backup
  (`influx backup`); S2 is not migrated by this cutover, but note the gap for disaster recovery.
- Action:
  `pg_dump --format=custom --no-owner --dbname="$PROD_DATABASE_URL" --file ems_prod_$(date +%Y%m%d_%H%M).dump`
  Verify it restores into a scratch DB:
  `createdb ems_restore_check`
  `pg_restore --no-owner --dbname=ems_restore_check ems_prod_*.dump`
  `psql -d ems_restore_check -c "SELECT stream, count(*) FROM stream_records GROUP BY stream;"`
- Expected: dump + restore complete with zero ERROR; restore row counts match prod.
- Pass/Fail: `pg_restore` exit 0, no ERROR lines, counts equal prod. Fail = any error/mismatch.
- Recovery: re-run (check disk/space/perms). DO NOT run migrations (0.2) or backfill without a
  verified restorable backup.

### 0.4 Backfill CLI — full required environment
The backfill imports `src.database`, which constructs `Settings` at import. Settings REQUIRES all
of the following even though the backfill never contacts Influx or Redis — a missing one aborts at
import, before any work:
- `DATABASE_URL` (postgresql+asyncpg://...)   <- the only one the backfill actually uses
- `INFLUX_URL`, `INFLUX_TOKEN`, `INFLUX_ORG`, `INFLUX_BUCKET_RAW`, `INFLUX_BUCKET_LONG`
- `REDIS_URL`
- `JWT_SECRET`
For the restore-copy dry run (1.5), the Influx/Redis/JWT values need only be present and well-formed
(no service is contacted); point `DATABASE_URL` at `ems_restore_check`. For the production run (2.x),
use the real production environment.
- Pass/Fail: `python -m scripts.backfill_structured --help` runs without an import/config error.
- Recovery: set the missing variable(s); re-check.

### 0.5 Production-state verification -> classify COLD or LIVE  [GATE]
Production ingestion state is UNCONFIRMED. Do NOT assume. Gather all three evidences, record the
outputs, then classify.
- Action (a) effective ingest gate:
  `curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer <prod gw token>" https://<prod-api>/api/v1/gateway/ingest`
  Also read the deployed app's effective `INGEST_ENABLED` from its environment/config.
- Action (b) recent ingest ledger activity:
  `psql "$PROD_DATABASE_URL" -c "SELECT max(created_at) AS last_ingest, count(*) FILTER (WHERE created_at > now() - interval '1 hour') AS last_hour, count(*) FILTER (WHERE created_at > now() - interval '24 hours') AS last_day FROM ingest_messages;"`
- Action (c) recent stream_records writes:
  `psql "$PROD_DATABASE_URL" -c "SELECT max(created_at) AS last_row, max(recorded_at) AS last_data_ts, count(*) FILTER (WHERE created_at > now() - interval '1 hour') AS last_hour FROM stream_records;"`
  Also inspect application access logs for `POST /api/v1/gateway/ingest` over the last hour.
  (Accepted messages leave an `ingest_messages` row; REJECTED messages do NOT — a rejected-rate
  reading must come from the access logs, not the DB.)
- Classification:
  - **COLD** = ingest probe returns 503 (gate OFF) AND no `ingest_messages`/`stream_records`
    inserts in the last hour/day AND no recent ingest log traffic.
  - **LIVE** = ingest probe returns 200 (gate ON) OR any recent inserts/log traffic are present.
  - Ambiguous (gate ON but no recent data): treat as LIVE and investigate before cutover.
- Pass/Fail [GATE]: a classification of COLD or LIVE is recorded with the pasted evidence from
  (a)(b)(c) and an approver. Do NOT pick a path without this evidence.
- Recovery: if evidence is inconsistent (e.g. gate OFF but recent inserts), stop and reconcile —
  a mis-classification risks either an outage (treating LIVE as COLD) or a redundant window.

---

## SECTION 1 — Staging verification  [TO RUN]
Staging only. Ingestion MAY be enabled on staging to exercise the write path. Replace <placeholders>.

### 1.1 Configure staging CORS
- Action: on staging env set `ENV=staging`, `CORS_ALLOWED_ORIGINS=https://<staging-frontend>`; restart.
  Verify: `curl -i -X OPTIONS "https://<staging-api>/api/v1/health/latest" -H "Origin: https://<staging-frontend>" -H "Access-Control-Request-Method: GET" -H "Access-Control-Request-Headers: authorization"`
- Expected: `access-control-allow-origin` == the staging origin; `access-control-allow-headers`
  includes `authorization`; a disallowed origin gets no ACAO header.
- Pass/Fail: header present for the real origin, absent for others; `ENV` is `staging` (not `dev`,
  which would wrongly allow localhost).
- Recovery: fix env + `ENV`, restart, re-test.

### 1.2 Deploy build to staging + smoke
- Action: deploy serving `src.main:asgi_app`; `curl /health`; `curl -w "%{http_code}" /ready`.
- Expected: `/health` -> ok; `/ready` -> 200 (postgres/redis/influx ok).
- Pass/Fail: both 200. Fail = `/ready` 503 (read the per-dependency field).
- Recovery: fix the failing dependency; re-check `/ready`.

### 1.3 Staging batch ingestion S1-S9 -> correct destinations
- Action: with staging `INGEST_ENABLED=true`, POST a known test batch per stream to
  `POST /api/v1/gateway/ingest` with a staging gateway token (pack `gateway_simulator.py` or saved
  samples). Cover S1, S2, S3-S9, S10.
- Expected: S1 -> fast_bess_records; S2 -> InfluxDB; S3-S9 -> their dedicated tables; S10 ->
  stream_records; S2 combiner_box_meter excluded. Verify each:
  `SELECT count(*) FROM <dest> WHERE message_id='<sent id>';`
- Pass/Fail: row only in the correct store; HTTP 200. Fail = wrong/duplicate store, or 5xx.
- Recovery: read `rejected[]`; fix payload (strict S3/S4/S8/S9 need contract fields/enums) or config;
  resend with a new message_id.

### 1.4 Gateway ACK behavior
- Action: (a) new message; (b) resend same message_id+payload; (c) unsupported stream / malformed.
- Expected: (a) accepted; (b) already_processed, NO second row; (c) rejected `retryable:true`;
  all HTTP 200.
- Pass/Fail: statuses exact; duplicate does not double-write; one `ingest_messages` row.
- Recovery: if a duplicate double-writes, STOP (idempotency regression) — do not go to production.

### 1.5 Backfill DRY-RUN on a restored copy + duration sizing
Uses the restore from Step 0.3. This is the safe dry run (reads source, writes only to the throwaway
copy) AND the measurement used to size the production maintenance window.
- Action (time it):
  bash:  `time DATABASE_URL=postgresql+asyncpg://<user>@<host>/ems_restore_check <other env from 0.4> python -m scripts.backfill_structured --streams S3,S4,S5,S6,S7,S8,S9 --errors-out backfill_dryrun_errors.json`
  PowerShell: `Measure-Command { python -m scripts.backfill_structured --streams S3,S4,S5,S6,S7,S8,S9 --errors-out backfill_dryrun_errors.json }` (env set in the session)
- Expected: per stream `source_rows` matches the prod copy; record-keyed streams migrate ~= source;
  `failed_messages`/`discrepancies` empty; `ok: true`. Note the elapsed time.
- Pass/Fail: exit 0 and no error file. Fail = any failed/discrepancy.
- Window sizing: the backfill commits per message, so elapsed time scales with message count. Use
  the measured dry-run time (adjusted for prod/restore hardware differences) to size the production
  backfill window. A LIVE cutover (2.B) adds a second, smaller catch-up pass.
- Recovery: open `backfill_dryrun_errors.json` (each entry: message_id + record_index + reason).
  Contract-violating live records (strict S3/S4/S8/S9) stay in stream_records and are reported, not
  lost — decide per record (accept, or adjust validators) before the real run.

### 1.6 Read API verification against staging data
- Action: with a staging user JWT + authorized site_id:
  `curl -H "Authorization: Bearer <jwt>" "/api/v1/health/latest?site_id=<uuid>"`
  `curl ... "/api/v1/alarms?site_id=<uuid>&range=this_month"`
  `curl ... "/api/v1/alarms/summary?site_id=<uuid>&group_by=day&range=this_month"`
  Also: no token; and a site the user cannot access.
- Expected: 200 with the data ingested in 1.3; summary buckets continuous/zero-filled; 401 without
  token; 403 for an unauthorized site.
- Pass/Fail: returned rows match ingested; auth 401 and scope 403 enforced.
- Recovery: check RBAC seeding and `user_site_access`; confirm `require_site_read` wiring.

---

## SECTION 2 — Production cutover  [TO RUN]
Pick the path from Step 0.5's recorded classification. Prerequisites 0.2 (migration head), 0.3
(backup), 0.4 (env), 0.5 (classification) MUST be complete. The production state has NOT been
independently confirmed by this document.

Key difference between paths — the new build's `INGEST_ENABLED`:
- COLD: new build deploys with `INGEST_ENABLED=false`; enabled only after the approval gate.
- LIVE: `INGEST_ENABLED=true` MUST be set for the new build so the deploy does not stop ingestion
  (a new build defaulting to false would 503 live gateway traffic — retryable, no loss, but data
  stops landing until re-enabled).

### 2.A — COLD cutover (no active ingestion confirmed)
1. Backup (0.3) + confirm migration head (0.2).
2. Backfill existing history (there may be little/none if truly cold):
   `python -m scripts.backfill_structured --streams S3,S4,S5,S6,S7,S8,S9 --errors-out backfill_errors.json`
   - Expected/Pass: `ok: true`, no error file.
3. Deploy the new routing build serving `src.main:asgi_app`, with `INGEST_ENABLED=false` and prod CORS
   (`ENV=prod`, `CORS_ALLOWED_ORIGINS=https://<prod-frontend-1>,https://<prod-frontend-2>`).
4. Smoke (2.x-smoke) + destination/parity checks (2.x-verify) over the backfilled data.
5. [GATE] On recorded approval to begin ingestion: set `INGEST_ENABLED=true`, restart, then confirm
   live batches land in the correct stores (repeat 1.3 destination checks on prod) and reads return
   them. Begin post-enable monitoring (2.x-monitor).
- Pass/Fail: ingestion gated off until approval; after enable, destinations correct, ACK intact.
- Recovery: set `INGEST_ENABLED=false`, restart (gateway retries retryable messages — no loss),
  investigate, re-enable.

### 2.B — LIVE cutover (existing build is writing data)
Goal: preserve ingestion continuity and reach destination parity with ZERO data loss and NO
unmigrated gap. A rolling deploy runs old-build and new-build instances concurrently for a window:
old instances write S3-S9 to stream_records, new instances write to the dedicated tables. That
overlap is expected and safe (see duplicate handling in step 6), but the catch-up backfill is NOT
complete until the source-write boundary (step 4) is established. `migrated:0` is meaningful ONLY
after that boundary; on its own it proves nothing.

1. Backup (0.3) + confirm/apply migration head (0.2) - additive, online-safe while the old build runs.
2. History backfill (snapshot pass), old build still serving:
   `python -m scripts.backfill_structured --streams S3,S4,S5,S6,S7,S8,S9 --errors-out backfill_hist_errors.json`
3. Deploy the new routing build (`src.main:asgi_app`, INGEST_ENABLED=true, prod CORS). The rolling
   deploy starts the overlap window; new instances now route S3-S9 to the dedicated tables.

4. DRAIN AND STOP ALL OLD WRITERS - establish the frozen source-write boundary (BEFORE the catch-up).
   Do 4a-4e in order; the boundary holds only when ALL pass.

   4a. Drain in-flight HTTP requests: deregister old instances from the load balancer, allow
       connection draining so in-flight ingest requests either commit or are cut. A cut request
       returns no 200, so the gateway retries it (retryable) to a NEW instance -> dedicated table;
       no loss. Then terminate the old instances. If the platform cannot drain, hard-stop and rely
       on gateway retries.

   4b. ALL OLD WRITERS ACCOUNTED FOR (not just HTTP app instances): enumerate EVERY old-build
       process able to write S3-S9 into stream_records and confirm each is stopped - HTTP app
       instances, background workers, scheduled jobs / cron, any separate ingestion or central-sync
       process, and one-off/maintenance tasks. Produce a written inventory with evidence each is
       stopped (orchestrator list, process table, systemd unit status, crontab, drained worker queue).
       [INFRA PLACEHOLDER] the writer set and how each is listed/stopped is deployment-specific.
       This build runs ingestion in the HTTP request path with no separate worker, but DO NOT assume
       that in production - inventory and confirm.
       - Pass/Fail: inventory complete AND every listed old writer provably stopped. Fail = any old
         writer unaccounted for or still running.

   4c. Verify EVERY running instance is the new build: confirm out-of-band via the deployment
       orchestrator - `kubectl rollout status` [INFRA PLACEHOLDER], compare RUNNING image digests to
       the new build's digest, and/or container/process inventory + start times. No in-app version
       endpoint exists and none is being added; orchestrator image-digest + instance inventory is the
       build-verification method. Pass ONLY when the old-build instance count is provably 0.

   4d. DATABASE TRANSACTION DRAIN: after old writers are terminated, confirm NO outstanding
       transaction from the old deployment can still commit a write to stream_records. Inspect
       active / idle-in-transaction / long-running sessions:
       `SELECT pid, state, application_name, usename, client_addr, backend_start, xact_start,`
       `       now()-xact_start AS xact_age, left(query,160) AS current_query`
       `FROM pg_stat_activity`
       `WHERE datname = current_database()`
       `  AND state IN ('active','idle in transaction','idle in transaction (aborted)')`
       `ORDER BY xact_start NULLS LAST;`
       [INFRA PLACEHOLDER] attribute sessions to the OLD deployment by whatever distinguishes them -
       application_name, usename, client_addr, or pod/host. If a connection pooler (e.g. PgBouncer)
       fronts PostgreSQL, terminating app instances may leave pooled SERVER connections open; confirm
       the old pool is closed/reset, because pg_stat_activity shows pooler connections, not app
       instances.
       - Pass/Fail: ZERO sessions attributable to the old deployment in 'active' or 'idle in
         transaction', AND no open transaction with xact_start before the boundary observation that
         could still write stream_records. Fail = any such session/transaction.
       - Recovery: wait for it to finish; last resort terminate with `SELECT pg_terminate_backend(<pid>);`
         [INFRA PLACEHOLDER] - this ROLLS BACK the transaction (no partial write; the gateway retries
         any affected message to a new instance).

   4e. Confirm old-build writes to stream_records have CEASED (the boundary): after 4a-4d, sample
       TWICE over an interval that MUST exceed the actual gateway batch cadence. The 5-minute value
       is PROVISIONAL - replace it with a value strictly greater than the measured/known cadence
       before relying on it:
       `SELECT stream, max(created_at) AS last_write, count(*) AS n FROM stream_records WHERE stream IN ('S3','S4','S5','S6','S7','S8','S9') GROUP BY stream;`  -- sample at T and at T + (> cadence; 5 min PROVISIONAL)
       Both samples must be IDENTICAL (same max(created_at) and n) for every S3-S9 stream. Record the
       boundary = that max(created_at)/max(id). (Exclude S10: it legitimately keeps writing to
       stream_records on the new build.)
       - Pass/Fail: 4a-4d all passed AND two identical S3-S9 samples. Fail = samples differ
         (stream_records still receiving S3-S9 writes) -> boundary NOT established; do not proceed.
       - Recovery: find the remaining writer (return to 4b/4c/4d); re-sample until stable.

5. Run the catch-up backfill (only now, with the source frozen):
   `python -m scripts.backfill_structured --streams S3,S4,S5,S6,S7,S8,S9 --errors-out backfill_catchup_errors.json`

6. COMPLETENESS EVIDENCE (prove no gap). Completeness requires ALL THREE of: (i) per-stream anti-join
   = 0 for EVERY stream, (ii) payload integrity = 0 mismatches, (iii) tenant/site identity = 0
   mismatches. Matching row counts and a second `migrated:0` run are necessary corroboration but are
   NOT sufficient and MUST NOT be treated as proof of completeness on their own.

   Exact matching keys (source stream_records -> dedicated table):
   | Stream | Dedicated table              | Match key                               |
   | S3     | gateway_health_records       | gateway_id + message_id + record_index  |
   | S4     | alarm_event_records          | gateway_id + event_id                   |
   | S5     | controller_state_records     | gateway_id + message_id + record_index  |
   | S6     | solis_telemetry_records      | gateway_id + message_id + record_index  |
   | S7     | edge_ai_inference_records    | gateway_id + message_id + record_index  |
   | S8     | configuration_audit_records  | gateway_id + audit_id                   |
   | S9     | command_result_records       | gateway_id + message_id + record_index  |
   (S4.event_id / S8.audit_id are read from the source payload: (s.payload->>'event_id')::uuid /
   (s.payload->>'audit_id')::uuid.)

   (i) ANTI-JOIN - zero unmatched source rows for EVERY stream. Record-keyed template (run for S3, S5,
       S6, S7, S9 with the stream code + its table):
       `SELECT count(*) FROM stream_records s LEFT JOIN <dedicated> d ON d.gateway_id=s.gateway_id AND d.message_id=s.message_id AND d.record_index=s.record_index WHERE s.stream='<Sx>' AND d.id IS NULL;  -- expect 0`
       Event-keyed:
       `SELECT count(*) FROM stream_records s LEFT JOIN alarm_event_records d ON d.gateway_id=s.gateway_id AND d.event_id=(s.payload->>'event_id')::uuid WHERE s.stream='S4' AND d.id IS NULL;  -- expect 0`
       `SELECT count(*) FROM stream_records s LEFT JOIN configuration_audit_records d ON d.gateway_id=s.gateway_id AND d.audit_id=(s.payload->>'audit_id')::uuid WHERE s.stream='S8' AND d.id IS NULL;  -- expect 0`

   (ii) PAYLOAD INTEGRITY - the dedicated payload must equal the source record verbatim (JSONB equal)
        for every matched row. Record-keyed template:
        `SELECT count(*) FROM stream_records s JOIN <dedicated> d ON d.gateway_id=s.gateway_id AND d.message_id=s.message_id AND d.record_index=s.record_index WHERE s.stream='<Sx>' AND d.payload <> s.payload;  -- expect 0`
        Event-keyed: same `d.payload <> s.payload` predicate, joining on event_id / audit_id as above.
        (Validated re-serialization should be semantically identical; investigate any mismatch.)

   (iii) TENANT / SITE IDENTITY - the dedicated row's site_id must equal BOTH the gateway's own
        site_id and the source row's site_id (no cross-tenant drift). Per dedicated table:
        `SELECT count(*) FROM <dedicated> d JOIN gateways g ON g.id=d.gateway_id WHERE d.site_id <> g.site_id;  -- expect 0`
        `SELECT count(*) FROM stream_records s JOIN <dedicated> d ON <match key> WHERE s.stream='<Sx>' AND d.site_id <> s.site_id;  -- expect 0`

   Corroboration ONLY (not proof): `python -m scripts.backfill_structured --streams S3,S4,S5,S6,S7,S8,S9`
   -> every stream `migrated_messages: 0`, all `skipped_already_present`.
   - Pass/Fail: (i) every anti-join = 0 AND (ii) every payload count = 0 AND (iii) every tenant/site
     count = 0. Only then is the catch-up complete. Fail = any non-zero in (i)/(ii)/(iii) -> investigate
     the offending message_ids/event_ids; a non-zero anti-join means the boundary did not hold (return
     to step 4). A count match or `migrated:0` alongside a non-zero (i)/(ii)/(iii) is NOT complete.

   DUPLICATE / RETRY HANDLING during the overlap: at-least-once delivery and the old/new overlap can
   place the same message in BOTH stores (old -> stream_records, new -> dedicated) or deliver it twice.
   This causes NO duplication and NO loss: every store keys on (gateway_id, message_id, record_index)
   - and S4/S8 additionally on event_id/audit_id - so a repeat is an idempotent upsert (last-write-
   wins), not a new row. The catch-up upserting a stream_records copy onto an already-present dedicated
   row is a correctness no-op. stream_records copies are never deleted, so nothing is lost either way.

7. Proceed to 2.x-verify, 2.x-smoke, 2.x-monitor. Declare the cutover complete ONLY after step 6's
   anti-joins are all 0.
- Recovery: see 2.x-rollback. Ingestion is continuous and the backfill is idempotent, so rollback
  loses no data, but a later re-attempt REQUIRES repeating step 4 (a fresh boundary) then steps 5-6.

### 2.x-verify — Destination counts, payload integrity, tenant/site identity (both paths)
- Action (prod, read-only):
  `SELECT stream, count(*) FROM stream_records WHERE stream IN ('S3','S4','S5','S6','S7','S8','S9') GROUP BY stream;`
  `SELECT count(*) FROM gateway_health_records;  -- repeat per dedicated table`
  `SELECT message_id, payload FROM alarm_event_records ORDER BY occurred_at DESC LIMIT 5;  -- compare vs source`
  `SELECT count(*) FROM gateway_health_records h JOIN gateways g ON g.id=h.gateway_id WHERE h.site_id <> g.site_id;  -- expect 0`
- Expected: record-keyed streams (S3/S5/S6/S7/S9) dedicated count == source count; event-keyed
  (S4/S8) distinct event_id/audit_id == source distinct; payloads byte-identical; 0 tenant mismatch.
- Pass/Fail: counts reconcile (event upsert may collapse redelivered duplicates — explainable);
  0 cross-tenant rows.
- Recovery: re-run backfill for the affected stream; inspect specific message_ids if persistent.

### 2.x-smoke — Deploy smoke + CORS
- Action: after deploy, `/health` -> 200; `/ready` -> 200 (pg/redis/influx ok). Preflight from the
  real prod origin returns ACAO for it (and NOT for others / not localhost).
- Pass/Fail: probes green; CORS correct; one structured access log per request with `X-Request-ID`.
- Recovery: if request IDs/CORS missing, you likely served `src.main:app` instead of `asgi_app` —
  redeploy correctly. Otherwise see 2.x-rollback.

### 2.x-monitor — Post-enable monitoring
Watch for at least the first hours after ingestion is live (COLD: after enable; LIVE: after deploy):
- **ACK rejected[] rate** (from ingest access logs): a rise means strict S3/S4/S8/S9 records are
  being rejected `retryable` and silently staying in stream_records instead of landing in dedicated
  tables — contract drift. Threshold: alert on any sustained non-zero reject rate.
- **Dedicated-table insert rate**: confirm each live stream's dedicated table is actually growing:
  `SELECT relname, n_tup_ins FROM pg_stat_user_tables WHERE relname IN ('gateway_health_records','alarm_event_records','controller_state_records','solis_telemetry_records','edge_ai_inference_records','configuration_audit_records','command_result_records');`
  (sample twice, confirm deltas). Zero growth on a stream that should be live = routing/validation
  problem.
- Plus `/ready`, error logs, and request-id continuity.
- Pass/Fail: reject rate ~0; expected dedicated tables growing. Fail = sustained rejects or a flat
  table for an active stream.
- Recovery: inspect rejected reasons in logs; if contract drift, decide validator vs producer fix;
  if severe, 2.x-rollback.

### 2.x-rollback — Rollback procedure
- Trigger: failed smoke, wrong destination, elevated errors, or (LIVE) catch-up not reaching parity.
- Action: redeploy the previous known-good build/tag.
  - COLD: keep `INGEST_ENABLED=false` during rollback.
  - LIVE: the previous build keeps `INGEST_ENABLED=true` and resumes writing S3-S9 to stream_records
    (no data loss; reads are non-mutating; backfill is idempotent).
- ROLL-FORWARD NOTE: after any rollback, the old path writes S3-S9 to stream_records again, so a
  later re-attempt of the cutover MUST repeat the catch-up backfill (2.B step 4-5) to re-migrate
  everything written to stream_records while rolled back. Do not skip the catch-up on a second
  attempt.
- Restore the DB from the Step 0.3 backup ONLY if data corruption is confirmed (not expected: reads
  are non-mutating and backfill never modifies stream_records).
- Pass/Fail: previous version serving; `/health`+`/ready` 200.

### 2.x-storage — Storage-growth monitoring (streams without retention)
S3-S7 have no contract-defined retention (Decision A) -> no cleanup worker -> unbounded growth.
S8/S9 are T5_5Y+ (kept long by design).
- Action (schedule daily):
  `SELECT relname, pg_size_pretty(pg_total_relation_size(relid)) AS size, n_live_tup FROM pg_stat_user_tables WHERE relname IN ('gateway_health_records','controller_state_records','solis_telemetry_records','edge_ai_inference_records','stream_records','alarm_event_records','command_result_records','configuration_audit_records') ORDER BY pg_total_relation_size(relid) DESC;`
- Expected: track size + rows per table; disk-projection alert set.
- Pass/Fail: projected free disk stays above the agreed threshold.
- Recovery: when retention is contract-approved, add a retention worker + migration; interim,
  provision disk or archive-and-prune oldest S3-S7 rows.

---

## Appendix — environment variables
- `ENV` = dev | staging | prod (controls CORS origin set; must be staging/prod there)
- `CORS_ALLOWED_ORIGINS` = comma-separated frontend origins (staging/prod; never `*`)
- `CORS_ALLOW_CREDENTIALS` = false (Bearer auth is header-based)
- `INGEST_ENABLED` = false in production for a COLD cutover until the approval gate; true for a LIVE
  cutover to preserve ingestion continuity
- `DATABASE_URL`, `REDIS_URL`, `INFLUX_URL`/`INFLUX_TOKEN`/`INFLUX_ORG`/`INFLUX_BUCKET_RAW`/
  `INFLUX_BUCKET_LONG`, `JWT_SECRET` per environment (all required for the backfill CLI to import)
