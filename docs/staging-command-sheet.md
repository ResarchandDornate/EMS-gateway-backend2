# UnityESS Central EMS — Staging Rehearsal Command Sheet (Docker Compose / macOS)

> STAGING ONLY. Never run against the live `ems-gateway-backend2` stack (that one is exposed to real
> gateways via the Cloudflare Tunnel). Use a separate compose project `<STAGING_PROJECT>`.
> Companion to `docs/staging-rehearsal-plan.md` (full SQL predicates are in its Section 4) and the
> frozen `docs/go-live-checklist.md`. Placeholders `<...>` are operator-supplied (see
> `docs/staging-operator-inputs.md`). Do not execute against production. These are reference commands;
> nothing here has been run.
>
> Placeholders: `<STAGING_PROJECT>` (compose project, e.g. ems-staging), `<STAGING_API_PORT>` (host
> port, dev default 8040), `<OLD_IMAGE>` / `<NEW_IMAGE>` (built api image refs), `<CADENCE>` (measured
> gateway batch interval; see operator inputs #6).

## 1. Running containers + image digests (4c)
```
docker compose -p <STAGING_PROJECT> ps
docker inspect --format '{{.Name}} image={{.Config.Image}} digest={{.Image}}' <STAGING_PROJECT>-api-1
docker image inspect --format '{{.Id}} {{index .RepoTags 0}}' <NEW_IMAGE>
# pass: only the new-build api container runs; old-build count = 0
```

## 2. Service health
```
docker compose -p <STAGING_PROJECT> ps            # HEALTH column
curl -s http://localhost:<STAGING_API_PORT>/health
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:<STAGING_API_PORT>/ready
```

## 3. PostgreSQL sessions + outstanding transactions (4d)
```
docker exec <STAGING_PROJECT>-postgres-1 psql -U ems -d ems -c "
  SELECT pid, state, application_name, usename, client_addr, backend_start, xact_start,
         now()-xact_start AS xact_age, left(query,160) AS current_query
  FROM pg_stat_activity
  WHERE datname = current_database()
    AND state IN ('active','idle in transaction','idle in transaction (aborted)')
  ORDER BY xact_start NULLS LAST;"
# pass: no session from the OLD api container in active/idle-in-transaction (attribute by client_addr).
# No PgBouncer in repo; if staging adds one, confirm its old server-connections are closed.
```

## 4. Staging PostgreSQL backup + restore-verify (runbook 0.3; staging DB only)
```
docker exec <STAGING_PROJECT>-postgres-1 pg_dump -U ems -d ems -Fc -f /tmp/ems_staging.dump
docker cp <STAGING_PROJECT>-postgres-1:/tmp/ems_staging.dump ./ems_staging_$(date +%Y%m%d_%H%M).dump
docker exec <STAGING_PROJECT>-postgres-1 createdb -U ems ems_restore_check
docker cp ./ems_staging_*.dump <STAGING_PROJECT>-postgres-1:/tmp/restore.dump
docker exec <STAGING_PROJECT>-postgres-1 pg_restore -U ems -d ems_restore_check /tmp/restore.dump
docker exec <STAGING_PROJECT>-postgres-1 psql -U ems -d ems_restore_check -c \
  "SELECT stream, count(*) FROM stream_records GROUP BY stream;"
# pass: restore has zero ERROR and counts match the staging source.
```

## 5. Establish the source-write boundary (2.B step 4)
```
# cut over (recreate): rebuild/retag then:  docker compose -p <STAGING_PROJECT> up -d api
# confirm only the new build runs (section 1), then sample TWICE, >  <CADENCE>  apart:
docker exec <STAGING_PROJECT>-postgres-1 psql -U ems -d ems -c "
  SELECT stream, max(created_at) AS last_write, count(*) AS n
  FROM stream_records WHERE stream IN ('S3','S4','S5','S6','S7','S8','S9') GROUP BY stream;"
# pass: the two samples are identical for every S3-S9 stream. Record boundary = max(created_at).
# (Exclude S10 - it legitimately keeps writing on the new build.)
```

## 6. Backfill against the isolated staging database (2.B steps 2 & 5)
```
# run INSIDE the api container so the full app env (DB/Influx/Redis/JWT) is already present:
docker exec <STAGING_PROJECT>-api-1 \
  python -m scripts.backfill_structured --streams S3,S4,S5,S6,S7,S8,S9 --errors-out /tmp/backfill_staging.json
docker cp <STAGING_PROJECT>-api-1:/tmp/backfill_staging.json ./backfill_staging.json
# pass: ok:true per stream; empty errors/discrepancies. Idempotent/resumable; reads stream_records only.
```

## 7. The seven anti-join / payload-integrity / tenant-identity checks (2.B step 6)
Run each via `docker exec <STAGING_PROJECT>-postgres-1 psql -U ems -d ems -c "<query>"`. Use the exact
NULL-safe, UUID-guarded predicates in `docs/staging-rehearsal-plan.md` Section 4. Destinations:
S3 gateway_health_records · S4 alarm_event_records(event_id) · S5 controller_state_records ·
S6 solis_telemetry_records · S7 edge_ai_inference_records · S8 configuration_audit_records(audit_id) ·
S9 command_result_records. First run the pre-validation (0) queries (expect 0 malformed), then:
```
# record-keyed example (S3; repeat for S5/S6/S7/S9 with their tables):
docker exec <STAGING_PROJECT>-postgres-1 psql -U ems -d ems -c "
  SELECT count(*) FROM stream_records s
  LEFT JOIN gateway_health_records d
    ON d.gateway_id=s.gateway_id AND d.message_id=s.message_id AND d.record_index=s.record_index
  WHERE s.stream='S3' AND d.id IS NULL;"      -- expect 0
# event-keyed S4 / S8 use the CASE-guarded uuid cast (see plan Section 4).
```
# pass: every anti-join = 0, every payload `IS DISTINCT FROM` count = 0, every tenant/site count = 0.
# A count match or a second `migrated:0` run is corroboration ONLY, not proof.
