# UnityESS Central EMS — Staging Operator Inputs

> Companion to `docs/staging-rehearsal-plan.md` (do not rewrite that or the frozen
> `docs/go-live-checklist.md`). Lists every input the LIVE-cutover rehearsal needs, with a status.
> STATUS KEY: VERIFIED (from repo evidence) · UNKNOWN (no authoritative source; must be determined)
> · OPERATOR REQUIRED (operator must supply/confirm, or stand up for the staging instance).
> No secret values appear here. Nothing was executed.

## Verified deployment facts (from the repository)
- Platform: single-host **Docker Compose on macOS**, public ingress via a **Cloudflare Tunnel**
  (`cloudflared`): `ems-gateway-api.unityess.cloud` -> `http://localhost:8040`.
  Evidence: `scripts/expose_public.sh`, `docker-compose.yml`.
- Existing compose project name: **`ems-gateway-backend2`** (containers `…-api-1`, `…-postgres-1`,
  `…-influxdb-1`). Evidence: `scripts/show_gateway_data.sh`.
- Postgres/Influx/Redis are **local containers** (postgres:16 db `ems`/user `ems`; influxdb:2.7 org
  `unityess`, buckets `telemetry_raw_90d`+`telemetry_1y`; redis:7). Not managed. Evidence: `docker-compose.yml`.
- No reverse-proxy load balancer (only the Cloudflare Tunnel, single upstream), **no replicas**, **no
  PgBouncer**, and **no background writers** (ingestion is in the HTTP request path; the sole S3-S9
  writer is the `api` container). Evidence: `docker-compose.yml`, `src/database.py`, grep of `src/`.
- api image: `build: .`, **no tag/registry** (compose builds locally). Evidence: `docker-compose.yml`.
- CONSEQUENCE: with one Cloudflare upstream, a cutover is **stop-then-start (recreate)**, not a rolling
  multi-instance overlap — the source-write boundary is simply "old `api` container stopped".

## Operator inputs

| # | Input | Why required | Where to obtain | Evidence to mark VERIFIED | Status |
|---|---|---|---|---|---|
| 1 | Isolated STAGING stack (separate compose project, NOT the live `ems-gateway-backend2`) | The live stack is tunnel-exposed to real gateways; rehearsing on it risks production data | Stand up a new project: `docker compose -p <STAGING_PROJECT> up -d` with staging env | `docker compose -p <STAGING_PROJECT> ps` shows the isolated stack, distinct from the live one | OPERATOR REQUIRED |
| 2 | STAGING topology confirmation (does it mirror prod: single host, Compose, Cloudflare upstream, no replicas/pooler) | Determines the drain/boundary mechanism in 2.B | Operator/infra | A written statement + `docker compose -p <STAGING_PROJECT> ps` | OPERATOR REQUIRED |
| 3 | Database topology (local container vs managed) for staging | Backup/restore + `pg_stat_activity` commands differ | Staging env `DATABASE_URL` host | Confirm host is the staging postgres container (or managed endpoint) | VERIFIED for the repo stack (local container); OPERATOR REQUIRED to confirm staging |
| 4 | PgBouncer / pooler presence in staging | Changes session-drain verification (4d) | Staging infra | None present -> state so; if present, document pool-reset step | VERIFIED none in repo; OPERATOR REQUIRED to confirm staging |
| 5 | Current "old" api image tag + digest | 4c proves only the new build runs after cutover | On the host: `docker image inspect --format '{{.Id}}' <image>` / `docker inspect <container>` | Recorded old digest vs new digest | UNKNOWN (no tag/registry in repo); OPERATOR REQUIRED |
| 6 | Gateway BATCH push cadence (interval between batches), NOT signal sampling cadence | Sets the 4e boundary double-sample interval (must exceed it) | Measure on staging via `scripts/show_gateway_data.sh` (deltas of `ingest_messages.created_at` per gateway), or the gateway team | Two+ consecutive batch arrivals timed, or a written gateway-team figure | UNKNOWN / OPERATOR REQUIRED (repo cadences are per-signal and UNAPPROVED; do not infer) |
| 7 | Staging frontend origin(s) for CORS | `CORS_ALLOWED_ORIGINS` + `ENV=staging` so the browser frontend is allowed | Frontend/infra owner | Preflight from the real origin returns matching `access-control-allow-origin` | OPERATOR REQUIRED (not in repo) |
| 8 | Staging credentials availability (gateway client id/secret, a staging user JWT, Influx token, DB creds) | To drive synthetic ingest and run read-API checks | Staging secret store | Confirmation they exist and are staging-scoped (values NOT recorded here) | OPERATOR REQUIRED |
| 9 | Deployment / drain mechanism choice | 2.B step 4: recreate gap (gateway retries) vs a front-proxy overlap rehearsal | Operator decision | Written choice; if overlap, the proxy config | VERIFIED mechanism (Compose recreate = stop-then-start); OPERATOR REQUIRED choice |
| 10 | Confirm no extra S3-S9 writer in the staging stack | 2.B/4b inventory completeness | Staging compose/process list | `docker compose -p <STAGING_PROJECT> ps` + process list show only `api` | VERIFIED none in repo; OPERATOR REQUIRED to confirm staging |

## Gateway batch cadence — explicit finding
No authoritative batch-transmission interval exists in the repository. `docs/gateway-contract-confirmation.md`
states the cadence table is per-signal, registry-derived, and NOT gateway-approved. The batch-push
interval (how often `central-sync` posts a batch) is distinct from S1/S2 signal sampling and must NOT
be inferred from it. It must be MEASURED on staging (consecutive `ingest_messages.created_at` deltas
via `scripts/show_gateway_data.sh`) or CONFIRMED by the gateway team. Do not invent a value.
