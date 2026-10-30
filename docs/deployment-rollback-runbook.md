# Deployment & rollback runbook — Central EMS backend

Cautious, platform-neutral guidance. The repo contains **no production launch artifact** (no
Dockerfile, compose app service, Procfile, or CI/CD per `README.md`), so this makes **no
assumption** about hosting, process manager, CI, or topology. It documents sequence and decision
points only; it does not change any configuration and does not authorize enablement.

## Entrypoint (repository-confirmed)
Serve **`src.main:asgi_app`** (not `src.main:app`), without `--reload`, behind your ASGI
server/process manager. `asgi_app` wraps `RequestContextMiddleware`; serving `app` bypasses
`X-Request-ID`, the access log, and the correlated `unhandled_exception` 500 log. This is the one
deployment fact the repo asserts (`README.md` "Deploying"); confirm the deployed command matches.

## Deployment sequence (cautious)
1. Confirm configuration via `docs/staging-config-checklist.md` — required settings present; the
   three flags (`INGEST_ENABLED`, `rate_limit_enabled`, `rate_limit_enforce`) **OFF**.
2. Deploy the new build serving `src.main:asgi_app`.
3. **Liveness:** `/health` returns `200`.
4. **Readiness:** `/ready` returns `200` with `{postgres, redis, influx}` all `ok` (bounded 3 s
   each). A `503` here blocks promotion — investigate the failing dependency before proceeding.
5. **Smoke (gate OFF):** an authenticated `POST /api/v1/gateway/ingest` should return **503**
   (ingestion gated) — this confirms auth + gate wiring without ingesting anything. Unauthenticated
   requests should still return **401/403**. Do **not** attempt real ingestion.
6. Watch `docs/observability-watchlist.md` events for a soak period: `access` latency/errors, any
   `unhandled_exception`, and (if the limiter were enabled) `ratelimit_degraded`.

## Rollback decision points
- `/ready` stuck `503`, or a spike of `unhandled_exception`, or elevated 5xx in `access` →
  consider rollback.
- Decide deliberately between the two levers below; they are **not** the same thing.

### Lever A — Code rollback (heavy)
Redeploy the previous known-good build (e.g. the commit before the merge, `8afd340`, or a revert
of the PR #1 merge `a561724`). This removes the merged code (rate-limiter mechanism, tests, docs).
Use when the new build itself is faulty. Mechanics depend on your deploy system and are out of
scope here.

### Lever B — Disable a feature via runtime flag (light)
`INGEST_ENABLED`, `rate_limit_enabled`, `rate_limit_enforce` are runtime settings. Today they are
**already OFF**, so there is nothing to disable — ingestion is gated and the limiter is shadow-only.
If a feature were ever enabled and misbehaved, turning its flag OFF mitigates **without** a code
rollback. This runbook does **not** recommend changing any flag now.

## Enablement — BLOCKED
Turning `INGEST_ENABLED` on (and later `rate_limit_enabled` / `rate_limit_enforce`) is **blocked**
pending: (1) the gateway/EMS **written contract approval** (transport, B1/B2, SoC, T3 retention),
(2) approved rate numbers and the fail-open policy decision, (3) verified deployed InfluxDB
retention, and (4) **explicit staging/production authorization**. Do not flip any flag before all
of these exist. Until then, deployments run with the gate OFF by design.

## Not covered / not assumed
Migration execution, secret management, load balancing, autoscaling, blue-green vs rolling
specifics, and the actual hosting platform are intentionally left to your deployment system and
are not asserted here.
