# Staging configuration checklist — Central EMS backend

**Status:** preparation only. Nothing below has been verified against a live staging
environment; this lists what to check, not what is confirmed. Do **not** print or paste secret
values while using it — check presence/shape only.

Authoritative source for setting NAMES is `src/config.py` (`Settings`). Values come from the
environment / `.env`; this file lists **names only**.

> **Inconsistency to reconcile first:** the committed `.env.example` uses `POSTGRES_DSN`,
> `APP_ENV`, and a single `INFLUX_BUCKET`, which do **not** match the current `Settings` fields
> (`database_url`, `env`, and the two buckets `influx_bucket_raw` / `influx_bucket_long`).
> Confirm which names the deployment actually provides before relying on either. Do not assume.

## 1. Required settings (no default in `Settings` — must be present)
- [ ] `DATABASE_URL` (field `database_url`) — PostgreSQL DSN
- [ ] `INFLUX_URL`
- [ ] `INFLUX_TOKEN`
- [ ] `INFLUX_ORG`
- [ ] `INFLUX_BUCKET_RAW` (field `influx_bucket_raw`)
- [ ] `INFLUX_BUCKET_LONG` (field `influx_bucket_long`)
- [ ] `REDIS_URL`
- [ ] `JWT_SECRET`

Check each is **set and non-empty** (not the value). A missing required field makes `Settings()`
fail at startup.

## 2. Defaulted settings (override only if staging needs it)
- [ ] `ENV` (field `env`, default `dev`) — set appropriately for staging
- [ ] `INFLUX_BUCKET_TEST` (default `telemetry_test`)
- [ ] `INFLUX_BUCKET_3Y` (default `telemetry_3y`)
- [ ] `INFLUX_BUCKET_3Y_RETENTION_SECONDS` (default `94608000` ≈ 1095 d) — **PROPOSED, not
      production-approved**; deployed InfluxDB retention is unverified. Do not treat as approved.
- [ ] `OTP_PROVIDER` (default `mock`) and `MSG91_*` — only if OTP is exercised in staging

## 3. Flags that MUST remain OFF (until written approval + staging authorization)
- [ ] `INGEST_ENABLED` = **False** (default) — ingestion stays gated until the gateway contract
      is confirmed in writing **and** staging enablement is explicitly authorized.
- [ ] `RATE_LIMIT_ENABLED` = **False** (default)
- [ ] `RATE_LIMIT_ENFORCE` = **False** (default) — shadow-only; no 429s
- [ ] Optional rate-limit policy (`TOKEN_RATE_CAPACITY`, `TOKEN_RATE_REFILL_PER_SEC`,
      `INGEST_RATE_CAPACITY`, `INGEST_RATE_REFILL_PER_SEC`) — leave unset until approved numbers
      exist; `rate_limit_redis_*` timeouts and `rate_limit_local_fallback_*` bounds have safe
      mechanism defaults.

## 4. Dependency connectivity (verify presence/health, not contents)
- [ ] **PostgreSQL** reachable at `DATABASE_URL`; migrations applied as your process requires
      (not covered here).
- [ ] **InfluxDB** reachable at `INFLUX_URL` with a valid org/token; required buckets exist.
      Note: 3-year retention is **proposed/unverified** — confirm the deployed policy separately.
- [ ] **Redis** reachable at `REDIS_URL`. (Only used today by the `/ready` probe and, when the
      limiter is enabled, the shadow limiter — which is OFF.)
- [ ] `/ready` returns `200` with `{postgres, redis, influx}` all `ok`; `/health` returns `200`.

## 5. ASGI entrypoint
- [ ] The deployment serves **`src.main:asgi_app`** (NOT `src.main:app`). `asgi_app` wraps
      `RequestContextMiddleware`; serving `app` bypasses `X-Request-ID`, the access log, and the
      correlated `unhandled_exception` log (per `README.md` "Deploying"). The repo contains no
      production launch artifact, so this must be verified in the deployment config itself.

## Not verified by this document
Staging credentials, connectivity, bucket existence, retention policy, and the deployed launch
command are **not** confirmed here — each box must be checked against the actual environment.
