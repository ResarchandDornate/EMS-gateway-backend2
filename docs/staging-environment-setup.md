# UnityESS Central EMS — Staging Environment Setup (Docker Compose)

> Stands up an ISOLATED staging stack for the LIVE-cutover rehearsal. It shares nothing with the
> production project `ems-gateway-backend2`: distinct project name (`ems-gateway-staging`), network
> (`ems-staging-net`), volumes (`*_staging`), and host ports. No Cloudflare Tunnel; no production
> hosts/volumes/credentials. `INGEST_ENABLED` defaults to false. Nothing here has been started.

## Files
- `docker-compose.staging.yml` — the isolated stack (postgres/influxdb/redis/api + influx-init).
- `.env.staging.example` — template; copy to `.env.staging` (gitignored) and fill real staging values.

## Host port mapping (no collision with the production stack)
| Service | Production (`docker-compose.yml`) | Staging (`docker-compose.staging.yml`) |
|---|---|---|
| PostgreSQL | 5436 -> 5432 | **5437 -> 5432** |
| InfluxDB   | 8086 -> 8086 | **8087 -> 8086** |
| Redis      | 6380 -> 6379 | **6381 -> 6379** |
| API        | 8040 -> 8000 | **8041 -> 8000** |

## Isolation guarantees
- **Project:** `name: ems-gateway-staging` (vs `ems-gateway-backend2`).
- **Network:** `ems-staging-net` (separate bridge).
- **Volumes:** `pgdata_staging`, `influxdata_staging`, `redisdata_staging` — created under the staging
  project only; the production volumes (`ems-gateway-backend2_pgdata`, ...) are never referenced.
- **No shared state:** separate DB (`ems_staging`), Influx org (`unityess_staging`) + buckets
  (`telemetry_1y_staging`, `telemetry_raw_90d_staging`), and a separate Redis container.
- **No Cloudflare Tunnel / ingress** in this file — staging is reachable only on localhost:8041.

## 1. Configure staging secrets (never committed)
```
cp .env.staging.example .env.staging
# edit .env.staging: set POSTGRES_PASSWORD, INFLUXDB_ADMIN_PASSWORD, INFLUXDB_ADMIN_TOKEN, JWT_SECRET,
# and CORS_ALLOWED_ORIGINS (your real staging frontend origin). Keep INGEST_ENABLED=false.
```
`.env.staging` is in `.gitignore`; `.env.staging.example` (placeholders only) is the committed template.
Do not paste production secrets into it — staging must use its own.

## 2. Start / stop the staging stack (independent of production)
```
# start (build the api image, create the staging volumes/network):
docker compose -f docker-compose.staging.yml --env-file .env.staging up -d --build

# stop but KEEP staging data:
docker compose -f docker-compose.staging.yml --env-file .env.staging down

# remove staging stack INCLUDING its volumes (staging ONLY — scoped to this file/project):
docker compose -f docker-compose.staging.yml --env-file .env.staging down -v
```
SAFETY: always pass `-f docker-compose.staging.yml`. Because volumes are project-scoped and named
`*_staging`, `down -v` on this file cannot touch the production `ems-gateway-backend2_*` volumes.
Never run a bare `docker compose down -v` from this directory expecting it to mean staging.

## 3. Validate
```
docker compose -f docker-compose.staging.yml --env-file .env.staging ps      # all healthy
curl -s http://localhost:8041/health
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8041/ready          # 200 when pg/redis/influx ok
```

## 4. Enabling ingestion on staging (EXPLICIT operator action — not the default)
Staging ships with `INGEST_ENABLED=false`, so `/api/v1/gateway/ingest` returns 503 and nothing is
written. ONLY for synthetic-batch testing during the rehearsal, and as a deliberate, logged step:
```
# set INGEST_ENABLED=true in .env.staging (staging only; production stays false), then recreate api:
docker compose -f docker-compose.staging.yml --env-file .env.staging up -d api
```
Drive SYNTHETIC batches only (the pack `gateway_simulator.py`); never replay real production traffic.
Set it back to false when the rehearsal is done.

## 5. Notes
- The api command serves `src.main:asgi_app` (RequestContextMiddleware + CORS) and runs
  `alembic upgrade head` against the isolated `ems_staging` DB on start.
- Gateway batch cadence is still unknown and is NOT configured here; measure it on staging (see
  `docs/staging-operator-inputs.md` #6) before relying on the 4e boundary interval.
