# UnityESS Central EMS Gateway — Scalability, Performance & Reliability Review

**Scope:** full read of the FastAPI + async SQLAlchemy + PostgreSQL + InfluxDB + Redis backend.
**Stance:** read-only assessment. Nothing changed. Senior-engineer prioritisation.
**Date:** 2026-11-04

---

## Executive summary

This is a well-structured codebase. The ingest reliability model (idempotent ledger, lease-based claim, last-write-wins, no DB transaction held across the Influx write), the shared Influx client with fail-fast timeout, bulk inserts, the atomic Redis token-bucket rate limiter, request-ID propagation, and a real three-tier test tree (unit/integration/e2e) are all things weaker backends get wrong. You do not need a rewrite.

The risks are concentrated in a short list. If you do only three things, do these:

1. **Stop blocking the event loop on password hashing** (P0 — latency).
2. **Add table partitioning + retention and the missing read indexes** (P1 — the time-series wall).
3. **Decouple ingest from synchronous DB/Influx writes with the existing worker/DLQ scaffold** (P1 — the fleet-growth ceiling, and the root shape of the 404-backlog incident).

Severity legend: **P0** hurts now under modest concurrency · **P1** hurts as data volume or gateway count grows · **P2** hardening for production-grade uptime and security.

---

## P0 — Fix before concurrency grows

### P0.1 Password hashing blocks the whole worker
`argon2.PasswordHasher.hash()` / `.verify()` in `src/auth/security.py` are synchronous, CPU-bound (argon2id, tens of ms each) and are called **directly inside async handlers**. While one login or OTP verify runs, that worker's entire event loop is frozen — every other concurrent request on the worker stalls. With only two workers, a small burst of logins briefly freezes the API.
**Direction:** offload to a thread — `await asyncio.to_thread(_ph.verify, hash, pw)` (and the same for `.hash`). Optionally cap concurrent hashes with a semaphore so a login flood can't exhaust the thread pool.

### P0.2 Two uvicorn workers on a four-core host
The Dockerfile runs `uvicorn … --workers 2` on a 4-core Mac mini. Raise it toward the core count (3–4, leaving headroom for Postgres/Influx/Redis/pm2 on the same box) — but **after** P0.1, because more workers only mask a blocked event loop. Keep `workers × DB pool_size` within Postgres `max_connections`.

### P0.3 Database pool runs on defaults
`create_async_engine(url, pool_pre_ping=True)` leaves `pool_size=5`, `max_overflow=10`, and no `pool_recycle`. Under concurrent reads + ingest, five base connections per worker queue up, and stale connections aren't recycled.
**Direction:** set `pool_size` / `max_overflow` deliberately against your Postgres connection budget, add `pool_recycle≈1800`. If you later front Postgres with pgbouncer in transaction mode, switch to `NullPool`.

---

## P1 — Fix before data volume / fleet grows

### P1.1 No partitioning or retention on the record tables
The PostgreSQL record tables (S1, S3–S9) are append-only and unbounded. As they grow, reads slow, autovacuum pressure rises, and storage climbs. This is the classic time-series scaling wall.
**Direction:** convert the high-volume tables to **TimescaleDB hypertables** (or native monthly partitioning) and attach a **retention policy** matching your Influx tiers. Do it while the tables are still small — partitioning a large live table is painful.

### P1.2 Missing composite indexes on the read paths
Every structured read filters on `site_id` + an event-time column (often + `gateway_id`). Without a matching composite index the queries degrade to sequential scans as tables grow.
**Direction:** add `(site_id, <event_time> DESC)` — and where filtered, `(site_id, gateway_id, <event_time> DESC)` — on each record table. Confirm the `ingest_messages` ledger has indexes supporting the monitor/backlog queries too.

### P1.3 Offset pagination
History endpoints use `limit`/`offset` (`MAX_LIMIT=1000`). Deep offsets (`OFFSET 50000`) scan and discard rows, so the deeper a client pages the slower it gets.
**Direction:** move hot history endpoints to **keyset/cursor pagination** (`WHERE event_time < :last_seen ORDER BY event_time DESC LIMIT n`). Keep offset only for shallow admin lists.

### P1.4 Synchronous ingest — ACK waits on the write
The gateway's ACK is coupled to the DB + Influx write. Throughput is bounded by write latency, and any DB/Influx hiccup turns directly into gateway retries and backlog — the exact shape of the telemetry_3y 404 incident. The `workers/consumer.py` + `workers/dlq.py` scaffold exists but appears unused.
**Direction:** as gateway count grows, decouple — **ACK on durable enqueue**, then write asynchronously via the worker, keeping the idempotent ledger. A durable queue that persists the payload also future-proofs against the "backend can't self-recover S2 backlog because payloads aren't retained" limitation.

### P1.5 Payload JSONB stored alongside typed columns
Each structured record stores both promoted typed columns **and** a full verbatim `payload` JSONB of the same record. That roughly doubles write volume and storage on your highest-rate tables, and JSONB is heavier to (de)serialise.
**Direction:** declare `payload` the source of truth and the columns a derived projection (ideally Postgres **generated columns**, so they can't drift); or drop `payload` on the highest-rate streams and keep only the typed columns + Influx.

---

## P2 — Production hardening

### P2.1 Single node is a single point of failure
API, Postgres, Influx, Redis, the dashboard, pm2, and the Cloudflare tunnel all run on one Mac mini. No horizontal redundancy, no failover.
**Direction:** for any uptime commitment, move to managed/replicated Postgres + Influx and run multiple API instances behind a load balancer — or, at minimum, a **tested** backup-and-restore + failover runbook. You have `postgres-backups/`; confirm a restore has actually been rehearsed, not just that backups exist.

### P2.2 Access tokens can't be revoked
JWTs carry `exp` but no `jti` / revocation list, so a leaked token is valid until it expires.
**Direction:** short access-token lifetime + refresh tokens, or a Redis revocation set checked on auth. Low effort, meaningful security gain.

### P2.3 Flux queries built by string interpolation
`src/monitor/service.py` builds Flux with f-strings. Today the interpolated values are validated enums (the S2 substream set), config buckets, and token-derived site UUIDs, so it's currently safe — but it's a fragile pattern: the day a user-supplied string reaches one of these templates unvalidated, it's a Flux-injection hole.
**Direction:** validate every interpolated value against an allow-list / cast site IDs to UUID before building the query, and centralise Flux construction so the rule is enforced in one place. (The main telemetry read path already avoids this — keep it that way.)

### P2.4 No global exception handler
`src/main.py` registers routers and probes but no catch-all exception handler, so unhandled errors return an inconsistent shape and can leak internals.
**Direction:** add a global handler that logs with the request ID and returns a uniform error envelope; map known domain errors to clean 4xx.

### P2.5 Redis outage degrades the global rate limit
By design, when Redis is unreachable the limiter falls back to a **per-worker** bucket, so the global limit isn't enforced during an outage (it fails open, which is the safe choice for availability). Fine for one node; for multi-instance you'll need **HA Redis** for the global limit to actually hold. This is already documented in `ratelimit.py` — flagging it so it's a conscious decision at scale-out time.

---

## What's already strong (leave it alone)

- **Ingest correctness:** idempotent ledger, lease/claim with CAS, last-write-wins, no DB transaction across the Influx write, reject-before-write validation.
- **Influx client:** reused app-lifespan client with an explicit fail-fast timeout; writes batched per bucket.
- **Writes:** bulk `pg_insert(...).values(rows)`, not row-by-row.
- **Rate limiting:** atomic single-EVAL Lua token bucket, graceful per-worker fallback.
- **Package layout:** consistent `models/schemas/repository/service/router` per domain; clean tenancy and RBAC normalisation; read/write split.
- **Observability & tests:** request-ID propagation; unit + integration + e2e test tree.

---

## Suggested sequencing

| Phase | Items | Why first |
|-------|-------|-----------|
| **1 — this week** | P0.1 (async hashing), P0.3 (pool config), P2.4 (exception handler) | Small, safe, immediate latency + robustness wins |
| **2 — before more gateways/data** | P1.2 (indexes), P1.1 (partitioning + retention), P0.2 (workers) | Removes the data-growth wall while tables are small |
| **3 — architectural** | P1.4 (async ingest + DLQ), P1.5 (payload strategy), P2.1 (HA / runbook) | Larger changes; plan and test deliberately |
| **4 — hardening** | P2.2 (token revocation), P2.3 (Flux allow-listing), P2.5 (HA Redis at scale-out) | Security + multi-instance readiness |

Each item above can be turned into a concrete, apply-yourself change (index DDL, the `to_thread` patch, the pool settings, a partitioning migration) on request — still read-only until you say go.
