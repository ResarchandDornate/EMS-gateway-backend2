# UnityESS Central EMS Gateway — Phased Refactoring Plan

**Purpose:** restructure the backend for long-term maintainability and scale **without a rewrite and without behaviour changes per step.**
**Stance:** proposal only — nothing is changed until you approve each phase.
**Date:** 2026-11-04

---

## Guiding principles (apply to every phase)

1. **One behaviour-preserving change per pull request.** Never mix a file move with a logic change — if something breaks, you must know which kind of change caused it.
2. **Tests stay green between phases.** The three-tier test tree (unit/integration/e2e) is your safety net; add characterization tests *before* moving code that is thinly covered.
3. **Migrate imports with re-export shims, not big-bang.** When a module moves, leave a one-line `from new.location import *` in the old path for one phase, rewire callers, then delete the shim. No single PR touches 40 files.
4. **Riskiest changes last, on staging first.** Data migrations (indexes, partitioning) and the ingest decoupling go after the structure is clean, and are rehearsed on `docker-compose.staging.yml` before production.
5. **One stream as a template.** For any per-stream restructure, do S4 (alarms) end-to-end first, review it, then apply the proven shape to the rest.

---

## Phase 0 — Safety net & hygiene *(days, near-zero risk)*

Make the refactor reversible and observable before touching structure.

- Confirm the full test tree passes locally and in CI; if there is no CI gate running `pytest` on every PR, add one — it is the precondition for everything below.
- Add characterization tests where coverage is thin: the two ingest paths (canonical envelope + legacy TransportBatch), and one structured read per stream. These lock current behaviour so later moves can't silently change it.
- Root cleanup: move loose docs (`BACKEND_CODE_AND_ARCHITECTURE.md`, `API_REFERENCE.md`, this file) into `docs/`; gitignore and remove the stray `s2_monitor_readonly_api.patch`; keep the repo root to config + packaging only.
- **Exit criteria:** green CI on PR, ingest + read characterization tests in place, clean root.

## Phase 1 — Low-risk correctness & performance *(days, low risk, no file moves)*

Do these before structural churn so you're never debugging performance and layout at the same time. (These are the P0 items from the scalability review.)

- Offload Argon2 hashing to a thread (`asyncio.to_thread`) so login/OTP verify stops blocking the event loop.
- Set explicit DB pool sizing + `pool_recycle` on the async engine.
- Add a global exception handler that logs with the request ID and returns a uniform error envelope.
- Raise uvicorn workers toward the core count (after the hashing fix).
- **Exit criteria:** no synchronous CPU work on the event loop; consistent error shape; load test shows concurrent logins no longer stall unrelated requests.

## Phase 2 — Extract a shared domain/records layer *(1–2 weeks, low–medium risk)*

**Problem being fixed:** the ORM table models live under `src/ingestion/` (the write side) but are imported by `structured_reads/`, `fast_bess/`, and `telemetry/` (the read side) — backwards coupling.

- Create `src/records/` (neutral domain layer). Move the nine `*_models.py` table definitions there, one per module.
- Leave re-export shims in the old `ingestion/*_models.py` paths for this phase; rewire read-side and write-side imports to `src/records/`; then delete the shims.
- This is a pure move + import rewiring — no schema change, no logic change. The characterization tests from Phase 0 guard it.
- **Exit criteria:** no module under `structured_reads/`/`telemetry/`/`fast_bess/` imports from `ingestion/`; all tables defined in `src/records/`.

## Phase 3 — Consolidate per-stream code into cohesive subpackages *(2–3 weeks, medium risk)*

**Problem being fixed:** per-stream logic is scattered across three flat naming conventions — `streams/sN_*.py`, `ingestion/sN_*.py`, `ingestion/*_service.py` — plus some apparently empty/stub `streams/` files. `ingestion/` is a 40-file god-package.

- Restructure into one cohesive unit per stream: `src/ingestion/streams/<stream>/{schema.py, service.py}`, with the ORM model already living in `src/records/` from Phase 2.
- Reconcile the duplicate per-stream definitions: merge the `streams/sN_*.py` (envelope/constants) and `ingestion/sN_*.py` (validation) content, and delete empty stubs.
- Define a common `StreamHandler` protocol — `validate → claim → persist → ack` — so adding a stream becomes "implement one interface in one subpackage" instead of edits across many files. The ledger/lease logic stays shared.
- **Do S4 (alarms) first as the template**, review, then replicate for S1, S3, S5–S9.
- **Exit criteria:** each stream is one subpackage implementing `StreamHandler`; `ingestion/` root holds only shared machinery (ledger, batch, queue, mapping, idempotency); no `sN_*` / `*_service` / `*_models` triple-spread remains.

## Phase 4 — De-duplicate and group cross-cutting modules *(days, low risk)*

- Resolve the two rate-limit implementations (`src/ratelimit.py` and `src/auth/rate_limit.py`) into a single module with one clear API; rewire callers.
- Optionally group loose infrastructure modules (`config`, `database`, `redis_client`, `influx`, `logging`, `middleware`, `ratelimit`) under `src/core/` (or `src/infra/`) so the top level reads as "features + core", matching the clean per-domain shape the feature packages already follow.
- **Exit criteria:** one rate-limiter; top-level `src/` is features + a single infra package.

## Phase 5 — Data-layer refactor *(2–4 weeks, medium–high risk, staging-first)*

Now that data access is isolated (`src/records/` + clean repositories), change the storage shape safely. (These are the P1 data items from the scalability review.)

- Add the missing composite indexes — `(site_id, <event_time> DESC)` (+ `gateway_id` where filtered) — as reviewable Alembic migrations.
- Convert high-volume record tables to **TimescaleDB hypertables** (or native monthly partitioning) with a retention policy aligned to the Influx tiers. Do it while tables are still small.
- Switch `payload` JSONB to a declared projection: Postgres **generated columns**, or drop `payload` on the highest-rate streams.
- Move hot history endpoints to **keyset/cursor pagination**; keep offset only for shallow admin lists.
- **Rehearse every migration on `docker-compose.staging.yml` first**, including a restore test.
- **Exit criteria:** indexed + partitioned record tables with retention; no scan-and-discard pagination on hot paths; verified backup/restore.

## Phase 6 — Decouple ingestion *(3–5 weeks, high risk, architectural)*

**Problem being fixed:** the gateway ACK is synchronous on the DB+Influx write, so any write hiccup becomes backlog (the shape of the telemetry_3y 404 incident). The `workers/consumer.py` + `workers/dlq.py` scaffold exists but is unused.

- Introduce a durable queue: **ACK on durable enqueue**, then write asynchronously via the worker, using the clean `StreamHandler` (Phase 3) and `records` layer (Phase 2). A queue that persists the payload also removes the "backend can't self-recover S2 backlog because payloads aren't retained" limitation.
- Keep the idempotent ledger and last-write-wins semantics unchanged — only *when* the write happens moves.
- Wire and load-test the existing DLQ path for poison messages.
- **Exit criteria:** ingest latency decoupled from write latency; a DB/Influx blip no longer drives gateway retries; backlog is drainable from a backend-owned durable store.

---

## Why this order

Phases 2–4 are **enablers**: they give you a clean data layer and a uniform stream interface, which is precisely what makes Phase 5 (data migrations touching one place) and Phase 6 (async writes behind one handler) tractable and low-drama. Doing the architecture or data work first — on top of the current god-package and backwards coupling — would mean changing structure and behaviour at once, which is where refactors go wrong.

| Phase | Theme | Risk | Pairs with review item |
|-------|-------|------|------------------------|
| 0 | Safety net + hygiene | Near-zero | — |
| 1 | Correctness/perf quick wins | Low | P0.1–P0.3, P2.4 |
| 2 | Shared records/domain layer | Low–Med | Structure |
| 3 | Per-stream subpackages + handler protocol | Medium | Structure |
| 4 | De-dup + infra grouping | Low | Structure |
| 5 | Indexes, partitioning, pagination, payload | Med–High | P1.1–P1.3, P1.5 |
| 6 | Async ingest + DLQ | High | P1.4 |

Any phase can be expanded into a concrete task breakdown (file-by-file moves, the `StreamHandler` interface, the Alembic migrations) whenever you want to start it — one phase at a time, tests green between each.
