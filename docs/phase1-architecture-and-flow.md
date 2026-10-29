# Central EMS Backend — Phase 1: Architecture, Request Flow & Completion Matrix

**Branch:** `tarun-DB`  **Baseline commit:** `2a4c748` (OTP F1/F2 hardening)
**Status:** source-grounded inventory for the Phase 1 *contract / architecture / interface freeze*.
Every claim below is traced to a file; items needing the gateway team are marked **BLOCKED**
and nothing about an external contract is invented. This document is read-only reference; it
changes no code, no flags, and authorizes no enablement.

---

## 1. What "Phase 1" means here

Per the roadmap, Phase 1 is the **contract, architecture and interface freeze**, not a coding
phase. Its exit gate is: *the contract/interface decisions needed for implementation are
confirmed in writing, or explicitly tracked as blockers.* Because obtaining gateway answers is
an external action (out of scope / forbidden here), the **freeze itself is BLOCKED on the
gateway team**, while all *independent* Phase 1 work — the architecture inventory, the S1/S2
flow trace, the stream-gate verification, the contract-question matrix, and the activation-gate
criteria — is completable and is captured here.

## 2. Architecture inventory (implemented vs placeholder)

| Area | Where | Status |
|---|---|---|
| ASGI entrypoint | `src/main.py` — serve `src.main:asgi_app` (wraps `RequestContextMiddleware`); `app` alone bypasses X-Request-ID/access log (`README.md`) | Implemented |
| Lifespan | `main.py` `_lifespan` → `init_rate_limiter`/`shutdown_rate_limiter` | Implemented |
| Health/readiness | `/health` (liveness); `/ready` bounded Postgres+Redis+Influx probes | Implemented |
| Config | `src/config.py` `Settings`; required `database_url, influx_*, redis_url, jwt_secret`; flags `ingest_enabled`, `rate_limit_*`, `otp_*` all default OFF | Implemented |
| Database | `src/database.py` async engine; `get_session` yields, does **not** commit (endpoints commit) | Implemented |
| Migrations | 6: auth_and_rbac, otp_challenges, tenancy, unique_gateway_client_id, signal_registry, ingest_messages | Implemented |
| Auth | `auth/router.py` `/login` (password), `/otp/request`+`/otp/verify` (OTP + F1/F2), `/me`; JWT aud `user` (15 min) / `gateway` (30 min) `auth/security.py` | Implemented |
| Rate limiting | `src/ratelimit.py` (gateway/ingest token bucket, shadow, OFF); `src/auth/rate_limit.py` (OTP F1, OFF); OTP F2 DB cap in `auth/otp.py` | Implemented, inert by default |
| Gateway identity | `gateway/router.py` `/gateway/token`, `/gateway/me`; `gateway/dependencies.py` | Implemented |
| Ingestion endpoint | `ingestion/router.py` `POST /api/v1/gateway/ingest`, gated by `require_ingest_enabled` (503 until `ingest_enabled`) | Implemented, gated OFF |
| Idempotency ledger | `ingestion/service.py` (`claim_or_resolve`, `mark_stored`, `canonical_hash`, `verify_scope`), table `ingest_messages` (`ingestion/models.py`) | Implemented |
| Mapping | `ingestion/mapping.py` `build_message_points` → registry-validated `(bucket, Point)` | Implemented |
| InfluxDB writes | `src/influx.py` `InfluxGateway.write_built`/`write` (registry-derived buckets, allow-listed), `InfluxDBClientAsync` awaited writes | Implemented |
| Telemetry read | `telemetry/router.py` `/latest`,`/series`,`/aggregate` (read-only) | Implemented |
| Signal registry | `signals/*` (+ `signal_registry` table/seed) | Implemented |
| Stream classification | `src/streams/__init__.py` INGEST `{S1,S2}`, DEFERRED `{S3..S9,D1}`, KNOWN=union | Implemented |
| Per-stream decoders | `src/streams/s1..s9.py`, `d1_commands.py`, `envelope.py` | **Empty placeholders — NOT used** (decoding is via `mapping.py` + registry) |
| Async workers | `src/workers/consumer.py`, `dlq.py`, `writers/influx_writer.py`, `writers/postgres_writer.py` | **Empty placeholders — NOT used** (write path is inline, §4) |
| Feature modules | `analytics, financials, fleet, strategy, genie, notifications, realtime, events/*` | **Empty placeholders** (later phases) |

126 of 178 `src/*.py` files are 0-byte scaffolds; the above marks which are load-bearing.

## 3. S1/S2 ingestion request flow (traced, `ingestion/router.py`)

```
POST /api/v1/gateway/ingest
  dep: ingest_rate_limit (shadow, OFF) -> require_ingest_enabled (401/403 auth, then 503 if gate OFF)
  A. decode_body(gzip/identity, size caps)            body.py
     EnvelopeIn(...)  -> 400 bad JSON / 422 invalid   schemas.py
  resolve_gateway_identity + verify_scope             -> 403 if envelope ids != DB identity
  route_stream(stream):  deferred -> 501 ; unknown -> 422 ; ingest (S1/S2) -> continue
  B. build_message_points(envelope, identity, index)  mapping.py  (registry-validate ALL records; no I/O)
  canonical_hash(envelope)                            service.py
  C. claim_or_resolve(...)  ledger:                   service.py
       DUPLICATE_STORED -> 200 "duplicate"
       HASH_MISMATCH    -> 409 ; IN_PROGRESS -> 409
       CLAIMED/RECLAIMED -> continue (lease token held)
  D. influx.write_built(built)   -> 503 on failure (row stays 'pending', retryable)
     mark_stored(CAS lease-owned) -> False => 503 "store not confirmed; retry"
     -> 200 "stored"
```

**Authoritative stores:** telemetry → **InfluxDB**; **Postgres holds only the ledger**
(`ingest_messages`) — there is no second telemetry copy in Postgres. Retries are at-least-once
with deterministic, last-write-wins Influx points (`service.py` docstring); **not** distributed
exactly-once.

## 4. Stream handling S1–S9 / D1 (source + tests)

| Stream | Endpoint behavior | Evidence |
|---|---|---|
| S1, S2 | ingest-eligible (stored when gate ON; **503** while `ingest_enabled=False`) | `streams/__init__.py`; `test_ingest_endpoint.py::test_happy_path_stored` |
| S3–S9, D1 | recognized, **501 Not Implemented** (deferred) | `route_stream` → `test_ingest_endpoint.py::test_deferred_stream_501`; sets in `tests/unit/test_streams.py` |
| anything else | **422** (envelope validation) | `schemas.py::_known_stream`; `test_unknown_stream_422` |

S3–D1 payload processing is **not implemented** and must not be, pending B1/B2 (see §6).

## 5. Topology & activation gates (B3/B6)

**Current topology (source-confirmed):** single ASGI app; ingestion write path is **inline in
the request** (`router.py` Phase D), Influx-only telemetry + Postgres ledger; no queue/worker.
A prior read-only verification confirmed this path is idempotent, tenant-isolated, and fails
safe. **B3 recommendation:** keep the inline path; async workers (`workers/*`) are **not required
for Phase 1** and would need a separate decision (queue choice, whether Postgres telemetry is
introduced, cross-store atomicity) — none of which current code establishes.

**Activation gates (B6) — all must hold before considering `ingest_enabled=True`:**
1. B1 + B2 confirmed **in writing** by the gateway team (see `docs/gateway-contract-confirmation.md`).
2. Staging config verified per `docs/staging-config-checklist.md`; the three flags OFF until step 1.
3. `/ready` returns 200 (Postgres/Redis/Influx ok); gate-OFF smoke: authenticated ingest → 503,
   unauth → 401/403 (`docs/deployment-rollback-runbook.md`).
4. Explicit staging enablement authorization recorded.
Flags remain in their current safe state here; this document authorizes nothing.

## 6. Gateway contract question matrix (B1/B2) — UNCONFIRMED

Detail lives in `docs/gateway-contract-confirmation.md`; this is the decision tracker.

| ID | Question | Current evidence (source) | Owner | Blocks | Acceptance test |
|---|---|---|---|---|---|
| B1 | Exact record JSON keys + **timestamp semantics** (`source_id, asset_id, signal_key, value, quality, timestamp`) | `schemas.py:13` + `mapping.py:7-8` mark these **PROVISIONAL/UNCONFIRMED** | Gateway/EMS team | S1/S2 enablement; all S3–D1 | A contract-approved sample payload validates + stores; timestamp parses to the agreed instant |
| B2 | Transport envelope: flat `EnvelopeIn` vs a wrapping `TransportBatch`; per-stream payload shapes for S3–D1 | Only flat `EnvelopeIn` implemented (`schemas.py`); confirmation doc "Transport envelope (CONFIRM)" | Gateway/EMS team | S3–D1 decoders; batch handling | Agreed envelope round-trips through `/ingest`; per-stream fixtures validate |

No payload schema, timestamp meaning, or gateway capability is assumed beyond what the source
already accepts. Everything above is **UNCONFIRMED** until the gateway team answers in writing.

## 7. Phase 1 completion matrix

| Item | Status | Evidence |
|---|---|---|
| A. Architecture + request-flow inventory | **DONE** | §2–§4 (this doc) |
| Stream-gate invariant (S1/S2 vs S3–D1 vs unknown) tested | **DONE** | `tests/unit/test_streams.py`, `test_ingest_endpoint.py` |
| B3 topology documented + safe default | **DONE** | §5; inline path verified |
| B6 activation gates defined (measurable) | **PARTIAL** | §5 + staging/rollback docs; staging authorization + B1/B2 still outstanding |
| B1 record schema + timestamp semantics | **BLOCKED** | external gateway confirmation required |
| B2 transport envelope + S3–D1 payloads | **BLOCKED** | external gateway confirmation required |
| Contract freeze (Phase 1 exit gate) | **BLOCKED** | depends on B1/B2 written confirmation |

## 8. Remaining external blockers
- **B1, B2** — written gateway contract (record keys/timestamps; envelope + S3–D1 payloads).
- **Staging enablement authorization** for `ingest_enabled` (B6 step 4).
- These gate S3–D1 work and ingestion go-live; none can be resolved from this repository.
