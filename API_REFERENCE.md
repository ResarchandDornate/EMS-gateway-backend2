# UnityESS Central EMS Gateway — API Reference

**Base URL:** `https://ems-gateway-api.unityess.cloud`
**Interactive docs:** `/docs` (Swagger UI) · `/openapi.json` (raw spec)
**API version prefix:** `/api/v1`
**Total endpoints:** 30 application endpoints, plus `/health` and `/ready`
**Last updated:** 2026-11-04

All timestamps are UTC (ISO 8601). All request/response bodies are JSON unless noted.

---

## 1. Authentication

The API uses two independent bearer-token identities. Every secured endpoint expects the token in an `Authorization: Bearer <token>` header (scheme name `HTTPBearer`).

| Identity | How a token is obtained | Used by |
|----------|------------------------|---------|
| **User** (super_admin / org_admin / site_operator) | `POST /api/v1/auth/login` (password) or the OTP pair | Admin and read APIs |
| **Gateway** (a registered edge device) | `POST /api/v1/gateway/token` | `POST /api/v1/gateway/ingest` only |

**User login** returns `{ "access_token": "<jwt>", "token_type": "bearer" }`. Pass that JWT on every subsequent user call. Login body: `{ "identifier": "<email>", "password": "<password>" }`.

**Authorization model:** user endpoints are role- and site-scoped. A caller sees only the sites their role grants. An unauthenticated call returns **401**; an authenticated call lacking the required permission returns **403**.

---

## 2. Conventions

**Site scoping.** Most read endpoints require a `site_id` query parameter and will only return data for sites the caller is entitled to.

**Time ranges (PostgreSQL read APIs — S1, S3–S9).** Either give an explicit window with `start` / `stop`, **or** give a single `range` keyword — they are mutually exclusive.
- `range` keywords: `this_week`, `this_month`, `this_year` (period-to-date).
- Relative timestamps in `start`/`stop`: `-7d`, `-4w`, `-6mo`, `-1y`, or any ISO 8601 instant.
- Invalid or conflicting range input → **422**.

**Pagination.** List endpoints accept `limit` and `offset`; summaries accept `group_by`.

**Standard errors:** `400` bad request · `401` unauthenticated · `403` forbidden · `404` not found · `422` validation error · `5xx` server/store error.

---

## 3. Stream catalog

The gateway sends ten logical streams. Storage differs by stream.

| Stream | Content | Storage | Read via |
|--------|---------|---------|----------|
| **S1** | Fast BESS records | PostgreSQL `fast_bess_records` (JSONB) | `/api/v1/bess/fast/*` |
| **S2** | High-rate telemetry (8 substreams) | InfluxDB (`telemetry_1y`; energy counters → `telemetry_3y`) | `/api/v1/telemetry/*` |
| **S3** | Gateway health | PostgreSQL `gateway_health_records` | `/api/v1/health/*` |
| **S4** | Alarm events | PostgreSQL `alarm_event_records` | `/api/v1/alarms*` |
| **S5** | Controller state | PostgreSQL `controller_state_records` | `/api/v1/controller/*` |
| **S6** | Solis inverter telemetry | PostgreSQL `solis_telemetry_records` | `/api/v1/solis/*` |
| **S7** | Edge-AI inference | PostgreSQL `edge_ai_inference_records` | `/api/v1/edge-ai/*` |
| **S8** | Configuration audit | PostgreSQL `configuration_audit_records` | `/api/v1/config-audit*` |
| **S9** | Command results | PostgreSQL `command_result_records` | `/api/v1/commands*` |
| **S10** | Generic stream records | PostgreSQL `stream_records` | (inventory only) |

**S2 substreams:** `bms_extended`, `pcs_extended`, `ems_system`, `remote_control`, `liquid_cooling`, `io_module`, `dehumidifier`, `fire_protection`.

**Ingest routing:** S1–S2 are the live-ingest streams; S3/S4/S8 are structured; S5/S6/S7/S9 are deferred.

---

## 4. Endpoint reference

### 4.1 System (public)

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/health` | Liveness probe. Returns 200 when the process is up. |
| GET | `/ready` | Readiness probe. Returns 200 when DB/Influx/Redis dependencies are reachable. |

### 4.2 Auth

| Method | Path | Auth | Body / params | Purpose |
|--------|------|------|---------------|---------|
| POST | `/api/v1/auth/login` | public | `{identifier, password}` | Password login (super_admin). Returns `{access_token, token_type}`. |
| POST | `/api/v1/auth/otp/request` | public | `{identifier}` | Start an OTP challenge; delivers a code to the resolved channel. |
| POST | `/api/v1/auth/otp/verify` | public | `{identifier, code}` | Verify the OTP code; returns `{access_token, token_type}`. |
| GET | `/api/v1/auth/me` | user | — | Current authenticated user (id, role, entitled sites). |

### 4.3 Accounts & gateways (admin)

| Method | Path | Auth | Params | Purpose |
|--------|------|------|--------|---------|
| GET | `/api/v1/admin/organizations` | admin | — | List organizations. |
| POST | `/api/v1/admin/organizations` | admin | body | Create an organization. |
| GET | `/api/v1/admin/sites/{site_id}/gateways` | admin | `site_id` | List gateways registered to a site. |
| POST | `/api/v1/admin/sites/{site_id}/gateways` | admin | `site_id` + body | Register a new gateway on a site. |

### 4.4 Operators (org)

| Method | Path | Auth | Params | Purpose |
|--------|------|------|--------|---------|
| GET | `/api/v1/org/operators` | org_admin | — | List site operators in the caller's org. |
| POST | `/api/v1/org/operators` | org_admin | body | Create a site-operator user. |
| POST | `/api/v1/org/operators/{operator_id}/deactivate` | org_admin | `operator_id` | Deactivate a site operator. |

### 4.5 Gateway (edge device)

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| POST | `/api/v1/gateway/token` | public (gateway creds) | Exchange gateway credentials for a gateway JWT. |
| GET | `/api/v1/gateway/me` | gateway | Resolved identity of the calling gateway (organization, site, gateway id). |
| POST | `/api/v1/gateway/ingest` | gateway | **Live ingestion.** Accepts a single canonical envelope or a legacy `TransportBatch` wrapper (`request_id/gateway_id/sent_at_utc/messages[]`). The batch path stores S1 fast_bess verbatim as structured JSONB and returns the gateway's `accepted[]/rejected[]` ACK contract. Idempotent, last-write-wins; failures are returned `retryable:true` so the gateway retains them. |

### 4.6 Telemetry — S2 reads (InfluxDB)

All require `site_id`. `stream`, `substream`, and `signal_key` identify the series.

| Method | Path | Required params | Purpose |
|--------|------|-----------------|---------|
| GET | `/api/v1/telemetry/latest` | `stream, substream, signal_key, site_id` | Latest point per series. Optional `asset_id`, `source_id`. |
| GET | `/api/v1/telemetry/series` | `stream, substream, signal_key, asset_id, start, site_id` | Raw point series over a window. Optional `source_id`, `stop`, `limit`. |
| GET | `/api/v1/telemetry/aggregate` | `stream, substream, signal_key, asset_id, start, window, fn, site_id` | Windowed aggregation (`fn` = the aggregate function). Optional `source_id`, `stop`. |

### 4.7 BESS fast — S1 reads (PostgreSQL)

| Method | Path | Required | Optional | Purpose |
|--------|------|----------|----------|---------|
| GET | `/api/v1/bess/fast/latest` | `site_id` | `gateway_id, source_id, bess_id, limit` | Most recent S1 fast_bess record(s). |
| GET | `/api/v1/bess/fast/history` | `site_id` | `gateway_id, source_id, bess_id, start, stop, range, limit, offset` | Historical S1 records over a time range. |

### 4.8 Structured reads — S3–S9 (PostgreSQL)

Every endpoint requires `site_id` and accepts the shared time grammar (`start`/`stop` **or** `range`), plus `gateway_id`, `limit`, `offset`, and (on list endpoints) `sort`. Summary endpoints accept `group_by`.

| Method | Path | Stream | Purpose |
|--------|------|--------|---------|
| GET | `/api/v1/health/latest` · `/health/history` | S3 | Gateway health — latest snapshot / history. |
| GET | `/api/v1/alarms` | S4 | Alarm events. Filters: `severity, event_type, state, gateway_id, correlation_id`. |
| GET | `/api/v1/alarms/summary` | S4 | Alarm counts grouped by `group_by`. |
| GET | `/api/v1/controller/latest` · `/controller/history` | S5 | Controller state. Filter: `controller_state`. |
| GET | `/api/v1/solis/latest` · `/solis/history` | S6 | Solis inverter telemetry. |
| GET | `/api/v1/edge-ai/latest` · `/edge-ai/history` | S7 | Edge-AI inference records. |
| GET | `/api/v1/config-audit` | S8 | Configuration-change audit. Filters: `actor, setting_path, change_source`. |
| GET | `/api/v1/config-audit/summary` | S8 | Config-audit counts grouped by `group_by`. |
| GET | `/api/v1/commands` | S9 | Command results. Filters: `command_id, final_status, command_type`. |
| GET | `/api/v1/commands/summary` | S9 | Command counts grouped by `group_by`. |

### 4.9 Monitor — read-only pipeline aggregates

Mounted at `/api/v1/monitor/*`. Every endpoint is GET, requires the `dashboard:view` permission, is tenant-scoped from the token, and is strictly read-only — nothing writes, replays, ACKs, dequeues, or backfills. Responses carry `scope`, `generated_at`, `evidence`, and `notes`.

| Method | Path | Params | Purpose |
|--------|------|--------|---------|
| GET | `/api/v1/monitor/overview` | — | Management snapshot: stored/pending per stream, stuck backlog, storage-success %, configured retention buckets. Storage success is a backend **storage** rate, never a gateway delivery rate. |
| GET | `/api/v1/monitor/ledger` | `window` | Stored vs pending message counts per stream within the window. |
| GET | `/api/v1/monitor/streams` | — | Per PostgreSQL record table: row count + event-time and storage-time spans, with records-per-message context. |
| GET | `/api/v1/monitor/s2` | `window` | Per-substream S2 point counts over the window (InfluxDB). Includes companion `_q` quality fields. |
| GET | `/api/v1/monitor/frequency` | `substream`*, `window` | Observed per-signal interval (window ÷ points) vs expected registry cadence, for one S2 substream. |
| GET | `/api/v1/monitor/live` | `substream`* | Live snapshot — latest value + event-timestamp per signal for one S2 substream. Read-only; nothing stored. |
| GET | `/api/v1/monitor/failures` | — | Stuck (lease-expired pending) messages per stream, with first/last receive time. A DERIVED proxy for stuck ingestion; per-failure error codes are **not instrumented** (ephemeral logs only). |
| GET | `/api/v1/monitor/backlog-items` | `limit` (≤1000), `order` | Individual backlog (pending) messages: `message_id, stream, substream, record_count, receive time`. |
| GET | `/api/v1/monitor/recent-messages` | `limit` (≤500) | Latest messages across all streams with **substream** and store **status**. Rows received before the substream column was added show `substream: null`. |

---

## 5. Notes on completeness

As of this revision the deployed `/docs` exposes every router in the codebase — the monitor module (including `recent-messages`) is live and mounted. There are no endpoints present in source but missing from the deployment, and no missing endpoint was identified against the current monitoring and gateway contracts. The known deferred item is the finance / frontend-calculated values question (whether derived commercial values are persisted), which is out of scope here and tracked separately.
