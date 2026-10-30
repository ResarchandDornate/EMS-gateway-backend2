# Central EMS — Product / Ops / Gateway Decision Request

**Purpose:** consolidated list of the external decisions that gate further backend
implementation, routed by owner. This document records what the repository has
**verified in source** versus what is **unknown/unconfirmed**; it invents no answers,
contacts, dates, or approvals, and it authorizes nothing.

## Current status
- The gateway confirmation email is **prepared but NOT sent**: `docs/gateway-contract-email-final.md`
  (attachment `docs/gateway-contract-confirmation.md`). Its **recipient (`<gateway/EMS team>`) and
  sender (`<your name>`) placeholders remain to be filled** before sending. Sending is a human action.
- All feature flags remain OFF (`ingest_enabled`, `rate_limit_*`, `otp_*`); nothing here enables them.
- No contact names, dates, numbers, schemas, or approvals are filled in below — those are the
  decisions being requested.

## Confirmed vs unknown (do not blur)
- **Confirmed in source:** S1 and S2 are ingest-eligible (stored when the gate is ON; HTTP 503 while
  `ingest_enabled=False`). S3–S9 and D1 are **recognized but deferred → HTTP 501**; any other stream
  → HTTP 422 (`src/streams/__init__.py`, `src/ingestion/router.py`).
- **Unknown / not assumed:** the **stream → event-role mapping** — i.e. which of S4 / S8 / S9 / D1
  carries alarms, command-results, or config-audit — is **not defined anywhere in the repo** and must
  **not** be assumed. S3–D1 payload schemas, timestamp semantics, and the wire representation of state/
  alarm signals (`system_alarm`/`system_data` categories, numeric in the registry) are likewise
  unconfirmed. These come only from written gateway/product input.

---

## GATEWAY team
The prepared email already requests all of this; action = fill placeholders and send, then record
answers in the attached document's Approval Record.

| Need | Unblocks |
|---|---|
| **B1** — exact record keys; required/optional + nullability; a real S1 (`bess_fast`) + S2 (`bms_extended`) example; the wire representation of `system_alarm` / `system_data` category signals (numeric `U16`/`FLOAT32` today — confirm numeric codes vs strings/enums); exact `quality` values; scope UUIDs (`gateway_id` / `site_id` / `organization_id`); timestamp semantics (measurement instant, format, precision, timezone, out-of-order handling) | S1/S2 enablement + all S3–D1 |
| **B2** — transport envelope (flat `EnvelopeIn` vs a wrapping `TransportBatch`); which streams are in scope; per-stream S3–D1 payload schemas (keys/types/**units**/**cadence**); unsupported-stream behavior | S3–D1 decoders; batch handling |

---

## OPS
| Need | Unblocks |
|---|---|
| Approved rate-limit numbers: `/token` + `/ingest` capacity & refill; OTP request/verify capacity & refill, `otp_verify_cap` + window; fail-open policy; authorization to flip `*_enforce` after a staging shadow run | enabling the (currently inert) rate limiter |
| Production-approved T3 retention (days) for the 12 `T3_3Y` signals + verification of the deployed InfluxDB retention vs the proposed 1095 d (proposed, not approved) | T3 retention correctness |
| Explicit staging/production enablement authorization for `ingest_enabled` — only after B1/B2 are confirmed in writing and staging config is verified | ingestion go-live |
| Confirm the deployment serves `src.main:asgi_app` (not `src.main:app`) and uses the env-var names in the corrected `.env.example` | request correlation / access logging + clean startup |

---

## PRODUCT
| Need | Unblocks |
|---|---|
| **Phase 3 events:** which event types (alarms / command-results / config-audit); each event's schema/fields; severity + lifecycle/state model; deduplication key; acknowledgment semantics; command→result linkage (D1); retention — **and** the stream→event-role mapping (which of S4/S8/S9/D1 carries what; unknown today, must not be assumed) | `src/events/*` (19 empty files) |
| **Phase 4:** fleet staleness thresholds + authoritative SoC mapping; realtime transport (WebSocket/SSE) + subscription rules; notification triggers / providers / recipients; Genie export contents / formats | fleet, realtime, notifications, genie |
| **Phase 5:** analytics KPI formulas; tariff / time-of-day / savings formulas; dispatch objectives + scheduling behavior | analytics, financials, strategy |
| **MSG91** (vendor + product): API contract — base URL / endpoint path, auth-key placement, request param names, DLT template ID(s) + sender ID, success/failure response schema, timeouts, email-channel decision | real OTP SMS/email delivery (`src/auth/otp_provider.py` stub) |

---

## Sources (for traceability)
- `docs/phase1-architecture-and-flow.md` — §6 (B1/B2 matrix), §7 (completion matrix), §8 (external blockers)
- `docs/gateway-contract-confirmation.md` — backend-verified behavior vs items to confirm; Approval Record
- `docs/gateway-contract-email-final.md` — the prepared (unsent) gateway email
- `docs/staging-config-checklist.md`, `docs/deployment-rollback-runbook.md` — Ops/enablement gates

*No answer, credential, contact, date, or approval in this document is assumed or filled in; each is
the decision being requested from the named owner.*
