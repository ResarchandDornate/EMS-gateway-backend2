# Approved-Contract Implementation Checklist — Phase 2 (fire on Gateway B1/B2)

When the Gateway returns the completed Approval Record (`docs/gateway-contract-confirmation.md`,
with B1 + B2 Confirmed), execute the steps below in order. Implement **only** what the approved
contract permits; invent no keys, types, units, cadence, or encodings. Production ingestion stays
gated (`ingest_enabled` OFF → 503) until Ops authorization (separate, below).

## Prerequisites (in hand before any code)
- [ ] Approval Record returned with **B1** and **B2** marked Confirmed, plus one real **S1 `bess_fast`** and one **S2 `bms_extended`** sample.
- [ ] For each stream to enable: its payload schema (keys/types/**units**/**cadence**), and the `system_alarm`/`system_data` encoding if those signals are in scope.
- [ ] **S4/S8/S9/D1 domain classification** answered — a *telemetry* answer routes here (Phase 2); an *event-domain* answer routes to Phase 3 (do **not** build it here).

## Step 1 — Map approved record fields & types (B1)
- [ ] Reconcile `EnvelopeIn`/`RecordIn` (`src/ingestion/schemas.py`) to the confirmed keys, required/optional, and nullability.
- [ ] Implement the confirmed **timestamp semantics** in the parse path (`src/influx.py` `parse_contract_timestamp` / `src/ingestion/mapping.py`): measurement instant, format, precision, timezone, out-of-order handling — exactly as confirmed.
- [ ] Implement the agreed encoding for `system_alarm`/`system_data` (and any other non-numeric) in `src/signals/types.py` (`field_type`/`coerce`) so they stop returning 422 — only per the confirmed encoding.
- [ ] Confirm the `quality` value set (`src/signals/types.py` `_QUALITY`) matches what the gateway emits.

## Step 2 — Transport envelope (B2)
- [ ] If the **flat `EnvelopeIn`** is confirmed: no transport change.
- [ ] If a **`TransportBatch`** is confirmed: implement the exact confirmed envelope (schema + router decode), preserving the existing ledger/idempotency contract.

## Step 3 — Registry seed + per-stream enablement (only where approved)
- [ ] Author registry rows for each approved stream/substream in `src/signals/seed/signal_registry.csv` per the confirmed schema (without entries, `build_point` cannot resolve S3–D1 signals).
- [ ] Move each approved stream from `DEFERRED_STREAMS` to `INGEST_STREAMS` (`src/streams/__init__.py`) **one at a time**, each with its registry entries + mapping.
- [ ] Add a migration **only if** the confirmed contract requires a new table/column (the `ingest_messages` ledger is stream-agnostic; likely none needed) — stop and justify before adding one.
- [ ] Leave `ingest_enabled` OFF; runtime enablement is an Ops decision (below).

## Step 4 — Tests per newly-enabled stream (readiness doc §6 matrix)
- [ ] envelope parse + route-to-write; registry validation (good/bad signal, type/range, quality, timestamp offset)
- [ ] happy-path store (200); duplicate (200); hash-mismatch (409); in-progress (409)
- [ ] influx-failure → 503 pending; retry converges; scope 403
- [ ] value-encoding: previously-422 `system_alarm`/`system_data` validate + store under the confirmed encoding
- [ ] reuse the harness: `test_ingest_endpoint.py`, `test_ingest_ledger.py`, `test_ingest_mapping.py`, `test_ingest_writer.py`, `tests/unit/test_streams.py`
- [ ] run focused per-stream tests, then the full suite (must stay green); `git diff --check` clean

## Blocker separation
### A. Gateway schema — BLOCKS Steps 1–4
B1 record contract + timestamp semantics; B2 transport envelope + per-stream S3–D1 schemas; `system_alarm`/`system_data` encoding; unsupported-stream handling; S4/S8/S9/D1 classification. (Request: `docs/gateway-contract-email-final.md`.)

### B. Ops go-live — does NOT block Steps 1–3; gates runtime enable only — SEPARATE
Approved rate-limit numbers; T3 3-year retention + deployed-InfluxDB verification; explicit authorization to set `ingest_enabled=true` (after staging verification). Until then flags stay OFF and `/ingest` returns 503.

### C. Product + Gateway alarm — Phase 3, NOT this checklist — SEPARATE
Alarm source/identity/fields/severity/lifecycle/dedup/ack/read-API/retention + Gateway source confirmation. If S4/S8/S9/D1 classify as **event domains**, their handling belongs to Phase 3, not Phase 2 enablement.

## Guardrails
Implement only what the approved contract permits; invent no keys/types/units/cadence/encoding; never add a stream to `INGEST_STREAMS` without its registry entries + approved schema; do not flip `ingest_enabled` or rate-limit flags without Ops authorization; do not weaken tests. This checklist authorizes nothing by itself.
