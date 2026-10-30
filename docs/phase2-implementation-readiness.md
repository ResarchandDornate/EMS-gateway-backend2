# Central EMS Backend — Phase 2 Ingestion: Implementation Readiness

**Branch:** `tarun-DB`  **Base commit:** `c63469c`
**Scope:** readiness audit + concrete plan for the remaining Phase 2 (telemetry ingestion &
persistence) work. Source-grounded; every current-state claim cites a file. S3–D1 payload
details are **not** invented — missing items are marked UNCONFIRMED/BLOCKED. This document
changes no code, flags, or other documents.

Legend: **[FACT]** source-verified · **[ASSUMPTION]** reasonable, unconfirmed · **[UNRESOLVED]**
needs gateway/product input.

---

## 1. Current-state findings (source-verified)

**S1/S2 ingestion — [FACT].** `POST /api/v1/gateway/ingest` (`ingestion/router.py:23`), gated by
`require_ingest_enabled` → **HTTP 503** when `ingest_enabled` is false (`ingestion/providers.py:36`;
`config.py:24`, default `False`). When enabled, the flow is: decode body (`router.py:39-49`;
`ingestion/body.py`) → `EnvelopeIn` parse (`schemas.py:22-57`) → scope check `verify_scope`
(403 on mismatch, `router.py:52-55`; `service.py:57`) → `route_stream` (`router.py:57-61`;
`service.py:49`) → `build_message_points` registry validation (`router.py:63-71`; `mapping.py`) →
`canonical_hash` (`service.py:66`) → `claim_or_resolve` ledger (`router.py:76-81`; `service.py:84`)
→ `influx.write_built` (`router.py:93-96`; `influx.py:222`) → `mark_stored` CAS (`router.py:97-103`;
`service.py:156`) → 200 "stored". Telemetry → InfluxDB; **Postgres holds only the ledger**
(`ingest_messages`, `ingestion/models.py`). Registry seed defines **S1 (63) and S2 (2471) signals
only** (`src/signals/seed/signal_registry.csv`).

**Deferred S3–S9/D1 — [FACT].** `DEFERRED_STREAMS = {S3..S9, D1}` (`streams/__init__.py:20`);
`route_stream` returns "deferred" → **HTTP 501** "stream {X} not supported yet" (`router.py:57-59`).
`EnvelopeIn` *accepts* a deferred stream at parse time (`schemas.py:44-51`), so the 501 is produced
by routing, not validation. **The registry contains no S3–S9/D1 signals at all** (§1 above) —
`primary_stream` is only S1/S2.

**Unknown stream — [FACT].** Any stream not in `KNOWN_STREAMS` (`streams/__init__.py:23`) fails
`EnvelopeIn._known_stream` → **HTTP 422** "invalid envelope" (`schemas.py:44-51`, `router.py:49`);
a known-but-unclassified value would hit `router.py:61` (422 "unsupported stream").

**Record validation & timestamp parsing — [FACT].** `RecordIn` keys: `source_id`, `asset_id`,
`signal_key`, `value` (float|int|str|bool|None), `quality` (str|None ≤32), `timestamp` (str ≤64)
(`schemas.py:12-19`). Per-record validation in `build_point` (`influx.py:156-180`): identity regex,
registry `resolve`, substream match, `asset_type` from registry, `coerce` value, `validate_quality`.
Timestamp: `parse_contract_timestamp` requires **ISO-8601 with a UTC offset**, rejects naive, and
normalizes to UTC (`influx.py:58-69`). A single invalid record rejects the whole message
(`mapping.py` builds all points before any I/O) → **HTTP 422** (`router.py:71`).

**`system_alarm` / `system_data` are categories, not data types — they ingest as numeric [FACT, CORRECTED].**
`field_type` raises `UnsupportedDataType` only for a `data_type` not float/int (`signals/types.py:19-25`);
`coerce` calls it first (`types.py:28-33`). Every value in the registry's **`data_type`** column is numeric
(FLOAT32 1330, Float 63, U16 1126, S16 14, U32 1 = 2534). `system_alarm`/`system_data` are values of the
**`category`** column (16 + 47 rows), and those signals' `data_type` is numeric (`system_alarm` all U16;
`system_data` U16×29 + FLOAT32×18). `load_registry_index` does not carry `category` into the write path,
so these signals coerce and **store as ordinary numeric points today** — they are NOT 422-rejected.
(A prior version of this doc wrongly read the `category` values as `data_type`s.)

**Idempotency & retry — [FACT].** Ledger keyed on `(gateway_id, message_id)` with a time-bounded
lease (`service.py:84-147`): new → CLAIMED; same payload already stored → DUPLICATE_STORED → 200
"duplicate"; same id, different `payload_hash` → HASH_MISMATCH → 409; active lease → IN_PROGRESS →
409; expired lease → reclaimed. `mark_stored` is a lease-owned CAS run **only after** a successful
Influx write (`router.py:97-103`; `service.py:156-180`); a failed write/CAS leaves the row
`pending` and returns 503 (retryable). Influx points are deterministic/last-write-wins, so retries
re-write identically — **at-least-once with idempotent points, NOT exactly-once** (`service.py`
module docstring).

## 2. Phase 2 work classification

**Implementable now (confirmed repo behavior, contract-independent) — thin, hardening only:**
- Optional: a sweep of stale `pending` ledger rows whose lease expired long ago (operational
  hygiene; the reclaim path already makes them retryable). Needs a design decision on *where* it
  runs (on-write vs a scheduler that does not exist) — **do not build a worker** just for this.
- Optional: connection-lifecycle review of `InfluxGateway` (a new `InfluxDBClientAsync` is created
  per call, `influx.py:198-199`) — a refactor, not a bug; current tests pass. Flag, don't rush.
- Note: multi-bucket writes are sequential and not atomic across buckets (`influx.py:236-237`);
  acceptable given idempotent points + the pending ledger. Documented, no change.
None of these are new *features*; the S1/S2 feature path is already complete and tested.

**Blocked on gateway written confirmation (B1/B2):**
- All S3–S9/D1 ingestion (payload schemas, substream vocabulary, units, cadence, timestamp rules).
- Wire representation for state/status/enumerated/alarm signals: `system_alarm`/`system_data` are
  category labels whose signals already ingest as numeric (U16/FLOAT32); confirm the gateway sends
  numeric codes rather than strings/enums. Only a genuinely non-numeric wire value needs a new encoding.
- Transport envelope shape (flat `EnvelopeIn` vs a `TransportBatch`).
- Whether S4/S8/S9/D1 are telemetry or map to event domains (Phase 3) — **[UNVERIFIED]**, inference
  from file naming only; not contract-backed.

**Blocked on product/business decisions:**
- SoC authoritative mappings; T3 3-year retention (approved policy + deployed InfluxDB verification);
  rate-limit operational numbers. (These are not S3–D1 ingestion blockers but gate go-live.)

## 3. Stream-by-stream matrix (S3–S9, D1)

For **every** deferred stream the current state is identical and source-verified; the per-stream
contract content is entirely UNCONFIRMED (no registry signals, no payload schema).

| Stream | Current status / response [FACT] | Missing info [UNRESOLVED] | Tests needed once confirmed | Dependencies | Acceptance criteria |
|---|---|---|---|---|---|
| S3 | recognized, deferred → **501** (`router.py:59`); 0 registry signals | payload schema, substreams, keys/types/units, cadence, timestamp rules | envelope-accept + route-to-write; per-substream mapping; registry validation; idempotency; failure/retry | B1+B2 written; registry seed entries for S3 | contract-approved S3 sample validates + stores; tests green |
| S4 | same → **501** | as S3; plus whether S4 = alarms event domain (**UNVERIFIED**) | as S3; + event-domain tests if applicable | B1+B2; registry; possibly Phase 3 events | as S3 (or routes to its event domain) |
| S5 | same → **501** | as S3 | as S3 | B1+B2; registry | as S3 |
| S6 | same → **501** | as S3 | as S3 | B1+B2; registry | as S3 |
| S7 | same → **501** | as S3 | as S3 | B1+B2; registry | as S3 |
| S8 | same → **501** | as S3; plus whether S8 = config-audit event domain (**UNVERIFIED**) | as S3; + event-domain tests if applicable | B1+B2; registry; possibly Phase 3 | as S4 |
| S9 | same → **501** | as S3; plus whether S9 = command-results event domain (**UNVERIFIED**) | as S3; + event-domain tests if applicable | B1+B2; registry; possibly Phase 3 | as S4 |
| D1 | same → **501** | as S3; plus whether D1 is a **downstream command/control** stream (naming only, **UNVERIFIED**) whose envelope/semantics differ from telemetry | as S3; + command-path tests if applicable | B1+B2; registry; design for command handling | contract-approved D1 behavior defined + tested |

No field names, payload examples, units, cadence, or timestamp rules are proposed for any deferred
stream — they are not in the repository and must come from the gateway contract.

## 4. Registry `category` vs `data_type` — correction [FACT]

Earlier versions of this doc and `docs/gateway-contract-confirmation.md` claimed the seed registry
has `system_alarm`/`system_data` signals with a **non-numeric `data_type`** that return HTTP 422.
That is **incorrect**: `system_alarm`/`system_data` are values of the registry **`category`** column,
not `data_type`. Those signals' `data_type` is numeric (`system_alarm` 16 rows, all U16; `system_data`
47 rows, U16×29 + FLOAT32×18); every registry `data_type` is numeric; and `load_registry_index` does
not carry `category` into the write path. So these signals **coerce and store as numeric points
today** — not 422-rejected. The open question is only the gateway's wire representation (numeric codes
vs strings/enums), not a backend rejection. Runtime behavior is unchanged; this is a documentation fix.

## 5. Proposed implementation order (once unblocked)

1. **Gateway contract freeze (B1/B2)** — external; nothing below can start without it.
2. **Registry seed** — add S3–D1 (and any new S2 encodings) signal definitions per the confirmed
   contract; without registry entries, `build_point` cannot resolve any S3–D1 signal.
3. **Wire-representation confirmation** — confirm whether state/alarm signals (`system_alarm`/
   `system_data` categories, numeric in the registry) arrive as numeric codes; only a genuinely
   non-numeric wire value would need a new encoding. No 422 fix is required for current signals.
4. **Per-stream enablement** — move streams from `DEFERRED_STREAMS` to `INGEST_STREAMS` one at a
   time, each with its registry entries and mapping, behind the existing `ingest_enabled` gate.
5. **Event-domain routing** (if S4/S8/S9/D1 are events, pending confirmation) — Phase 3.
6. **Optional hardening** (stale-pending sweep, client lifecycle) — any time; contract-independent.

## 6. Test & acceptance plan

- **Per newly-enabled stream:** envelope parse + route-to-write; registry validation (good/bad
  signal, type/range, quality, timestamp offset); happy-path store (200); duplicate (200);
  hash-mismatch (409); in-progress (409); influx-failure → 503 pending; retry converges; scope 403.
  Reuse the existing harness (`tests/integration/test_ingest_endpoint.py`, `test_ingest_ledger.py`,
  `test_ingest_mapping.py`, `test_ingest_writer.py`) and `tests/unit/test_streams.py` patterns.
- **Wire representation:** if the gateway confirms any non-numeric state/alarm wire value, a test
  for the agreed encoding; current `system_alarm`/`system_data` signals already validate + store as numeric.
- **Acceptance (per stream):** a contract-approved representative payload travels end-to-end into
  the correct bucket (or its event domain), with idempotency/failure/retry tests green, and the
  full suite remains green. Runtime stays gated (`ingest_enabled` OFF) until staging authorization.

## Blockers & external questions
- **B1/B2 (gateway, written):** record keys/types/nullability + timestamp semantics; transport
  envelope; per-stream S3–D1 payload schemas, units, cadence; unsupported-stream handling;
  `system_alarm`/`system_data` wire representation (numeric codes vs strings/enums). (See `docs/gateway-contract-email-final.md`.)
- **Registry gap [FACT]:** no S3–D1 signals are seeded — even with the contract, registry entries
  must be authored before those streams can validate.
- **Event-domain mapping [UNVERIFIED]:** S4/S8/S9/D1 → alarms/config-audit/command-results is naming
  inference only; confirm before wiring Phase 3.
- **Product:** SoC mappings; T3 retention; rate-limit numbers.

## Facts vs assumptions vs unresolved
- **Facts:** all §1 behavior (with file:line); the registry has only S1/S2 signals; `system_alarm`/
  `system_data` are numeric `category` signals that ingest today (not 422).
- **Assumptions:** the optional hardening items are safe and contract-independent (not yet designed).
- **Unresolved:** every S3–D1 payload detail; the gateway wire representation for state/alarm signals; event-domain mapping;
  product decisions (SoC/T3/rate limits). None guessed.
