# UnityESS Central EMS — Northbound Ingestion: Gateway Contract Confirmation

**Status:** Draft for gateway/EMS team confirmation
**Prepared:** 2026-10-25
**Reviewed:** 2026-10-28 (aligned to current implementation and tests)
**Scope:** `POST /api/v1/gateway/ingest` (Northbound telemetry ingestion, streams S1 & S2)
**Current state:** The ingestion endpoint is implemented and tested but **disabled by default**
(`ingest_enabled = false`); it authenticates the gateway and returns **HTTP 503** until the
items below are confirmed in writing. The record JSON keys and timestamp meaning are
currently **assumptions** and must be confirmed before live ingestion is enabled.

This document separates **what the backend currently assumes (verified from our source)**
from **what we need the gateway/EMS team to confirm**. Please complete the Approval Record
at the end.

---

## How the endpoint works today (source-verified)

- **Transport:** HTTP POST with a JSON body. `Content-Encoding: gzip` or `identity` is
  accepted. Limits enforced: compressed body ≤ 5 MiB, decompressed ≤ 50 MiB, and
  `record_count` ≤ 10,000 records per message.
- **Message envelope fields currently accepted** (`schema_version = "1.0"`):

  | Field | Type | Notes (verified) |
  |---|---|---|
  | `schema_version` | string | Only `"1.0"` accepted today |
  | `message_id` | UUID | Idempotency key (per gateway) |
  | `gateway_id` | string | Must match the authenticated gateway's registered identity |
  | `site_id` | string | Must match the gateway's DB-derived site |
  | `organization_id` | string | Must match the gateway's DB-derived organization |
  | `stream` | string | `S1` or `S2` ingested; `S3`–`S9`/`D1` → 501; others → 422 |
  | `substream` | string | e.g. `bess_fast` (S1), `bms_extended`/`ems_system`/… (S2) |
  | `sequence` | integer ≥ 0 | |
  | `created_at_utc` | string | Envelope-level timestamp |
  | `record_count` | integer | Must equal the number of records |
  | `records` | array | See B1 |
  | `compression` | string or null | Optional |
  | `gateway_software_version` | string or null | Optional |

- **Scope/authorization (verified):** the backend derives `organization → site → gateway`
  from its own database for the authenticated gateway and **rejects the message with 403 if
  the envelope's `gateway_id`/`site_id`/`organization_id` do not match**. Client-supplied IDs
  are never trusted for authorization — they must equal the registered values.
- **Signal validation (verified):** every record is validated against our canonical signal
  registry (stream/substream, signal identity, numeric type and range, quality, timestamp).
  Any invalid record rejects the **whole** message before anything is stored.
- **Idempotency / retries (verified):** messages are de-duplicated per `(gateway_id,
  message_id)`. A re-sent, already-stored message with the **same** payload is acknowledged as
  a duplicate (no re-write); the **same** `message_id` with a **different** payload is rejected
  with **409**; a message still being processed returns **409**. A write that fails after the
  message is claimed leaves it retryable (it is **not** acknowledged as stored).


---

## Transport envelope (CONFIRM)

The backend currently accepts the **flat `EnvelopeIn`** described above - a single JSON object
with top-level `records` - posted to `POST /api/v1/gateway/ingest` using a bearer token obtained
from `POST /api/v1/gateway/token`. A separate `TransportBatch` envelope is **not** implemented.

**Please confirm:** will the gateway send the flat `EnvelopeIn` above, or is a distinct
`TransportBatch` envelope (e.g. one wrapping multiple messages) required? If the latter, please
provide its exact field layout so both sides align before ingestion is enabled.

---

## Target gateway, scope & cadence (CONFIRM cadence)

**Target gateway:** this contract covers the **Kalpa S2 northbound gateway**. The authoritative
Kalpa signal definitions available in this repository are the seeded signal registry
(`src/signals/seed/signal_registry.csv`, `source = kalpa`); the original Kalpa workbook is **not**
present in the repo, so the cadences below are taken from the **registry only**.

**Current implementation scope (source-verified):** ingestion accepts **S1 and S2 only**.
Streams `S3`-`S9`/`D1` are recognized but **not yet supported** (-> HTTP 501); any other stream -> 422.

**Proposed per-substream cadence (registry-derived - TARGET, pending gateway confirmation):**

| Stream / substream | Signals | Cadence(s) in registry (seconds) | Retention |
|---|---|---|---|
| S1 / `bess_fast` | 63 | 1 | T1_RAW_90D |
| S2 / `bms_extended` | 2255 | 30 / 60 / 120 / 180 / 300 / 600 | T2_1Y (6 signals T3_3Y) |
| S2 / `pcs_extended` | 86 | 15 / 60 / 21600 | T2_1Y |
| S2 / `ems_system` | 45 | 30 / 120 / 1800 | T2_1Y (6 signals T3_3Y) |
| S2 / `liquid_cooling` | 26 | 30 / 60 / 120 | T2_1Y |
| S2 / `remote_control` | 26 | 600 | T2_1Y |
| S2 / `io_module` | 17 | 60 / 120 | T2_1Y |
| S2 / `dehumidifier` | 9 | 60 / 300 | T2_1Y |
| S2 / `fire_protection` | 7 | 60 | T2_1Y |

These cadences are the backend's **proposed target** and **require gateway-team confirmation**
(or a corrected cadence table). The gateway team has **not** approved them.

**Utility-meter note:** the Kalpa S2 registry contains **no utility-meter / revenue-meter signals**
(none found). S2 utility-meter forwarding was **not represented** in the supplied workload benchmark.
If utility-meter data is in scope for this gateway, it must be defined and added to this contract separately.

---

## B1 — Record JSON contract (CONFIRM)

**Backend currently assumes each record has these keys** (marked *provisional* in our source):

| Key | Type assumed | Required? | Notes |
|---|---|---|---|
| `source_id` | string (≤128) | required | Tag; identifies the data source |
| `asset_id` | string (≤128) | required | Tag; identifies the asset (e.g. rack/bank/PCS) |
| `signal_key` | string (≤128) | required | Must exist in the registry for that substream |
| `value` | number (float/int) | required (non-null) | Schema accepts number/string/bool/null. The backend coerces each value to the registry `data_type` and **rejects (HTTP 422) any non-numeric, null, or boolean value**. Every registry `data_type` is numeric (`U16`/`S16`/`U32`/`FLOAT32`/`Float`); `system_alarm`/`system_data` are **category** labels (not data types) whose signals carry numeric data types, so they ingest as numbers today. See B1 Q3. |
| `quality` | string or null (≤32) | optional | Backend accepts only `good`/`stale`/`offline`/`error` (case-insensitive) or null; any other value is rejected (HTTP 422). See B1 Q5. |
| `timestamp` | string | required | See B2 |

**Please confirm or correct:**
1. The exact literal key names above (any renames, casing, or additional keys we must accept/ignore?).
2. A **representative real JSON envelope + records** example (one S1 `bess_fast` and one S2
   `bms_extended` message), including field types, which fields are required vs optional, and
   nullability.
3. `value` encoding: the backend coerces each value to the registry `data_type` and **rejects
   (HTTP 422) any non-numeric, null, or boolean value**. Every registry `data_type` is numeric
   (`U16`/`S16`/`U32`/`FLOAT32`/`Float`); `system_alarm`/`system_data` are **category** labels, not
   data types, and those signals carry numeric data types, so they ingest as numbers today. Please
   confirm the wire representation for **state / status / enumerated / alarm** signals — are they
   sent as numeric codes (matching the registry type), or as strings / booleans / null? If any are
   non-numeric we must agree an encoding (e.g. numeric codes) before enabling; we will not silently
   narrow or widen the contract.
4. That the `gateway_id`/`site_id`/`organization_id` the gateway will send are the registered
   UUIDs (so the 403 scope check passes).
5. `quality` values: the backend accepts only `good`/`stale`/`offline`/`error` (or null).
   Please confirm the exact quality/status values the gateway emits and how they map to these.

---

## B2 — Timestamp semantics (CONFIRM)

**Backend currently (verified):** each record `timestamp` is parsed as **ISO-8601 and must
include a UTC offset** (naive timestamps are rejected); invalid calendar dates/offsets are
rejected; the instant is normalized to UTC and used as the **measurement time of the stored
point**. The envelope's `created_at_utc` is a separate field (currently used only in the
message idempotency hash, not as the point time).

**Please confirm:**
1. Does each record `timestamp` represent the **actual measurement instant** (not gateway
   send/collection time)? If different fields exist for measurement vs transmission time,
   which should be the stored point time?
2. Format: ISO-8601 with offset (e.g. `2026-09-04T07:59:14+05:30`) — or epoch
   (seconds/milliseconds)? If epoch, specify unit and timezone rules.
3. Precision (seconds / milliseconds / microseconds) and timezone handling (always UTC? local
   with offset?).
4. How are **missing**, **delayed**, or **out-of-order** measurements represented and expected
   to be handled?

---

## Payload-hash canonicalization (CONFIRM)

For idempotency the backend computes a SHA-256 over a canonical JSON of **only** the
telemetry-bearing fields, in sorted-key order with no whitespace. It **includes exactly**:
`schema_version`, `stream`, `substream`, `sequence`, `created_at_utc`, `record_count`, and
`records`; it **excludes** `message_id`, `gateway_id`, `site_id`, `organization_id`,
`compression`, and `gateway_software_version`. The hash is computed over the parsed values, so
numeric representation (`1` vs `1.0`), timestamp string form (`Z` vs `+00:00`), and `quality`
casing are currently **significant** - no normalization is applied.

**Please confirm:** that the gateway's notion of a "same payload" retry matches this field set
and canonical form - in particular, that a retried message reproduces identical
numeric/timestamp/quality encodings. If the gateway canonicalizes differently, we should agree
the rule before enabling ingestion so legitimate retries are not misclassified as a changed
payload (HTTP 409).

---

## T3 — Long-retention (3-year) signals (CONFIRM)

Our registry classifies these **12** signals as `T3_3Y`.

(a) **Implemented backend routing (source-verified):** the backend routes `T3_3Y` to a dedicated
3-year bucket, **`telemetry_3y`** (`src/config.py`), and such messages are **accepted and written**
when ingestion is enabled. Fail-closed behavior (-> HTTP 501) applies **only** when that bucket is
missing/blank, not on the normal path. This code path is exercised by the test
`test_t3_stored_200` - that is **test evidence for the code path only**, not proof of production
behavior or of any deployed retention guarantee.

(b) **Configured / proposed retention (NOT approved):** the T3 bucket's retention is a configured
value of **1095 days** (`influx_bucket_3y_retention_seconds`, default 94,608,000 s), explicitly
**proposed and not production-approved**. It is checked against the configured seconds only at
provisioning time (`src/influx_provision.py`).

(c) **Deployed InfluxDB policy (UNVERIFIED):** the actual retention policy enforced on the deployed
InfluxDB instance has **not** been independently verified and must be confirmed separately. The
S1/S2 `telemetry_raw_90d` / `telemetry_1y` durations are likewise development `docker-compose.yml`
values, not verified production guarantees.

Because (b) and (c) are unresolved, a configured T3 bucket **does not make enabling ingestion
safe** - it only means T3 data would be written to a bucket whose production retention is neither
approved nor verified. We will **not** represent any of these as production-approved, and we will
**not** invent a bucket or reroute signals.

| # | Stream/Substream | `signal_key` | asset_type | type | unit |
|---|---|---|---|---|---|
| 1 | S2/ems_system | `system_charge_energy_session` | bank | FLOAT32 | kWh |
| 2 | S2/ems_system | `system_discharge_energy_session` | bank | FLOAT32 | kWh |
| 3 | S2/ems_system | `system_full_charge_capacity` | bank | FLOAT32 | kWh |
| 4 | S2/ems_system | `system_full_discharge_capacity` | bank | FLOAT32 | kWh |
| 5 | S2/ems_system | `system_accumulated_charge_energy` | bank | FLOAT32 | kWh |
| 6 | S2/ems_system | `system_accumulated_discharge_energy` | bank | FLOAT32 | kWh |
| 7 | S2/bms_extended | `rack_charge_energy_session` | rack | FLOAT32 | kWh |
| 8 | S2/bms_extended | `rack_discharge_energy_session` | rack | FLOAT32 | kWh |
| 9 | S2/bms_extended | `rack_full_charge_capacity` | rack | FLOAT32 | kWh |
| 10 | S2/bms_extended | `rack_full_discharge_capacity` | rack | FLOAT32 | kWh |
| 11 | S2/bms_extended | `rack_accumulated_charge_energy` | rack | FLOAT32 | kWh |
| 12 | S2/bms_extended | `rack_accumulated_discharge_energy` | rack | FLOAT32 | kWh |

**Please confirm:**
- (a) The **production-approved 3-year retention policy** (duration and bucket) for these 12
  signals, and confirmation that the **deployed InfluxDB** retention matches it (today the
  1095-day value is configured/proposed only, and the deployed policy is unverified); and
- (b) **When** the gateway will begin transmitting these signals, so retention is approved and
  verified before sustained 3-year data accumulates.

---

## SoC — Authoritative State-of-Charge mappings (CONFIRM)

Overall-BESS SoC and per-rack SoC are **currently unresolved** in our source (both mappings
are explicitly unset; we do not guess, duplicate across racks, or average rack values to
fabricate an overall figure). Note: an S1 `bess_fast` signal named `soc` exists (asset_type
`pcs_1`), but it has **not** been confirmed as the authoritative overall-BESS or per-rack SoC.
There is no single `system_soc` field in the registry; the only SoC signals in the S2 `ems_system` substream are `system_max_rack_soc` and
`system_min_rack_soc` (bank, %, T2_1Y) — **neither is an authoritative overall-BESS SoC**, and
the registry contains **no whole-rack SoC** signal (rack level has only cell-level and max/min
aggregates).

**Please confirm:**
1. The **exact source signal identifier(s)** for **overall-BESS SoC** and for **per-rack SoC**
   (stream, substream, `signal_key`, `source_id`/`asset_id` scope).
2. **Units** and value range (e.g. % 0–100).
3. **Aggregation semantics** for overall SoC — is it a single reported signal, or must it be
   derived? If derived, the exact method (we will not average rack SoC to invent it without a
   documented rule).

---

## Rate limiting (CONFIRM operational requirements)

A rate-limiting **mechanism** now exists for `POST /api/v1/gateway/token` and
`POST /api/v1/gateway/ingest`, but it is **disabled by default and not enforcing**
(`rate_limit_enabled = false`, `rate_limit_enforce = false`; shadow mode only - it never blocks
and never returns 429). No limits are configured and **no numeric values are approved**. To set
them from real requirements, please provide:
1. Expected **token** request rate per gateway (and token lifetime/refresh behavior).
2. Expected **ingest** request rate and message size per gateway (steady-state and peak).
3. **Burst** limits and expected **retry/backoff** behavior on 429/503 (so retries interact
   correctly with our idempotency ledger).
4. **Acknowledgement handling:** how the gateway treats each ingest response — success (200
   stored), duplicate (200 already-stored), validation failure (422), and retryable failure
   (409/503) — including which responses it treats as final vs. retryable.
5. **Outage backlog recovery:** how queued messages are replayed once connectivity returns —
   expected replay rate/burst, ordering guarantees (in-order vs. out-of-order), and
   retry/backoff schedule — so replays interact correctly with the idempotency ledger.

---

## Approval Record

Please mark each item and return to the EMS backend team.

| Item | Confirmed / Rejected / Pending | Notes / corrections |
|---|---|---|
| B1 — Record JSON contract + example | | |
| B2 — Timestamp semantics | | |
| T3 — 3-year retention decision | | |
| SoC — authoritative mappings | | |
| Rate limiting — operational rates | | |
| Transport/envelope & endpoint - the flat `EnvelopeIn` is posted to `POST /api/v1/gateway/ingest` (bearer token obtained from `POST /api/v1/gateway/token`), or is a separate `TransportBatch` contract required? | | |
| Streams - which streams will the gateway send (including any currently deferred by the backend), and what handling is expected for unsupported streams? | | |
| Payload-hash canonicalization - do gateway and backend agree on the exact canonical form (numeric representation, timestamp strings, quality casing)? | | |
| Retry semantics - which HTTP statuses are final vs retryable, and the agreed retry/backoff and duplicate-acknowledgement behavior? | | |
| T3 production retention - approved duration and deployed InfluxDB policy for the 12 T3 signals, and who verifies them? | | |

**Responsible contact (gateway/EMS team):** ______________________
**Confirmation date:** ______________________

> Until B1 and B2 are confirmed, ingestion remains gated OFF. T3 signals are ingested to the
> configured `telemetry_3y` bucket, but their 3-year retention guarantee is proposed/unapproved
> and the deployed InfluxDB policy is unverified. SoC remains unresolved and will not be fabricated.
