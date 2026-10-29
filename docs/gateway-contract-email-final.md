# Gateway Contract Confirmation — final email

**Suggested subject:** UnityESS Central EMS — Northbound ingestion contract: please confirm

**Attachment:** `docs/gateway-contract-confirmation.md`
**Recipient / sender:** fill the placeholders below before sending (left as placeholders on purpose).

> Terminology note: this email uses plain descriptive headings. The attached document numbers its
> sections "B1 — Record JSON contract" and "B2 — Timestamp semantics"; the project roadmap uses
> "B1/B2" with a different split. To avoid ambiguity, each section below names the attached
> document's own section label it maps to.

---

Hi <gateway/EMS team>,

We're preparing the Central EMS northbound ingestion endpoint (`POST /api/v1/gateway/ingest`) for
the Kalpa S2 gateway. It is implemented and tested (latest full suite: **350 passed, 2 skipped**) but **currently disabled by configuration**: the
`ingest_enabled` flag is `false`, so the endpoint authenticates the gateway and then returns
**HTTP 503** while ingestion is disabled. The application does **not** check for contract approval —
the 503 is driven solely by that static configuration flag. As a process decision on our side, we
will not enable the flag until the contract points below are confirmed in writing, because some
record details are still assumptions on our side. The attached `gateway-contract-confirmation.md`
separates **what the backend does today (verified from our source)** from **what we need you to
confirm**.

The two items we need before the ingestion rework can proceed are the two sections below. Partial
confirmations are welcome, and nothing is enabled on our side until these are confirmed in writing.

## Record JSON keys, required/optional fields, and timestamp semantics
*(attached document: "B1 — Record JSON contract" and "B2 — Timestamp semantics")*

- **Exact literal record key names** — any renames, casing, or additional keys we must accept or
  ignore? Today we assume: `source_id`, `asset_id`, `signal_key`, `value`, `quality`, `timestamp`.
- **Required vs optional** for each key, and nullability.
- A **representative real JSON envelope + records example** — one S1 `bess_fast` and one S2
  `bms_extended` message.
- **`value` encoding** — for signals whose registry `data_type` is numeric (float and integer
  types such as FLOAT32 / Float / S16 / U16 / U32) the backend coerces the value to that type and
  **rejects (HTTP 422) any non-numeric, null, or boolean value** (it never clamps, rounds, or
  substitutes). Note: `system_alarm` and `system_data` are registry **category** labels, not data
  types — the signals in those categories (e.g. digital inputs and door / switch / fault / alarm
  feedback in S2 `ems_system`, `io_module`, `fire_protection`) carry **numeric** registry data types
  (`U16` / `FLOAT32`), so the backend treats them as numeric signals and **ingests them today**.
  Please confirm the actual wire representation for **state / status / enumerated / alarm** signals:
  are these values sent as **numeric codes** (matching the numeric registry type), or as strings /
  enums / booleans? If any arrive non-numeric we must agree an encoding before enabling; we will not
  silently narrow or widen the contract.
- **`quality` values** — the backend accepts only `good` / `stale` / `offline` / `error`
  (case-insensitive) or null. Please confirm the exact quality/status values the gateway emits and
  how they map to these.
- **Scope IDs** — confirm the `gateway_id` / `site_id` / `organization_id` the gateway sends are
  the registered UUIDs (so the 403 scope check passes).
- **Timestamp semantics** — does each record `timestamp` represent the **actual measurement
  instant** (not gateway send/collection time)? Format: **ISO-8601 with a UTC offset** (naive
  timestamps are rejected today) or epoch (specify unit and timezone)? Precision (seconds / ms /
  µs) and timezone handling? How are **missing / delayed / out-of-order** measurements represented
  and expected to be handled?

## Transport envelope and S3–D1 payload schemas
*(attached document: "Transport envelope (CONFIRM)" and the stream rows of the "Approval Record")*

- **Transport envelope** — will the gateway post the flat `EnvelopeIn` (a single JSON object with a
  top-level `records` array, `schema_version = "1.0"`) to `POST /api/v1/gateway/ingest` using a
  bearer token from `POST /api/v1/gateway/token`, or is a distinct `TransportBatch` envelope (e.g.
  one wrapping multiple messages) required? If the latter, please provide its exact field layout.
- **Streams in scope** — which streams will the gateway send? The backend today ingests **S1 and
  S2 only**; **S3–S9 and D1 are recognized but not yet supported (HTTP 501)**, and any other stream
  returns HTTP 422.
- **Stream domain classification** — for **S4 / S8 / S9 / D1** specifically: are these telemetry
  streams (time-series signals stored like S1/S2), or do they carry event-type data (e.g. alarms,
  command results, config-audit) that belongs to a separate events domain rather than the
  telemetry ingestion path? We do not assume either way from the stream names.
- **S3–D1 payload schemas** — for any of S3–S9 / D1 the gateway will send, we need the per-stream /
  per-substream payload schema (record keys, types, **units**, and **cadence**) **in writing before
  we implement them**. The backend currently has **no S3–D1 payload definition**, and we will not
  invent one.
- **Unsupported-stream handling** — please confirm the expected behavior for streams the backend
  does not support (today: HTTP 501 for recognized-but-deferred S3–S9/D1, HTTP 422 for anything
  else).

## Also open — secondary (see the attached document; NOT part of the minimum approval gate and NOT approved)
These are open questions, not confirmed items; there is no need to answer them all at once, and
none is enabled or approved on our side.
- **SoC** — the authoritative overall-BESS and per-rack SoC signal(s), units/range, and aggregation
  (there is no single `system_soc` field in our registry today).
- **T3 retention** — the production-approved 3-year policy and the deployed InfluxDB retention for
  the 12 `T3_3Y` signals (our 1095-day value is proposed, not approved; deployed retention is
  unverified).
- **Payload-hash canonicalization** — agreement on the exact fields and canonical form so retries
  are matched correctly.
- **Per-substream cadence** — please confirm (or correct) the cadence table in the attached
  document (it is the backend's proposed target, not approved).
- **Rate limits** — expected `/token` and `/ingest` request rates, burst allowances, retry/backoff
  on 429/503, and outage backlog replay (our limiter exists but is off and unenforced; no numbers
  are approved).

Please complete the **Approval Record** at the end of the attached document (mark each item
Confirmed / Rejected / Pending with any corrections, and fill in the responsible contact and date)
and send it back. The minimum we need before ingestion rework proceeds is the **transport envelope,
the record JSON contract, and timestamp semantics**; nothing is enabled on our side until those are
confirmed in writing.

Happy to walk through any of it on a quick call if that's easier.

Thanks,
<your name>
UnityESS / Central EMS backend
