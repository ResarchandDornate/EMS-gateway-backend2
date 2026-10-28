# Cover email — Gateway Contract Confirmation (copy/paste)

**Suggested subject:** UnityESS Central EMS — Northbound ingestion contract: please confirm (S1/S2)

---

Hi <gateway/EMS team>,

We're getting the Central EMS northbound ingestion endpoint ready for the Kalpa S2 gateway. The
endpoint is implemented and tested but is **disabled by default** — it authenticates and returns
HTTP 503 until a few contract points are confirmed in writing, because some record-level details
are still assumptions on our side.

The attached document, `gateway-contract-confirmation.md`, separates **what the backend does
today (verified from our source)** from **what we need you to confirm**. Could you review it and
either confirm each item or send corrections? The open points are:

- **Transport envelope** — flat `EnvelopeIn` as implemented, or is a separate `TransportBatch`
  contract required?
- **B1 — record keys & value encoding** — exact key names, a representative JSON example, and
  how status/enumerated signals are encoded (we currently require numeric values).
- **B2 — timestamp semantics** — measurement vs transmission time, format (ISO-8601 offset vs
  epoch), precision, and out-of-order handling.
- **SoC mapping** — the authoritative overall-BESS and per-rack SoC signals, units, and range
  (there is no single `system_soc` field in our registry today).
- **T3 retention** — the production-approved 3-year policy and deployed InfluxDB retention (our
  1095-day value is proposed, not approved; deployed retention is unverified).
- **Payload-hash canonicalization** — agreement on the exact fields and canonical form so retries
  are matched correctly.
- **Retry / duplicate / acknowledgement / in-progress** — which responses you treat as final vs
  retryable, and your retry/backoff behavior.
- **Rate limits** — expected `/token` and `/ingest` request rates, burst allowances, and backlog
  replay behavior (our limiter exists but is off and unenforced; no numbers are approved).

Please complete the **Approval Record** at the end of the document (mark each item
Confirmed/Rejected/Pending with any corrections, and fill in the responsible contact and date)
and send it back. Nothing is enabled on our side until B1 and B2 are confirmed, so there's no
rush to answer everything at once — partial confirmations are welcome.

Happy to walk through any of it on a quick call if that's easier.

Thanks,
<your name>
UnityESS / Central EMS backend
