"""Backward-compatible legacy batch adaptation (Central Sync / Kalpa i.MX93 gateway).

The legacy gateway uploader (built for the old bess_ems_platform_backend) posts a
TransportBatch wrapper:

    {"request_id": "...", "gateway_id": "...", "sent_at_utc": "...",
     "messages": [ {per-message envelope with a LEGACY stream name}, ... ]}

The new backend's /api/v1/gateway/ingest expects ONE canonical EnvelopeIn per request
with codes S1..S9/D1 and one flat RecordIn per signal. This module translates a batch's
messages into canonical EnvelopeIn dicts WITHOUT weakening the single-message endpoint.

S1 record conversion (evidence-backed, not guessed)
---------------------------------------------------
The real `fast_bess_telemetry` record is a nested per-BESS bundle (confirmed by the gateway
team's own simulator `tools/simulator.py::s1_records` and by how the OLD backend stored it):

    {"source_id","bess_id","pcs_asset_id","bms_asset_id","quality","timestamp_utc",
     "pcs_values": {<28 signal_key: value>}, "bms_values": {<35 signal_key: value>}, ...}

The 28 `pcs_values` keys + 35 `bms_values` keys are EXACTLY the 63 `S1/bess_fast`
signal_keys in this backend's authoritative signal registry
(`src/signals/seed/signal_registry.csv`, source=kalpa) - verified key-for-key. So the
explode is mechanical: each nested `signal_key -> value` becomes one canonical RecordIn,
tagged with the section's asset id (`pcs_asset_id` for pcs_values, `bms_asset_id` for
bms_values), the record's `source_id`, its record-level `quality`, and its `timestamp_utc`
(falling back to the message `created_at_utc`). No signal is renamed or invented: every
exploded signal_key is validated downstream against the registry by
`build_message_points`/`build_point` (an unknown key -> UnknownSignal -> HTTP 422), so a
real message whose field names differ fails safe with a diagnostic rather than mis-storing.

Already-flat records (canonical RecordIn keys) are passed through unchanged. Any record
that is neither flat nor a recognized nested fast_bess_sample is rejected explicitly.

Hard rules (do not guess):
  * Only the legacy stream name `fast_bess_telemetry` is mapped, to canonical S1/bess_fast.
    S2 and the deferred streams are intentionally NOT mapped here.
  * message_id, sequence, created_at_utc, source_id, asset ids, quality, timestamps and
    every value (including 0 / false / null) are preserved exactly; missing required
    fields are rejected, never defaulted.
  * Identity (gateway_id / site_id / organization_id) is stamped from the authenticated
    gateway's DB-resolved identity by the caller; the legacy strings in the batch are never
    trusted for authorization.
"""

# Legacy stream name -> (canonical stream code, substream). S1 ONLY (S2 and deferred
# streams are not enabled by assumption).
LEGACY_STREAM_MAP = {
    "fast_bess_telemetry": ("S1", "bess_fast"),
}

# Canonical flat RecordIn keys. quality is optional; the rest are required.
_FLAT_REQUIRED = ("source_id", "asset_id", "signal_key", "value", "timestamp")
_FLAT_OPTIONAL = ("quality",)

# Nested fast_bess_sample value sections -> the record field holding that section's asset id.
_NESTED_SECTIONS = (("pcs_values", "pcs_asset_id"), ("bms_values", "bms_asset_id"))

# Upper bound on messages per batch (body size is already capped in body.decode_body).
MAX_BATCH_MESSAGES = 10_000

# Canonical schema version this adapter targets.
TARGET_SCHEMA_VERSION = "1.0"


class BatchMessageError(Exception):
    """A single batch message could not be adapted. Carries the per-message HTTP status
    and a useful diagnostic; message_id is best-effort for correlation."""

    def __init__(self, status_code: int, detail: str, message_id=None):
        self.status_code = status_code
        self.detail = detail
        self.message_id = message_id
        super().__init__(detail)


def is_batch_body(body) -> bool:
    """True iff body is a legacy batch wrapper: a dict with a `messages` list and NO
    top-level `stream`. A single canonical envelope always carries a top-level `stream`,
    so this never intercepts the individual-message path."""
    return (
        isinstance(body, dict)
        and "stream" not in body
        and isinstance(body.get("messages"), list)
    )


def _is_nested_fast_bess(rec) -> bool:
    return isinstance(rec, dict) and any(section in rec for section, _ in _NESTED_SECTIONS)


def _flat_passthrough(rec, i: int) -> dict:
    missing = [k for k in _FLAT_REQUIRED if k not in rec]
    if missing:
        raise BatchMessageError(
            422,
            f"record[{i}] missing required canonical keys {missing}; present keys "
            f"{sorted(rec.keys())}.",
        )
    out = {k: rec[k] for k in _FLAT_REQUIRED}      # values preserved verbatim (0/false/null)
    for k in _FLAT_OPTIONAL:
        if k in rec:
            out[k] = rec[k]
    return out


def _explode_nested(rec, i: int, fallback_ts) -> list[dict]:
    """Explode a nested fast_bess_sample record into one canonical flat record per signal.
    Raises BatchMessageError (no guessing) on any missing structural field."""
    source_id = rec.get("source_id")
    if not isinstance(source_id, str) or not source_id:
        raise BatchMessageError(422, f"record[{i}] (fast_bess_sample) missing 'source_id'")
    ts = rec.get("timestamp_utc")
    if not (isinstance(ts, str) and ts):
        ts = fallback_ts                           # message created_at_utc
    if not (isinstance(ts, str) and ts):
        raise BatchMessageError(
            422, f"record[{i}] has no 'timestamp_utc' and the message has no 'created_at_utc'"
        )
    quality = rec.get("quality")                   # record-level; may be None (RecordIn allows)

    out: list[dict] = []
    for section, asset_field in _NESTED_SECTIONS:
        vals = rec.get(section)
        if vals is None:
            continue
        if not isinstance(vals, dict):
            raise BatchMessageError(
                422, f"record[{i}] {section!r} must be an object of signal_key -> value"
            )
        asset_id = rec.get(asset_field)
        if not isinstance(asset_id, str) or not asset_id:
            raise BatchMessageError(
                422, f"record[{i}] has {section!r} but no {asset_field!r} to tag those signals"
            )
        for signal_key, value in vals.items():
            # value preserved verbatim (incl. 0 / false / null); the registry validates it.
            out.append({
                "source_id": source_id, "asset_id": asset_id, "signal_key": signal_key,
                "value": value, "quality": quality, "timestamp": ts,
            })
    if not out:
        raise BatchMessageError(
            422, f"record[{i}] fast_bess_sample has empty pcs_values/bms_values"
        )
    return out


def _records_for(rec, i: int, fallback_ts) -> list[dict]:
    if not isinstance(rec, dict):
        raise BatchMessageError(422, f"record[{i}] is not a JSON object")
    if _is_nested_fast_bess(rec):
        return _explode_nested(rec, i, fallback_ts)
    if all(k in rec for k in _FLAT_REQUIRED):
        return [_flat_passthrough(rec, i)]
    raise BatchMessageError(
        422,
        f"record[{i}] is neither a canonical flat record (keys {list(_FLAT_REQUIRED)}) nor a "
        f"fast_bess_sample with pcs_values/bms_values; present keys {sorted(rec.keys())}. "
        f"The adapter will not infer a mapping for an unrecognized record shape.",
    )


def adapt_batch_message(msg, identity: dict) -> dict:
    """Translate one legacy batch message into a canonical EnvelopeIn dict (exploding
    nested S1 records into one flat RecordIn per signal). Raises BatchMessageError (with a
    per-message HTTP status + diagnostic) if the message cannot be safely adapted."""
    if not isinstance(msg, dict):
        raise BatchMessageError(422, "batch message is not a JSON object")

    mid = msg.get("message_id")
    raw_stream = msg.get("stream")
    if not isinstance(raw_stream, str) or raw_stream not in LEGACY_STREAM_MAP:
        raise BatchMessageError(
            422,
            f"batch message stream {raw_stream!r} is not handled by the adapter; only "
            f"{sorted(LEGACY_STREAM_MAP)} (-> S1/bess_fast) is enabled. S2 and deferred "
            f"streams are intentionally not mapped.",
            message_id=mid,
        )
    code, substream = LEGACY_STREAM_MAP[raw_stream]

    records = msg.get("records")
    if not isinstance(records, list):
        raise BatchMessageError(422, "batch message has no `records` array", message_id=mid)

    # Required envelope fields are preserved from the message; never defaulted/invented.
    for req in ("message_id", "sequence", "created_at_utc"):
        if msg.get(req) is None:
            raise BatchMessageError(
                422, f"batch message missing required field {req!r}", message_id=mid
            )

    created_at = msg["created_at_utc"]
    flat: list[dict] = []
    for i, rec in enumerate(records):
        try:
            flat.extend(_records_for(rec, i, created_at))
        except BatchMessageError as e:
            e.message_id = e.message_id or mid
            raise

    return {
        "schema_version": TARGET_SCHEMA_VERSION,
        "message_id": str(mid),
        # DB-derived identity stamped by the caller; legacy batch IDs are ignored for auth.
        "gateway_id": str(identity["gateway_id"]),
        "site_id": str(identity["site_id"]),
        "organization_id": str(identity["organization_id"]),
        "stream": code,
        "substream": substream,
        "sequence": msg["sequence"],
        "created_at_utc": created_at,
        "record_count": len(flat),
        "records": flat,
    }
