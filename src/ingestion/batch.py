"""Backward-compatible legacy batch adaptation (Central Sync / Kalpa i.MX93 gateway).

The legacy gateway uploader posts a TransportBatch wrapper:

    {"request_id": "...", "gateway_id": "...", "sent_at_utc": "...",
     "messages": [ {per-message envelope with a LEGACY stream name}, ... ]}

Each message is translated into a canonical envelope and stored VERBATIM as structured
JSONB (no explode, no per-signal coercion, no registry lookup) - S1 fast_bess in its own
table, S2..S10 in the generic stream store. The single-message endpoint is untouched.

Legacy stream name -> canonical code + substream (Kalpa Central Sync Master Contract v1.1
STREAM INDEX). S2 carries its substream per message. D1 (downlink) is out of scope and
S2's combiner_box_meter substream is excluded by contract decision.

Identity (gateway_id / site_id / organization_id) is stamped by the caller from the
authenticated gateway's DB-resolved identity; the legacy strings in the batch are never
trusted for authorization. message_id, sequence, created_at_utc and every record value
are preserved exactly.
"""

# Legacy stream name -> (canonical stream code, fixed substream | None).
# None substream means "take substream from the message" (S2 general_asset_telemetry).
LEGACY_STREAM_MAP = {
    "fast_bess_telemetry": ("S1", "bess_fast"),
    "general_asset_telemetry": ("S2", None),
    "gateway_health": ("S3", "gateway"),
    "alarms_events": ("S4", "events"),
    "soc_controller": ("S5", "controller"),
    "solis": ("S6", "solis"),
    "edge_ai": ("S7", "per_bess"),
    "configuration_audit": ("S8", "gateway"),
    "command_results": ("S9", "commands"),
    "battery_charge_discharge_control": ("S10", "pair_controller"),
}

# S2 substreams intentionally NOT ingested (contract decision).
EXCLUDED_SUBSTREAMS = frozenset({"combiner_box_meter"})

# Upper bound on messages per batch (body size is already capped in body.decode_body).
MAX_BATCH_MESSAGES = 10_000

# Canonical schema version this adapter targets.
TARGET_SCHEMA_VERSION = "1.0"


class BatchMessageError(Exception):
    """A single batch message could not be adapted. Carries the per-message HTTP status,
    an ACK error_code, and a useful diagnostic; message_id is best-effort."""

    def __init__(self, status_code: int, detail: str, *,
                 error_code: str = "invalid_payload", message_id=None):
        self.status_code = status_code
        self.detail = detail
        self.error_code = error_code
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


def adapt_batch_message(msg, identity: dict) -> dict:
    """Translate one legacy batch message into a canonical envelope dict (records verbatim).
    Raises BatchMessageError (with a per-message error_code + diagnostic) for unsupported
    streams, excluded substreams, or missing required fields."""
    if not isinstance(msg, dict):
        raise BatchMessageError(422, "batch message is not a JSON object")

    mid = msg.get("message_id")
    raw_stream = msg.get("stream")
    if not isinstance(raw_stream, str) or raw_stream not in LEGACY_STREAM_MAP:
        raise BatchMessageError(
            422,
            f"stream {raw_stream!r} is not supported yet; supported: {sorted(LEGACY_STREAM_MAP)}.",
            error_code="stream_not_yet_supported", message_id=mid,
        )
    code, fixed_sub = LEGACY_STREAM_MAP[raw_stream]

    if fixed_sub is not None:
        substream = fixed_sub
    else:
        substream = msg.get("substream")                      # S2: per-message substream
        if not isinstance(substream, str) or not substream:
            raise BatchMessageError(422, "batch message missing 'substream'", message_id=mid)
    if substream in EXCLUDED_SUBSTREAMS:
        raise BatchMessageError(
            422, f"substream {substream!r} is excluded from ingestion",
            error_code="substream_excluded", message_id=mid)

    records = msg.get("records")
    if not isinstance(records, list):
        raise BatchMessageError(422, "batch message has no 'records' array", message_id=mid)

    for req in ("message_id", "sequence", "created_at_utc"):
        if msg.get(req) is None:
            raise BatchMessageError(
                422, f"batch message missing required field {req!r}", message_id=mid
            )

    env = {
        "schema_version": TARGET_SCHEMA_VERSION,
        "message_id": str(mid),
        # DB-derived identity stamped by the caller; legacy batch IDs are ignored for auth.
        "gateway_id": str(identity["gateway_id"]),
        "site_id": str(identity["site_id"]),
        "organization_id": str(identity["organization_id"]),
        "stream": code,
        "substream": substream,
        "sequence": msg["sequence"],
        "created_at_utc": msg["created_at_utc"],
        "record_count": len(records),
        "records": records,                       # VERBATIM -> stored as JSONB
    }
    for opt in ("priority", "gateway_software_version", "compression"):
        if isinstance(msg.get(opt), str):
            env[opt] = msg[opt]
    return env
