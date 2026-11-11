"""Characterization: the pure ingest primitives (no database, no network).

Locks the idempotency- and routing-critical pure functions in ``src.ingestion.service``
that the planned refactor (Phase 3 per-stream restructure) must not change:

  * ``canonical_hash`` — the deterministic content hash that drives idempotent,
    last-write-wins de-duplication. A golden value is pinned, and the two defining
    properties are asserted: identity/compression/software-version are EXCLUDED, and any
    telemetry-bearing change flips the hash.
  * ``route_stream`` — the stream -> handler-class routing table.
  * ``verify_scope`` — envelope-vs-authenticated-identity equality.

These are guarded today only by the Docker-backed integration suite
(``test_ingest_endpoint`` / ``test_ingest_ledger``). This unit-level lock runs with no
infrastructure, so the ingest contract is protected in plain CI while modules are moved.

Golden captured from tarun-DB @ d4eca47 on 2026-11-04.
"""
from src.ingestion.service import canonical_hash, route_stream, verify_scope

# A fixed envelope and the canonical_hash it produces today. The hash is computed over
# telemetry-bearing content only (schema_version, stream, substream, sequence,
# created_at_utc, record_count, records), sorted keys, no whitespace.
_ENVELOPE = {
    "schema_version": "1.0",
    "stream": "S2",
    "substream": "pcs_extended",
    "sequence": 7,
    "created_at_utc": "2026-11-04T00:00:00Z",
    "record_count": 1,
    "records": [{"t": "2026-11-04T00:00:00Z", "activepower_a": 12.5}],
    "organization_id": "o1",
    "gateway_id": "g1",
    "compression": "none",
    "gateway_software_version": "9.9",
}
_GOLDEN_HASH = "51a9bee739dd1c731883009968e609e5003b38637f3dcccc446e46edf76933a2"


def test_canonical_hash_golden_is_stable():
    assert canonical_hash(_ENVELOPE) == _GOLDEN_HASH


def test_canonical_hash_excludes_identity_and_transport_fields():
    changed = dict(_ENVELOPE)
    changed["gateway_software_version"] = "0.0"
    changed["compression"] = "gzip"
    changed["organization_id"] = "other"
    changed["gateway_id"] = "other"
    assert canonical_hash(changed) == _GOLDEN_HASH


def test_canonical_hash_changes_on_telemetry_content():
    changed = dict(_ENVELOPE)
    changed["record_count"] = 2
    assert canonical_hash(changed) != _GOLDEN_HASH


def test_route_stream_mapping():
    assert route_stream("S1") == "ingest"
    assert route_stream("S2") == "ingest"
    assert route_stream("S3") == "structured"
    assert route_stream("S4") == "structured"
    assert route_stream("S8") == "structured"
    assert route_stream("S5") == "deferred"
    assert route_stream("S6") == "deferred"
    assert route_stream("S7") == "deferred"
    assert route_stream("S9") == "deferred"
    assert route_stream("D1") == "deferred"
    assert route_stream("S99") == "unknown"


def test_verify_scope_requires_full_identity_match():
    identity = {"gateway_id": "g1", "site_id": "s1", "organization_id": "o1"}
    assert verify_scope(identity, gateway_id="g1", site_id="s1", organization_id="o1") is True
    assert verify_scope(identity, gateway_id="gX", site_id="s1", organization_id="o1") is False
    assert verify_scope(identity, gateway_id="g1", site_id="sX", organization_id="o1") is False
    assert verify_scope(identity, gateway_id="g1", site_id="s1", organization_id="oX") is False
