"""Single-source-of-truth + immutability guard for stream classification (Option B)."""
import uuid

import pytest
from pydantic import ValidationError

import src.ingestion.schemas as ingest_schemas
import src.telemetry.service as telemetry_service
from src import streams
from src.ingestion.schemas import EnvelopeIn
from src.ingestion.service import route_stream


def _env(stream):
    return dict(schema_version="1.0", message_id=uuid.uuid4(), gateway_id="g",
                site_id="s", organization_id="o", stream=stream, substream="x",
                sequence=1, created_at_utc="2026-01-01T00:00:00Z",
                record_count=0, records=[])


def test_canonical_values_unchanged():
    assert set(streams.INGEST_STREAMS) == {"S1", "S2"}
    assert set(streams.STRUCTURED_STREAMS) == {"S3", "S4", "S8"}
    assert set(streams.DEFERRED_STREAMS) == {"S5", "S6", "S7", "S9", "D1"}
    assert set(streams.KNOWN_STREAMS) == (set(streams.INGEST_STREAMS)
                                          | set(streams.STRUCTURED_STREAMS)
                                          | set(streams.DEFERRED_STREAMS))


def test_classification_sets_are_disjoint():
    assert streams.INGEST_STREAMS.isdisjoint(streams.DEFERRED_STREAMS)
    assert streams.INGEST_STREAMS.isdisjoint(streams.STRUCTURED_STREAMS)
    assert streams.STRUCTURED_STREAMS.isdisjoint(streams.DEFERRED_STREAMS)


def test_classifications_are_immutable_frozensets():
    for s in (streams.INGEST_STREAMS, streams.STRUCTURED_STREAMS, streams.DEFERRED_STREAMS, streams.KNOWN_STREAMS):
        assert isinstance(s, frozenset)          # immutable: no add/discard/update
        assert not hasattr(s, "add")


def test_ingestion_and_telemetry_use_the_shared_classifications():
    # behavior/value: both expose the same vocabulary as the single source
    assert ingest_schemas.KNOWN_STREAMS == streams.KNOWN_STREAMS
    assert telemetry_service.SUPPORTED_STREAMS == streams.INGEST_STREAMS
    # drift guard (secondary): they are literally the shared objects
    assert ingest_schemas.KNOWN_STREAMS is streams.KNOWN_STREAMS
    assert telemetry_service.SUPPORTED_STREAMS is streams.INGEST_STREAMS


def test_route_stream_behavior_preserved():
    assert route_stream("S1") == "ingest" and route_stream("S2") == "ingest"
    assert route_stream("S3") == "structured" and route_stream("S4") == "structured"
    assert route_stream("S8") == "structured"
    assert route_stream("S5") == "deferred" and route_stream("D1") == "deferred"
    assert route_stream("S42") == "unknown"


def test_envelope_validation_behavior_preserved():
    EnvelopeIn(**_env("S1"))                 # ingest-eligible -> ok
    EnvelopeIn(**_env("S5"))                 # deferred -> parses (routed to 501 later)
    with pytest.raises(ValidationError):     # unknown -> 422 at the envelope
        EnvelopeIn(**_env("S42"))


def test_telemetry_supported_membership():
    assert "S1" in telemetry_service.SUPPORTED_STREAMS
    assert "S2" in telemetry_service.SUPPORTED_STREAMS
    assert "S9" not in telemetry_service.SUPPORTED_STREAMS
