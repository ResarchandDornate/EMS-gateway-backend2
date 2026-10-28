"""Canonical stream classification - the single source of truth for which telemetry
streams are ingest-eligible, recognized-but-deferred, or known at all.

Defined on this package's __init__ so `from src.streams import ...` yields the shared,
immutable classification. Dependency-free (no imports from other src modules) to avoid
any circular import. These are CURRENT, source-verified classifications; the Gateway v2
stream/substream vocabulary is NOT finalized, so this is not a contract and holds no
per-substream vocabulary - substreams remain validated at runtime against the signal
registry, not a hardcoded list here.

The placeholder submodules (s1_fast_bess, ... d1_commands, envelope) are unused scaffolding
and are left untouched.
"""

# Streams accepted by the ingestion write path. Telemetry read support mirrors this set
# (see SUPPORTED_STREAMS in src/telemetry/service.py).
INGEST_STREAMS = frozenset({"S1", "S2"})

# Recognized by the contract but not yet implemented -> HTTP 501 at ingest.
DEFERRED_STREAMS = frozenset({"S3", "S4", "S5", "S6", "S7", "S8", "S9", "D1"})

# Every stream the envelope validator accepts; anything else -> HTTP 422.
KNOWN_STREAMS = INGEST_STREAMS | DEFERRED_STREAMS   # frozenset | frozenset -> frozenset
