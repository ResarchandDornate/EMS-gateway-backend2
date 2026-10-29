"""Seed integrity: every signal_registry.csv row must be ingestable by construction.

Pure/offline guard (no DB, no network): asserts each seeded signal's data_type is a
supported numeric type (field_type), each retention_class routes to a configured bucket
(bucket_for), and the whole seed builds a RegistryIndex with no duplicate
(measurement, signal_key). This catches a hand-edit that would otherwise make a signal
silently un-ingestable (HTTP 422) or unroutable (HTTP 501) only at runtime -- especially
when S3-D1 rows are added under the gateway contract later. It changes no behavior.
"""
import csv
import types

from src.influx import RegistryIndex, bucket_for
from src.signals.service import SEED_PATH
from src.signals.types import field_type

# bucket_for only reads these three attributes; non-blank values make T1/T2/T3 resolve.
_BUCKETS = types.SimpleNamespace(
    influx_bucket_raw="raw", influx_bucket_long="long", influx_bucket_3y="3y",
)


def _seed_rows():
    with open(SEED_PATH, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_seed_is_non_empty():
    rows = _seed_rows()
    assert len(rows) > 0, "signal_registry.csv seed is empty"


def test_every_seed_data_type_is_supported():
    bad = []
    for r in _seed_rows():
        try:
            field_type((r["data_type"] or "").strip())
        except Exception as e:  # noqa: BLE001 - any failure is a seed defect we want to surface
            bad.append((r["primary_stream"], r["substream"], r["signal_key"],
                        r["data_type"], type(e).__name__))
    assert not bad, f"seed rows with unsupported data_type (first 10): {bad[:10]}"


def test_every_seed_retention_class_resolves_to_a_bucket():
    bad = []
    for r in _seed_rows():
        try:
            bucket_for((r["retention_class"] or "").strip(), _BUCKETS)
        except Exception as e:  # noqa: BLE001
            bad.append((r["signal_key"], r["retention_class"], type(e).__name__))
    assert not bad, f"seed rows with unroutable retention_class (first 10): {bad[:10]}"


def test_seed_builds_registry_index_without_conflicts():
    rows = [(r["primary_stream"], r["substream"], r["signal_key"],
             (r["asset_type"] or "").strip() or None,
             (r["data_type"] or "").strip(),
             (r["retention_class"] or "").strip()) for r in _seed_rows()]
    idx = RegistryIndex.from_rows(rows)   # raises RegistryConflict on duplicate (measurement, signal_key)
    assert len(idx) == len(rows)          # every row produced a distinct (measurement, signal_key)
