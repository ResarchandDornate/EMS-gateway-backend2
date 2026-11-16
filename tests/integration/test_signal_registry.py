import pytest

from src.signals.service import (
    count_by_stream,
    count_s2_by_substream,
    load_seed,
)

# expected canonical counts verified against the workbooks
S1_COUNT = 63
S2_TOTAL = 2909  # Kalpa 2471 + CleanEdge v2.0: 72 meters/solar + 255 pcs + 111 io/liquid/dehum
S2_BY_SUBSTREAM = {
    "ems_system": 45,
    "pcs_extended": 341,  # 86 Kalpa + 255 CleanEdge v2.0 (Phase 5.9.2)
    "bms_extended": 2255,
    "io_module": 70,          # 17 Kalpa + 53 CleanEdge
    "liquid_cooling": 66,     # 26 Kalpa + 40 CleanEdge
    "fire_protection": 7,
    "dehumidifier": 27,       # 9 Kalpa + 18 CleanEdge
    "remote_control": 26,
    # CleanEdge v2.0 AVAILABLE_NOW meter/solar signals
    "bess_aux_meter": 31,
    "combiner_box_meter": 11,
    "solar_inverter": 30,
}


@pytest.fixture
async def seeded(session):
    await load_seed(session)
    await session.commit()
    return session


async def test_total_and_per_stream_counts(seeded):
    counts = await count_by_stream(seeded)
    assert counts.get("S1") == S1_COUNT
    assert counts.get("S2") == S2_TOTAL
    assert sum(counts.values()) == S1_COUNT + S2_TOTAL


async def test_s2_substream_counts_match_kalpa(seeded):
    subs = await count_s2_by_substream(seeded)
    assert subs == S2_BY_SUBSTREAM


async def test_s2_sources_are_kalpa_or_cleanhedge(seeded):
    from sqlalchemy import select
    from src.signals.models import SignalRegistryModel
    rows = (await seeded.execute(
        select(SignalRegistryModel.source).where(SignalRegistryModel.primary_stream == "S2")
    )).scalars().all()
    # Kalpa base vocabulary plus the CleanEdge v2.0 meter/solar additions.
    assert set(rows) == {"kalpa", "cleanhedge"}


async def test_utility_meter_absent(seeded):
    subs = await count_s2_by_substream(seeded)
    # Kalpa S2.9 Utility Meter has zero signals -> must not appear
    assert "utility_meter" not in subs


async def test_load_is_idempotent(seeded):
    added_again = await load_seed(seeded)
    await seeded.commit()
    assert added_again == 0
    counts = await count_by_stream(seeded)
    assert sum(counts.values()) == S1_COUNT + S2_TOTAL
