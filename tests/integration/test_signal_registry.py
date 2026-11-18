import pytest

from src.signals.service import (
    count_by_stream,
    count_s2_by_substream,
    load_seed,
)

# expected canonical counts verified against the workbooks
S1_COUNT = 55  # CleanEdge v2.1 Phase 6.1: legacy 63 ornate S1 replaced by 55 normalized fields
S2_TOTAL = 3401  # Phase F-1: +di4h +di13h fire DIs (was 3399 after Phase C)
S2_BY_SUBSTREAM = {
    "ems_system": 88,  # 45 Kalpa + 43 CleanEdge (Phase 5.11: 41; Phase 5.13: +2 plant daily energy)
    "pcs_extended": 343,  # 86 Kalpa + 257 CleanEdge (Phase 5.9.2: 255; Phase 5.13: +2 PCS cumulative energy)
    "bms_extended": 2692,  # 2255 Kalpa + 437 CleanEdge v2.1 (Phase 5.10 BMS)
    "io_module": 77,          # 17 Kalpa + 60 CleanEdge (Phase C: +7 aircond HVAC)
    "liquid_cooling": 66,     # 26 Kalpa + 40 CleanEdge
    "fire_protection": 10,  # 7 Kalpa + 3 CleanEdge (di6h, di4h, di13h)
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
