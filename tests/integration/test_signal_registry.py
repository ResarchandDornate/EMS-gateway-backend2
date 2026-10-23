import pytest

from src.signals.service import (
    count_by_stream,
    count_s2_by_substream,
    load_seed,
)

# expected canonical counts verified against the workbooks
S1_COUNT = 63
S2_TOTAL = 2471
S2_BY_SUBSTREAM = {
    "ems_system": 45,
    "pcs_extended": 86,
    "bms_extended": 2255,
    "io_module": 17,
    "liquid_cooling": 26,
    "fire_protection": 7,
    "dehumidifier": 9,
    "remote_control": 26,
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


async def test_s2_comes_only_from_kalpa(seeded):
    from sqlalchemy import select
    from src.signals.models import SignalRegistryModel
    rows = (await seeded.execute(
        select(SignalRegistryModel.source).where(SignalRegistryModel.primary_stream == "S2")
    )).scalars().all()
    assert set(rows) == {"kalpa"}


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
