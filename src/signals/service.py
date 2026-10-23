# import csv
# from pathlib import Path

# from sqlalchemy import func, select

# from src.signals.models import SignalRegistryModel

# SEED_PATH = Path(__file__).parent / "seed" / "signal_registry.csv"


# def _s(v):
#     v = (v or "").strip()
#     return v or None


# def _int(v):
#     v = (v or "").strip()
#     return int(v) if v else None


# def _bool(v):
#     return (v or "").strip().lower() in ("true", "1", "yes")


# async def load_seed(session, csv_path=SEED_PATH):
#     """Idempotent load. Skips rows already present by (primary_stream, substream, signal_key)."""
#     existing = {
#         (r.primary_stream, r.substream, r.signal_key)
#         for r in (await session.execute(select(SignalRegistryModel))).scalars()
#     }
#     new_rows = []
#     with open(csv_path, newline="", encoding="utf-8") as f:
#         for row in csv.DictReader(f):
#             key = (row["primary_stream"], row["substream"], row["signal_key"])
#             if key in existing:
#                 continue
#             existing.add(key)
#             new_rows.append(SignalRegistryModel(
#                 point_id=_s(row["point_id"]),
#                 source=row["source"],
#                 primary_stream=row["primary_stream"],
#                 substream=row["substream"],
#                 asset_type=_s(row["asset_type"]),
#                 signal_key=row["signal_key"],
#                 display_name=_s(row["display_name"]),
#                 unit=_s(row["unit"]),
#                 category=_s(row["category"]),
#                 data_type=_s(row["data_type"]),
#                 policy_mode=_s(row["policy_mode"]),
#                 cadence_sec=_int(row["cadence_sec"]),
#                 deadband=_s(row["deadband"]),
#                 policy_group=_s(row["policy_group"]),
#                 retention_class=_s(row["retention_class"]),
#                 event_eligible=_bool(row["event_eligible"]),
#                 rw=_s(row["rw"]),
#                 protocol_ref=_s(row["protocol_ref"]),
#                 source_ref=_s(row["source_ref"]),
#             ))
#     session.add_all(new_rows)
#     await session.flush()
#     return len(new_rows)


# async def count_by_stream(session):
#     rows = await session.execute(
#         select(SignalRegistryModel.primary_stream, func.count())
#         .group_by(SignalRegistryModel.primary_stream)
#     )
#     return dict(rows.all())


# async def count_s2_by_substream(session):
#     rows = await session.execute(
#         select(SignalRegistryModel.substream, func.count())
#         .where(SignalRegistryModel.primary_stream == "S2")
#         .group_by(SignalRegistryModel.substream)
#     )
#     return dict(rows.all())


import csv
from collections import defaultdict
from pathlib import Path

from sqlalchemy import func, select

from src.influx import RegistryIndex, measurement_for
from src.signals.models import SignalRegistryModel
from src.signals.types import quality_collisions

SEED_PATH = Path(__file__).parent / "seed" / "signal_registry.csv"


def _s(v):
    v = (v or "").strip()
    return v or None


def _int(v):
    v = (v or "").strip()
    return int(v) if v else None


def _bool(v):
    return (v or "").strip().lower() in ("true", "1", "yes")


async def load_seed(session, csv_path=SEED_PATH):
    existing = {
        (r.primary_stream, r.substream, r.signal_key)
        for r in (await session.execute(select(SignalRegistryModel))).scalars()
    }
    new_rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            key = (row["primary_stream"], row["substream"], row["signal_key"])
            if key in existing:
                continue
            existing.add(key)
            new_rows.append(SignalRegistryModel(
                point_id=_s(row["point_id"]), source=row["source"],
                primary_stream=row["primary_stream"], substream=row["substream"],
                asset_type=_s(row["asset_type"]), signal_key=row["signal_key"],
                display_name=_s(row["display_name"]), unit=_s(row["unit"]),
                category=_s(row["category"]), data_type=_s(row["data_type"]),
                policy_mode=_s(row["policy_mode"]), cadence_sec=_int(row["cadence_sec"]),
                deadband=_s(row["deadband"]), policy_group=_s(row["policy_group"]),
                retention_class=_s(row["retention_class"]),
                event_eligible=_bool(row["event_eligible"]), rw=_s(row["rw"]),
                protocol_ref=_s(row["protocol_ref"]), source_ref=_s(row["source_ref"]),
            ))
    session.add_all(new_rows)
    await session.flush()
    return len(new_rows)


async def count_by_stream(session):
    rows = await session.execute(
        select(SignalRegistryModel.primary_stream, func.count())
        .group_by(SignalRegistryModel.primary_stream)
    )
    return dict(rows.all())


async def count_s2_by_substream(session):
    rows = await session.execute(
        select(SignalRegistryModel.substream, func.count())
        .where(SignalRegistryModel.primary_stream == "S2")
        .group_by(SignalRegistryModel.substream)
    )
    return dict(rows.all())


async def load_registry_index(session) -> RegistryIndex:
    """Trusted (measurement, signal_key) -> RegistrySignal index for the write path."""
    rows = (await session.execute(
        select(SignalRegistryModel.primary_stream, SignalRegistryModel.substream,
               SignalRegistryModel.signal_key, SignalRegistryModel.asset_type,
               SignalRegistryModel.data_type, SignalRegistryModel.retention_class)
    )).all()
    return RegistryIndex.from_rows(rows)


async def quality_field_collisions(session):
    rows = (await session.execute(
        select(SignalRegistryModel.primary_stream, SignalRegistryModel.substream,
               SignalRegistryModel.signal_key)
    )).all()
    by_meas = defaultdict(set)
    for ps, sub, key in rows:
        by_meas[measurement_for(ps, sub)].add(key)
    return quality_collisions(by_meas)