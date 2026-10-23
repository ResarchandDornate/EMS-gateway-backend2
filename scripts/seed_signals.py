import asyncio

from src.database import SessionLocal, engine
from src.signals.service import count_by_stream, load_seed


async def main() -> None:
    async with SessionLocal() as s:
        added = await load_seed(s)
        await s.commit()
        counts = await count_by_stream(s)
    await engine.dispose()
    print(f"signal_registry: {added} rows loaded; by stream = {counts}")


if __name__ == "__main__":
    asyncio.run(main())
