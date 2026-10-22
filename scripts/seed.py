import asyncio

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.config import get_settings
from src.rbac.service import seed_roles_permissions


async def main() -> None:
    engine = create_async_engine(get_settings().database_url)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as s:
        await seed_roles_permissions(s)
        await s.commit()
    await engine.dispose()
    print("seed complete")


if __name__ == "__main__":
    asyncio.run(main())