# preflight_check.py  (READ-ONLY: SELECT only, no DDL/DML) - disposable helper
import asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from src.config import get_settings


async def main():
    e = create_async_engine(get_settings().database_url)
    async with e.connect() as c:
        print("connected_db:", (await c.execute(text("select current_database()"))).scalar())
        reg = (await c.execute(text("select to_regclass('public.ingest_messages')"))).scalar()
        print("ingest_messages:", reg)
        if reg is not None:
            rows = (await c.execute(text(
                "select column_name, data_type, is_nullable "
                "from information_schema.columns where table_name='ingest_messages' "
                "order by ordinal_position"))).all()
            for r in rows:
                print("col:", tuple(r))
    await e.dispose()


asyncio.run(main())
