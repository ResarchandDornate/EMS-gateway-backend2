import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.config import get_settings
from src.models import Base
# import every model so create_all builds all tables
from src.tenancy.organizations import models as _o  # noqa: F401
from src.tenancy.sites import models as _s  # noqa: F401
from src.tenancy.gateways import models as _g  # noqa: F401
from src.tenancy.bess_units import models as _b  # noqa: F401
from src.auth import models as _a  # noqa: F401
from src.rbac import models as _r  # noqa: F401
from src.auth.otp_provider import MockOtpProvider, get_otp_provider
from src.database import get_session
from src.main import app

TEST_DB_URL = get_settings().database_url.rsplit("/", 1)[0] + "/ems_test"


@pytest_asyncio.fixture
async def session():
    # fresh engine per test → stays on this test's event loop
    engine = create_async_engine(TEST_DB_URL)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as s:
        yield s

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest_asyncio.fixture
async def client(session):
    # HTTP test client; reuses the test session and a mock OTP provider
    mock = MockOtpProvider()

    async def _session_override():
        yield session

    app.dependency_overrides[get_session] = _session_override
    app.dependency_overrides[get_otp_provider] = lambda: mock
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c, mock
    app.dependency_overrides.clear()