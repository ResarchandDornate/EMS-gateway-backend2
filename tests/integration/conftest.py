import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.engine import make_url

from src.config import get_settings
from src.models import Base
# import every model so create_all builds all tables
from src.tenancy.organizations import models as _o  # noqa: F401
from src.tenancy.sites import models as _s  # noqa: F401
from src.tenancy.gateways import models as _g  # noqa: F401
from src.tenancy.bess_units import models as _b  # noqa: F401
from src.auth import models as _a  # noqa: F401
from src.rbac import models as _r  # noqa: F401
from src.signals import models as _sig  # noqa: F401
from src.ingestion import models as _ing  # noqa: F401
from src.ingestion import health_models as _ingh  # noqa: F401
from src.ingestion import alarm_models as _inga  # noqa: F401
from src.ingestion import controller_models as _ingc  # noqa: F401
from src.ingestion import solis_models as _ings  # noqa: F401
from src.ingestion import edge_ai_models as _inge  # noqa: F401
from src.ingestion import config_audit_models as _ingca  # noqa: F401
from src.ingestion import command_result_models as _ingcr  # noqa: F401
from src.ingestion import fast_bess_models as _ingfb  # noqa: F401
from src.ingestion import stream_models as _ingsr  # noqa: F401
from src.auth.otp_provider import MockOtpProvider, get_otp_provider
from src.database import get_session
from src.main import app

TEST_DB_URL = make_url(get_settings().database_url).set(database="ems_test")
assert TEST_DB_URL.database == "ems_test", f"refusing non-test DB: {TEST_DB_URL.database}"


@pytest.fixture
def test_db_url():
    """The one test DB URL, reused by the session fixture and the concurrency racers."""
    return TEST_DB_URL


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