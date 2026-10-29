import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from sqlalchemy import text

from src.accounts.gateways_router import router as gateways_router
from src.accounts.router import router as accounts_router
from src.auth.router import router as auth_router
from src.accounts.operators_router import router as operators_router
from src.gateway.router import router as gateway_router
from src.ingestion.router import router as ingest_router
from src.telemetry.router import router as telemetry_router
from src.config import get_settings
from src.database import engine
from src.influx import InfluxGateway
from src.logging import configure_logging
from src.middleware import RequestContextMiddleware
from src.ratelimit import init_rate_limiter, shutdown_rate_limiter

# Bound all three readiness probes (Postgres, Redis, Influx) so a slow/hung
# dependency can't stall /ready.
READY_PROBE_TIMEOUT_SECONDS = 3


@asynccontextmanager
async def _lifespan(app: FastAPI):
    # Build the shared rate limiter (no-op unless rate_limit_enabled). Never fails startup.
    await init_rate_limiter(app)
    try:
        yield
    finally:
        await shutdown_rate_limiter(app)


def create_app() -> FastAPI:
    configure_logging()
    app = FastAPI(title="UnityESS Central EMS", lifespan=_lifespan)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "service": "ems-backend"}

    @app.get("/ready")
    async def ready() -> JSONResponse:
        checks: dict[str, str] = {}
        try:
            async def _probe_postgres():
                async with engine.connect() as conn:
                    await conn.execute(text("SELECT 1"))
            await asyncio.wait_for(_probe_postgres(), timeout=READY_PROBE_TIMEOUT_SECONDS)
            checks["postgres"] = "ok"
        except Exception as exc:  # noqa: BLE001 - reports any failure, incl. TimeoutError
            checks["postgres"] = f"error: {type(exc).__name__}"
        try:
            client = Redis.from_url(get_settings().redis_url)
            try:
                await asyncio.wait_for(client.ping(), timeout=READY_PROBE_TIMEOUT_SECONDS)
            finally:
                await client.aclose()
            checks["redis"] = "ok"
        except Exception as exc:  # noqa: BLE001 - readiness probe reports any failure
            checks["redis"] = f"error: {type(exc).__name__}"
        try:
            healthy = await asyncio.wait_for(
                InfluxGateway(get_settings()).health(),
                timeout=READY_PROBE_TIMEOUT_SECONDS,
            )
            checks["influx"] = "ok" if healthy else "error: unhealthy"
        except Exception as exc:  # noqa: BLE001 - reports any failure, incl. TimeoutError
            checks["influx"] = f"error: {type(exc).__name__}"
        ok = all(v == "ok" for v in checks.values())
        return JSONResponse(
            {"status": "ok" if ok else "unavailable", **checks}, status_code=200 if ok else 503
        )

    app.include_router(auth_router)      # <-- add (mounts /api/v1/auth/*)
    app.include_router(accounts_router) 
    app.include_router(operators_router) 
    app.include_router(gateways_router)  # mounts /api/v1/admin/sites/{site_id}/gateways
    app.include_router(gateway_router)   # mounts /api/v1/gateway/*
    app.include_router(ingest_router)    # mounts /api/v1/gateway/ingest (503 until 9b-ii)
    app.include_router(telemetry_router) # mounts /api/v1/telemetry/* (read-only; ungated)
    return app


app = create_app()

# Served ASGI entrypoint: request-correlation middleware mounted OUTERMOST so it
# wraps Starlette's ServerErrorMiddleware -> framework-generated 500s still carry
# X-Request-ID and normal framework exception handling runs. Serve src.main:asgi_app.
asgi_app = RequestContextMiddleware(app)
