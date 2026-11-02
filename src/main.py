import asyncio
from contextlib import asynccontextmanager

import structlog
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
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
from src.fast_bess.router import router as fast_bess_router
from src.structured_reads.router import router as structured_reads_router
from src.config import get_settings, resolve_cors_origins
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
    # Shared Influx client: one connection pool reused across requests, so a burst does
    # not open a fresh connection per S2 message (the condition behind client-side write
    # failures). Never fail startup if it can't be built; providers fall back to a per-call
    # client when app.state.influx_client is None.
    app.state.influx_client = None
    try:
        _s = get_settings()
        app.state.influx_client = InfluxDBClientAsync(
            url=_s.influx_url, token=_s.influx_token, org=_s.influx_org,
            timeout=int(getattr(_s, "influx_timeout_ms", 20_000)))
    except Exception:
        structlog.get_logger("startup").warning("influx_shared_client_init_failed")
    try:
        yield
    finally:
        _client = getattr(app.state, "influx_client", None)
        if _client is not None:
            try:
                await _client.close()
            except Exception:
                pass
        await shutdown_rate_limiter(app)


CORS_METHODS = ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS']
CORS_HEADERS = ['Authorization', 'Content-Type']


def add_cors(app: FastAPI, settings) -> FastAPI:
    # Mount CORS from configured origins; no-op when none set (prod stays locked down
    # until origins are configured). Bearer auth is header-based, so credentials default off.
    origins = resolve_cors_origins(settings)
    if origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=origins,
            allow_credentials=settings.cors_allow_credentials,
            allow_methods=CORS_METHODS,
            allow_headers=CORS_HEADERS,
            expose_headers=['X-Request-ID'],
            max_age=600,
        )
    return app


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
    app.include_router(fast_bess_router) # mounts /api/v1/bess/fast/* (S1 reads; PostgreSQL)
    app.include_router(structured_reads_router) # S3-S9 reads (health/alarms/controller/solis/edge-ai/config-audit/commands)
    add_cors(app, get_settings())
    return app


app = create_app()

# Served ASGI entrypoint: request-correlation middleware mounted OUTERMOST so it
# wraps Starlette's ServerErrorMiddleware -> framework-generated 500s still carry
# X-Request-ID and normal framework exception handling runs. Serve src.main:asgi_app.
asgi_app = RequestContextMiddleware(app)
