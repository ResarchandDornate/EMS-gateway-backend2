from fastapi import FastAPI
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from sqlalchemy import text

from src.accounts.gateways_router import router as gateways_router
from src.accounts.router import router as accounts_router
from src.auth.router import router as auth_router  
from src.accounts.operators_router import router as operators_router
from src.config import get_settings
from src.database import engine
from src.logging import configure_logging


def create_app() -> FastAPI:
    configure_logging()
    app = FastAPI(title="UnityESS Central EMS")

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "service": "ems-backend"}

    @app.get("/ready")
    async def ready() -> JSONResponse:
        checks: dict[str, str] = {}
        try:
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            checks["postgres"] = "ok"
        except Exception as exc:  # noqa: BLE001 - readiness probe reports any failure
            checks["postgres"] = f"error: {type(exc).__name__}"
        try:
            client = Redis.from_url(get_settings().redis_url)
            try:
                await client.ping()
            finally:
                await client.aclose()
            checks["redis"] = "ok"
        except Exception as exc:  # noqa: BLE001 - readiness probe reports any failure
            checks["redis"] = f"error: {type(exc).__name__}"
        ok = all(v == "ok" for v in checks.values())
        return JSONResponse(
            {"status": "ok" if ok else "unavailable", **checks}, status_code=200 if ok else 503
        )

    app.include_router(auth_router)      # <-- add (mounts /api/v1/auth/*)
    app.include_router(accounts_router) 
    app.include_router(operators_router) 
    app.include_router(gateways_router)  # mounts /api/v1/admin/sites/{site_id}/gateways
    return app


app = create_app()