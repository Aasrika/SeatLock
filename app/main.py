"""Seatlock FastAPI application entrypoint."""

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy import text

from app.api.routes import admin, booking
from app.infra.db import engine
from app.infra.redis import get_redis

app = FastAPI(title="Seatlock")

app.include_router(booking.router, prefix="/api", tags=["booking"])
app.include_router(admin.router, prefix="/api/admin", tags=["admin"])


@app.get("/health")
async def health() -> JSONResponse:
    """Report Postgres and Redis connectivity independently.

    Each dependency is checked in isolation so a failure in one is never
    masked by the other — see CLAUDE.md rule 4 on Postgres being the source
    of truth and Redis being a cache optimisation only.
    """
    checks: dict[str, str] = {}

    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        checks["postgres"] = "ok"
    except Exception as exc:  # noqa: BLE001 - report status, never crash /health
        checks["postgres"] = f"error: {exc}"

    try:
        await get_redis().ping()
        checks["redis"] = "ok"
    except Exception as exc:  # noqa: BLE001 - report status, never crash /health
        checks["redis"] = f"error: {exc}"

    healthy = all(status == "ok" for status in checks.values())
    return JSONResponse(status_code=200 if healthy else 503, content=checks)
