"""Seatlock FastAPI application entrypoint."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text

from app.api.routes import admin, booking, bookings, metrics, webhooks, ws
from app.infra.config import settings
from app.infra.db import async_session_factory, engine
from app.infra.redis import get_redis
from app.inventory.strategies.base import StrategyUnavailable
from app.realtime.hub import init_hub


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Pre-fill the DB connection pool at startup instead of letting it fill
    lazily on first use.

    Each worker's asyncpg pool otherwise creates connections one at a time,
    on demand, the first time each is needed -- so a sudden burst of
    concurrent requests arriving right after startup (a flash sale, or a
    load test's first tick) pays connection-setup latency on top of the
    request itself, for every request beyond however many connections
    happened to already exist. Opening pool_size connections up front, here,
    means the pool is already warm before the first real request arrives.
    This was investigated as Experiment 1 for the connection-refused
    bursts seen in early Phase 1 benchmarking (see loadtest/results/ and
    the diagnosis recorded there) -- see that writeup for whether it
    resolved the issue.
    """
    conns = await asyncio.gather(*(engine.connect() for _ in range(settings.pool_size)))
    await asyncio.gather(*(conn.close() for conn in conns))

    # One RealtimeHub per worker process (Phase 7, SPEC.md section 9) --
    # see app/realtime/hub.py's own docstring for why this is per-process,
    # not a cross-worker singleton.
    hub = init_hub(get_redis(), async_session_factory)
    await hub.start()
    try:
        yield
    finally:
        await hub.stop()


app = FastAPI(title="Seatlock", lifespan=lifespan)

app.include_router(booking.router, prefix="/api", tags=["booking"])
app.include_router(bookings.router, prefix="/api", tags=["bookings"])
app.include_router(webhooks.router, prefix="/api", tags=["webhooks"])
app.include_router(admin.router, prefix="/api/admin", tags=["admin"])
app.include_router(metrics.router, tags=["metrics"])
app.include_router(ws.router, tags=["realtime"])


@app.exception_handler(StrategyUnavailable)
async def _strategy_unavailable_handler(request: Request, exc: StrategyUnavailable) -> JSONResponse:
    """A strategy hit an infrastructure condition (lock timeout, deadlock)
    rather than making a business decision about seat availability -- 503,
    not a generic 500 or a business-level 409. Registered here (strategy-
    agnostic) rather than in booking.py so the route stays thin and doesn't
    need to know which strategy is configured.
    """
    return JSONResponse(status_code=503, content={"reason": str(exc)})


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
