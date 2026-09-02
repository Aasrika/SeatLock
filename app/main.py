"""Seatlock FastAPI application entrypoint."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import asyncpg.exceptions
import structlog
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from app.api.routes import admin, booking, bookings, demo, metrics, webhooks, ws
from app.infra.config import settings
from app.infra.db import async_session_factory, engine
from app.infra.redis import get_redis
from app.inventory.strategies.base import StrategyUnavailable
from app.realtime.hub import init_hub

log = structlog.get_logger(__name__)


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
# Always registered, gated at the route level (require_demo_mode, 404
# when Settings.demo_mode is off) rather than conditionally included --
# see app/api/routes/demo.py's own module docstring.
app.include_router(demo.router, prefix="/api/demo", tags=["demo"])


@app.exception_handler(StrategyUnavailable)
async def _strategy_unavailable_handler(request: Request, exc: StrategyUnavailable) -> JSONResponse:
    """A strategy hit an infrastructure condition (lock timeout, deadlock)
    rather than making a business decision about seat availability -- 503,
    not a generic 500 or a business-level 409. Registered here (strategy-
    agnostic) rather than in booking.py so the route stays thin and doesn't
    need to know which strategy is configured.
    """
    return JSONResponse(status_code=503, content={"reason": str(exc)})


def _database_unavailable_response(exc: Exception) -> JSONResponse:
    log.warning(
        "db.unavailable",
        error=str(exc),
        sqlstate=getattr(getattr(exc, "orig", exc), "sqlstate", None),
    )
    return JSONResponse(status_code=503, content={"reason": "database temporarily unavailable"})


@app.exception_handler(DBAPIError)
async def _database_unavailable_handler(request: Request, exc: DBAPIError) -> JSONResponse:
    """A DBAPIError that reaches all the way here -- past app/inventory/
    strategies/pessimistic.py's own sqlstate-based translation of
    lock_timeout/deadlock into StrategyUnavailable above -- is a
    connectivity failure SQLAlchemy wrapped while executing a statement
    on an already-open connection (confirmed directly: a Postgres
    restart mid-request raises asyncpg.exceptions.ConnectionDoesNotExistError,
    sqlstate 08003, which SQLAlchemy wraps as exactly this). 503, not the
    bare 500 this would otherwise fall through to -- same reasoning as
    StrategyUnavailable's handler above: a client retrying later should
    see "try again," never "something is broken here."

    This does NOT cover every connection failure -- see
    _raw_asyncpg_connection_error_handler below for the other half.
    """
    return _database_unavailable_response(exc)


@app.exception_handler(asyncpg.exceptions.PostgresConnectionError)
@app.exception_handler(asyncpg.exceptions.OperatorInterventionError)
async def _raw_asyncpg_connection_error_handler(request: Request, exc: Exception) -> JSONResponse:
    """A connectivity failure that happens while OPENING a connection (a
    pool checkout during pool_pre_ping's revalidation, or the pool
    creating a brand-new connection after the old one died) is raised by
    asyncpg directly and reaches here WITHOUT ever passing through
    SQLAlchemy's DBAPIError wrapping -- that wrapping happens around
    statement execution on an already-established connection, not around
    connection establishment itself. Confirmed directly: restarting
    Postgres mid-load surfaced asyncpg.exceptions.CannotConnectNowError
    ("the database system is shutting down") as a raw, unwrapped
    exception, and app/main.py's DBAPIError handler above did not catch
    it -- clients saw a bare 500 until this handler was added.

    Two asyncpg base classes, not `PostgresError` (asyncpg's true root):
    PostgresConnectionError covers "can't reach it"
    (ConnectionDoesNotExistError, ConnectionFailureError, ...);
    OperatorInterventionError covers "it's telling you to go away"
    (CannotConnectNowError, AdminShutdownError, CrashShutdownError, ...).
    Both mean "infrastructure, try later," matching StrategyUnavailable's
    and DBAPIError's handlers above. PostgresError itself is deliberately
    NOT caught here -- it also covers genuine SQL bugs (a syntax error, a
    constraint violation reaching this far), and mapping those to 503
    would hide a real bug behind "the database was briefly unavailable,"
    the exact mistake app/inventory/strategies/pessimistic.py's own
    _raise_translated already warns against.

    Found by, and fixed ahead of relying on, Phase 8a's chaos suite
    (loadtest/chaos/scenarios/postgres_restarted.py). See
    docs/chaos-results.md for what the scenario showed once both halves
    of this fix were in place.
    """
    return _database_unavailable_response(exc)


@app.exception_handler(ConnectionError)
async def _transport_connection_error_handler(
    request: Request, exc: ConnectionError
) -> JSONResponse:
    """A THIRD layer, below both handlers above: a connection attempt
    that fails before asyncpg's own protocol code ever runs -- e.g. the
    TCP/SSL handshake itself getting aborted mid-negotiation because the
    server it was connecting to was already tearing down -- surfaces as
    a plain stdlib socket exception (confirmed directly:
    ConnectionAbortedError, WinError 10053, raised from inside asyncio's
    own transport code during asyncpg's SSL upgrade), not anything
    asyncpg- or SQLAlchemy-specific. ConnectionError is Python's own
    base for exactly this family (ConnectionRefusedError/-ResetError/
    -AbortedError, BrokenPipeError) -- narrow enough to still mean
    "the network failed," unlike the bare OSError or Exception this
    would otherwise have to catch to close the gap.
    """
    return _database_unavailable_response(exc)
    return JSONResponse(status_code=503, content={"reason": "database temporarily unavailable"})


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
