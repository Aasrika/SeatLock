"""Interactive walkthrough page's backend (web/'s third tab) -- an
interview/portfolio artifact, not a product feature. Every route here is
gated behind Settings.demo_mode (default False, see its own comment in
app/infra/config.py for the gate reasoning) via require_demo_mode below.

THE BROWSER CANNOT GENERATE REAL CONCURRENCY, AND THAT IS THE REASON THIS
ROUTER EXISTS AS SERVER-SIDE FAN-OUT, NOT A FRONTEND LOOP OF FETCH() CALLS.
Browsers cap concurrent connections per origin at roughly six for HTTP/1.1
(uvicorn, as run here, IS HTTP/1.1), so `Promise.all` over even 50 fetches
arrives at the server in sequential batches of six -- each batch's requests
serialize behind whichever six are already in flight, and the TOCTOU race
window naive.py depends on to oversell never actually opens at any
meaningful scale. A client-side "race demo" would make naive look safe: a
client artifact masquerading as a server property, and it fails in the
single most misleading direction -- toward the broken strategy looking
correct. POST /api/demo/race instead spawns `concurrency` acquisition
attempts INSIDE this process, barrier-synchronised with asyncio.Barrier
(the exact mechanism tests/integration/test_optimistic.py already uses to
prove the same thing deterministically) so they are released at the same
instant regardless of what any browser's connection pool would allow. The
browser only ever sends the one POST that triggers this; it never
generates the concurrency itself.
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy import update as sa_update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.api.routes.admin import InvariantResult, _compute_invariants, _get_event_or_404
from app.domain.state_machine import is_hold_expired
from app.infra import hold_cache
from app.infra.config import settings
from app.infra.db import get_session
from app.infra.mappers import seat_to_domain
from app.infra.tables import SeatRow
from app.inventory.strategies.base import AcquireResult, StrategyUnavailable, get_strategy

router = APIRouter()

StrategyName = Literal["naive", "pessimistic", "optimistic"]


def require_demo_mode() -> None:
    """404, not 403: with DEMO_MODE off, these routes should look like
    they don't exist at all, not like a locked door worth trying to pick
    -- see Settings.demo_mode's own comment for the full gate reasoning
    (selecting the deliberately-broken naive strategy per request is
    exactly what this gate exists to keep unreachable by default).
    """
    if not settings.demo_mode:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)


# --- shared invariant shape, reused from admin.py verbatim ------------------
#
# Every /invariants and /dashboard check_conservation/check_no_double_
# booking/check_state_coherence/check_booking_linkage (I2, a structural I1
# check, state-coherence, and booking-linkage) is what _compute_invariants
# actually verifies -- NOT I3 (sweeper-interval staleness), I4
# (idempotency), or I5 (webhook exactly-once). This router labels its own
# output "4 of 5 verified live" rather than "all five invariants," and
# points at docs/chaos-results.md and this project's own test suite for
# the other three: I3 is exercised by loadtest/chaos/scenarios/
# sweeper_killed.py, I4 by tests/integration/test_idempotency.py, I5 by
# tests/integration/test_webhooks.py. A dashboard that said "all five:
# PASS" while checking four would be the tenth instance in this project
# of a label overstating what a measurement covers -- and the first to
# reach a UI, in front of the audience most likely to ask which five.
UNCHECKED_INVARIANT_NAMES = ("I3", "I4", "I5")


class InvariantSummary(BaseModel):
    results: dict[str, InvariantResult]
    checked_count: int
    total_count: Literal[5] = 5
    unchecked: list[str]
    unchecked_note: str


def _summarize_invariants(results: dict[str, InvariantResult]) -> InvariantSummary:
    return InvariantSummary(
        results=results,
        checked_count=len(results),
        unchecked=list(UNCHECKED_INVARIANT_NAMES),
        unchecked_note=(
            "I3/I4/I5 are not evaluated by this live checker -- they are covered by "
            "the project's test suite and chaos scenarios instead. See "
            "docs/chaos-results.md."
        ),
    )


# --- POST /api/demo/race -----------------------------------------------


class RaceRequest(BaseModel):
    event_id: int
    seat_id: int
    concurrency: int
    strategy: StrategyName


class AttemptResult(BaseModel):
    session_id: str
    outcome: Literal["acquired", "rejected", "error"]
    latency_ms: float
    attempts: int
    reason: str | None = None


class RaceResponse(BaseModel):
    event_id: int
    seat_id: int
    strategy: StrategyName
    concurrency: int
    attempts: list[AttemptResult]
    successful_holders: int
    excess_holders: int
    invariants: InvariantSummary


async def _run_one_attempt(
    engine: AsyncEngine,
    barrier: asyncio.Barrier,
    *,
    seat_id: int,
    holder: str,
    strategy_name: StrategyName,
    hold_duration: timedelta,
    now: datetime,
) -> AttemptResult:
    """One barrier-synchronised acquisition attempt, its own session
    (never shared across concurrent coroutines -- AsyncSession is not
    safe for that), its own strategy instance (strategies are stateless
    aside from config, but a fresh one per attempt keeps this function
    free of any shared mutable state between concurrent attempts).
    """
    strategy = get_strategy(strategy_name)
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)
    async with session_factory() as session:
        # Barrier BEFORE anything that can block, matching
        # tests/integration/test_optimistic.py's own TestConcurrentSingleSeat
        # exactly -- opening a session above is lazy (no connection is
        # checked out until the first execute()), so nothing before
        # barrier.wait() can itself block and desynchronise the release.
        await barrier.wait()
        start = time.monotonic()
        try:
            result: AcquireResult = await strategy.acquire(
                session, [seat_id], holder, hold_duration, now
            )
        except StrategyUnavailable as exc:
            latency_ms = (time.monotonic() - start) * 1000
            return AttemptResult(
                session_id=holder,
                outcome="error",
                latency_ms=latency_ms,
                attempts=1,
                reason=str(exc),
            )
        except Exception as exc:  # noqa: BLE001 -- one attempt's bug must not sink the whole race
            latency_ms = (time.monotonic() - start) * 1000
            return AttemptResult(
                session_id=holder,
                outcome="error",
                latency_ms=latency_ms,
                attempts=1,
                reason=f"{type(exc).__name__}: {exc}",
            )
        latency_ms = (time.monotonic() - start) * 1000

    if result.success:
        return AttemptResult(
            session_id=holder, outcome="acquired", latency_ms=latency_ms, attempts=result.attempts
        )
    return AttemptResult(
        session_id=holder,
        outcome="rejected",
        latency_ms=latency_ms,
        attempts=result.attempts,
        reason=result.reason,
    )


@router.post("/race", response_model=RaceResponse)
async def run_race(
    body: RaceRequest, session: Annotated[AsyncSession, Depends(get_session)]
) -> RaceResponse:
    require_demo_mode()
    if not 2 <= body.concurrency <= 100:
        raise HTTPException(status_code=422, detail="concurrency must be between 2 and 100")

    event = await _get_event_or_404(session, body.event_id)

    # Reset just the target seat before firing -- lets "Fire" be clicked
    # repeatedly with no separate reset step in between. POST /api/demo/
    # reset handles resetting a WHOLE event's seats for the other
    # sections; this is scoped to the one seat the race actually targets.
    await session.execute(
        sa_update(SeatRow)
        .where(SeatRow.id == body.seat_id, SeatRow.event_id == body.event_id)
        .values(
            status="AVAILABLE",
            version=0,
            held_by_session_id=None,
            hold_expires_at=None,
            booking_id=None,
        )
    )
    await session.commit()

    now = datetime.now(UTC)
    hold_duration = timedelta(seconds=settings.demo_default_hold_duration_seconds)
    engine = AsyncEngine(session.get_bind())
    barrier = asyncio.Barrier(body.concurrency)

    attempts = await asyncio.gather(
        *[
            _run_one_attempt(
                engine,
                barrier,
                seat_id=body.seat_id,
                holder=f"race-{i}",
                strategy_name=body.strategy,
                hold_duration=hold_duration,
                now=now,
            )
            for i in range(body.concurrency)
        ]
    )

    successful_holders = sum(1 for a in attempts if a.outcome == "acquired")
    invariant_results = await _compute_invariants(session, event)

    return RaceResponse(
        event_id=body.event_id,
        seat_id=body.seat_id,
        strategy=body.strategy,
        concurrency=body.concurrency,
        attempts=list(attempts),
        successful_holders=successful_holders,
        excess_holders=max(0, successful_holders - 1),
        invariants=_summarize_invariants(invariant_results),
    )


# --- POST /api/demo/reset -----------------------------------------------


class ResetRequest(BaseModel):
    event_id: int


class ResetResponse(BaseModel):
    event_id: int
    seats_reset: int


@router.post("/reset", response_model=ResetResponse)
async def reset_event(
    body: ResetRequest, session: Annotated[AsyncSession, Depends(get_session)]
) -> ResetResponse:
    require_demo_mode()
    await _get_event_or_404(session, body.event_id)
    result = await session.execute(
        sa_update(SeatRow)
        .where(SeatRow.event_id == body.event_id)
        .values(
            status="AVAILABLE",
            version=0,
            held_by_session_id=None,
            hold_expires_at=None,
            booking_id=None,
        )
    )
    await session.commit()
    return ResetResponse(event_id=body.event_id, seats_reset=result.rowcount or 0)


# --- GET /api/demo/state -------------------------------------------------


class DemoSeat(BaseModel):
    id: int
    section: str
    row_label: str
    seat_number: int
    status: str
    held_by_session_id: str | None
    hold_expires_at: datetime | None
    booking_id: int | None
    # Side by side with `status` deliberately -- see this module's own
    # comment on lazy expiry (Phase 4, CLAUDE.md I3): with the sweeper
    # running on a multi-second interval, `status` frequently still reads
    # HELD for seconds after a hold has genuinely expired. `bookable` is
    # the SAME lazy-expiry-aware check every real acquisition path already
    # uses (app.domain.state_machine.is_hold_expired) -- showing both
    # fields next to each other is the walkthrough's illustration of "the
    # status column is not the source of truth about availability, it's
    # cleanup's own bookkeeping."
    bookable: bool


class DemoStateResponse(BaseModel):
    event_id: int
    checked_at: datetime
    seats: list[DemoSeat]
    invariants: InvariantSummary


@router.get("/state", response_model=DemoStateResponse)
async def get_demo_state(
    event_id: int, session: Annotated[AsyncSession, Depends(get_session)]
) -> DemoStateResponse:
    require_demo_mode()
    event = await _get_event_or_404(session, event_id)
    now = datetime.now(UTC)

    seat_rows = (
        (await session.execute(select(SeatRow).where(SeatRow.event_id == event_id))).scalars().all()
    )
    seats = [
        DemoSeat(
            id=row.id,
            section=row.section,
            row_label=row.row_label,
            seat_number=row.seat_number,
            status=row.status,
            held_by_session_id=row.held_by_session_id,
            hold_expires_at=row.hold_expires_at,
            booking_id=row.booking_id,
            bookable=(
                row.status == "AVAILABLE"
                or (row.status == "HELD" and is_hold_expired(seat_to_domain(row), now))
            ),
        )
        for row in seat_rows
    ]

    invariant_results = await _compute_invariants(session, event)

    return DemoStateResponse(
        event_id=event_id,
        checked_at=now,
        seats=seats,
        invariants=_summarize_invariants(invariant_results),
    )


# --- POST /api/demo/hold -------------------------------------------------
#
# Mirrors POST /api/holds (app/api/routes/booking.py) exactly, with one
# difference: an explicit, short hold_duration_seconds. The production
# endpoint is never touched by this feature -- Settings.hold_duration_
# seconds (8 minutes) stays the real default for real holds; a walkthrough
# for an interviewer needs a hold that visibly expires within the ~90
# seconds the whole page is meant to take, which is what this endpoint,
# and only this endpoint, exists to provide.


class DemoHoldRequest(BaseModel):
    event_id: int
    seat_id: int
    session_id: str
    hold_duration_seconds: float | None = None


class DemoHoldResponse(BaseModel):
    event_id: int
    seat_id: int
    session_id: str
    hold_expires_at: datetime


@router.post("/hold", response_model=DemoHoldResponse, status_code=status.HTTP_201_CREATED)
async def create_demo_hold(
    body: DemoHoldRequest, session: Annotated[AsyncSession, Depends(get_session)]
) -> DemoHoldResponse:
    require_demo_mode()
    await _get_event_or_404(session, body.event_id)
    now = datetime.now(UTC)
    duration_seconds = (
        body.hold_duration_seconds
        if body.hold_duration_seconds is not None
        else settings.demo_default_hold_duration_seconds
    )
    hold_duration = timedelta(seconds=duration_seconds)

    strategy = get_strategy(settings.strategy)
    result = await strategy.acquire(session, [body.seat_id], body.session_id, hold_duration, now)
    if not result.success:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=result.reason)

    hold_expires_at = now + hold_duration
    # Best-effort, matching app/api/routes/booking.py's own create_hold --
    # a Redis failure must never turn an already-committed Postgres hold
    # into an HTTP error (CLAUDE.md rule 4).
    await hold_cache.set_hold_mirror(body.seat_id, body.session_id, hold_expires_at, now)

    return DemoHoldResponse(
        event_id=body.event_id,
        seat_id=body.seat_id,
        session_id=body.session_id,
        hold_expires_at=hold_expires_at,
    )
