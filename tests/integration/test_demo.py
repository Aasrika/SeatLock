"""app/api/routes/demo.py -- the walkthrough page's backend.

Calls the route functions directly (this suite's established convention
-- see test_admin_dashboard.py's own docstring for why: a FastAPI route
decorated with @router.post is still a plain callable). DEMO_MODE is a
global Settings singleton (app/infra/config.py), toggled per test and
always restored, matching test_naive_strategy.py's own
_widen_race_window fixture pattern.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from fastapi import HTTPException
from sqlalchemy import insert, update
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

import app.infra.config as infra_config
from app.api.routes.demo import DemoHoldRequest as HoldRequest
from app.api.routes.demo import (
    RaceRequest,
    ResetRequest,
    create_demo_hold,
    get_demo_state,
    reset_event,
    run_race,
)
from app.infra.tables import EventRow, SeatRow

NOW = datetime.now(UTC)
CONCURRENCY = 25


@pytest_asyncio.fixture(scope="module")
async def big_pool_engine(database_url: str, migrated_schema: None):
    # pool_size comfortably above CONCURRENCY -- one connection per
    # concurrent race attempt, plus headroom for this fixture's own
    # setup/teardown queries. Same reasoning as test_admin_dashboard.py's
    # identically-named fixture.
    eng = create_async_engine(database_url, pool_size=CONCURRENCY + 5, max_overflow=10)
    yield eng
    await eng.dispose()


@pytest.fixture
def session_factory(big_pool_engine: AsyncEngine):
    return async_sessionmaker(bind=big_pool_engine, expire_on_commit=False)


@pytest.fixture
def _demo_mode_on():
    original = infra_config.settings.demo_mode
    infra_config.settings.demo_mode = True
    yield
    infra_config.settings.demo_mode = original


@pytest.fixture
def _widen_naive_race_window():
    """Same fixture as test_naive_strategy.py's own -- widening the
    TOCTOU window turns naive's oversell from "might happen under this
    test's timing" into "will happen," which is what a deterministic
    assertion on `successful_holders` needs.
    """
    original = infra_config.settings.naive_race_window_ms
    infra_config.settings.naive_race_window_ms = 100
    yield
    infra_config.settings.naive_race_window_ms = original


async def _seed_event(session_factory, seat_count: int = 1) -> tuple[int, int]:
    async with session_factory() as session:
        event_id = (
            await session.execute(
                insert(EventRow).returning(EventRow.id),
                {
                    "name": "Demo Test",
                    "venue": "Test Venue",
                    "starts_at": NOW,
                    "total_seats": seat_count,
                },
            )
        ).scalar_one()
        seat_id = (
            await session.execute(
                insert(SeatRow).returning(SeatRow.id),
                {
                    "event_id": event_id,
                    "section": "A",
                    "row_label": "1",
                    "seat_number": 1,
                    "status": "AVAILABLE",
                    "version": 0,
                },
            )
        ).scalar_one()
        await session.commit()
    return event_id, seat_id


class TestDemoModeGate:
    """DEMO_MODE=false (the default) must 404 every demo route -- not
    error, not succeed, 404, as if the route did not exist.
    """

    async def test_race_404s_when_demo_mode_off(self, session_factory):
        event_id, seat_id = await _seed_event(session_factory)
        async with session_factory() as session:
            with pytest.raises(HTTPException) as exc_info:
                await run_race(
                    RaceRequest(
                        event_id=event_id, seat_id=seat_id, concurrency=2, strategy="pessimistic"
                    ),
                    session,
                )
        assert exc_info.value.status_code == 404

    async def test_reset_404s_when_demo_mode_off(self, session_factory):
        event_id, _ = await _seed_event(session_factory)
        async with session_factory() as session:
            with pytest.raises(HTTPException) as exc_info:
                await reset_event(ResetRequest(event_id=event_id), session)
        assert exc_info.value.status_code == 404

    async def test_state_404s_when_demo_mode_off(self, session_factory):
        event_id, _ = await _seed_event(session_factory)
        async with session_factory() as session:
            with pytest.raises(HTTPException) as exc_info:
                await get_demo_state(event_id, session)
        assert exc_info.value.status_code == 404

    async def test_hold_404s_when_demo_mode_off(self, session_factory):
        event_id, seat_id = await _seed_event(session_factory)
        async with session_factory() as session:
            with pytest.raises(HTTPException) as exc_info:
                await create_demo_hold(
                    HoldRequest(event_id=event_id, seat_id=seat_id, session_id="s1"), session
                )
        assert exc_info.value.status_code == 404


class TestRaceHolderCounts:
    """The headline claim of section 1: naive oversells under real
    concurrency, pessimistic and optimistic never do -- CONCURRENCY
    simultaneous attempts against the SAME seat, released by one
    asyncio.Barrier inside run_race itself (not orchestrated by this
    test), matching how a real browser click fires exactly one request.
    """

    async def test_naive_produces_more_than_one_holder(
        self, session_factory, _demo_mode_on, _widen_naive_race_window
    ):
        event_id, seat_id = await _seed_event(session_factory)
        async with session_factory() as session:
            response = await run_race(
                RaceRequest(
                    event_id=event_id, seat_id=seat_id, concurrency=CONCURRENCY, strategy="naive"
                ),
                session,
            )
        assert response.successful_holders > 1, (
            "naive should oversell under a widened race window -- got exactly "
            f"{response.successful_holders} holder(s)"
        )
        assert response.excess_holders == response.successful_holders - 1

    async def test_pessimistic_produces_exactly_one_holder(self, session_factory, _demo_mode_on):
        event_id, seat_id = await _seed_event(session_factory)
        async with session_factory() as session:
            response = await run_race(
                RaceRequest(
                    event_id=event_id,
                    seat_id=seat_id,
                    concurrency=CONCURRENCY,
                    strategy="pessimistic",
                ),
                session,
            )
        assert response.successful_holders == 1
        assert response.excess_holders == 0
        assert response.invariants.results["conservation"].passed

    async def test_optimistic_produces_exactly_one_holder(self, session_factory, _demo_mode_on):
        event_id, seat_id = await _seed_event(session_factory)
        async with session_factory() as session:
            response = await run_race(
                RaceRequest(
                    event_id=event_id,
                    seat_id=seat_id,
                    concurrency=CONCURRENCY,
                    strategy="optimistic",
                ),
                session,
            )
        assert response.successful_holders == 1
        assert response.excess_holders == 0
        assert response.invariants.results["conservation"].passed

    async def test_invariant_summary_discloses_the_gap(self, session_factory, _demo_mode_on):
        """The refinement this whole endpoint exists to get right: never
        claim "all five," always disclose exactly which four are live and
        point at where the other three are actually verified.
        """
        event_id, seat_id = await _seed_event(session_factory)
        async with session_factory() as session:
            response = await run_race(
                RaceRequest(
                    event_id=event_id,
                    seat_id=seat_id,
                    concurrency=CONCURRENCY,
                    strategy="pessimistic",
                ),
                session,
            )
        assert response.invariants.checked_count == 4
        assert response.invariants.total_count == 5
        assert set(response.invariants.unchecked) == {"I3", "I4", "I5"}
        assert "docs/chaos-results.md" in response.invariants.unchecked_note


class TestResetAndState:
    async def test_reset_restores_all_available(self, session_factory, _demo_mode_on):
        event_id, seat_id = await _seed_event(session_factory)
        async with session_factory() as session:
            await run_race(
                RaceRequest(
                    event_id=event_id, seat_id=seat_id, concurrency=2, strategy="pessimistic"
                ),
                session,
            )
        async with session_factory() as session:
            reset_response = await reset_event(ResetRequest(event_id=event_id), session)
        assert reset_response.seats_reset == 1

        async with session_factory() as session:
            state = await get_demo_state(event_id, session)
        assert state.seats[0].status == "AVAILABLE"
        assert state.seats[0].bookable is True

    async def test_state_reports_bookable_true_for_lazily_expired_held_seat(
        self, session_factory, _demo_mode_on
    ):
        """The side-by-side field this endpoint exists to expose: a seat
        whose `status` column still says HELD, past its own
        hold_expires_at, must report `bookable=True` -- the same lazy-
        expiry check every real acquisition path already uses, not a
        second, possibly-diverging one written for this endpoint.
        """
        event_id, seat_id = await _seed_event(session_factory)
        async with session_factory() as session:
            await session.execute(
                update(SeatRow)
                .where(SeatRow.id == seat_id)
                .values(
                    status="HELD",
                    held_by_session_id="stale-session",
                    hold_expires_at=NOW - timedelta(seconds=1),
                )
            )
            await session.commit()

        async with session_factory() as session:
            state = await get_demo_state(event_id, session)
        seat = state.seats[0]
        assert seat.status == "HELD"
        assert seat.bookable is True
