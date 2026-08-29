"""Proves Strategy B's (pessimistic locking's) actual correctness
properties against real Postgres -- not just that it "looks right."

(a)/(b) deliberately do NOT call PessimisticStrategy.acquire(): that
method always locks via a single ORDER BY id'd statement, so it cannot
deadlock by construction, which means calling it could never demonstrate
the failure mode ORDER BY id prevents. Instead, two manual coroutines each
issue two *separate* single-row SELECT ... FOR UPDATE statements,
synchronised with asyncio.Barrier so both acquire their first lock before
either requests the second -- that is what actually creates a circular
wait. (a) locks in opposing order and proves Postgres detects and aborts
one side (40P01). (b) locks in the SAME order and proves no deadlock
occurs -- the fix, demonstrated against (a).

Uses a dedicated, larger-pool engine (`big_pool_engine`) rather than
conftest.py's shared `engine`: SQLAlchemy's pool defaults (5 + 10
overflow = 15) are well under the 50 genuinely-concurrent connections
several tests below need open at once.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from sqlalchemy import insert, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from app.domain.invariants import check_conservation, check_no_double_booking, check_state_coherence
from app.infra.mappers import seat_to_domain
from app.infra.tables import EventRow, SeatRow
from app.inventory.strategies.base import StrategyUnavailable
from app.inventory.strategies.pessimistic import PessimisticStrategy

NOW = datetime(2026, 6, 1, tzinfo=UTC)
HOLD_DURATION = timedelta(minutes=8)


@pytest_asyncio.fixture(scope="module")
async def big_pool_engine(database_url: str, migrated_schema: None):
    """A separate engine with enough pool capacity for the heaviest tests
    below (50 genuinely concurrent connections) -- conftest.py's shared
    `engine` fixture uses SQLAlchemy's default pool (15 max), which is
    plenty for other test files but not for this one.

    Depends on `migrated_schema` explicitly (even though it never uses the
    value) -- this engine doesn't go through conftest.py's `engine`
    fixture, so without this dependency nothing would trigger the
    migration and every table would be missing.
    """
    eng = create_async_engine(database_url, pool_size=60, max_overflow=10)
    yield eng
    await eng.dispose()


@pytest.fixture
def session_factory(big_pool_engine: AsyncEngine):
    return async_sessionmaker(bind=big_pool_engine, expire_on_commit=False)


async def _seed_event_with_seats(session_factory, total_seats: int) -> tuple[int, list[int]]:
    async with session_factory() as session:
        event_id = (
            await session.execute(
                insert(EventRow).returning(EventRow.id),
                {
                    "name": "Pessimistic Test",
                    "venue": "Test Venue",
                    "starts_at": NOW,
                    "total_seats": total_seats,
                },
            )
        ).scalar_one()
        await session.execute(
            insert(SeatRow),
            [
                {
                    "event_id": event_id,
                    "section": "A",
                    "row_label": "1",
                    "seat_number": i + 1,
                    "status": "AVAILABLE",
                    "version": 0,
                }
                for i in range(total_seats)
            ],
        )
        await session.commit()

    async with session_factory() as session:
        seat_ids = (
            (
                await session.execute(
                    select(SeatRow.id).where(SeatRow.event_id == event_id).order_by(SeatRow.id)
                )
            )
            .scalars()
            .all()
        )
    return event_id, list(seat_ids)


async def _assert_invariants(session_factory, event_id: int, total_seats: int) -> None:
    """I1/I2/state-coherence must hold after every test in this module."""
    async with session_factory() as session:
        rows = (
            (await session.execute(select(SeatRow).where(SeatRow.event_id == event_id)))
            .scalars()
            .all()
        )
    seats = [seat_to_domain(row) for row in rows]
    check_conservation(seats, total_seats)
    check_no_double_booking(seats)
    check_state_coherence(seats)


class TestDeadlockOrdering:
    """Proves ORDER BY id is necessary, not merely assumed (SPEC.md section
    4's "classic interview question"): reproduce the deadlock without it,
    then show it vanish with it.
    """

    async def test_opposing_lock_order_deadlocks(self, session_factory):
        event_id, seat_ids = await _seed_event_with_seats(session_factory, 10)
        seat_a, seat_b = seat_ids[4], seat_ids[8]
        barrier = asyncio.Barrier(2)

        async def lock_in_order(first_id: int, second_id: int):
            async with session_factory() as session:
                await session.execute(
                    text("SELECT id FROM seats WHERE id = :id FOR UPDATE"), {"id": first_id}
                )
                await barrier.wait()
                await session.execute(
                    text("SELECT id FROM seats WHERE id = :id FOR UPDATE"), {"id": second_id}
                )
                await session.commit()

        results = await asyncio.gather(
            lock_in_order(seat_a, seat_b),
            lock_in_order(seat_b, seat_a),
            return_exceptions=True,
        )

        errors = [r for r in results if isinstance(r, BaseException)]
        successes = [r for r in results if not isinstance(r, BaseException)]
        assert len(errors) == 1, f"expected exactly one deadlock victim, got: {results}"
        assert len(successes) == 1
        assert isinstance(errors[0], DBAPIError)
        # Matched on SQLSTATE, not isinstance against the original asyncpg
        # exception class -- confirmed by direct inspection that
        # SQLAlchemy's asyncpg dialect re-wraps PostgresError descendants
        # in its own generic Error class and does not preserve the
        # specific asyncpg subclass on .orig. See
        # PessimisticStrategy._raise_translated's docstring for the same
        # finding, applied to production code.
        assert getattr(errors[0].orig, "sqlstate", None) == "40P01"

        await _assert_invariants(session_factory, event_id, 10)

    async def test_same_lock_order_never_deadlocks(self, session_factory):
        event_id, seat_ids = await _seed_event_with_seats(session_factory, 10)
        seat_a, seat_b = seat_ids[4], seat_ids[8]

        async def lock_in_order(first_id: int, second_id: int):
            async with session_factory() as session:
                await session.execute(
                    text("SELECT id FROM seats WHERE id = :id FOR UPDATE"), {"id": first_id}
                )
                await session.execute(
                    text("SELECT id FROM seats WHERE id = :id FOR UPDATE"), {"id": second_id}
                )
                await session.commit()

        # Both lock in the SAME (ascending) order -- deliberately NO
        # barrier here, unlike the opposing-order test above: forcing both
        # to have their first lock before either attempts the second would
        # itself deadlock the TEST (not Postgres) whenever both race for
        # seat_a first -- whichever loses that race blocks on the row lock
        # before it ever reaches a barrier.wait(), so the winner would sit
        # at the barrier forever waiting for a party that can't arrive
        # until the winner commits and lets it go. asyncio.gather alone
        # still runs both concurrently; one simply queues behind the other
        # for seat_a, and no circular wait is possible with a shared order.
        results = await asyncio.gather(
            lock_in_order(seat_a, seat_b),
            lock_in_order(seat_a, seat_b),
            return_exceptions=True,
        )

        errors = [r for r in results if isinstance(r, BaseException)]
        assert errors == [], f"expected no errors, got: {errors}"

        await _assert_invariants(session_factory, event_id, 10)

        await _assert_invariants(session_factory, event_id, 10)


class TestConcurrentSingleSeat:
    """(c)/(d): under real contention for one seat, exactly one contender
    wins -- and that holds up over repeated runs, not just one lucky one.
    """

    async def _run_once(self, session_factory, *, label: str) -> None:
        event_id, seat_ids = await _seed_event_with_seats(session_factory, 1)
        seat_id = seat_ids[0]
        strategy = PessimisticStrategy()
        barrier = asyncio.Barrier(50)

        async def attempt(holder: str):
            async with session_factory() as session:
                await barrier.wait()
                return await strategy.acquire(session, [seat_id], holder, HOLD_DURATION, NOW)

        results = await asyncio.gather(
            *[attempt(f"{label}-session-{i}") for i in range(50)], return_exceptions=True
        )

        exceptions = [r for r in results if isinstance(r, BaseException)]
        assert exceptions == [], f"{label}: unexpected exceptions: {exceptions}"

        successes = [r for r in results if r.success]
        failures = [r for r in results if not r.success]
        assert len(successes) == 1, f"{label}: expected exactly 1 success, got {len(successes)}"
        assert len(failures) == 49, f"{label}: expected exactly 49 failures, got {len(failures)}"
        # Every loser's failure must be the domain's seat-unavailable
        # rejection -- not a lock timeout, not a crash wearing a
        # success=False costume.
        assert all(f.reason is not None and "unavailable" in f.reason.lower() for f in failures)

        await _assert_invariants(session_factory, event_id, 1)

    async def test_fifty_concurrent_acquires_exactly_one_wins(self, session_factory):
        await self._run_once(session_factory, label="single-run")

    async def test_fifty_concurrent_acquires_repeated_20_times(self, session_factory):
        # Races are probabilistic; one green run proves nothing.
        for iteration in range(20):
            await self._run_once(session_factory, label=f"iter-{iteration}")


class TestSkipLockedAnyN:
    """(e): SKIP LOCKED lets contenders spread across different rows
    instead of queueing on the same one -- and (new) never partially
    fulfils a request it can't fully satisfy.
    """

    async def test_ten_concurrent_requests_for_two_seats_get_distinct_seats(self, session_factory):
        event_id, seat_ids = await _seed_event_with_seats(session_factory, 20)
        strategy = PessimisticStrategy()
        barrier = asyncio.Barrier(10)

        async def attempt(holder: str):
            async with session_factory() as session:
                await barrier.wait()
                return await strategy.acquire_any_n(
                    session, event_id, 2, holder, HOLD_DURATION, NOW
                )

        results = await asyncio.gather(*[attempt(f"session-{i}") for i in range(10)])

        assert all(r.success for r in results), [r for r in results if not r.success]
        all_acquired = [seat_id for r in results for seat_id in r.acquired]
        assert len(all_acquired) == 20
        assert len(set(all_acquired)) == 20, "some seats were double-acquired"

        await _assert_invariants(session_factory, event_id, 20)

    async def test_never_partially_fulfils_an_under_supplied_request(self, session_factory):
        event_id, seat_ids = await _seed_event_with_seats(session_factory, 20)

        # Lock 17 of 20 from another, uncommitted session -- only 3 remain
        # lockable, fewer than the 4 about to be requested.
        blocker_session = session_factory()
        await blocker_session.execute(
            text("SELECT id FROM seats WHERE id = ANY(:ids) FOR UPDATE"),
            {"ids": seat_ids[:17]},
        )
        try:
            strategy = PessimisticStrategy()
            async with session_factory() as session:
                result = await strategy.acquire_any_n(
                    session, event_id, 4, "holder", HOLD_DURATION, NOW
                )

            assert result.success is False
            assert result.acquired == [], (
                "must acquire ZERO seats on partial availability, not some"
            )

            async with session_factory() as verify_session:
                held = (
                    (
                        await verify_session.execute(
                            select(SeatRow.id).where(
                                SeatRow.event_id == event_id, SeatRow.status == "HELD"
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
            assert held == [], "no seat should have been transitioned to HELD"
        finally:
            await blocker_session.rollback()
            await blocker_session.close()

        await _assert_invariants(session_factory, event_id, 20)


class TestModeADoesNotSkipLockedSeats:
    """Makes the mode (a) vs (b) distinction a tested property, not just a
    comment: mode (a) must block/time out on a specifically-requested seat
    that's locked elsewhere, never silently return only the other one.
    """

    async def test_blocks_rather_than_silently_returning_only_the_free_seat(self, session_factory):
        event_id, seat_ids = await _seed_event_with_seats(session_factory, 2)
        seat_14c, seat_14d = seat_ids

        blocker_session = session_factory()
        await blocker_session.execute(
            text("SELECT id FROM seats WHERE id = :id FOR UPDATE"), {"id": seat_14c}
        )
        try:
            strategy = PessimisticStrategy(lock_timeout_ms=200)
            async with session_factory() as session:
                with pytest.raises(StrategyUnavailable):
                    await strategy.acquire(
                        session, [seat_14c, seat_14d], "holder", HOLD_DURATION, NOW
                    )
        finally:
            await blocker_session.rollback()
            await blocker_session.close()

        await _assert_invariants(session_factory, event_id, 2)


class TestLockTimeout:
    """(f): a blocked acquire fails cleanly -- StrategyUnavailable, not a
    hang and not an unhandled 500-shaped crash.
    """

    async def test_lock_timeout_fires_cleanly(self, session_factory):
        event_id, seat_ids = await _seed_event_with_seats(session_factory, 1)
        seat_id = seat_ids[0]

        blocker_session = session_factory()
        await blocker_session.execute(
            text("SELECT id FROM seats WHERE id = :id FOR UPDATE"), {"id": seat_id}
        )
        try:
            strategy = PessimisticStrategy(lock_timeout_ms=200)
            async with session_factory() as session:
                with pytest.raises(StrategyUnavailable):
                    await strategy.acquire(session, [seat_id], "blocked-holder", HOLD_DURATION, NOW)
        finally:
            await blocker_session.rollback()
            await blocker_session.close()

        await _assert_invariants(session_factory, event_id, 1)
