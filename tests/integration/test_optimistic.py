"""Proves Strategy C's (optimistic locking's) actual correctness
properties against real Postgres -- not just that it "looks right."

Uses the same dedicated, larger-pool engine pattern as
tests/integration/test_pessimistic.py (`big_pool_engine`) -- several tests
below need dozens of genuinely concurrent connections open at once, well
beyond SQLAlchemy's pool defaults.

Metric assertions (c)/(d)/(e) read app/infra/metrics.py's module-level
Counter/Histogram objects directly via their own .collect() -- this is a
single OS process, unlike test_metrics.py (which specifically proves
cross-process multiprocess aggregation); reading the in-process object
is simpler and sufficient here. Each assertion uses a BEFORE/AFTER delta
around exactly one acquire() call so it is correct even though these
metrics are module-level globals shared across every test in this file.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from sqlalchemy import event, insert, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from app.domain.invariants import check_conservation, check_no_double_booking, check_state_coherence
from app.infra.mappers import seat_to_domain
from app.infra.metrics import (
    deadlocks_total,
    optimistic_attempts,
    optimistic_conflicts_total,
    optimistic_exhausted_total,
)
from app.infra.tables import EventRow, SeatRow
from app.inventory.strategies.optimistic import _CONDITIONAL_UPDATE_SQL, OptimisticStrategy

NOW = datetime(2026, 6, 1, tzinfo=UTC)
HOLD_DURATION = timedelta(minutes=8)


@pytest_asyncio.fixture(scope="module")
async def big_pool_engine(database_url: str, migrated_schema: None):
    """See test_pessimistic.py's identical fixture for the rationale --
    50+ genuinely concurrent connections needed for several tests below.
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
                    "name": "Optimistic Test",
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


def _counter_value(counter) -> float:
    """Current aggregate value of a label-less Counter, read directly from
    its own in-process collect() -- see module docstring.
    """
    for metric in counter.collect():
        for sample in metric.samples:
            if sample.name == f"{metric.name}_total":
                return sample.value
    return 0.0


def _histogram_sum(histogram) -> float:
    """Current aggregate _sum of a label-less Histogram -- see
    module docstring. Since Histogram.observe(v) adds exactly v to _sum
    and 1 to _count, a delta across exactly one observation IS that
    observation's value -- the cleanest way to assert "attempts > 1 was
    recorded" for one specific call amid a shared, module-level metric.
    """
    for metric in histogram.collect():
        for sample in metric.samples:
            if sample.name == f"{metric.name}_sum":
                return sample.value
    return 0.0


class TestConcurrentSingleSeat:
    """(a): under real contention for one seat with no lock taken at all,
    exactly one contender's optimistic UPDATE ever commits -- and that
    holds up over repeated runs, not just one lucky one.
    """

    async def _run_once(self, session_factory, *, label: str) -> None:
        event_id, seat_ids = await _seed_event_with_seats(session_factory, 1)
        seat_id = seat_ids[0]
        strategy = OptimisticStrategy()
        barrier = asyncio.Barrier(50)

        async def attempt(holder: str):
            # Barrier BEFORE anything that can block -- opening a session
            # here is lazy (no connection is checked out until the first
            # execute()), so nothing before barrier.wait() can itself
            # block. Same bug class as Phase 2's
            # test_same_lock_order_never_deadlocks: put the barrier after
            # a blocking call and whichever party loses that race never
            # reaches the barrier, hanging every other party forever.
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

        await _assert_invariants(session_factory, event_id, 1)

    async def test_fifty_concurrent_acquires_exactly_one_wins(self, session_factory):
        await self._run_once(session_factory, label="single-run")

    async def test_fifty_concurrent_acquires_repeated_20_times(self, session_factory):
        # Races are probabilistic; one green run proves nothing.
        for iteration in range(20):
            await self._run_once(session_factory, label=f"iter-{iteration}")


class TestConflictDetection:
    """(b): the WHERE clause matching zero rows on a stale version IS the
    conflict detection mechanism -- proven directly against the
    strategy's own conditional-UPDATE method, not asserted from theory.
    """

    async def test_stale_version_update_matches_zero_rows(self, session_factory):
        event_id, seat_ids = await _seed_event_with_seats(session_factory, 1)
        seat_id = seat_ids[0]
        strategy = OptimisticStrategy()

        async with session_factory() as reader:
            seat_row = (
                await reader.execute(select(SeatRow).where(SeatRow.id == seat_id))
            ).scalar_one()
            stale_version = seat_row.version  # 0, captured before the bump below

        # "Another session" bumps the version -- still AVAILABLE, just a
        # newer version -- and commits, simulating any concurrent writer.
        async with session_factory() as bumper:
            await bumper.execute(
                text("UPDATE seats SET version = version + 1 WHERE id = :id"), {"id": seat_id}
            )
            await bumper.commit()

        # Attempt the real conditional UPDATE using the now-stale version.
        async with session_factory() as session:
            rowcount = await strategy._attempt_update(
                session, [seat_id], {seat_id: stale_version}, "holder", HOLD_DURATION, NOW
            )
            await session.rollback()

        assert rowcount == 0, "a stale expected version must match zero rows, not update anyway"

        await _assert_invariants(session_factory, event_id, 1)


class TestRetrySucceedsAfterConflict:
    """(c): a conflict does not mean failure -- a fresh read on retry can
    still succeed, and the optimistic_attempts histogram must show it
    took more than one attempt. Verified via the actual metric, not by
    reading the code.
    """

    async def test_retry_succeeds_after_a_single_conflict(self, session_factory):
        event_id, seat_ids = await _seed_event_with_seats(session_factory, 1)
        seat_id = seat_ids[0]
        strategy = OptimisticStrategy(max_attempts=5)

        bumped = False

        async def bump_version_once() -> None:
            # Fires once per attempt, right before that attempt's UPDATE
            # (see optimistic.py's _test_hook_after_read). Bumping only on
            # the FIRST call manufactures exactly one conflict: attempt 1
            # reads version V, we bump to V+1 out from under it before its
            # UPDATE runs (that UPDATE now matches zero rows), attempt 2
            # re-reads V+1 fresh and succeeds against it.
            nonlocal bumped
            if not bumped:
                bumped = True
                async with session_factory() as bumper:
                    await bumper.execute(
                        text("UPDATE seats SET version = version + 1 WHERE id = :id"),
                        {"id": seat_id},
                    )
                    await bumper.commit()

        attempts_sum_before = _histogram_sum(optimistic_attempts)

        async with session_factory() as session:
            result = await strategy.acquire(
                session,
                [seat_id],
                "holder",
                HOLD_DURATION,
                NOW,
                _test_hook_after_read=bump_version_once,
            )

        assert result.success is True, result.reason
        attempts_this_call = _histogram_sum(optimistic_attempts) - attempts_sum_before
        assert attempts_this_call > 1, (
            f"expected this call to need >1 attempts, histogram delta was {attempts_this_call}"
        )

        await _assert_invariants(session_factory, event_id, 1)


class TestRetryBudgetExhausts:
    """(d): sustained conflict exhausts the retry budget cleanly -- a
    domain failure and optimistic_exhausted_total incrementing, never an
    infinite loop and never a 500.
    """

    async def test_sustained_conflict_exhausts_cleanly(self, session_factory):
        event_id, seat_ids = await _seed_event_with_seats(session_factory, 1)
        seat_id = seat_ids[0]
        strategy = OptimisticStrategy(max_attempts=3, base_seconds=0.001)

        async def bump_version_always() -> None:
            # Fires before EVERY attempt's UPDATE -- guarantees every
            # single attempt's expected version is stale by the time its
            # UPDATE runs, so the whole budget is exhausted.
            async with session_factory() as bumper:
                await bumper.execute(
                    text("UPDATE seats SET version = version + 1 WHERE id = :id"), {"id": seat_id}
                )
                await bumper.commit()

        exhausted_before = _counter_value(optimistic_exhausted_total)

        async with session_factory() as session:
            result = await asyncio.wait_for(
                strategy.acquire(
                    session,
                    [seat_id],
                    "holder",
                    HOLD_DURATION,
                    NOW,
                    _test_hook_after_read=bump_version_always,
                ),
                timeout=10,
            )

        assert result.success is False
        assert result.reason is not None and "retry_budget_exhausted" in result.reason
        assert _counter_value(optimistic_exhausted_total) - exhausted_before == 1

        await _assert_invariants(session_factory, event_id, 1)


class TestStaleReadGuard:
    """(e): every retry issues a genuinely NEW select -- verified by
    counting actual SELECT statements sent to Postgres via SQLAlchemy's
    own before_cursor_execute event, not by reading the code.
    """

    async def test_each_retry_issues_a_new_select(self, session_factory, big_pool_engine):
        event_id, seat_ids = await _seed_event_with_seats(session_factory, 1)
        seat_id = seat_ids[0]
        strategy = OptimisticStrategy(max_attempts=3, base_seconds=0.001)

        select_count = 0

        def count_seat_selects(conn, cursor, statement, parameters, context, executemany):
            nonlocal select_count
            normalized = statement.strip().upper()
            if normalized.startswith("SELECT") and "SEATS" in normalized:
                select_count += 1

        event.listens_for(big_pool_engine.sync_engine, "before_cursor_execute")(count_seat_selects)
        try:

            async def bump_version_always() -> None:
                async with session_factory() as bumper:
                    await bumper.execute(
                        text("UPDATE seats SET version = version + 1 WHERE id = :id"),
                        {"id": seat_id},
                    )
                    await bumper.commit()

            async with session_factory() as session:
                result = await asyncio.wait_for(
                    strategy.acquire(
                        session,
                        [seat_id],
                        "holder",
                        HOLD_DURATION,
                        NOW,
                        _test_hook_after_read=bump_version_always,
                    ),
                    timeout=10,
                )
        finally:
            event.remove(big_pool_engine.sync_engine, "before_cursor_execute", count_seat_selects)

        assert result.success is False  # sustained conflict -> exhausted, by construction
        # One fresh SELECT per attempt (the strategy's own reads), plus
        # one per bump_version_always() call (the "seat_row" table is the
        # only one either query touches, so both are counted -- the
        # strategy issues exactly max_attempts of its own SELECTs
        # regardless of the bumper's additional ones).
        assert select_count >= strategy.max_attempts, (
            f"expected at least {strategy.max_attempts} SELECTs (one per attempt), got "
            f"{select_count}"
        )

        await _assert_invariants(session_factory, event_id, 1)


class TestDeadlockDuringConditionalUpdate:
    """Ruling 2: a multi-row UPDATE...FROM unnest() can still deadlock
    against another overlapping one in a different physical order (see
    optimistic.py's module docstring) -- proving the catch/retry path
    works is more valuable, and far more reliable, than trying to force a
    genuine Postgres deadlock through a query whose internal row-touch
    order this code does not control.
    """

    async def test_simulated_deadlock_is_retried_not_raised(self, session_factory):
        """Deterministic: injects a real DBAPIError shaped exactly like a
        40P01 deadlock on the FIRST call to the conditional UPDATE only,
        then lets every subsequent call run for real. Proves
        _attempt_update's except-and-retry path, rather than hoping
        Postgres's planner happens to deadlock two real transactions this
        run (it might not -- see the class docstring and the soak test
        below for the best-effort real-concurrency companion).
        """
        event_id, seat_ids = await _seed_event_with_seats(session_factory, 2)
        strategy = OptimisticStrategy(max_attempts=3, base_seconds=0.001)

        class _FakeDeadlockOrig(Exception):
            sqlstate = "40P01"

        call_count = 0
        deadlocks_before = _counter_value(deadlocks_total)
        conflicts_before = _counter_value(optimistic_conflicts_total)

        async with session_factory() as session:
            real_execute = session.execute

            async def flaky_execute(statement, *args, **kwargs):
                nonlocal call_count
                if statement is _CONDITIONAL_UPDATE_SQL:
                    call_count += 1
                    if call_count == 1:
                        raise DBAPIError(
                            "simulated deadlock", {}, _FakeDeadlockOrig(), hide_parameters=True
                        )
                return await real_execute(statement, *args, **kwargs)

            session.execute = flaky_execute  # type: ignore[method-assign]

            result = await strategy.acquire(session, seat_ids, "holder", HOLD_DURATION, NOW)

        assert result.success is True, result.reason
        assert call_count >= 2, "the simulated failure must have been followed by a real retry"
        assert _counter_value(deadlocks_total) - deadlocks_before == 1
        assert _counter_value(optimistic_conflicts_total) - conflicts_before >= 1

        await _assert_invariants(session_factory, event_id, 2)

    async def test_overlapping_concurrent_multi_seat_updates_never_crash_or_hang(
        self, session_factory
    ):
        """Best-effort, real-concurrency companion to the deterministic
        test above: two sessions repeatedly acquire the SAME two seats in
        OPPOSITE order. This may or may not actually trigger a real
        40P01 (Postgres's own plan decides physical row-touch order, not
        this test) -- the assertion that matters either way is that
        nothing ever raises, hangs, or oversells, across many attempts.

        A fresh event+seat pair is seeded each iteration (rather than
        resetting the same two seats back to AVAILABLE in between) so
        nothing here ever writes a seat's status outside
        app/domain/state_machine.py, including in test code -- CLAUDE.md
        rule 3 doesn't carve out an exception for tests.
        """
        for i in range(10):
            event_id, seat_ids = await _seed_event_with_seats(session_factory, 2)
            seat_a, seat_b = seat_ids
            strategy_1 = OptimisticStrategy(max_attempts=5, base_seconds=0.001)
            strategy_2 = OptimisticStrategy(max_attempts=5, base_seconds=0.001)
            barrier = asyncio.Barrier(2)

            async def attempt(strategy, ids, holder, barrier=barrier):
                async with session_factory() as session:
                    await barrier.wait()
                    return await strategy.acquire(session, ids, holder, HOLD_DURATION, NOW)

            results = await asyncio.wait_for(
                asyncio.gather(
                    attempt(strategy_1, [seat_a, seat_b], f"iter{i}-session-1"),
                    attempt(strategy_2, [seat_b, seat_a], f"iter{i}-session-2"),
                    return_exceptions=True,
                ),
                timeout=15,
            )

            exceptions = [r for r in results if isinstance(r, BaseException)]
            assert exceptions == [], f"iteration {i}: unexpected exceptions: {exceptions}"

            await _assert_invariants(session_factory, event_id, 2)


class TestFullJitterConfigurability:
    """The Phase 3 jitter ablation (loadtest/run_benchmark.py) depends on
    full_jitter=True/False actually producing different backoff behavior --
    verified here via instrumentation on random.uniform, not by reading
    the code. No database needed: _backoff is pure asyncio + random.
    """

    async def test_full_jitter_true_calls_random_uniform(self, monkeypatch):
        import app.inventory.strategies.optimistic as optimistic_module

        calls = []
        monkeypatch.setattr(
            optimistic_module.random, "uniform", lambda a, b: calls.append((a, b)) or 0.0
        )
        strategy = OptimisticStrategy(base_seconds=0.05, full_jitter=True)

        await strategy._backoff(attempt=2)

        assert calls == [(0, 0.05 * 2**1)]

    async def test_full_jitter_false_never_calls_random_uniform(self, monkeypatch):
        import app.inventory.strategies.optimistic as optimistic_module

        def fail_if_called(a, b):
            raise AssertionError("random.uniform must not be called when full_jitter=False")

        monkeypatch.setattr(optimistic_module.random, "uniform", fail_if_called)
        strategy = OptimisticStrategy(base_seconds=0.001, full_jitter=False)

        # Must complete (and complete FAST -- base_seconds=0.001) without
        # ever touching random.uniform; fixed backoff sleeps the exact
        # ceiling deterministically.
        await asyncio.wait_for(strategy._backoff(attempt=2), timeout=2)
