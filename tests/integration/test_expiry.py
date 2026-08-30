"""Phase 4: production-grade holds, expiry, and reconciliation.

Covers, against real Postgres (and real Redis where noted):
  (a) I3 -- no seat remains HELD past hold_expires_at beyond one sweeper
      interval.
  (b) Two concurrent sweepers over the same expired backlog: disjoint
      work via SKIP LOCKED, no invariant violation, IllegalTransition
      counted not raised.
  (c) Sweeper and booker contending for the same expiring seat: exactly
      one outcome, no lost seat, invariants hold.
  (d) Redis killed mid-hold: holds still work, reconciler repairs on the
      next pass, divergence counted.
  (e) Redis key present for a released/unheld seat: reconciler deletes
      it, counts it.
  (f) Extension boundary: before, exactly at, and after expiry.
  (g) Sweeper backlog gauge rises when the sweeper is stopped, falls
      when it resumes.

Plus the lazy-expiry test called for directly in the Phase 4 plan: with
the sweeper never having run at all, an expired hold must still be
reclaimable AND must read as available through the reporting path -- this
is what allows Settings.sweeper_interval_seconds to be seconds rather
than milliseconds (see workers/sweeper.py's module docstring).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from redis.exceptions import RedisError
from sqlalchemy import insert, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from app.api.routes import admin
from app.api.routes.booking import extend_hold_at
from app.domain import state_machine as state_machine_module
from app.domain.errors import IllegalTransition
from app.domain.invariants import check_conservation, check_no_double_booking, check_state_coherence
from app.infra import hold_cache
from app.infra.mappers import seat_to_domain
from app.infra.metrics import (
    hold_cache_errors_total,
    reconciliation_divergence_total,
    sweeper_backlog_gauge,
    sweeper_illegal_transition_total,
)
from app.infra.redis import get_redis
from app.infra.tables import EventRow, SeatRow
from app.inventory.strategies.pessimistic import PessimisticStrategy
from workers.reconciler import reconcile_once
from workers.sweeper import measure_backlog, sweep_once

NOW = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
PAST = NOW - timedelta(minutes=10)
FUTURE = NOW + timedelta(minutes=10)
HOLD_DURATION = timedelta(minutes=8)


@pytest_asyncio.fixture(scope="module")
async def big_pool_engine(database_url: str, migrated_schema: None):
    """See test_pessimistic.py's identical fixture for the rationale."""
    eng = create_async_engine(database_url, pool_size=30, max_overflow=10)
    yield eng
    await eng.dispose()


@pytest.fixture
def session_factory(big_pool_engine: AsyncEngine):
    return async_sessionmaker(bind=big_pool_engine, expire_on_commit=False)


@pytest_asyncio.fixture(autouse=True)
async def _clean_global_state(session_factory):
    """workers/sweeper.py's measure_backlog and workers/reconciler.py's
    reconcile_once are correctly GLOBAL in scope (production must sweep/
    reconcile every event, not just one) -- which means, unlike every
    other test file's event-scoped assertions, tests in THIS file are
    vulnerable to leftover HELD seats and Redis keys from earlier tests
    in the same session contaminating later ones (confirmed directly:
    without this fixture, a reconciler test saw 5 "missing" seats instead
    of the 1 it seeded, the other 4 left behind by earlier tests reusing
    the same low seat ids after RESTART IDENTITY). Runs before EVERY test
    in this module: truncate Postgres, and delete every seat:*:hold Redis
    key so a leftover key can't attach itself to a DIFFERENT seat that
    happens to get the same id after RESTART IDENTITY resets it.
    """
    async with session_factory() as session:
        await session.execute(
            text(
                "TRUNCATE TABLE hold_audit, booking_seats, bookings, seats, events "
                "RESTART IDENTITY CASCADE"
            )
        )
        await session.commit()
    redis_client = get_redis()
    async for key in redis_client.scan_iter(match="seat:*:hold"):
        await redis_client.delete(key)


async def _seed_seats(session_factory, seats: list[dict]) -> tuple[int, list[int]]:
    async with session_factory() as session:
        event_id = (
            await session.execute(
                insert(EventRow).returning(EventRow.id),
                {
                    "name": "Expiry Test",
                    "venue": "Test Venue",
                    "starts_at": NOW,
                    "total_seats": len(seats),
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
                    "status": s.get("status", "AVAILABLE"),
                    "version": s.get("version", 0),
                    "held_by_session_id": s.get("held_by_session_id"),
                    "hold_expires_at": s.get("hold_expires_at"),
                }
                for i, s in enumerate(seats)
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


async def _get_seat(session_factory, seat_id: int) -> SeatRow:
    async with session_factory() as session:
        return (await session.execute(select(SeatRow).where(SeatRow.id == seat_id))).scalar_one()


async def _assert_invariants(session_factory, event_id: int, total_seats: int) -> None:
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


def _counter_value(counter, **labels) -> float:
    """Current aggregate value of a Counter (labelled or not), read
    directly from its own in-process collect() -- these tests run in a
    single process, so this is simpler and sufficient (see
    test_metrics.py for the cross-process multiprocess-aggregation test).
    """
    for metric in counter.collect():
        for sample in metric.samples:
            if sample.name == f"{metric.name}_total" and sample.labels == labels:
                return sample.value
    return 0.0


def _gauge_value(gauge) -> float:
    for metric in gauge.collect():
        for sample in metric.samples:
            if sample.name == metric.name:
                return sample.value
    return 0.0


class TestLazyExpiryWithoutSweeper:
    """The mechanism, not the cleanup: with the sweeper never having run
    at all, an expired hold must already be reclaimable (via the
    acquisition path) and already read as available (via the reporting
    path). This is what makes a multi-second sweeper interval safe.
    """

    async def test_expired_unswept_seat_is_reclaimable_via_acquire_any_n(self, session_factory):
        event_id, seat_ids = await _seed_seats(
            session_factory,
            [{"status": "HELD", "held_by_session_id": "stale-session", "hold_expires_at": PAST}],
        )
        strategy = PessimisticStrategy()

        async with session_factory() as session:
            result = await strategy.acquire_any_n(
                session, event_id, 1, "new-session", HOLD_DURATION, NOW
            )

        assert result.success is True, result.reason
        assert result.acquired == seat_ids

        row = await _get_seat(session_factory, seat_ids[0])
        assert row.status == "HELD"
        assert row.held_by_session_id == "new-session"

        await _assert_invariants(session_factory, event_id, 1)

    async def test_expired_unswept_seat_reads_as_available_in_seat_status_counts(
        self, session_factory
    ):
        event_id, seat_ids = await _seed_seats(
            session_factory,
            [
                {"status": "HELD", "held_by_session_id": "stale-session", "hold_expires_at": PAST},
                {"status": "AVAILABLE"},
            ],
        )

        async with session_factory() as session:
            counts = await admin.get_seat_status_counts(event_id, session)

        # Both seats read as available -- the genuinely-available one, and
        # the expired-but-never-swept one -- with none reported as HELD.
        assert counts.available == 2
        assert counts.held == 0
        assert counts.booked == 0

        await _assert_invariants(session_factory, event_id, 2)


class TestExtensionBoundary:
    """(f): before, exactly at, and after expiry. extend_hold_at takes
    `now` explicitly (see its docstring) specifically so this boundary
    can be constructed deterministically instead of racing a real clock.
    """

    async def test_extend_before_expiry_succeeds(self, session_factory):
        event_id, seat_ids = await _seed_seats(
            session_factory,
            [{"status": "HELD", "held_by_session_id": "s1", "hold_expires_at": FUTURE}],
        )
        seat_id = seat_ids[0]
        new_hold_expires_at = NOW + timedelta(minutes=20)

        async with session_factory() as session:
            succeeded = await extend_hold_at(session, seat_id, "s1", NOW, new_hold_expires_at)

        assert succeeded is True
        row = await _get_seat(session_factory, seat_id)
        assert row.hold_expires_at == new_hold_expires_at
        assert row.status == "HELD"

        await _assert_invariants(session_factory, event_id, 1)

    async def test_extend_exactly_at_expiry_fails(self, session_factory):
        """is_hold_expired uses `<=` -- a hold expiring at EXACTLY `now`
        is already considered expired by the domain layer, so extension
        must fail here too, not succeed. hold_expires_at == NOW,
        extend requested at exactly NOW.
        """
        event_id, seat_ids = await _seed_seats(
            session_factory,
            [{"status": "HELD", "held_by_session_id": "s1", "hold_expires_at": NOW}],
        )
        seat_id = seat_ids[0]

        async with session_factory() as session:
            succeeded = await extend_hold_at(
                session, seat_id, "s1", NOW, NOW + timedelta(minutes=20)
            )

        assert succeeded is False
        row = await _get_seat(session_factory, seat_id)
        assert row.hold_expires_at == NOW, "a failed extension must not have changed anything"

        await _assert_invariants(session_factory, event_id, 1)

    async def test_extend_after_expiry_fails(self, session_factory):
        event_id, seat_ids = await _seed_seats(
            session_factory,
            [{"status": "HELD", "held_by_session_id": "s1", "hold_expires_at": PAST}],
        )
        seat_id = seat_ids[0]

        async with session_factory() as session:
            succeeded = await extend_hold_at(
                session, seat_id, "s1", NOW, NOW + timedelta(minutes=20)
            )

        assert succeeded is False
        row = await _get_seat(session_factory, seat_id)
        assert row.hold_expires_at == PAST

        await _assert_invariants(session_factory, event_id, 1)

    async def test_extend_by_a_different_session_fails(self, session_factory):
        event_id, seat_ids = await _seed_seats(
            session_factory,
            [{"status": "HELD", "held_by_session_id": "owner", "hold_expires_at": FUTURE}],
        )
        seat_id = seat_ids[0]

        async with session_factory() as session:
            succeeded = await extend_hold_at(
                session, seat_id, "someone-else", NOW, NOW + timedelta(minutes=20)
            )

        assert succeeded is False
        row = await _get_seat(session_factory, seat_id)
        assert row.held_by_session_id == "owner"

        await _assert_invariants(session_factory, event_id, 1)

    async def test_extend_refreshes_redis_ttl_to_the_new_expiry_not_the_original_duration(
        self, session_factory
    ):
        """Ruling: TTL must be derived from hold_expires_at, not from
        Settings.hold_duration_seconds -- after an extension those
        differ. Uses a new expiry (2 hours out) that is wildly different
        from the product default hold duration (8 minutes, 480s) so a
        bug reintroducing duration-based TTL would be unmistakable
        rather than coincidentally close.
        """
        event_id, seat_ids = await _seed_seats(
            session_factory,
            [{"status": "HELD", "held_by_session_id": "s1", "hold_expires_at": FUTURE}],
        )
        seat_id = seat_ids[0]
        new_hold_expires_at = NOW + timedelta(hours=2)

        async with session_factory() as session:
            succeeded = await extend_hold_at(session, seat_id, "s1", NOW, new_hold_expires_at)
        assert succeeded is True

        ttl = await get_redis().ttl(f"seat:{seat_id}:hold")
        expected_ttl = (new_hold_expires_at - NOW).total_seconds()
        assert ttl == pytest.approx(expected_ttl, abs=5), (
            f"TTL {ttl}s should match the new 2-hour expiry ({expected_ttl:.0f}s), "
            "not the ~480s product default hold_duration"
        )

        await _assert_invariants(session_factory, event_id, 1)


class TestInvariantI3AfterSweeping:
    """(a): no seat remains HELD past hold_expires_at beyond one sweeper
    interval -- run the sweeper once, assert every expired seat is gone.
    """

    async def test_a_full_backlog_is_cleared_within_one_pass(self, session_factory):
        event_id, seat_ids = await _seed_seats(
            session_factory,
            [
                {"status": "HELD", "held_by_session_id": f"s{i}", "hold_expires_at": PAST}
                for i in range(10)
            ],
        )

        async with session_factory() as session:
            result = await sweep_once(session, batch_size=100, now=NOW)
        assert result.seats_expired == 10

        for seat_id in seat_ids:
            row = await _get_seat(session_factory, seat_id)
            assert row.status == "AVAILABLE"
            assert row.hold_expires_at is None

        await _assert_invariants(session_factory, event_id, 10)


class TestConcurrentSweepers:
    """(b): two concurrent sweepers over the same expired backlog do
    disjoint work (SKIP LOCKED), no invariant violation, and any
    IllegalTransition is counted, not raised.
    """

    async def test_two_concurrent_sweepers_split_the_backlog_disjointly(self, session_factory):
        event_id, seat_ids = await _seed_seats(
            session_factory,
            [
                {"status": "HELD", "held_by_session_id": f"s{i}", "hold_expires_at": PAST}
                for i in range(20)
            ],
        )

        async def run_sweep():
            async with session_factory() as session:
                return await sweep_once(session, batch_size=10, now=NOW)

        results = await asyncio.gather(run_sweep(), run_sweep(), return_exceptions=True)

        exceptions = [r for r in results if isinstance(r, BaseException)]
        assert exceptions == [], f"unexpected exceptions: {exceptions}"

        total_expired = sum(r.seats_expired for r in results)
        assert total_expired == 20, "SKIP LOCKED must make the two passes disjoint -- nothing "
        "double-processed, nothing missed"

        await _assert_invariants(session_factory, event_id, 20)

    async def test_illegal_transition_is_caught_counted_and_does_not_abort_the_batch(
        self, session_factory, monkeypatch
    ):
        """Deterministic, not relying on SKIP LOCKED's own timing to
        naturally produce a race: simulates exactly one candidate row
        raising IllegalTransition, proving sweep_once's catch/log/count
        path actually works, and that it does not abort the rest of the
        batch -- the same "inject a deterministic failure rather than
        hope a real race happens to occur" approach as
        test_optimistic.py's simulated-deadlock test.
        """
        event_id, seat_ids = await _seed_seats(
            session_factory,
            [
                {"status": "HELD", "held_by_session_id": f"s{i}", "hold_expires_at": PAST}
                for i in range(3)
            ],
        )

        real_expire = state_machine_module.expire
        call_count = 0

        def flaky_expire(seat, now):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise IllegalTransition(seat.id, seat.status, seat.status)
            return real_expire(seat, now)

        monkeypatch.setattr("workers.sweeper.state_machine.expire", flaky_expire)

        illegal_before = _counter_value(sweeper_illegal_transition_total)

        async with session_factory() as session:
            result = await sweep_once(session, batch_size=100, now=NOW)

        assert call_count == 3, "all 3 candidates must still be attempted despite the first failing"
        assert result.seats_expired == 2, "the 2 that did not raise must still be expired"
        assert _counter_value(sweeper_illegal_transition_total) - illegal_before == 1

        await _assert_invariants(session_factory, event_id, 3)


class TestSweeperVsBookerRace:
    """(c): sweeper and booker contending for the same expiring seat --
    exactly one outcome, no lost seat, invariants hold. Pessimistic mode
    (a) never needs the sweeper to have already run (it reads by id and
    lets state_machine.hold() decide, same as every other lazy-expiry
    path) -- whichever side reaches the row first, the seat ends up
    correctly reassigned to the booker, never stuck with the old holder
    and never double-held.
    """

    async def test_exactly_one_outcome_seat_ends_up_with_the_new_booker(self, session_factory):
        event_id, seat_ids = await _seed_seats(
            session_factory,
            [{"status": "HELD", "held_by_session_id": "old-session", "hold_expires_at": PAST}],
        )
        seat_id = seat_ids[0]
        strategy = PessimisticStrategy()
        barrier = asyncio.Barrier(2)

        async def run_sweep():
            async with session_factory() as session:
                await barrier.wait()
                return await sweep_once(session, batch_size=10, now=NOW)

        async def run_booker():
            async with session_factory() as session:
                await barrier.wait()
                return await strategy.acquire(session, [seat_id], "new-session", HOLD_DURATION, NOW)

        results = await asyncio.gather(run_sweep(), run_booker(), return_exceptions=True)
        exceptions = [r for r in results if isinstance(r, BaseException)]
        assert exceptions == [], f"unexpected exceptions: {exceptions}"

        row = await _get_seat(session_factory, seat_id)
        assert row.status == "HELD"
        assert row.held_by_session_id == "new-session"

        await _assert_invariants(session_factory, event_id, 1)


class TestRedisFailureDegradesGracefully:
    """(d), first half: Redis killed mid-hold -- the hold itself must
    still succeed in Postgres, and the failure must be observable
    (hold_cache_errors_total), not silent and not propagated as an
    error. The reconciler side of (d) is covered by TestReconciler below.
    """

    async def test_hold_succeeds_in_postgres_even_when_the_redis_mirror_write_fails(
        self, session_factory, monkeypatch
    ):
        event_id, seat_ids = await _seed_seats(session_factory, [{"status": "AVAILABLE"}])
        seat_id = seat_ids[0]
        strategy = PessimisticStrategy()

        async def _raise(*args, **kwargs):
            raise RedisError("simulated redis outage")

        monkeypatch.setattr(get_redis(), "set", _raise)

        errors_before = _counter_value(hold_cache_errors_total, operation="set")

        async with session_factory() as session:
            result = await strategy.acquire(session, [seat_id], "s1", HOLD_DURATION, NOW)
        assert result.success is True, result.reason

        hold_expires_at = NOW + HOLD_DURATION
        # This is what create_hold (app/api/routes/booking.py) calls on
        # a successful acquire -- exercised directly here since this test
        # goes through the strategy, not the HTTP route.
        await hold_cache.set_hold_mirror(seat_id, "s1", hold_expires_at, NOW)

        assert _counter_value(hold_cache_errors_total, operation="set") - errors_before == 1

        row = await _get_seat(session_factory, seat_id)
        assert row.status == "HELD"
        assert row.held_by_session_id == "s1"

        await _assert_invariants(session_factory, event_id, 1)


class TestReconciler:
    """(d) second half, (e), and the two ruling additions: each
    divergence kind gets its own dedicated test, plus one pass
    constructing all three at once to prove they're counted distinctly,
    not collapsed.
    """

    async def test_redis_key_missing_for_held_seat_is_repaired(self, session_factory):
        event_id, seat_ids = await _seed_seats(
            session_factory,
            [{"status": "HELD", "held_by_session_id": "s1", "hold_expires_at": FUTURE}],
        )
        seat_id = seat_ids[0]
        # No Redis key exists for this seat at all -- simulates a mirror
        # SET that failed (TestRedisFailureDegradesGracefully) once
        # Redis is back.
        await get_redis().delete(f"seat:{seat_id}:hold")

        divergence_before = _counter_value(
            reconciliation_divergence_total, kind="redis_key_missing_for_held_seat"
        )

        async with session_factory() as session:
            result = await reconcile_once(session, NOW)

        assert result.redis_key_missing_for_held_seat == 1
        assert result.redis_key_present_for_unheld_seat == 0
        assert result.redis_session_mismatch == 0
        assert (
            _counter_value(reconciliation_divergence_total, kind="redis_key_missing_for_held_seat")
            - divergence_before
            == 1
        )

        assert await get_redis().get(f"seat:{seat_id}:hold") == "s1"

        await _assert_invariants(session_factory, event_id, 1)

    async def test_redis_key_present_for_unheld_seat_is_repaired(self, session_factory):
        event_id, seat_ids = await _seed_seats(session_factory, [{"status": "AVAILABLE"}])
        seat_id = seat_ids[0]
        # A stale key for a seat Postgres does not consider HELD --
        # simulates the sweeper's post-commit Redis delete having failed.
        await get_redis().set(f"seat:{seat_id}:hold", "stale-session", ex=60)

        async with session_factory() as session:
            result = await reconcile_once(session, NOW)

        assert result.redis_key_present_for_unheld_seat == 1
        assert result.redis_key_missing_for_held_seat == 0
        assert result.redis_session_mismatch == 0
        assert await get_redis().get(f"seat:{seat_id}:hold") is None

        await _assert_invariants(session_factory, event_id, 1)

    async def test_redis_session_mismatch_is_repaired(self, session_factory):
        """The one divergence kind where Redis serves a WRONG answer, not
        a merely stale one: seat is genuinely HELD by session A in
        Postgres, but the mirror key names session B -- deliberately
        constructed exactly as the docstring describes it arising, a
        stale key surviving an expire-and-reacquire cycle.
        """
        event_id, seat_ids = await _seed_seats(
            session_factory,
            [{"status": "HELD", "held_by_session_id": "session-A", "hold_expires_at": FUTURE}],
        )
        seat_id = seat_ids[0]
        await get_redis().set(f"seat:{seat_id}:hold", "session-B", ex=60)

        async with session_factory() as session:
            result = await reconcile_once(session, NOW)

        assert result.redis_session_mismatch == 1
        assert result.redis_key_missing_for_held_seat == 0
        assert result.redis_key_present_for_unheld_seat == 0
        assert await get_redis().get(f"seat:{seat_id}:hold") == "session-A"

        await _assert_invariants(session_factory, event_id, 1)

    async def test_all_three_divergence_kinds_counted_distinctly_in_one_pass(self, session_factory):
        event_id, seat_ids = await _seed_seats(
            session_factory,
            [
                {"status": "HELD", "held_by_session_id": "s-missing", "hold_expires_at": FUTURE},
                {"status": "AVAILABLE"},
                {"status": "HELD", "held_by_session_id": "session-A", "hold_expires_at": FUTURE},
            ],
        )
        missing_seat, unheld_seat, mismatch_seat = seat_ids

        await get_redis().delete(f"seat:{missing_seat}:hold")
        await get_redis().set(f"seat:{unheld_seat}:hold", "stale-session", ex=60)
        await get_redis().set(f"seat:{mismatch_seat}:hold", "session-B", ex=60)

        before = {
            kind: _counter_value(reconciliation_divergence_total, kind=kind)
            for kind in (
                "redis_key_missing_for_held_seat",
                "redis_key_present_for_unheld_seat",
                "redis_session_mismatch",
            )
        }

        async with session_factory() as session:
            result = await reconcile_once(session, NOW)

        assert result.redis_key_missing_for_held_seat == 1
        assert result.redis_key_present_for_unheld_seat == 1
        assert result.redis_session_mismatch == 1
        for kind, before_value in before.items():
            assert _counter_value(reconciliation_divergence_total, kind=kind) - before_value == 1, (
                f"{kind} must be incremented exactly once, independently of the other two kinds"
            )

        await _assert_invariants(session_factory, event_id, 3)

    async def test_sweeper_redis_delete_failure_is_repaired_by_reconciler(
        self, session_factory, monkeypatch
    ):
        """Ruling: simulates the crash window in workers/sweeper.py's
        ordering comment directly -- the Postgres commit succeeds, but
        the Redis delete that should follow it is skipped (as if the
        process died in between). Asserts the seat is correctly AVAILABLE
        (and therefore bookable) in Postgres regardless, and that the
        stale key left behind is exactly what the reconciler cleans up
        on its next pass.
        """
        event_id, seat_ids = await _seed_seats(
            session_factory,
            [{"status": "HELD", "held_by_session_id": "s1", "hold_expires_at": PAST}],
        )
        seat_id = seat_ids[0]
        await get_redis().set(f"seat:{seat_id}:hold", "s1", ex=600)

        async def _skip_delete(*args, **kwargs):
            # Simulates the crash: does nothing, as if the process died
            # after the commit below but before this call.
            return None

        # workers.sweeper.hold_cache and workers.reconciler.hold_cache are
        # the SAME module object (app.infra.hold_cache) -- patching it
        # would also break the reconciler's own repair call below if left
        # active, which is not what this test is simulating. undo() it
        # immediately after the sweep, before the reconciler ever runs.
        monkeypatch.setattr("workers.sweeper.hold_cache.delete_hold_mirror", _skip_delete)
        async with session_factory() as session:
            result = await sweep_once(session, batch_size=10, now=NOW)
        monkeypatch.undo()
        assert result.seats_expired == 1

        # Postgres is correct regardless of the skipped Redis delete --
        # the seat is genuinely bookable now.
        row = await _get_seat(session_factory, seat_id)
        assert row.status == "AVAILABLE"

        # The stale key is exactly the crash-window artifact described --
        # confirm it's actually there before asking the reconciler to
        # clean it up, so this test would fail loudly if the simulation
        # didn't work as intended.
        assert await get_redis().get(f"seat:{seat_id}:hold") == "s1"

        async with session_factory() as session:
            reconcile_result = await reconcile_once(session, NOW)

        assert reconcile_result.redis_key_present_for_unheld_seat == 1
        assert await get_redis().get(f"seat:{seat_id}:hold") is None

        await _assert_invariants(session_factory, event_id, 1)


class TestBacklogGauge:
    """(g): the backlog gauge rises while the sweeper is stopped (nothing
    clearing it) and falls once it resumes -- measure_backlog is callable
    independently of sweep_once for exactly this reason (see its
    docstring).
    """

    async def test_rises_when_stopped_and_falls_when_swept(self, session_factory):
        event_id, seat_ids = await _seed_seats(
            session_factory,
            [
                {"status": "HELD", "held_by_session_id": f"s{i}", "hold_expires_at": PAST}
                for i in range(5)
            ],
        )

        # "Stopped": nothing sweeps, but the backlog can still be
        # observed rising by measuring it directly.
        async with session_factory() as session:
            backlog_while_stopped = await measure_backlog(session, NOW)
        assert backlog_while_stopped == 5
        assert _gauge_value(sweeper_backlog_gauge) == 5

        # "Resumes": an actual sweep pass clears it.
        async with session_factory() as session:
            await sweep_once(session, batch_size=100, now=NOW)

        async with session_factory() as session:
            backlog_after_sweep = await measure_backlog(session, NOW)
        assert backlog_after_sweep == 0
        assert _gauge_value(sweeper_backlog_gauge) == 0

        await _assert_invariants(session_factory, event_id, 5)
