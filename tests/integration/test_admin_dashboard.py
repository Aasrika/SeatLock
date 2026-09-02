"""GET /api/admin/dashboard -- a typed, tested JSON shape, per review:
Prometheus exposition format is an interface to Prometheus, not to a
UI, and a frontend parsing it directly can't tell which multiprocess_
mode a given metric uses (see app/infra/metrics.py's own docstring --
ws_connections_gauge is "livesum", sweeper_backlog_gauge is
"mostrecent"; a bare line-parser has no way to know which is which).

Calls get_dashboard/get_invariants directly (this suite's established
convention -- see test_idempotency.py's own docstring for why: a
FastAPI route decorated with @router.get is still a plain callable).
Counter/Gauge deltas, not absolute values, throughout -- these are
module-level globals shared across the whole test session (same
reasoning as test_optimistic.py's own metric assertions).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from sqlalchemy import insert, update
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from app.api.routes.admin import _compute_invariants, get_dashboard, get_invariants
from app.infra.metrics import (
    deadlocks_total,
    optimistic_conflicts_total,
    reconciliation_divergence_total,
    sweeper_backlog_gauge,
)
from app.infra.tables import BookingRow, BookingSeatRow, EventRow, SeatRow

NOW = datetime.now(UTC)


@pytest_asyncio.fixture(scope="module")
async def big_pool_engine(database_url: str, migrated_schema: None):
    eng = create_async_engine(database_url, pool_size=5, max_overflow=5)
    yield eng
    await eng.dispose()


@pytest.fixture
def session_factory(big_pool_engine: AsyncEngine):
    return async_sessionmaker(bind=big_pool_engine, expire_on_commit=False)


async def _seed_healthy_event(session_factory, seat_count: int = 3) -> int:
    async with session_factory() as session:
        event_id = (
            await session.execute(
                insert(EventRow).returning(EventRow.id),
                {
                    "name": "Dashboard Test",
                    "venue": "Test Venue",
                    "starts_at": NOW,
                    "total_seats": seat_count,
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
                for i in range(seat_count)
            ],
        )
        await session.commit()
    return event_id


class TestDashboardShapeWithoutEventId:
    async def test_no_event_id_omits_invariants_but_includes_metrics(self, session_factory):
        async with session_factory() as session:
            response = await get_dashboard(session, event_id=None)

        assert response.event_id is None
        assert response.invariants is None
        # Every field must be present and typed -- a KeyError/AttributeError
        # here would mean the endpoint's own shape is broken, which is
        # exactly the failure mode a frontend text-parser could never
        # catch at type-check time.
        assert isinstance(response.metrics.sweeper_backlog, float)
        assert isinstance(response.metrics.reconciliation_divergence_by_kind, dict)


class TestDashboardReflectsRealMetrics:
    async def test_sweeper_backlog_reflects_the_gauges_current_value(self, session_factory):
        sweeper_backlog_gauge.set(7)
        async with session_factory() as session:
            response = await get_dashboard(session, event_id=None)
        assert response.metrics.sweeper_backlog == 7

    async def test_counter_increments_are_reflected(self, session_factory):
        # Both "before" and "after" go through get_dashboard itself --
        # not counter.collect() called directly, which reads a
        # DIFFERENT (single-process-local) view than the aggregated
        # multiprocess registry the endpoint actually reads (see
        # app/api/routes/admin.py's _collect_samples docstring). Reading
        # "before" any other way risks comparing two different sources
        # of truth against each other.
        async with session_factory() as session:
            before = await get_dashboard(session, event_id=None)

        deadlocks_total.inc()
        optimistic_conflicts_total.inc(3)
        reconciliation_divergence_total.labels(kind="redis_session_mismatch").inc()

        async with session_factory() as session:
            after = await get_dashboard(session, event_id=None)

        assert after.metrics.deadlocks_total == before.metrics.deadlocks_total + 1
        assert (
            after.metrics.optimistic_conflicts_total
            == before.metrics.optimistic_conflicts_total + 3
        )
        before_divergence = before.metrics.reconciliation_divergence_by_kind.get(
            "redis_session_mismatch", 0.0
        )
        after_divergence = after.metrics.reconciliation_divergence_by_kind.get(
            "redis_session_mismatch", 0.0
        )
        assert after_divergence == before_divergence + 1


class TestDashboardInvariantsPerEvent:
    async def test_event_id_given_includes_a_healthy_events_invariant_status(self, session_factory):
        event_id = await _seed_healthy_event(session_factory)

        async with session_factory() as session:
            response = await get_dashboard(session, event_id=event_id)

        assert response.event_id == event_id
        assert response.invariants is not None
        assert all(result.passed for result in response.invariants.values())

    async def test_matches_get_invariants_for_the_same_event(self, session_factory):
        """The dashboard's invariant section and the dedicated
        /invariants endpoint share one implementation
        (_compute_invariants) -- this is the test that would catch them
        silently diverging if that sharing were ever removed.
        """
        event_id = await _seed_healthy_event(session_factory)

        async with session_factory() as session:
            dashboard = await get_dashboard(session, event_id=event_id)
        async with session_factory() as session:
            standalone = await get_invariants(event_id, session)

        assert dashboard.invariants == standalone.results


class TestInvariantsReadSnapshotConsistency:
    """Phase 8a's chaos suite caught _compute_invariants reporting a
    false booking_linkage violation under real sustained load: it reads
    `seats` and `booking_seats` as two SEPARATE statements, and Postgres's
    default READ COMMITTED isolation gives each statement its own
    snapshot, not the whole transaction. A booking confirm's single
    atomic commit (both tables, together) landing BETWEEN those two reads
    produces a torn cross-section -- the OLD seats snapshot (still HELD)
    alongside the NEW booking_seats snapshot (already active). Fixed by
    giving _compute_invariants its own REPEATABLE READ session, which
    fixes the snapshot at the first statement regardless of what commits
    elsewhere in between.

    Reproducing the exact race deterministically in one shot would need
    a way to pause _compute_invariants between its two reads -- not
    something worth adding a test-only hook for. Instead: hammer the
    actual race window many times (a real confirm-shaped toggle,
    committing repeatedly, concurrently with many _compute_invariants
    calls) and assert zero false violations across all of them -- the
    same "run it enough times that a probabilistic race would have to
    show up" discipline this project already applies to its concurrency
    tests (SPEC.md section 10, Layer 3).
    """

    async def test_concurrent_confirm_toggle_never_produces_a_torn_read(self, session_factory):
        async with session_factory() as session:
            event_id = (
                await session.execute(
                    insert(EventRow).returning(EventRow.id),
                    {
                        "name": "Snapshot Consistency Test",
                        "venue": "Test Venue",
                        "starts_at": NOW,
                        "total_seats": 1,
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
                        "status": "HELD",
                        "held_by_session_id": "s1",
                        "hold_expires_at": NOW + timedelta(minutes=5),
                        "version": 0,
                    },
                )
            ).scalar_one()
            booking_id = (
                await session.execute(
                    insert(BookingRow).returning(BookingRow.id),
                    {
                        "event_id": event_id,
                        "user_id": 1,
                        "session_id": "s1",
                        "status": "CONFIRMED",
                        "total_amount": "42.00",
                        "currency": "USD",
                        "seat_ids": [seat_id],
                    },
                )
            ).scalar_one()
            await session.execute(
                insert(BookingSeatRow),
                {"booking_id": booking_id, "seat_id": seat_id, "released_at": NOW},
            )
            await session.commit()
            event = await session.get(EventRow, event_id)

        stop = asyncio.Event()

        async def toggle_confirm_state_repeatedly() -> None:
            # Both tables, one commit each time -- the exact shape a real
            # confirm produces (app/api/routes/bookings.py's
            # confirm_booking_transaction, single commit).
            booked = True
            while not stop.is_set():
                async with session_factory() as toggle_session:
                    if booked:
                        # Going TO booked: active booking_seats row (NULL
                        # released_at) together with status=BOOKED.
                        await toggle_session.execute(
                            update(SeatRow)
                            .where(SeatRow.id == seat_id)
                            .values(
                                status="BOOKED",
                                booking_id=booking_id,
                                held_by_session_id=None,
                                hold_expires_at=None,
                            )
                        )
                        await toggle_session.execute(
                            update(BookingSeatRow)
                            .where(BookingSeatRow.seat_id == seat_id)
                            .values(released_at=None)
                        )
                    else:
                        # Going TO held (not booked): the booking_seats
                        # row is released together with status=HELD.
                        await toggle_session.execute(
                            update(SeatRow)
                            .where(SeatRow.id == seat_id)
                            .values(
                                status="HELD",
                                booking_id=None,
                                held_by_session_id="s1",
                                hold_expires_at=NOW + timedelta(minutes=5),
                            )
                        )
                        await toggle_session.execute(
                            update(BookingSeatRow)
                            .where(BookingSeatRow.seat_id == seat_id)
                            .values(released_at=NOW)
                        )
                    await toggle_session.commit()
                booked = not booked
                await asyncio.sleep(0)

        toggle_task = asyncio.create_task(toggle_confirm_state_repeatedly())
        try:
            violations = []
            for _ in range(200):
                async with session_factory() as session:
                    results = await _compute_invariants(session, event)
                if not results["booking_linkage"].passed:
                    violations.append(results["booking_linkage"].detail)
        finally:
            stop.set()
            await toggle_task

        assert violations == [], (
            "REPEATABLE READ should make a torn cross-section between the seats and "
            "booking_seats reads impossible, regardless of how many confirm-shaped "
            f"commits land concurrently -- got {len(violations)} false violation(s)"
        )
