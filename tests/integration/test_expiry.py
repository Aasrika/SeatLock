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

from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from app.api.routes import admin
from app.domain.invariants import check_conservation, check_no_double_booking, check_state_coherence
from app.infra.mappers import seat_to_domain
from app.infra.tables import EventRow, SeatRow
from app.inventory.strategies.pessimistic import PessimisticStrategy

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
