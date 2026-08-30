"""Proves the hold sweeper's actual correctness properties against real
Postgres -- not just that it "looks right."

Uses the same dedicated, larger-pool engine pattern as
tests/integration/test_pessimistic.py -- the SKIP LOCKED test needs a
second, genuinely concurrent session holding a lock open.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from sqlalchemy import insert, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from app.domain.invariants import check_conservation, check_no_double_booking, check_state_coherence
from app.infra.mappers import seat_to_domain
from app.infra.metrics import sweeper_seats_expired_total
from app.infra.tables import BookingRow, EventRow, SeatRow
from app.inventory.sweeper import sweep_once

NOW = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
PAST = NOW - timedelta(minutes=10)
FUTURE = NOW + timedelta(minutes=10)


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
    """Seeds one event with exactly the seat rows described by `seats` --
    each dict may set status/held_by_session_id/hold_expires_at/version
    explicitly, unlike the other test files' uniform-AVAILABLE helper,
    since these tests need precise control over each seat's starting
    state. A BOOKED seat with no explicit booking_id gets a real BookingRow
    created for it automatically -- state_coherence requires a BOOKED seat
    to have one (app/domain/invariants.py), and these tests assert
    invariants after every run.
    """
    async with session_factory() as session:
        event_id = (
            await session.execute(
                insert(EventRow).returning(EventRow.id),
                {
                    "name": "Sweeper Test",
                    "venue": "Test Venue",
                    "starts_at": NOW,
                    "total_seats": len(seats),
                },
            )
        ).scalar_one()

        rows = []
        for i, s in enumerate(seats):
            booking_id = s.get("booking_id")
            if s.get("status") == "BOOKED" and booking_id is None:
                booking_id = (
                    await session.execute(
                        insert(BookingRow).returning(BookingRow.id),
                        {
                            "event_id": event_id,
                            "user_id": 1,
                            "session_id": "seed-booking-session",
                            "status": "CONFIRMED",
                            "total_amount": "0.00",
                            "currency": "USD",
                        },
                    )
                ).scalar_one()
            rows.append(
                {
                    "event_id": event_id,
                    "section": "A",
                    "row_label": "1",
                    "seat_number": i + 1,
                    "status": s.get("status", "AVAILABLE"),
                    "version": s.get("version", 0),
                    "held_by_session_id": s.get("held_by_session_id"),
                    "hold_expires_at": s.get("hold_expires_at"),
                    "booking_id": booking_id,
                }
            )
        await session.execute(insert(SeatRow), rows)
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


def _counter_value(counter) -> float:
    for metric in counter.collect():
        for sample in metric.samples:
            if sample.name == f"{metric.name}_total":
                return sample.value
    return 0.0


class TestExpiresPastHolds:
    async def test_expired_held_seat_returns_to_available_with_fields_reset(self, session_factory):
        event_id, seat_ids = await _seed_seats(
            session_factory,
            [
                {
                    "status": "HELD",
                    "held_by_session_id": "some-session",
                    "hold_expires_at": PAST,
                    "version": 3,
                }
            ],
        )
        seat_id = seat_ids[0]
        expired_before = _counter_value(sweeper_seats_expired_total)

        async with session_factory() as session:
            result = await sweep_once(session, batch_size=100, now=NOW)

        assert result.candidates_found == 1
        assert result.seats_expired == 1
        assert _counter_value(sweeper_seats_expired_total) - expired_before == 1

        row = await _get_seat(session_factory, seat_id)
        assert row.status == "AVAILABLE"
        assert row.held_by_session_id is None
        assert row.hold_expires_at is None
        assert row.booking_id is None
        assert row.version == 4  # bumped, per state_machine.expire()

        await _assert_invariants(session_factory, event_id, 1)


class TestLeavesNonExpiredAlone:
    async def test_held_seat_not_yet_expired_is_untouched(self, session_factory):
        event_id, seat_ids = await _seed_seats(
            session_factory,
            [
                {
                    "status": "HELD",
                    "held_by_session_id": "some-session",
                    "hold_expires_at": FUTURE,
                    "version": 0,
                }
            ],
        )
        seat_id = seat_ids[0]

        async with session_factory() as session:
            result = await sweep_once(session, batch_size=100, now=NOW)

        assert result.candidates_found == 0
        assert result.seats_expired == 0

        row = await _get_seat(session_factory, seat_id)
        assert row.status == "HELD"
        assert row.held_by_session_id == "some-session"
        assert row.version == 0

        await _assert_invariants(session_factory, event_id, 1)

    async def test_available_and_booked_seats_are_untouched(self, session_factory):
        event_id, seat_ids = await _seed_seats(
            session_factory,
            [
                {"status": "AVAILABLE"},
                # BOOKED with no hold_expires_at -- the sweeper's WHERE
                # clause (status = 'HELD') already excludes this on its
                # own; included to prove it explicitly, not by omission.
                {"status": "BOOKED"},
            ],
        )

        async with session_factory() as session:
            result = await sweep_once(session, batch_size=100, now=NOW)

        assert result.candidates_found == 0
        assert result.seats_expired == 0

        statuses = {
            seat_id: (await _get_seat(session_factory, seat_id)).status for seat_id in seat_ids
        }
        assert set(statuses.values()) == {"AVAILABLE", "BOOKED"}

        await _assert_invariants(session_factory, event_id, 2)


class TestSkipLocked:
    async def test_a_seat_locked_elsewhere_is_skipped_not_waited_on(self, session_factory):
        event_id, seat_ids = await _seed_seats(
            session_factory,
            [
                {
                    "status": "HELD",
                    "held_by_session_id": "some-session",
                    "hold_expires_at": PAST,
                }
            ],
        )
        seat_id = seat_ids[0]

        blocker_session = session_factory()
        await blocker_session.execute(
            text("SELECT id FROM seats WHERE id = :id FOR UPDATE"), {"id": seat_id}
        )
        try:
            async with session_factory() as session:
                # Must return quickly (SKIP LOCKED never blocks) -- the
                # timeout proves absence of a hang, not just eventual
                # success.
                result = await asyncio.wait_for(
                    sweep_once(session, batch_size=100, now=NOW), timeout=5
                )

            assert result.candidates_found == 0
            assert result.seats_expired == 0

            row = await _get_seat(session_factory, seat_id)
            assert row.status == "HELD", "a locked seat must not be swept out from under its locker"
        finally:
            await blocker_session.rollback()
            await blocker_session.close()

        # Once released, the NEXT pass picks it up.
        async with session_factory() as session:
            result = await sweep_once(session, batch_size=100, now=NOW)
        assert result.seats_expired == 1

        row = await _get_seat(session_factory, seat_id)
        assert row.status == "AVAILABLE"

        await _assert_invariants(session_factory, event_id, 1)


class TestBatchSize:
    async def test_batch_size_limits_seats_processed_per_pass(self, session_factory):
        event_id, seat_ids = await _seed_seats(
            session_factory,
            [
                {"status": "HELD", "held_by_session_id": f"session-{i}", "hold_expires_at": PAST}
                for i in range(5)
            ],
        )

        async with session_factory() as session:
            first_pass = await sweep_once(session, batch_size=2, now=NOW)
        assert first_pass.candidates_found == 2
        assert first_pass.seats_expired == 2

        available_count = 0
        for seat_id in seat_ids:
            row = await _get_seat(session_factory, seat_id)
            if row.status == "AVAILABLE":
                available_count += 1
        assert available_count == 2, "batch_size=2 must expire exactly 2 of the 5 expired seats"

        # A second pass cleans up the remaining backlog -- oldest-expired-
        # first ordering means this always terminates in ceil(5/2) passes.
        async with session_factory() as session:
            second_pass = await sweep_once(session, batch_size=2, now=NOW)
        async with session_factory() as session:
            third_pass = await sweep_once(session, batch_size=2, now=NOW)

        assert second_pass.seats_expired + third_pass.seats_expired == 3

        await _assert_invariants(session_factory, event_id, 5)


class TestOldestExpiredFirst:
    async def test_reclaims_the_longest_expired_seat_first_when_batch_is_smaller_than_backlog(
        self, session_factory
    ):
        older = NOW - timedelta(minutes=20)
        newer = NOW - timedelta(minutes=1)
        event_id, seat_ids = await _seed_seats(
            session_factory,
            [
                {"status": "HELD", "held_by_session_id": "s-newer", "hold_expires_at": newer},
                {"status": "HELD", "held_by_session_id": "s-older", "hold_expires_at": older},
            ],
        )
        newer_seat_id, older_seat_id = seat_ids

        async with session_factory() as session:
            result = await sweep_once(session, batch_size=1, now=NOW)

        assert result.seats_expired == 1
        assert (await _get_seat(session_factory, older_seat_id)).status == "AVAILABLE"
        assert (await _get_seat(session_factory, newer_seat_id)).status == "HELD"

        await _assert_invariants(session_factory, event_id, 2)
