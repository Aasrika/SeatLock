"""Phase 5 items 3/4/6f-j: SPEC.md section 7 -- webhook ingestion
(app/payments/ingest.py, app/api/routes/webhooks.py) and effect
application (workers/payment_worker.py).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
import pytest_asyncio
from sqlalchemy import insert, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from app.booking.confirm import attempt_confirm_write
from app.domain.invariants import check_conservation, check_no_double_booking, check_state_coherence
from app.infra.config import settings
from app.infra.mappers import seat_to_domain
from app.infra.metrics import (
    late_payment_refund_required_total,
    webhook_signature_failures_total,
)
from app.infra.tables import BookingRow, EventRow, PaymentEventRow, SeatRow
from app.payments.ingest import Accepted, Duplicate, SignatureInvalid, Unresolved, ingest_webhook
from app.payments.signature import sign
from workers.payment_worker import process_once

NOW = datetime.now(UTC)
FUTURE = NOW + timedelta(minutes=10)
SECRET = settings.webhook_hmac_secret


@pytest_asyncio.fixture(scope="module")
async def big_pool_engine(database_url: str, migrated_schema: None):
    eng = create_async_engine(database_url, pool_size=20, max_overflow=10)
    yield eng
    await eng.dispose()


@pytest.fixture
def session_factory(big_pool_engine: AsyncEngine):
    return async_sessionmaker(bind=big_pool_engine, expire_on_commit=False)


@pytest_asyncio.fixture(autouse=True)
async def _clean_global_state(session_factory):
    """payment_events (like idempotency_keys, see test_idempotency.py's
    identical fixture) has no event_id scoping -- correctly, a real
    payment provider isn't scoped to one event either.
    """
    async with session_factory() as session:
        await session.execute(
            text(
                "TRUNCATE TABLE payment_events, idempotency_keys, outbox, hold_audit, "
                "booking_seats, bookings, seats, events RESTART IDENTITY CASCADE"
            )
        )
        await session.commit()


def _counter_value(counter, **labels) -> float:
    for metric in counter.collect():
        for sample in metric.samples:
            if sample.name == f"{metric.name}_total" and sample.labels == labels:
                return sample.value
    return 0.0


async def _seed_confirmed_booking(
    session_factory, *, session_id: str = "s1"
) -> tuple[int, int, int]:
    """Returns (event_id, booking_id, seat_id) for a booking already
    CONFIRMED and its seat already BOOKED -- the payment_worker tests
    exercise effects on bookings that reached this state through the
    normal hold->create->confirm path, not through webhook-driven
    confirmation, so a real earlier delivery isn't a precondition for
    testing a LATER one.
    """
    async with session_factory() as session:
        event_id = (
            await session.execute(
                insert(EventRow).returning(EventRow.id),
                {"name": "Webhook Test", "venue": "Test Venue", "starts_at": NOW, "total_seats": 1},
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
                    "held_by_session_id": session_id,
                    "hold_expires_at": FUTURE,
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
                    "session_id": session_id,
                    "status": "PENDING",
                    "total_amount": Decimal("10.00"),
                    "currency": "USD",
                    "seat_ids": [seat_id],
                    "created_at": NOW,
                },
            )
        ).scalar_one()
        ok = await attempt_confirm_write(session, booking_id, [seat_id], session_id, NOW)
        assert ok
        await session.commit()
    return event_id, booking_id, seat_id


async def _seed_pending_booking(session_factory, *, session_id: str = "s2") -> tuple[int, int, int]:
    """A PENDING booking whose seat is still validly HELD -- the
    late-success test moves the seat out from under it afterward.
    """
    async with session_factory() as session:
        event_id = (
            await session.execute(
                insert(EventRow).returning(EventRow.id),
                {"name": "Webhook Test", "venue": "Test Venue", "starts_at": NOW, "total_seats": 1},
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
                    "held_by_session_id": session_id,
                    "hold_expires_at": FUTURE,
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
                    "session_id": session_id,
                    "status": "PENDING",
                    "total_amount": Decimal("10.00"),
                    "currency": "USD",
                    "seat_ids": [seat_id],
                    "created_at": NOW,
                },
            )
        ).scalar_one()
        await session.commit()
    return event_id, booking_id, seat_id


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


def _webhook_body(event_id: str, event_type: str, booking_id: int | None, **extra) -> bytes:
    payload = {"event_id": event_id, "event_type": event_type, "booking_id": booking_id, **extra}
    return json.dumps(payload).encode()


class TestSignatureVerification:
    """(i): invalid signature -> 401 equivalent (SignatureInvalid),
    nothing inserted, counted.
    """

    async def test_invalid_signature_inserts_nothing_and_is_counted(self, session_factory):
        before = _counter_value(webhook_signature_failures_total)
        raw_body = _webhook_body("evt-bad-sig", "payment.succeeded", None)

        async with session_factory() as session:
            outcome = await ingest_webhook(session, raw_body, "not-a-real-signature", SECRET, NOW)

        assert isinstance(outcome, SignatureInvalid)
        async with session_factory() as session:
            rows = (await session.execute(select(PaymentEventRow))).scalars().all()
        assert rows == []
        assert _counter_value(webhook_signature_failures_total) == before + 1


class TestSignatureIsOverRawBytes:
    """(j): a semantically-identical but byte-different body must FAIL
    verification -- the signature is over the RAW bytes, not a
    re-serialization of the parsed structure.
    """

    async def test_reordered_keys_and_whitespace_fail_verification(self, session_factory):
        canonical = b'{"event_id":"evt-raw-1","event_type":"payment.succeeded","booking_id":1}'
        # Same keys, same values, different byte sequence (key order +
        # whitespace) -- decodes to an EQUAL Python dict.
        reordered = (
            b'{ "booking_id": 1, "event_type": "payment.succeeded", "event_id": "evt-raw-1" }'
        )
        assert json.loads(canonical) == json.loads(reordered)

        signature = sign(canonical, SECRET)

        async with session_factory() as session:
            outcome = await ingest_webhook(session, reordered, signature, SECRET, NOW)

        assert isinstance(outcome, SignatureInvalid)
        async with session_factory() as session:
            rows = (await session.execute(select(PaymentEventRow))).scalars().all()
        assert rows == []


class TestDuplicateEvent:
    """(f): duplicate provider_event_id -> 200 (Duplicate, not an
    error), processed exactly once.
    """

    async def test_duplicate_is_accepted_but_processed_once(self, session_factory):
        _, booking_id, _ = await _seed_confirmed_booking(session_factory, session_id="dup-s1")
        raw_body = _webhook_body("evt-dup-1", "payment.refunded", booking_id)
        signature = sign(raw_body, SECRET)

        async with session_factory() as session:
            first = await ingest_webhook(session, raw_body, signature, SECRET, NOW)
        async with session_factory() as session:
            second = await ingest_webhook(session, raw_body, signature, SECRET, NOW)

        assert isinstance(first, Accepted)
        assert isinstance(second, Duplicate)

        async with session_factory() as session:
            rows = (await session.execute(select(PaymentEventRow))).scalars().all()
        assert len(rows) == 1

        async with session_factory() as session:
            result = await process_once(session, batch_size=10, now=NOW)
        assert result.processed == 1
        assert result.applied == 1

        # A second processing pass finds nothing left to do -- the event
        # was consumed exactly once, not reprocessed.
        async with session_factory() as session:
            second_pass = await process_once(session, batch_size=10, now=NOW)
        assert second_pass.processed == 0


class TestUnresolvedBookingId:
    async def test_unknown_booking_id_is_accepted_and_flagged_unresolved(self, session_factory):
        raw_body = _webhook_body("evt-unresolved-1", "payment.succeeded", 999_999_999)
        signature = sign(raw_body, SECRET)

        async with session_factory() as session:
            outcome = await ingest_webhook(session, raw_body, signature, SECRET, NOW)

        assert isinstance(outcome, Unresolved)
        async with session_factory() as session:
            row = (
                await session.execute(
                    select(PaymentEventRow).where(
                        PaymentEventRow.provider_event_id == "evt-unresolved-1"
                    )
                )
            ).scalar_one()
        assert row.processing_status == "UNRESOLVED"
        assert row.booking_id is None

        # Never picked up by the worker -- process_once only selects
        # processing_status IS NULL.
        async with session_factory() as session:
            result = await process_once(session, batch_size=10, now=NOW)
        assert result.processed == 0


class TestOutOfOrderRefundedThenSucceeded:
    """(g): payment.refunded then payment.succeeded -> the second event
    is an illegal transition, recorded, not applied.
    """

    async def test_late_succeeded_after_refunded_is_rejected_not_applied(self, session_factory):
        event_id, booking_id, seat_id = await _seed_confirmed_booking(
            session_factory, session_id="oo-s1"
        )

        refunded_body = _webhook_body("evt-oo-refunded", "payment.refunded", booking_id)
        async with session_factory() as session:
            await ingest_webhook(session, refunded_body, sign(refunded_body, SECRET), SECRET, NOW)
        async with session_factory() as session:
            result = await process_once(session, batch_size=10, now=NOW)
        assert result.applied == 1

        async with session_factory() as session:
            booking = (
                await session.execute(select(BookingRow).where(BookingRow.id == booking_id))
            ).scalar_one()
        assert booking.status == "REFUNDED"

        # Now the late/duplicate payment.succeeded arrives.
        succeeded_body = _webhook_body("evt-oo-succeeded", "payment.succeeded", booking_id)
        async with session_factory() as session:
            await ingest_webhook(session, succeeded_body, sign(succeeded_body, SECRET), SECRET, NOW)
        async with session_factory() as session:
            result = await process_once(session, batch_size=10, now=NOW)
        assert result.applied == 0
        assert result.rejected == 1

        async with session_factory() as session:
            booking = (
                await session.execute(select(BookingRow).where(BookingRow.id == booking_id))
            ).scalar_one()
        # Still REFUNDED -- the illegal transition was rejected, not applied.
        assert booking.status == "REFUNDED"
        await _assert_invariants(session_factory, event_id, 1)


class TestLateSuccessAfterResale:
    """(h)/item 4: payment.succeeded arrives after the hold expired and
    the seat was resold -- booking -> REFUND_REQUIRED, seat NOT touched,
    counted.
    """

    async def test_late_success_moves_booking_to_refund_required_seat_untouched(
        self, session_factory
    ):
        event_id, booking_id, seat_id = await _seed_pending_booking(
            session_factory, session_id="late-s1"
        )

        # Simulate resale: the seat's hold expired and a DIFFERENT
        # session/booking now holds it -- exactly what would happen if
        # the sweeper (or lazy expiry) reclaimed it and someone else
        # acquired it before this late payment.succeeded arrived.
        async with session_factory() as session:
            await session.execute(
                text(
                    "UPDATE seats SET held_by_session_id = 'someone-else', "
                    "hold_expires_at = :future, version = version + 1 WHERE id = :seat_id"
                ),
                {"future": FUTURE, "seat_id": seat_id},
            )
            await session.commit()

        before = _counter_value(late_payment_refund_required_total)

        succeeded_body = _webhook_body("evt-late-1", "payment.succeeded", booking_id)
        async with session_factory() as session:
            await ingest_webhook(session, succeeded_body, sign(succeeded_body, SECRET), SECRET, NOW)
        async with session_factory() as session:
            result = await process_once(session, batch_size=10, now=NOW)

        assert result.applied == 1
        assert _counter_value(late_payment_refund_required_total) == before + 1

        async with session_factory() as session:
            booking = (
                await session.execute(select(BookingRow).where(BookingRow.id == booking_id))
            ).scalar_one()
        assert booking.status == "REFUND_REQUIRED"

        async with session_factory() as session:
            seat = (
                await session.execute(select(SeatRow).where(SeatRow.id == seat_id))
            ).scalar_one()
        # Untouched by this event -- still held by whoever resale gave it to.
        assert seat.held_by_session_id == "someone-else"
        assert seat.status == "HELD"

        await _assert_invariants(session_factory, event_id, 1)
