"""Phase 5 item 2/6: SPEC.md section 6, I4 ("the same Idempotency-Key +
same fingerprint = same response, one booking").

Calls app/api/routes/bookings.py's route functions DIRECTLY -- there is
no precedent anywhere in this test suite for driving routes over real
HTTP (every other integration test calls the underlying function
directly, e.g. test_expiry.py's extend_hold_at), and a FastAPI route
decorated with @router.post(...) is still a plain callable: the
decorator only registers routing metadata, it does not wrap the
function. `_FakeRequest` supplies the only two attributes these routes
read off `request` (method, url.path) without needing a real ASGI scope.
This exercises the actual production code path, not a reimplementation
of it in the test.

Crash-simulation tests call the lower-level functions
(idempotency.begin_idempotent_request, app.booking.create.create_booking)
directly instead of the route, specifically so the test can stop
mid-sequence at an exact point the route itself never exposes a hook
for -- same "deterministic failure injection over relying on real races"
principle as test_optimistic.py's simulated-deadlock test.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
import pytest_asyncio
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy import insert, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from app.api.routes.bookings import (
    ConfirmBookingRequest,
    CreateBookingRequest,
    confirm_booking_route,
    create_booking_route,
)
from app.booking.confirm import attempt_confirm_write
from app.booking.create import CreateBookingParams, create_booking
from app.booking.responses import BookingResponse
from app.domain.invariants import check_conservation, check_no_double_booking, check_state_coherence
from app.infra import idempotency
from app.infra.mappers import seat_to_domain
from app.infra.metrics import oversell_blocked_total
from app.infra.tables import BookingRow, BookingSeatRow, EventRow, IdempotencyKeyRow, SeatRow
from workers.idempotency_reaper import reap_once

# Real wall-clock time, NOT a fixed historical constant like test_expiry.py's
# NOW: the routes under test (create_booking_route, confirm_booking_route)
# compute `now = datetime.now(UTC)` internally rather than accepting it as a
# parameter (unlike extend_hold_at et al.), so seeded seat/booking timestamps
# in THIS file must be relative to the real clock or every "unexpired" hold
# below would appear already expired by the time a route call checks it.
NOW = datetime.now(UTC)
PAST = NOW - timedelta(minutes=1)
FUTURE = NOW + timedelta(minutes=10)


class _FakeURL:
    def __init__(self, path: str) -> None:
        self.path = path


class _FakeRequest:
    """Supplies exactly the two attributes the routes under test read off
    `request` -- see module docstring.
    """

    def __init__(self, method: str, path: str) -> None:
        self.method = method
        self.url = _FakeURL(path)


@pytest_asyncio.fixture(scope="module")
async def big_pool_engine(database_url: str, migrated_schema: None):
    eng = create_async_engine(database_url, pool_size=30, max_overflow=10)
    yield eng
    await eng.dispose()


@pytest.fixture
def session_factory(big_pool_engine: AsyncEngine):
    return async_sessionmaker(bind=big_pool_engine, expire_on_commit=False)


@pytest_asyncio.fixture(autouse=True)
async def _clean_global_state(session_factory):
    """idempotency_keys and reap_once() are correctly GLOBAL in scope (no
    event_id filter, by design -- a real reaper must sweep every key
    regardless of which event it belongs to), which makes this test file
    vulnerable to the exact cross-test contamination test_expiry.py's
    identical fixture documents fighting for the sweeper/reconciler:
    confirmed directly (a reaper-count assertion here saw stale rows left
    behind by an EARLIER test in the same session before this fixture was
    added). Truncate before every test in this module.
    """
    async with session_factory() as session:
        await session.execute(
            text(
                "TRUNCATE TABLE idempotency_keys, payment_events, outbox, hold_audit, "
                "booking_seats, bookings, seats, events RESTART IDENTITY CASCADE"
            )
        )
        await session.commit()


async def _seed_held_seats(
    session_factory, *, count: int, session_id: str, hold_expires_at: datetime = FUTURE
) -> tuple[int, list[int]]:
    async with session_factory() as session:
        event_id = (
            await session.execute(
                insert(EventRow).returning(EventRow.id),
                {
                    "name": "Idempotency Test",
                    "venue": "Test Venue",
                    "starts_at": NOW,
                    "total_seats": count,
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
                    "status": "HELD",
                    "held_by_session_id": session_id,
                    "hold_expires_at": hold_expires_at,
                    "version": 0,
                }
                for i in range(count)
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


def _create_body(
    event_id: int, seat_ids: list[int], session_id: str, user_id: int
) -> CreateBookingRequest:
    return CreateBookingRequest(
        event_id=event_id,
        seat_ids=seat_ids,
        session_id=session_id,
        user_id=user_id,
        total_amount=Decimal("42.00"),
        currency="USD",
    )


async def _call_create(session_factory, key: str, body: CreateBookingRequest):
    async with session_factory() as session:
        return await create_booking_route(
            body, _FakeRequest("POST", "/api/bookings"), session, idempotency_key=key
        )


def _response_body(response: BookingResponse | JSONResponse) -> dict:
    """A fresh (New) call returns a BookingResponse; a replayed
    (idempotency.Replay) call returns a JSONResponse wrapping the SAME
    content instead -- see app/api/routes/bookings.py. Normalising both
    to a plain dict is how "identical response both times" (item 6a) is
    actually checked: comparing the two objects' TYPES would fail even
    when the content -- what a real client actually receives -- matches
    exactly.
    """
    if isinstance(response, JSONResponse):
        return json.loads(response.body)
    return response.model_dump(mode="json")


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


async def _booking_count(session_factory, event_id: int) -> int:
    async with session_factory() as session:
        rows = (
            (await session.execute(select(BookingRow).where(BookingRow.event_id == event_id)))
            .scalars()
            .all()
        )
        return len(rows)


class TestSameKeySameBody:
    """(a): same key + same body twice -> one booking, identical response
    both times.
    """

    async def test_second_call_replays_without_re_executing(self, session_factory):
        event_id, seat_ids = await _seed_held_seats(session_factory, count=1, session_id="s1")
        body = _create_body(event_id, seat_ids, "s1", user_id=1)
        key = "idem-key-same-body"

        first = await _call_create(session_factory, key, body)
        second = await _call_create(session_factory, key, body)

        assert isinstance(first, BookingResponse), f"expected a fresh execution, got {first!r}"
        assert isinstance(second, JSONResponse), f"expected a replay, got {second!r}"
        assert _response_body(first) == _response_body(second)
        assert await _booking_count(session_factory, event_id) == 1
        await _assert_invariants(session_factory, event_id, 1)


class TestSameKeyDifferentBody:
    """(b): same key + different body -> 422."""

    async def test_different_body_is_rejected(self, session_factory):
        event_id, seat_ids = await _seed_held_seats(session_factory, count=1, session_id="s1")
        key = "idem-key-different-body"
        first_body = _create_body(event_id, seat_ids, "s1", user_id=1)
        await _call_create(session_factory, key, first_body)

        second_body = _create_body(event_id, seat_ids, "s1", user_id=1)
        second_body.total_amount = Decimal("999.00")  # only field that differs

        with pytest.raises(HTTPException) as exc_info:
            await _call_create(session_factory, key, second_body)
        assert exc_info.value.status_code == 422
        assert await _booking_count(session_factory, event_id) == 1


class TestConcurrentSameKey:
    """(c): concurrent requests with the same key -> exactly one executes,
    the rest get 409 or the stored response, never two bookings.
    """

    async def test_ten_concurrent_identical_requests_produce_one_booking(self, session_factory):
        event_id, seat_ids = await _seed_held_seats(session_factory, count=1, session_id="s1")
        body = _create_body(event_id, seat_ids, "s1", user_id=1)
        key = "idem-key-concurrent"
        barrier = asyncio.Barrier(10)

        async def attempt():
            async with session_factory() as session:
                await barrier.wait()
                return await create_booking_route(
                    body, _FakeRequest("POST", "/api/bookings"), session, idempotency_key=key
                )

        results = await asyncio.gather(*[attempt() for _ in range(10)], return_exceptions=True)

        exceptions = [r for r in results if isinstance(r, BaseException)]
        assert exceptions == [], f"unexpected exceptions: {exceptions}"
        statuses = {getattr(r, "status_code", 201) for r in results}
        # Every outcome must be either the 201 created response or a 409
        # (in-progress) -- never a silent second booking, never a 500.
        assert statuses <= {201, 409}

        assert await _booking_count(session_factory, event_id) == 1
        await _assert_invariants(session_factory, event_id, 1)


class TestCrashSimulation:
    """(d): execute the booking, abort before marking COMPLETED, assert
    the transaction rolled back entirely -- no orphan booking. Then a
    retry succeeds cleanly.
    """

    async def test_abort_before_commit_leaves_no_orphan_and_retry_succeeds(self, session_factory):
        event_id, seat_ids = await _seed_held_seats(session_factory, count=1, session_id="s1")
        key = "idem-key-crash"
        body = _create_body(event_id, seat_ids, "s1", user_id=1)
        # Same fingerprint computation the route itself uses (see
        # _call_create) -- the retry below goes through the real route,
        # so this manually-driven first attempt must fingerprint its
        # request identically or the retry would see a spurious 422
        # (different fingerprint) instead of proceeding/replaying.
        fingerprint = idempotency.compute_fingerprint(
            "POST", "/api/bookings", body.model_dump(mode="json")
        )

        async with session_factory() as session:
            outcome = await idempotency.begin_idempotent_request(
                session,
                key,
                user_id=1,
                fingerprint=fingerprint,
                now=NOW,
                ttl_seconds=86400.0,
                stale_timeout_seconds=60.0,
            )
            assert isinstance(outcome, idempotency.New)

            await create_booking(
                session,
                CreateBookingParams(
                    event_id=event_id,
                    seat_ids=seat_ids,
                    session_id="s1",
                    user_id=1,
                    total_amount=Decimal("10.00"),
                    currency="USD",
                    idempotency_key=key,
                ),
                NOW,
            )
            # CRASH: never call complete_idempotent_request, never commit.
            await session.rollback()

        # No orphan booking -- the INSERT above was rolled back with
        # everything else in that uncommitted transaction.
        assert await _booking_count(session_factory, event_id) == 0

        # The key itself is a SEPARATE, already-committed transaction
        # (begin_idempotent_request's own INSERT) -- it survives the
        # rollback above and is still IN_PROGRESS.
        async with session_factory() as session:
            row = (
                await session.execute(select(IdempotencyKeyRow).where(IdempotencyKeyRow.key == key))
            ).scalar_one()
            assert row.status == "IN_PROGRESS"

        # An IMMEDIATE retry does NOT succeed -- the key is still
        # IN_PROGRESS, so it gets 409, exactly per SPEC.md section 6's
        # own interview answer ("the key stays IN_PROGRESS, a retry gets
        # 409, and a stale-key reaper marks rows older than a timeout as
        # failed so the client can safely retry"). Only AFTER the reaper
        # runs does a subsequent retry succeed.
        immediate_retry = await _call_create(session_factory, key, body)
        assert immediate_retry.status_code == 409

        async with session_factory() as session:
            # timeout_seconds=0.0: real, tiny wall-clock elapsed time
            # since begin_idempotent_request's commit above already
            # exceeds a zero-length timeout -- no clock mocking needed.
            result = await reap_once(session, timeout_seconds=0.0, now=datetime.now(UTC))
        assert result.reaped == 1
        assert result.recovered == 0

        response = await _call_create(session_factory, key, body)
        assert response.status == "PENDING"
        assert await _booking_count(session_factory, event_id) == 1
        await _assert_invariants(session_factory, event_id, 1)


class TestReaperRecoversCommittedBookingBehindALostCompletionMarker:
    """The correction to (d): a crash AFTER the booking commits but
    BEFORE the key is marked COMPLETED must NOT be treated the same as
    (d)'s "nothing happened" case. workers/idempotency_reaper.py must
    recover to COMPLETED (reconstructing the response from the booking),
    never FAILED -- FAILED here would let a retry double-book.
    """

    async def test_reaper_recovers_completed_not_failed_and_retry_replays(self, session_factory):
        event_id, seat_ids = await _seed_held_seats(session_factory, count=1, session_id="s1")
        key = "idem-key-split-crash"
        body = _create_body(event_id, seat_ids, "s1", user_id=1)
        fingerprint = idempotency.compute_fingerprint(
            "POST", "/api/bookings", body.model_dump(mode="json")
        )

        async with session_factory() as session:
            outcome = await idempotency.begin_idempotent_request(
                session,
                key,
                user_id=1,
                fingerprint=fingerprint,
                now=NOW,
                ttl_seconds=86400.0,
                stale_timeout_seconds=60.0,
            )
            assert isinstance(outcome, idempotency.New)

            await create_booking(
                session,
                CreateBookingParams(
                    event_id=event_id,
                    seat_ids=seat_ids,
                    session_id="s1",
                    user_id=1,
                    total_amount=Decimal("10.00"),
                    currency="USD",
                    idempotency_key=key,
                ),
                NOW,
            )
            # THE SPLIT: the booking write commits here...
            await session.commit()
            # ...but complete_idempotent_request() is never called. This
            # simulates a crash landing exactly between those two steps
            # (see BookingRow.idempotency_key's own docstring: this is
            # the failure the reaper's booking-lookup is defence against,
            # not something the normal one-transaction path should ever
            # itself produce).

        assert await _booking_count(session_factory, event_id) == 1

        async with session_factory() as session:
            # timeout_seconds=0.0: real, tiny wall-clock elapsed time
            # since the commit above is already "past" a zero-length
            # timeout -- no clock mocking needed.
            result = await reap_once(session, timeout_seconds=0.0, now=datetime.now(UTC))
        assert result.recovered == 1
        assert result.reaped == 0

        async with session_factory() as session:
            row = (
                await session.execute(select(IdempotencyKeyRow).where(IdempotencyKeyRow.key == key))
            ).scalar_one()
            assert row.status == "COMPLETED"
            assert row.response_body is not None
            assert row.response_body["status"] == "PENDING"

        # A retry with the same key now replays the recovered response
        # (a JSONResponse, not a fresh BookingResponse) rather than
        # re-executing, which would double-book.
        response = await _call_create(session_factory, key, body)
        assert isinstance(response, JSONResponse), f"expected a replay, got {response!r}"
        assert _response_body(response)["status"] == "PENDING"
        assert await _booking_count(session_factory, event_id) == 1
        await _assert_invariants(session_factory, event_id, 1)


class TestStaleReaperWithNoBooking:
    """(e): IN_PROGRESS past timeout with NO booking ever created -> the
    reaper marks it FAILED, and a retry then succeeds.
    """

    async def test_stale_key_with_no_booking_is_failed_then_retry_succeeds(self, session_factory):
        event_id, seat_ids = await _seed_held_seats(session_factory, count=1, session_id="s1")
        key = "idem-key-stale-no-booking"
        body = _create_body(event_id, seat_ids, "s1", user_id=1)
        fingerprint = idempotency.compute_fingerprint(
            "POST", "/api/bookings", body.model_dump(mode="json")
        )

        async with session_factory() as session:
            outcome = await idempotency.begin_idempotent_request(
                session,
                key,
                user_id=1,
                fingerprint=fingerprint,
                now=NOW,
                ttl_seconds=86400.0,
                stale_timeout_seconds=60.0,
            )
            assert isinstance(outcome, idempotency.New)
            # Nothing else happens -- the "request" crashed before doing
            # any durable work at all.

        assert await _booking_count(session_factory, event_id) == 0

        async with session_factory() as session:
            result = await reap_once(session, timeout_seconds=0.0, now=datetime.now(UTC))
        assert result.reaped == 1
        assert result.recovered == 0

        async with session_factory() as session:
            row = (
                await session.execute(select(IdempotencyKeyRow).where(IdempotencyKeyRow.key == key))
            ).scalar_one()
            assert row.status == "FAILED"

        response = await _call_create(session_factory, key, body)
        assert response.status == "PENDING"
        assert await _booking_count(session_factory, event_id) == 1
        await _assert_invariants(session_factory, event_id, 1)


class TestConfirmIdempotency:
    """Correction 2: confirm needs the same machinery, for the same
    reason -- a client that times out on confirm cannot tell "confirmed,
    response lost" from "never processed," and a bare retry would
    otherwise see a clean 409 from the conditional UPDATE for a booking
    that actually succeeded (and was charged).
    """

    async def _create_pending(
        self, session_factory, session_id: str, user_id: int
    ) -> tuple[int, int, list[int]]:
        event_id, seat_ids = await _seed_held_seats(session_factory, count=1, session_id=session_id)
        create_key = f"create-{session_id}"
        response = await _call_create(
            session_factory, create_key, _create_body(event_id, seat_ids, session_id, user_id)
        )
        return event_id, response.id, seat_ids

    async def test_same_key_same_body_confirms_once(self, session_factory):
        event_id, booking_id, seat_ids = await self._create_pending(
            session_factory, "confirm-s1", user_id=2
        )
        body = ConfirmBookingRequest(session_id="confirm-s1")
        key = "confirm-key-same"

        async def call():
            async with session_factory() as session:
                return await confirm_booking_route(
                    booking_id,
                    body,
                    _FakeRequest("POST", f"/api/bookings/{booking_id}/confirm"),
                    session,
                    idempotency_key=key,
                )

        first = await call()
        second = await call()
        assert isinstance(first, BookingResponse), f"expected a fresh execution, got {first!r}"
        assert isinstance(second, JSONResponse), f"expected a replay, got {second!r}"
        assert _response_body(first) == _response_body(second)
        assert first.status == "CONFIRMED"

        async with session_factory() as session:
            confirmed_count = (
                (
                    await session.execute(
                        select(BookingRow).where(
                            BookingRow.id == booking_id, BookingRow.status == "CONFIRMED"
                        )
                    )
                )
                .scalars()
                .all()
            )
        assert len(confirmed_count) == 1
        await _assert_invariants(session_factory, event_id, 1)

    async def test_concurrent_confirm_same_key_confirms_exactly_once(self, session_factory):
        event_id, booking_id, seat_ids = await self._create_pending(
            session_factory, "confirm-s2", user_id=3
        )
        body = ConfirmBookingRequest(session_id="confirm-s2")
        key = "confirm-key-concurrent"
        barrier = asyncio.Barrier(5)

        async def attempt():
            async with session_factory() as session:
                await barrier.wait()
                return await confirm_booking_route(
                    booking_id,
                    body,
                    _FakeRequest("POST", f"/api/bookings/{booking_id}/confirm"),
                    session,
                    idempotency_key=key,
                )

        results = await asyncio.gather(*[attempt() for _ in range(5)], return_exceptions=True)
        exceptions = [r for r in results if isinstance(r, BaseException)]
        assert exceptions == [], f"unexpected exceptions: {exceptions}"

        async with session_factory() as session:
            confirmed_count = (
                (
                    await session.execute(
                        select(BookingRow).where(
                            BookingRow.id == booking_id, BookingRow.status == "CONFIRMED"
                        )
                    )
                )
                .scalars()
                .all()
            )
        assert len(confirmed_count) == 1
        await _assert_invariants(session_factory, event_id, 1)


def _counter_value(counter, **labels) -> float:
    for metric in counter.collect():
        for sample in metric.samples:
            if sample.name == f"{metric.name}_total" and sample.labels == labels:
                return sample.value
    return 0.0


class TestOversellBlockedAtDatabaseLayer:
    """Item 5: confirm is where oversell_blocked_total{layer="database"}
    (app/infra/metrics.py -- dormant since Phase 1, "booking_seats isn't
    written until Phase 5's confirm/booking path exists") finally gets a
    firing point, via booking_seats' partial unique index. It should
    stay at zero under every NORMAL confirm -- every other test in this
    file confirms bookings without it ever firing -- so this test proves
    the wiring itself works by forcing the one condition that should
    make it fire: two different bookings' booking_seats rows both
    targeting the same seat, active at once.

    Reaching that condition through attempt_confirm_write's own seat-
    status UPDATE (rather than inserting into booking_seats directly)
    means the seat genuinely reaches BOOKED first, exactly as it would
    under a real (hypothetical) upstream bug that let the seat-level
    guard get bypassed -- this is deterministic failure injection at the
    one layer meant to catch that, not a contrived shortcut around it.
    """

    async def test_second_active_booking_seats_row_for_same_seat_increments_metric(
        self, session_factory
    ):
        event_id, seat_ids = await _seed_held_seats(session_factory, count=1, session_id="s1")
        seat_id = seat_ids[0]

        async with session_factory() as session:
            first_booking_id = (
                await session.execute(
                    insert(BookingRow).returning(BookingRow.id),
                    {
                        "event_id": event_id,
                        "user_id": 1,
                        "session_id": "s1",
                        "status": "PENDING",
                        "total_amount": Decimal("10.00"),
                        "currency": "USD",
                        "seat_ids": [seat_id],
                        "created_at": NOW,
                    },
                )
            ).scalar_one()
            await session.execute(
                insert(BookingSeatRow), {"booking_id": first_booking_id, "seat_id": seat_id}
            )
            await session.commit()

        # A SECOND booking, whose seat happens to still independently
        # satisfy attempt_confirm_write's own seat-level guard (HELD by
        # the same session, unexpired) -- simulating the seat-level
        # check having been bypassed somehow for this second booking, so
        # the ONLY thing left to catch the collision is booking_seats'
        # own partial unique index.
        async with session_factory() as session:
            await session.execute(
                text(
                    "UPDATE seats SET status='HELD', held_by_session_id='s1', "
                    "hold_expires_at=:future, booking_id=NULL WHERE id=:seat_id"
                ),
                {"future": FUTURE, "seat_id": seat_id},
            )
            second_booking_id = (
                await session.execute(
                    insert(BookingRow).returning(BookingRow.id),
                    {
                        "event_id": event_id,
                        "user_id": 1,
                        "session_id": "s1",
                        "status": "PENDING",
                        "total_amount": Decimal("10.00"),
                        "currency": "USD",
                        "seat_ids": [seat_id],
                        "created_at": NOW,
                    },
                )
            ).scalar_one()
            await session.commit()

        before = _counter_value(oversell_blocked_total, layer="database")

        async with session_factory() as session:
            ok = await attempt_confirm_write(session, second_booking_id, [seat_id], "s1", NOW)

        assert ok is False
        assert _counter_value(oversell_blocked_total, layer="database") == before + 1

        # The first booking's active booking_seats row is exactly what
        # should have blocked the second -- I1 held, even though it took
        # the database layer, not the application layer, to do it here.
        async with session_factory() as session:
            active_rows = (
                (
                    await session.execute(
                        select(BookingSeatRow).where(
                            BookingSeatRow.seat_id == seat_id, BookingSeatRow.released_at.is_(None)
                        )
                    )
                )
                .scalars()
                .all()
            )
        assert len(active_rows) == 1
        assert active_rows[0].booking_id == first_booking_id
