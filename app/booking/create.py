"""Booking creation (Phase 5) -- the missing first half of the two-phase
booking flow SPEC.md section 5 describes ("hold (free, instant,
TTL-bounded) then confirm (payment)"). Nothing before this phase ever
wrote a BookingRow at all; app/infra/metrics.py's oversell_blocked_total
docstring has said since Phase 1 that "booking_seats isn't written until
Phase 5's confirm/booking path exists" -- this module is that path's
creation half, app/booking/confirm.py is its confirm half.

Deliberately does NOT insert booking_seats (see BookingSeatRow/
PaymentEventRow docstrings) -- a seat only gets an active booking_seats
row once it is actually BOOKED, at confirm time. At creation time the
seat is still HELD, and this module never writes to SeatRow at all:
which seats a PENDING booking claims is recorded on BookingRow.seat_ids
instead (see that column's own docstring). An earlier version of this
module set SeatRow.booking_id here, on a still-HELD seat -- confirmed
directly (test_idempotency.py's own invariant check caught it) that this
violates app/domain/invariants.py's check_state_coherence(), which has
always required HELD seats to have booking_id=None. BookingRow.seat_ids
is the fix: it lets this module (and confirm.py, which reads it back)
track claimed seats without ever touching the seats table itself until
a seat is genuinely BOOKED.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.booking.responses import BookingResponse, build_booking_response
from app.infra.tables import BookingRow, SeatRow


class BookingCreationFailed(Exception):
    """Raised when the requested seats are not all HELD by the requesting
    session and unexpired. The route translates this into a 409 -- never
    retried automatically, same as any other domain-level rejection in
    this codebase (SeatUnavailable, HoldExpired): the caller asked for
    something that is not currently true, and blindly retrying the exact
    same request would fail identically.
    """

    def __init__(self, reason: str, failed_seat_ids: list[int]) -> None:
        self.reason = reason
        self.failed_seat_ids = failed_seat_ids
        super().__init__(reason)


@dataclass
class CreateBookingParams:
    event_id: int
    seat_ids: list[int]
    session_id: str
    user_id: int
    total_amount: Decimal
    currency: str
    idempotency_key: str


async def create_booking(
    session: AsyncSession, params: CreateBookingParams, now: datetime
) -> BookingResponse:
    """No commit here -- the caller (app/api/routes/bookings.py) commits
    once, together with app/infra/idempotency.py's completion marker. See
    that module's docstring for why the two must share one transaction.

    Raises BookingCreationFailed if any requested seat is not currently
    HELD by `session_id` with an unexpired hold. `hold_expires_at > now`,
    strictly greater-than, matching extend_hold_at's own boundary choice:
    a hold expiring at EXACTLY `now` must not be usable to start a
    booking, the same instant the domain layer would already consider it
    gone.
    """
    rows = (
        (await session.execute(select(SeatRow).where(SeatRow.id.in_(params.seat_ids))))
        .scalars()
        .all()
    )
    by_id = {row.id: row for row in rows}
    invalid = [
        seat_id
        for seat_id in params.seat_ids
        if (row := by_id.get(seat_id)) is None
        or row.status != "HELD"
        or row.held_by_session_id != params.session_id
        or row.hold_expires_at is None
        or row.hold_expires_at <= now
    ]
    if invalid:
        raise BookingCreationFailed("seats_not_held_by_session_or_expired", failed_seat_ids=invalid)

    booking_id = (
        await session.execute(
            insert(BookingRow).returning(BookingRow.id),
            {
                "event_id": params.event_id,
                "user_id": params.user_id,
                "session_id": params.session_id,
                "status": "PENDING",
                "total_amount": params.total_amount,
                "currency": params.currency,
                "idempotency_key": params.idempotency_key,
                "seat_ids": params.seat_ids,
                "created_at": now,
            },
        )
    ).scalar_one()

    booking = (
        await session.execute(select(BookingRow).where(BookingRow.id == booking_id))
    ).scalar_one()
    return await build_booking_response(session, booking)
