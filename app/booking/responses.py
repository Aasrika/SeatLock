"""The one BookingResponse shape, shared by three call sites:

  1. app/api/routes/bookings.py's POST /api/bookings (creation)
  2. app/api/routes/bookings.py's POST /api/bookings/{id}/confirm
  3. workers/idempotency_reaper.py's crash recovery

Using the SAME builder in all three is what makes (3) correct without
needing to know which of (1) or (2) actually crashed: the reaper finds a
booking carrying a stale key and reconstructs "what a fresh response for
this booking, right now, would look like" -- which is exactly what (1)
and (2) themselves return, by construction, since they call this too.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.infra.tables import BookingRow


class BookingResponse(BaseModel):
    id: int
    event_id: int
    user_id: int
    session_id: str
    status: str
    seat_ids: list[int]
    total_amount: Decimal
    currency: str
    created_at: datetime
    confirmed_at: datetime | None = None


async def build_booking_response(session: AsyncSession, booking: BookingRow) -> BookingResponse:
    """`session` is unused -- kept as a parameter (rather than making this
    a plain sync function) because build_booking_response is called from
    async contexts throughout and the seat-lookup used to require it
    (an earlier version queried SeatRow.booking_id here; see
    BookingRow.seat_ids' own docstring for why that was wrong). Now that
    seat_ids lives directly on the booking row, this never actually does
    I/O -- kept async anyway so call sites don't need to change if that
    ever stops being true.
    """
    return BookingResponse(
        id=booking.id,
        event_id=booking.event_id,
        user_id=booking.user_id,
        session_id=booking.session_id,
        status=booking.status,
        seat_ids=list(booking.seat_ids),
        total_amount=booking.total_amount,
        currency=booking.currency,
        created_at=booking.created_at,
        confirmed_at=booking.confirmed_at,
    )
