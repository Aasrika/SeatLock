"""Booking confirmation (Phase 5) -- the second phase of SPEC.md section
5's two-phase booking flow, and the first live firing point for
oversell_blocked_total{layer="database"} (app/infra/metrics.py has
documented that counter as dormant since Phase 1, waiting for exactly
this).

Same pattern app/inventory/strategies/optimistic.py established for
acquisition, reused here for confirmation: the domain layer
(app/domain/state_machine.py's confirm(), app/domain/
booking_state_machine.py's confirm_booking()) is the AUTHORITATIVE
legality check -- CLAUDE.md rule 3 requires going through it for any
seat status change -- but its result is never written back directly.
Instead, a single atomic conditional UPDATE whose WHERE clause mirrors
exactly what the domain call just validated is the actual persistence
mechanism, so the check-then-write has no window for something else to
invalidate it in between. See optimistic.py's own module docstring for
the full reasoning; it applies identically here.

No commit anywhere in this module -- app/api/routes/bookings.py's
confirm route commits once, together with app/infra/idempotency.py's
completion marker. See that module's docstring for why.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import insert, select
from sqlalchemy import update as sa_update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.booking.responses import BookingResponse, build_booking_response
from app.domain import state_machine
from app.domain.booking_state_machine import confirm_booking as domain_confirm_booking
from app.domain.errors import HoldExpired, IllegalBookingTransition, IllegalTransition, NotHoldOwner
from app.infra.mappers import booking_to_domain, seat_to_domain
from app.infra.metrics import oversell_blocked_total
from app.infra.tables import BookingRow, BookingSeatRow, OutboxRow, SeatRow


class BookingNotFound(Exception):
    def __init__(self, booking_id: int) -> None:
        self.booking_id = booking_id
        super().__init__(f"booking {booking_id} not found")


class ConfirmFailed(Exception):
    """The route translates this into a 409 -- same "clean rejection, not
    a retry-worthy error" treatment as extend_hold_at's False return and
    every domain-level rejection elsewhere in this codebase.
    """

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


async def load_booking(session: AsyncSession, booking_id: int) -> BookingRow | None:
    """Used both by the confirm route (to learn user_id BEFORE starting
    the idempotency check -- see app/infra/idempotency.py's
    begin_idempotent_request, which needs it) and internally by
    confirm_booking_transaction below. Two separate reads of the same
    row within one request handling introduces no race: nothing between
    them mutates it.
    """
    return (
        await session.execute(select(BookingRow).where(BookingRow.id == booking_id))
    ).scalar_one_or_none()


async def attempt_confirm_write(
    session: AsyncSession,
    booking_id: int,
    seat_ids: list[int],
    session_id: str,
    now: datetime,
    *,
    idempotency_key: str | None = None,
) -> bool:
    """The actual write, shared by confirm_booking_transaction (below,
    the synchronous HTTP path) and workers/payment_worker.py (the
    asynchronous webhook-driven path) -- both need EXACTLY this same
    atomic transition and neither should reimplement it separately.
    Callers still differ in what a False return MEANS (the route raises
    ConfirmFailed for a clean 409; the payment worker treats it as the
    late-success case, SPEC.md section 7), so this only performs the
    write and reports success/failure -- it never raises for "the hold
    was no longer valid," only for genuine unexpected DB errors.

    `idempotency_key`: None for the payment-worker caller, which has no
    client-supplied key to record -- BookingRow.idempotency_key is left
    untouched in that case rather than cleared, since its own docstring
    describes it as "whichever operation MOST RECENTLY wrote this row,"
    and a webhook-driven confirm recording nothing is more honest than
    recording a fabricated key.

    One atomic conditional UPDATE, WHERE clause AND values mirroring
    exactly what state_machine.confirm() validates for every seat (HELD,
    held by THIS session, not expired -> BOOKED, booking_id set, hold
    fields cleared) -- see optimistic.py's module docstring for why the
    domain call and the raw UPDATE must agree on both the check and the
    write. Rowcount less than requested means something changed between
    the caller's read and this UPDATE -- roll back rather than partially
    confirm, same all-or-nothing rule every multi-seat acquire in this
    codebase follows. booking_id is set HERE, not at creation (see
    BookingRow.seat_ids' docstring) -- this is the one place a seat's
    booking_id legitimately becomes non-NULL while HELD... becoming
    BOOKED in the same UPDATE, so check_state_coherence's HELD-implies-
    no-booking_id rule is never observed to be violated, even
    transiently.
    """
    result = await session.execute(
        sa_update(SeatRow)
        .where(
            SeatRow.id.in_(seat_ids),
            SeatRow.status == "HELD",
            SeatRow.held_by_session_id == session_id,
            SeatRow.hold_expires_at > now,
        )
        .values(
            status="BOOKED",
            version=SeatRow.version + 1,
            held_by_session_id=None,
            hold_expires_at=None,
            booking_id=booking_id,
            updated_at=now,
        )
    )
    if result.rowcount != len(seat_ids):
        await session.rollback()
        return False

    # First live firing point for oversell_blocked_total{layer="database"}
    # (app/infra/metrics.py has documented it as dormant since Phase 1,
    # waiting for exactly this insert to exist). This should NEVER raise
    # in correct operation -- the conditional UPDATE above already
    # guarantees these seats were exclusively HELD by this session the
    # instant it ran -- but the partial unique index on booking_seats is
    # the DB-level last line of defence SPEC.md section 3 specifies, and
    # if it ever fires, application logic has a bug that let two bookings
    # reach this point for the same seat.
    try:
        await session.execute(
            insert(BookingSeatRow), [{"booking_id": booking_id, "seat_id": sid} for sid in seat_ids]
        )
    except IntegrityError:
        await session.rollback()
        oversell_blocked_total.labels(layer="database").inc()
        return False

    # Booking status update -- guarded by status='PENDING' too, defence
    # in depth alongside the seat UPDATE's own guard above.
    values: dict[str, object] = {"status": "CONFIRMED", "confirmed_at": now}
    if idempotency_key is not None:
        values["idempotency_key"] = idempotency_key
    result = await session.execute(
        sa_update(BookingRow)
        .where(BookingRow.id == booking_id, BookingRow.status == "PENDING")
        .values(**values)
    )
    if result.rowcount != 1:
        await session.rollback()
        return False

    await session.execute(
        insert(OutboxRow),
        {
            "aggregate_id": f"booking:{booking_id}",
            "event_type": "booking.confirmed",
            "payload": {"booking_id": booking_id, "seat_ids": seat_ids},
            "created_at": now,
        },
    )
    return True


async def confirm_booking_transaction(
    session: AsyncSession, booking_id: int, session_id: str, idempotency_key: str, now: datetime
) -> BookingResponse:
    booking = await load_booking(session, booking_id)
    if booking is None:
        raise BookingNotFound(booking_id)

    seat_ids = list(booking.seat_ids)
    seat_rows = (
        (await session.execute(select(SeatRow).where(SeatRow.id.in_(seat_ids)))).scalars().all()
    )
    if len(seat_rows) != len(seat_ids):
        # Seats are never hard-deleted in this design (see SeatRow's own
        # docstring) -- this should be structurally impossible, kept as
        # an explicit check rather than silently validating a subset.
        raise ConfirmFailed("booking_seats_missing")

    # Authoritative legality check, one call per seat plus one for the
    # booking itself -- validated BEFORE any write is issued, so a
    # rejection here leaves the database untouched.
    try:
        for seat_row in seat_rows:
            state_machine.confirm(seat_to_domain(seat_row), session_id, booking_id, now)
        domain_confirm_booking(booking_to_domain(booking), now)
    except (IllegalTransition, NotHoldOwner, HoldExpired) as exc:
        raise ConfirmFailed(f"seat_confirm_rejected: {exc}") from exc
    except IllegalBookingTransition as exc:
        raise ConfirmFailed(f"booking_confirm_rejected: {exc}") from exc

    ok = await attempt_confirm_write(
        session, booking_id, seat_ids, session_id, now, idempotency_key=idempotency_key
    )
    if not ok:
        raise ConfirmFailed("hold_no_longer_valid")

    confirmed = await load_booking(session, booking_id)
    assert confirmed is not None  # just updated it in this same transaction
    return await build_booking_response(session, confirmed)
