"""The seat state machine.

This module is the ONLY place a Seat's status may change (CLAUDE.md rule 3).
Every function here is pure: given a Seat and explicit inputs, it returns a
brand-new Seat via `dataclasses.replace`, or raises. Nothing in this module
reads the clock -- `now` is always an explicit parameter, never
`datetime.now()` -- so every boundary condition (a hold expiring mid-request,
two transitions racing at the same instant) is exactly reproducible in a
test with no sleeps, no mocking, and no containers.

    AVAILABLE --hold--> HELD --confirm--> BOOKED
        ^                 |                  |
        |                 |                  |
        +----expire-------+                  |
        +----------release-------------------+
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta

from app.domain.errors import HoldExpired, IllegalTransition, NotHoldOwner, SeatUnavailable
from app.domain.models import Seat, SeatStatus, require_utc


def is_hold_expired(seat: Seat, now: datetime) -> bool:
    """The single definition of "expired" for a hold.

    hold() and confirm() both call this rather than comparing
    `hold_expires_at` inline -- a duplicated comparison would diverge under
    maintenance and create a window where a seat is neither confirmable nor
    reclaimable (or, worse, confirmable by two sessions at once).
    """
    return seat.hold_expires_at is not None and seat.hold_expires_at <= now


def hold(seat: Seat, session_id: str, now: datetime, hold_duration: timedelta) -> Seat:
    """Transition a seat to HELD.

    Legal from AVAILABLE, and also from HELD if the existing hold has
    already expired -- expiry is evaluated lazily at read/write time, the
    sweeper is only eventual cleanup (see SPEC.md section 5). An expired
    hold is reclaimable by a different session.
    """
    require_utc(now, field_name="now")
    if not isinstance(hold_duration, timedelta) or hold_duration <= timedelta(0):
        raise ValueError(f"hold_duration must be a positive timedelta, got {hold_duration!r}")

    if seat.status == SeatStatus.AVAILABLE:
        pass
    elif seat.status == SeatStatus.HELD:
        if not is_hold_expired(seat, now):
            raise SeatUnavailable(seat.id, "already held and not yet expired")
    else:  # BOOKED
        raise SeatUnavailable(seat.id, "already booked")

    # Every field below is set fresh -- nothing is carried forward from a
    # previous holder, even when reclaiming an expired HELD seat.
    return replace(
        seat,
        status=SeatStatus.HELD,
        version=seat.version + 1,
        held_by_session_id=session_id,
        hold_expires_at=now + hold_duration,
        booking_id=None,
    )


def confirm(seat: Seat, session_id: str, booking_id: int, now: datetime) -> Seat:
    """Transition a HELD seat to BOOKED.

    Only the session that holds the seat may confirm it, and only before the
    hold expires.
    """
    require_utc(now, field_name="now")

    if seat.status != SeatStatus.HELD:
        raise IllegalTransition(seat.id, seat.status, SeatStatus.BOOKED)
    if seat.held_by_session_id != session_id:
        raise NotHoldOwner(seat.id, session_id, seat.held_by_session_id)
    if is_hold_expired(seat, now):
        raise HoldExpired(seat.id)

    return replace(
        seat,
        status=SeatStatus.BOOKED,
        version=seat.version + 1,
        held_by_session_id=None,
        hold_expires_at=None,
        booking_id=booking_id,
    )


def expire(seat: Seat, now: datetime) -> Seat:
    """Transition an expired HELD seat back to AVAILABLE.

    This is what the sweeper calls in bulk. It is legal only when the seat
    is actually HELD and its hold has actually expired.
    """
    require_utc(now, field_name="now")

    if seat.status != SeatStatus.HELD or not is_hold_expired(seat, now):
        raise IllegalTransition(seat.id, seat.status, SeatStatus.AVAILABLE)

    return replace(
        seat,
        status=SeatStatus.AVAILABLE,
        version=seat.version + 1,
        held_by_session_id=None,
        hold_expires_at=None,
        booking_id=None,
    )


def release(seat: Seat, now: datetime) -> Seat:
    """Return a HELD or BOOKED seat to AVAILABLE (cancellation / refund path)."""
    require_utc(now, field_name="now")

    if seat.status not in (SeatStatus.HELD, SeatStatus.BOOKED):
        raise IllegalTransition(seat.id, seat.status, SeatStatus.AVAILABLE)

    return replace(
        seat,
        status=SeatStatus.AVAILABLE,
        version=seat.version + 1,
        held_by_session_id=None,
        hold_expires_at=None,
        booking_id=None,
    )
