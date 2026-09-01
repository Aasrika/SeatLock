"""The booking state machine (Phase 5).

Sibling to app/domain/state_machine.py, not an extension of it -- that
module's docstring is specifically about SEAT status ("the ONLY place a
Seat's status may change", CLAUDE.md rule 3, which is scoped to seats).
Booking status was never covered by that rule, but it has the identical
illegal-transition risk seat status does (SPEC.md section 7's out-of-order
webhook case: a late payment.succeeded arriving after a payment.refunded
must not resurrect a refunded booking to CONFIRMED), and until this module
existed nothing guarded it -- app/infra/mappers.py's booking_to_domain/
booking_apply have existed since Phase 0 with no corresponding transition
functions to call between them. This module is that guardian.

Same conventions as state_machine.py: every function is pure (returns a
new Booking via dataclasses.replace, or raises IllegalBookingTransition),
`now` is always an explicit parameter, nothing here does I/O.

    PENDING --confirm--> CONFIRMED --refund--> REFUNDED
       |
       +--require_refund--> REFUND_REQUIRED   (terminal: late-success,
                                                see SPEC.md section 7)
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime

from app.domain.errors import IllegalBookingTransition
from app.domain.models import Booking, BookingStatus, require_utc


def confirm_booking(booking: Booking, now: datetime) -> Booking:
    """PENDING -> CONFIRMED. Legal only from PENDING.

    Called once the booking's seats have themselves been confirmed
    (app/domain/state_machine.py's confirm(), one call per seat) --
    this is the booking-level half of the same transition, kept as a
    separate pure function so illegal cases (a booking that is already
    CONFIRMED, REFUNDED, or REFUND_REQUIRED) are rejected the same
    explicit way seat transitions are, rather than by an ad hoc `if` at
    each call site.
    """
    require_utc(now, field_name="now")
    if booking.status != BookingStatus.PENDING:
        raise IllegalBookingTransition(booking.id, booking.status, BookingStatus.CONFIRMED)
    return replace(booking, status=BookingStatus.CONFIRMED, confirmed_at=now)


def require_refund(booking: Booking, now: datetime) -> Booking:
    """PENDING -> REFUND_REQUIRED. The late-success case (SPEC.md section
    7): a payment.succeeded webhook arrived after the booking's hold had
    already expired and the seat was resold to someone else. Legal only
    from PENDING -- a booking that already reached CONFIRMED needs
    refund_booking() instead (a *different* later event, e.g.
    payment.refunded, undoing a booking that DID succeed); one that is
    already REFUND_REQUIRED, REFUNDED, or CANCELLED does not need this
    called again.

    Terminal like CANCELLED/REFUNDED: nothing transitions OUT of
    REFUND_REQUIRED in this system. A human/finance process takes over
    from here (SPEC.md section 7 says "alert raised") -- modelling
    that resolution as a further automated state transition is out of
    scope for this phase.
    """
    require_utc(now, field_name="now")
    if booking.status != BookingStatus.PENDING:
        raise IllegalBookingTransition(booking.id, booking.status, BookingStatus.REFUND_REQUIRED)
    return replace(booking, status=BookingStatus.REFUND_REQUIRED)


def refund_booking(booking: Booking, now: datetime) -> Booking:
    """CONFIRMED -> REFUNDED. Legal only from CONFIRMED.

    This is what rejects SPEC.md section 7's out-of-order case: a
    payment.refunded event correctly moves a CONFIRMED booking here, but
    a LATE payment.succeeded arriving afterward, attempting
    confirm_booking() on what is now a REFUNDED booking, must raise
    IllegalBookingTransition rather than resurrecting it -- see
    workers/payment_worker.py, which logs and counts that rejection
    without applying it.
    """
    require_utc(now, field_name="now")
    if booking.status != BookingStatus.CONFIRMED:
        raise IllegalBookingTransition(booking.id, booking.status, BookingStatus.REFUNDED)
    return replace(booking, status=BookingStatus.REFUNDED)
