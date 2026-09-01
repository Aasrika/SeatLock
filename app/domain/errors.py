"""All domain-layer exceptions.

Every error the domain layer can raise lives in this one module, so any
caller can catch `DomainError` from a single import regardless of which
specific rule was broken. Do not scatter domain exceptions across other
modules -- if a new failure mode needs a new exception type, it goes here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.domain.models import BookingStatus, SeatStatus


class DomainError(Exception):
    """Base class for all domain-layer errors."""


class InvalidTimestamp(DomainError):
    """A naive (non-timezone-aware) datetime was used somewhere in the domain.

    Every timestamp in this system must be UTC-aware (see CLAUDE.md
    conventions) -- hold expiry is the single worst place for a naive
    datetime to hide.
    """

    def __init__(self, field_name: str, value: object) -> None:
        self.field_name = field_name
        self.value = value
        super().__init__(f"{field_name} must be timezone-aware (UTC); got naive value: {value!r}")


class IllegalTransition(DomainError):
    """A state machine function was called on a seat in an unsupported status.

    Raised for transitions that are structurally illegal, independent of the
    more specific reasons below (SeatUnavailable, HoldExpired, NotHoldOwner).
    """

    def __init__(self, seat_id: int, from_status: SeatStatus, to_status: SeatStatus) -> None:
        self.seat_id = seat_id
        self.from_status = from_status
        self.to_status = to_status
        super().__init__(
            f"Seat {seat_id}: cannot transition from {from_status.value} to {to_status.value}"
        )


class SeatUnavailable(DomainError):
    """hold() was called on a seat that cannot be held right now.

    Either it is HELD by an unexpired hold, or it is already BOOKED.
    """

    def __init__(self, seat_id: int, reason: str) -> None:
        self.seat_id = seat_id
        self.reason = reason
        super().__init__(f"Seat {seat_id} unavailable: {reason}")


class HoldExpired(DomainError):
    """confirm() was called against a hold that has already expired."""

    def __init__(self, seat_id: int) -> None:
        self.seat_id = seat_id
        super().__init__(f"Seat {seat_id}: hold has expired")


class NotHoldOwner(DomainError):
    """confirm() was called by a session that does not hold the seat."""

    def __init__(
        self, seat_id: int, attempted_session_id: str, actual_session_id: str | None
    ) -> None:
        self.seat_id = seat_id
        self.attempted_session_id = attempted_session_id
        self.actual_session_id = actual_session_id
        super().__init__(
            f"Seat {seat_id}: session {attempted_session_id!r} is not the hold owner "
            f"(held by {actual_session_id!r})"
        )


class InvariantViolation(DomainError):
    """A system-wide invariant (I1, I2, or state coherence) does not hold
    over a given seat snapshot. See app/domain/invariants.py.
    """


class IllegalBookingTransition(DomainError):
    """A booking_state_machine.py function was called on a booking in an
    unsupported status. The booking-level sibling of IllegalTransition
    above -- kept as a separate type rather than reusing IllegalTransition
    (whose fields/message are seat-shaped: seat_id, SeatStatus) because a
    booking and a seat are different aggregates with different id spaces,
    and conflating their exceptions would make a caller's `except` clause
    ambiguous about which kind of illegal transition it just caught.
    """

    def __init__(
        self, booking_id: int, from_status: BookingStatus, to_status: BookingStatus
    ) -> None:
        self.booking_id = booking_id
        self.from_status = from_status
        self.to_status = to_status
        super().__init__(
            f"Booking {booking_id}: cannot transition from {from_status.value} to {to_status.value}"
        )
