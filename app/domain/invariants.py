"""I1, I2, and state-coherence checks over an in-memory seat snapshot.

Pure, synchronous assertions with no I/O -- used by unit tests,
property-based tests (Phase 4), and the load-test invariant poller alike.
"""

from __future__ import annotations

from collections.abc import Sequence
from collections.abc import Set as AbstractSet

from app.domain.errors import InvariantViolation
from app.domain.models import Seat, SeatStatus


def check_conservation(seats: Sequence[Seat], total_seats: int) -> None:
    """I2: count(AVAILABLE) + count(HELD) + count(BOOKED) == total_seats, always."""
    counts: dict[SeatStatus, int] = {status: 0 for status in SeatStatus}
    for seat in seats:
        counts[seat.status] += 1

    accounted = sum(counts.values())
    if accounted != total_seats:
        readable_counts = {status.value: n for status, n in counts.items()}
        raise InvariantViolation(
            f"I2 violated: accounted seats ({accounted}) != total_seats ({total_seats}); "
            f"counts={readable_counts}"
        )


def check_no_double_booking(seats: Sequence[Seat]) -> None:
    """I1: at most one active booking per seat.

    A single Seat record has exactly one booking_id field, so a single seat
    cannot literally carry two bookings in this representation. The one way
    this invariant can still fail in a snapshot is a duplicate record for
    the same seat id -- e.g. two rows both claiming to be the current state
    of one physical seat. (booking_id/status coherence is checked separately
    by check_state_coherence.)
    """
    seen_ids: set[int] = set()
    for seat in seats:
        if seat.id in seen_ids:
            raise InvariantViolation(f"I1 violated: duplicate record for seat {seat.id}")
        seen_ids.add(seat.id)


def check_state_coherence(seats: Sequence[Seat]) -> None:
    """Each seat's auxiliary fields must match its status:

    - AVAILABLE -> held_by_session_id, hold_expires_at, booking_id all None
    - HELD      -> held_by_session_id AND hold_expires_at set, booking_id None
    - BOOKED    -> booking_id set, held_by_session_id and hold_expires_at None
    """
    for seat in seats:
        if seat.status == SeatStatus.AVAILABLE:
            if seat.held_by_session_id is not None:
                raise InvariantViolation(
                    f"Seat {seat.id} is AVAILABLE but held_by_session_id="
                    f"{seat.held_by_session_id!r} is set"
                )
            if seat.hold_expires_at is not None:
                raise InvariantViolation(
                    f"Seat {seat.id} is AVAILABLE but hold_expires_at="
                    f"{seat.hold_expires_at!r} is set"
                )
            if seat.booking_id is not None:
                raise InvariantViolation(
                    f"Seat {seat.id} is AVAILABLE but booking_id={seat.booking_id!r} is set"
                )
        elif seat.status == SeatStatus.HELD:
            if seat.held_by_session_id is None:
                raise InvariantViolation(f"Seat {seat.id} is HELD but held_by_session_id is None")
            if seat.hold_expires_at is None:
                raise InvariantViolation(f"Seat {seat.id} is HELD but hold_expires_at is None")
            if seat.booking_id is not None:
                raise InvariantViolation(
                    f"Seat {seat.id} is HELD but booking_id={seat.booking_id!r} is set"
                )
        else:  # BOOKED
            if seat.booking_id is None:
                raise InvariantViolation(f"Seat {seat.id} is BOOKED but booking_id is None")
            if seat.held_by_session_id is not None:
                raise InvariantViolation(
                    f"Seat {seat.id} is BOOKED but held_by_session_id="
                    f"{seat.held_by_session_id!r} is set"
                )
            if seat.hold_expires_at is not None:
                raise InvariantViolation(
                    f"Seat {seat.id} is BOOKED but hold_expires_at={seat.hold_expires_at!r} is set"
                )


def check_booking_linkage(seats: Sequence[Seat], active_booking_seat_ids: AbstractSet[int]) -> None:
    """The denormalised seats.booking_id cache must never diverge from
    booking_seats, the authoritative source (see SeatRow.booking_id's
    comment in app/infra/tables.py -- both must be written in the same
    transaction).

    `active_booking_seat_ids` is the set of seat ids that currently have an
    active booking_seats row (released_at IS NULL). A seat is coherent only
    if its own view of being booked and the join table's view agree in both
    directions.
    """
    for seat in seats:
        is_active = seat.id in active_booking_seat_ids

        if seat.status == SeatStatus.BOOKED and not is_active:
            raise InvariantViolation(
                f"Seat {seat.id} is BOOKED but has no active booking_seats row"
            )
        if seat.booking_id is not None and not is_active:
            raise InvariantViolation(
                f"Seat {seat.id} has booking_id={seat.booking_id!r} but no active booking_seats row"
            )
        if is_active and seat.status != SeatStatus.BOOKED:
            raise InvariantViolation(
                f"Seat {seat.id} has an active booking_seats row but status="
                f"{seat.status.value} (expected BOOKED)"
            )
