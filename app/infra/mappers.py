"""Conversions between app/domain/'s pure dataclasses and app/infra/tables.py's
SQLAlchemy rows.

This is the ONLY place the two representations meet. They are kept
deliberately separate: the domain model must stay frozen, I/O-free, and
session-independent so it can be unit-tested with no containers running and
reasoned about without a database in front of it, while an ORM row is
mutable and bound to whatever session loaded it. Collapsing the two would
leak a live DB session into app/domain/ and violate CLAUDE.md rule 2.
"""

from __future__ import annotations

from app.domain.models import Booking, BookingStatus, Seat, SeatStatus
from app.infra.tables import BookingRow, SeatRow


def seat_to_domain(row: SeatRow) -> Seat:
    """Build a domain Seat snapshot from a SeatRow."""
    return Seat(
        id=row.id,
        event_id=row.event_id,
        status=SeatStatus(row.status),
        version=row.version,
        held_by_session_id=row.held_by_session_id,
        hold_expires_at=row.hold_expires_at,
        booking_id=row.booking_id,
    )


def seat_apply(row: SeatRow, seat: Seat) -> None:
    """Write a domain Seat's mutable state back onto an existing SeatRow.

    Only the fields a state-machine transition can change are written --
    identity fields (id, event_id) are never touched here.
    """
    row.status = seat.status.value
    row.version = seat.version
    row.held_by_session_id = seat.held_by_session_id
    row.hold_expires_at = seat.hold_expires_at
    row.booking_id = seat.booking_id


def booking_to_domain(row: BookingRow) -> Booking:
    """Build a domain Booking snapshot from a BookingRow."""
    return Booking(
        id=row.id,
        event_id=row.event_id,
        user_id=row.user_id,
        session_id=row.session_id,
        status=BookingStatus(row.status),
        total_amount=row.total_amount,
        currency=row.currency,
        idempotency_key=row.idempotency_key,
        created_at=row.created_at,
        confirmed_at=row.confirmed_at,
    )


def booking_apply(row: BookingRow, booking: Booking) -> None:
    """Write a domain Booking's mutable state back onto an existing BookingRow.

    Only `status` and `confirmed_at` change over a booking's lifecycle --
    everything else (amount, currency, idempotency key, who/when created)
    is fixed at creation and never revised by this function.
    """
    row.status = booking.status.value
    row.confirmed_at = booking.confirmed_at
