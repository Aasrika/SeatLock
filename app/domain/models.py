"""Domain entities: Seat, Booking, and their status enums.

Pure data -- zero behavior beyond validation. All status transitions happen
in state_machine.py; nothing here may mutate a Seat's status directly (see
CLAUDE.md rule 3).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from app.domain.errors import InvalidTimestamp


def require_utc(dt: datetime | None, *, field_name: str) -> None:
    """Reject naive datetimes. Every timestamp in this system is UTC-aware.

    A no-op for `None` -- callers decide separately whether a field is
    required.
    """
    if dt is not None and dt.tzinfo is None:
        raise InvalidTimestamp(field_name, dt)


class SeatStatus(StrEnum):
    AVAILABLE = "AVAILABLE"
    HELD = "HELD"
    BOOKED = "BOOKED"


class BookingStatus(StrEnum):
    PENDING = "PENDING"
    CONFIRMED = "CONFIRMED"
    CANCELLED = "CANCELLED"
    REFUNDED = "REFUNDED"


@dataclass(frozen=True, slots=True)
class Seat:
    """A single seat's current state. See SPEC.md section 3 for the schema."""

    id: int
    event_id: int
    status: SeatStatus
    version: int
    held_by_session_id: str | None = None
    hold_expires_at: datetime | None = None
    booking_id: int | None = None

    def __post_init__(self) -> None:
        require_utc(self.hold_expires_at, field_name="hold_expires_at")


@dataclass(frozen=True, slots=True)
class Booking:
    """A booking spanning one or more seats. See SPEC.md section 3."""

    id: int
    event_id: int
    user_id: int
    session_id: str
    status: BookingStatus
    total_amount: Decimal
    currency: str
    idempotency_key: str | None
    created_at: datetime
    confirmed_at: datetime | None = None

    def __post_init__(self) -> None:
        require_utc(self.created_at, field_name="created_at")
        require_utc(self.confirmed_at, field_name="confirmed_at")
