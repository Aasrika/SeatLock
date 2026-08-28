"""Unit tests for app/domain/models.py -- entity validation only.

No state transitions here (see test_state_machine.py); this file is purely
about the invariants the dataclasses enforce on construction.
"""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.domain.errors import InvalidTimestamp
from app.domain.models import Booking, BookingStatus, SeatStatus, require_utc

AWARE_NOW = datetime(2026, 6, 1, tzinfo=UTC)
NAIVE_NOW = datetime(2026, 6, 1)


class TestRequireUtc:
    def test_none_is_allowed(self):
        require_utc(None, field_name="whatever")  # must not raise

    def test_aware_datetime_is_allowed(self):
        require_utc(AWARE_NOW, field_name="whatever")  # must not raise

    def test_naive_datetime_raises_invalid_timestamp(self):
        with pytest.raises(InvalidTimestamp):
            require_utc(NAIVE_NOW, field_name="whatever")


class TestSeat:
    def test_construction_with_no_hold_expiry_succeeds(self, make_seat):
        seat = make_seat()
        assert seat.status == SeatStatus.AVAILABLE

    def test_construction_with_aware_hold_expiry_succeeds(self, make_seat):
        seat = make_seat(status=SeatStatus.HELD, hold_expires_at=AWARE_NOW)
        assert seat.hold_expires_at == AWARE_NOW

    def test_construction_with_naive_hold_expiry_raises(self, make_seat):
        with pytest.raises(InvalidTimestamp):
            make_seat(status=SeatStatus.HELD, hold_expires_at=NAIVE_NOW)

    def test_seat_is_frozen(self, make_seat):
        seat = make_seat()
        with pytest.raises(dataclasses.FrozenInstanceError):
            seat.status = SeatStatus.HELD  # type: ignore[misc]


class TestBooking:
    @staticmethod
    def _make(**overrides):
        defaults = dict(
            id=1,
            event_id=1,
            user_id=1,
            session_id="session-a",
            status=BookingStatus.PENDING,
            total_amount=Decimal("42.00"),
            currency="USD",
            idempotency_key=None,
            created_at=AWARE_NOW,
            confirmed_at=None,
        )
        defaults.update(overrides)
        return Booking(**defaults)

    def test_construction_with_aware_datetimes_succeeds(self):
        booking = self._make(confirmed_at=AWARE_NOW)
        assert booking.status == BookingStatus.PENDING

    def test_construction_with_naive_created_at_raises(self):
        with pytest.raises(InvalidTimestamp):
            self._make(created_at=NAIVE_NOW)

    def test_construction_with_naive_confirmed_at_raises(self):
        with pytest.raises(InvalidTimestamp):
            self._make(confirmed_at=NAIVE_NOW)

    def test_booking_is_frozen(self):
        booking = self._make()
        with pytest.raises(dataclasses.FrozenInstanceError):
            booking.status = BookingStatus.CONFIRMED  # type: ignore[misc]
