"""Unit tests for app/domain/invariants.py -- I1, I2, and state coherence
over in-memory seat snapshots. No I/O.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.domain.errors import InvariantViolation
from app.domain.invariants import (
    check_booking_linkage,
    check_conservation,
    check_no_double_booking,
    check_state_coherence,
)
from app.domain.models import SeatStatus

NOW = datetime(2026, 6, 1, tzinfo=UTC)


class TestCheckConservation:
    def test_passes_when_counts_match_total(self, make_seat):
        seats = [
            make_seat(id=1, status=SeatStatus.AVAILABLE),
            make_seat(id=2, status=SeatStatus.HELD, held_by_session_id="s", hold_expires_at=NOW),
            make_seat(id=3, status=SeatStatus.BOOKED, booking_id=1),
        ]
        check_conservation(seats, total_seats=3)  # must not raise

    def test_raises_when_seats_missing(self, make_seat):
        seats = [make_seat(id=1, status=SeatStatus.AVAILABLE)]
        with pytest.raises(InvariantViolation):
            check_conservation(seats, total_seats=3)

    def test_raises_when_seats_overcounted(self, make_seat):
        seats = [make_seat(id=i, status=SeatStatus.AVAILABLE) for i in range(5)]
        with pytest.raises(InvariantViolation):
            check_conservation(seats, total_seats=3)


class TestCheckNoDoubleBooking:
    def test_passes_with_unique_seat_ids(self, make_seat):
        seats = [make_seat(id=1), make_seat(id=2, status=SeatStatus.BOOKED, booking_id=1)]
        check_no_double_booking(seats)  # must not raise

    def test_raises_on_duplicate_seat_id(self, make_seat):
        seats = [make_seat(id=1), make_seat(id=1, status=SeatStatus.BOOKED, booking_id=1)]
        with pytest.raises(InvariantViolation):
            check_no_double_booking(seats)


class TestCheckStateCoherence:
    def test_passes_for_coherent_available_seat(self, make_seat):
        check_state_coherence([make_seat(status=SeatStatus.AVAILABLE)])

    def test_passes_for_coherent_held_seat(self, make_seat):
        seat = make_seat(status=SeatStatus.HELD, held_by_session_id="s", hold_expires_at=NOW)
        check_state_coherence([seat])

    def test_passes_for_coherent_booked_seat(self, make_seat):
        seat = make_seat(status=SeatStatus.BOOKED, booking_id=1)
        check_state_coherence([seat])

    def test_available_with_held_by_session_id_raises(self, make_seat):
        seat = make_seat(status=SeatStatus.AVAILABLE, held_by_session_id="s")
        with pytest.raises(InvariantViolation):
            check_state_coherence([seat])

    def test_available_with_hold_expires_at_raises(self, make_seat):
        seat = make_seat(status=SeatStatus.AVAILABLE, hold_expires_at=NOW)
        with pytest.raises(InvariantViolation):
            check_state_coherence([seat])

    def test_available_with_booking_id_raises(self, make_seat):
        seat = make_seat(status=SeatStatus.AVAILABLE, booking_id=1)
        with pytest.raises(InvariantViolation):
            check_state_coherence([seat])

    def test_held_without_session_id_raises(self, make_seat):
        seat = make_seat(status=SeatStatus.HELD, hold_expires_at=NOW)
        with pytest.raises(InvariantViolation):
            check_state_coherence([seat])

    def test_held_without_expiry_raises(self, make_seat):
        seat = make_seat(status=SeatStatus.HELD, held_by_session_id="s")
        with pytest.raises(InvariantViolation):
            check_state_coherence([seat])

    def test_held_with_booking_id_raises(self, make_seat):
        seat = make_seat(
            status=SeatStatus.HELD, held_by_session_id="s", hold_expires_at=NOW, booking_id=1
        )
        with pytest.raises(InvariantViolation):
            check_state_coherence([seat])

    def test_booked_without_booking_id_raises(self, make_seat):
        seat = make_seat(status=SeatStatus.BOOKED)
        with pytest.raises(InvariantViolation):
            check_state_coherence([seat])

    def test_booked_with_held_by_session_id_raises(self, make_seat):
        seat = make_seat(status=SeatStatus.BOOKED, booking_id=1, held_by_session_id="s")
        with pytest.raises(InvariantViolation):
            check_state_coherence([seat])

    def test_booked_with_hold_expires_at_raises(self, make_seat):
        seat = make_seat(status=SeatStatus.BOOKED, booking_id=1, hold_expires_at=NOW)
        with pytest.raises(InvariantViolation):
            check_state_coherence([seat])


class TestCheckBookingLinkage:
    def test_passes_when_booked_seat_is_in_active_set(self, make_seat):
        seat = make_seat(id=1, status=SeatStatus.BOOKED, booking_id=10)
        check_booking_linkage([seat], active_booking_seat_ids={1})  # must not raise

    def test_passes_when_available_seat_is_absent_from_active_set(self, make_seat):
        seat = make_seat(id=1, status=SeatStatus.AVAILABLE)
        check_booking_linkage([seat], active_booking_seat_ids=set())  # must not raise

    def test_booked_seat_missing_from_active_set_raises(self, make_seat):
        # seats.booking_id/status say BOOKED, but booking_seats disagrees --
        # exactly the divergence the denormalised cache must never allow.
        seat = make_seat(id=1, status=SeatStatus.BOOKED, booking_id=10)
        with pytest.raises(InvariantViolation):
            check_booking_linkage([seat], active_booking_seat_ids=set())

    def test_booking_id_set_but_seat_missing_from_active_set_raises(self, make_seat):
        # Even independent of status, a non-None booking_id with no active
        # booking_seats row is the cache pointing at nothing authoritative.
        seat = make_seat(id=1, status=SeatStatus.HELD, booking_id=10)
        with pytest.raises(InvariantViolation):
            check_booking_linkage([seat], active_booking_seat_ids=set())

    def test_active_set_membership_without_booked_status_raises(self, make_seat):
        # booking_seats says this seat is actively booked, but the seat's
        # own status contradicts it.
        seat = make_seat(id=1, status=SeatStatus.AVAILABLE)
        with pytest.raises(InvariantViolation):
            check_booking_linkage([seat], active_booking_seat_ids={1})
