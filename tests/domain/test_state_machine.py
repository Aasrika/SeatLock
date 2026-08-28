"""Exhaustive tests for app/domain/state_machine.py.

Every legal transition, every illegal transition (with the exact exception
type), and the expiry boundary down to the microsecond. No containers, no
mocking, no I/O -- if any of this needed a database running, the domain
boundary would already be broken.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.domain.errors import (
    HoldExpired,
    IllegalTransition,
    InvalidTimestamp,
    NotHoldOwner,
    SeatUnavailable,
)
from app.domain.models import SeatStatus
from app.domain.state_machine import confirm, expire, hold, is_hold_expired, release

NOW = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
HOLD_DURATION = timedelta(minutes=8)
NAIVE_NOW = datetime(2026, 6, 1, 12, 0, 0)  # no tzinfo -- deliberately invalid


# ---------------------------------------------------------------------------
# hold()
# ---------------------------------------------------------------------------


class TestHold:
    def test_hold_available_seat_succeeds(self, make_seat):
        seat = make_seat(status=SeatStatus.AVAILABLE, version=3)
        result = hold(seat, "session-a", NOW, HOLD_DURATION)

        assert result.status == SeatStatus.HELD
        assert result.version == 4
        assert result.held_by_session_id == "session-a"
        assert result.hold_expires_at == NOW + HOLD_DURATION
        assert result.booking_id is None

    def test_hold_unexpired_held_seat_raises_seat_unavailable(self, make_seat):
        seat = make_seat(
            status=SeatStatus.HELD,
            held_by_session_id="session-a",
            hold_expires_at=NOW + timedelta(minutes=1),
        )
        with pytest.raises(SeatUnavailable):
            hold(seat, "session-b", NOW, HOLD_DURATION)

    def test_hold_booked_seat_always_raises_seat_unavailable(self, make_seat):
        seat = make_seat(status=SeatStatus.BOOKED, booking_id=99)
        with pytest.raises(SeatUnavailable):
            hold(seat, "session-a", NOW, HOLD_DURATION)

    def test_reclaiming_expired_hold_by_different_session_succeeds(self, make_seat):
        seat = make_seat(
            status=SeatStatus.HELD,
            version=5,
            held_by_session_id="session-old",
            hold_expires_at=NOW - timedelta(seconds=1),
        )
        result = hold(seat, "session-new", NOW, HOLD_DURATION)

        assert result.status == SeatStatus.HELD
        assert result.version == 6
        assert result.held_by_session_id == "session-new"
        assert result.hold_expires_at == NOW + HOLD_DURATION

    def test_reclaim_does_not_carry_forward_any_field_from_previous_holder(self, make_seat):
        seat = make_seat(
            status=SeatStatus.HELD,
            version=1,
            held_by_session_id="session-old",
            hold_expires_at=NOW - timedelta(seconds=1),
        )
        result = hold(seat, "session-new", NOW, HOLD_DURATION)

        assert result.held_by_session_id == "session-new"
        assert result.hold_expires_at == NOW + HOLD_DURATION
        assert result.booking_id is None

    @pytest.mark.parametrize("bad_duration", [timedelta(0), timedelta(seconds=-1)])
    def test_hold_rejects_non_positive_hold_duration(self, make_seat, bad_duration):
        seat = make_seat(status=SeatStatus.AVAILABLE)
        with pytest.raises(ValueError):
            hold(seat, "session-a", NOW, bad_duration)

    def test_hold_rejects_naive_now(self, make_seat):
        seat = make_seat(status=SeatStatus.AVAILABLE)
        with pytest.raises(InvalidTimestamp):
            hold(seat, "session-a", NAIVE_NOW, HOLD_DURATION)


# ---------------------------------------------------------------------------
# confirm()
# ---------------------------------------------------------------------------


class TestConfirm:
    def test_confirm_held_seat_by_owner_before_expiry_succeeds(self, make_seat):
        seat = make_seat(
            status=SeatStatus.HELD,
            version=2,
            held_by_session_id="session-a",
            hold_expires_at=NOW + timedelta(minutes=1),
        )
        result = confirm(seat, "session-a", booking_id=42, now=NOW)

        assert result.status == SeatStatus.BOOKED
        assert result.version == 3
        assert result.booking_id == 42
        assert result.held_by_session_id is None
        assert result.hold_expires_at is None

    def test_confirm_from_available_raises_illegal_transition(self, make_seat):
        seat = make_seat(status=SeatStatus.AVAILABLE)
        with pytest.raises(IllegalTransition):
            confirm(seat, "session-a", booking_id=42, now=NOW)

    def test_confirm_from_booked_raises_illegal_transition(self, make_seat):
        seat = make_seat(status=SeatStatus.BOOKED, booking_id=1)
        with pytest.raises(IllegalTransition):
            confirm(seat, "session-a", booking_id=42, now=NOW)

    def test_confirm_by_non_owning_session_raises_not_hold_owner(self, make_seat):
        seat = make_seat(
            status=SeatStatus.HELD,
            held_by_session_id="session-a",
            hold_expires_at=NOW + timedelta(minutes=1),
        )
        with pytest.raises(NotHoldOwner):
            confirm(seat, "session-b", booking_id=42, now=NOW)

    def test_confirm_expired_hold_raises_hold_expired(self, make_seat):
        seat = make_seat(
            status=SeatStatus.HELD,
            held_by_session_id="session-a",
            hold_expires_at=NOW - timedelta(seconds=1),
        )
        with pytest.raises(HoldExpired):
            confirm(seat, "session-a", booking_id=42, now=NOW)

    def test_confirm_rejects_naive_now(self, make_seat):
        seat = make_seat(
            status=SeatStatus.HELD,
            held_by_session_id="session-a",
            hold_expires_at=NOW + timedelta(minutes=1),
        )
        with pytest.raises(InvalidTimestamp):
            confirm(seat, "session-a", booking_id=42, now=NAIVE_NOW)


# ---------------------------------------------------------------------------
# expire()
# ---------------------------------------------------------------------------


class TestExpire:
    def test_expire_held_expired_seat_succeeds(self, make_seat):
        seat = make_seat(
            status=SeatStatus.HELD,
            version=1,
            held_by_session_id="session-a",
            hold_expires_at=NOW - timedelta(seconds=1),
        )
        result = expire(seat, NOW)

        assert result.status == SeatStatus.AVAILABLE
        assert result.version == 2
        assert result.held_by_session_id is None
        assert result.hold_expires_at is None
        assert result.booking_id is None

    def test_expire_not_yet_expired_hold_raises_illegal_transition(self, make_seat):
        seat = make_seat(
            status=SeatStatus.HELD,
            held_by_session_id="session-a",
            hold_expires_at=NOW + timedelta(seconds=1),
        )
        with pytest.raises(IllegalTransition):
            expire(seat, NOW)

    def test_expire_from_available_raises_illegal_transition(self, make_seat):
        seat = make_seat(status=SeatStatus.AVAILABLE)
        with pytest.raises(IllegalTransition):
            expire(seat, NOW)

    def test_expire_from_booked_raises_illegal_transition(self, make_seat):
        seat = make_seat(status=SeatStatus.BOOKED, booking_id=1)
        with pytest.raises(IllegalTransition):
            expire(seat, NOW)

    def test_expire_rejects_naive_now(self, make_seat):
        seat = make_seat(
            status=SeatStatus.HELD,
            held_by_session_id="session-a",
            hold_expires_at=NOW - timedelta(seconds=1),
        )
        with pytest.raises(InvalidTimestamp):
            expire(seat, NAIVE_NOW)


# ---------------------------------------------------------------------------
# release()
# ---------------------------------------------------------------------------


class TestRelease:
    def test_release_held_seat_succeeds(self, make_seat):
        seat = make_seat(
            status=SeatStatus.HELD,
            version=1,
            held_by_session_id="session-a",
            hold_expires_at=NOW + timedelta(minutes=1),
        )
        result = release(seat, NOW)

        assert result.status == SeatStatus.AVAILABLE
        assert result.version == 2
        assert result.held_by_session_id is None
        assert result.hold_expires_at is None
        assert result.booking_id is None

    def test_release_booked_seat_succeeds(self, make_seat):
        seat = make_seat(status=SeatStatus.BOOKED, version=4, booking_id=7)
        result = release(seat, NOW)

        assert result.status == SeatStatus.AVAILABLE
        assert result.version == 5
        assert result.booking_id is None

    def test_release_from_available_raises_illegal_transition(self, make_seat):
        seat = make_seat(status=SeatStatus.AVAILABLE)
        with pytest.raises(IllegalTransition):
            release(seat, NOW)

    def test_release_rejects_naive_now(self, make_seat):
        seat = make_seat(status=SeatStatus.BOOKED, booking_id=1)
        with pytest.raises(InvalidTimestamp):
            release(seat, NAIVE_NOW)


# ---------------------------------------------------------------------------
# Expiry boundary -- exact microsecond, both directions, both call sites
# ---------------------------------------------------------------------------


class TestExpiryBoundary:
    """hold_expires_at <= now is the ONLY definition of expiry
    (see is_hold_expired). These tests pin the boundary down to the
    microsecond so a future change to either comparison operator fails
    loudly instead of silently.
    """

    @staticmethod
    def _held_seat(make_seat, hold_expires_at):
        return make_seat(
            status=SeatStatus.HELD,
            held_by_session_id="session-old",
            hold_expires_at=hold_expires_at,
        )

    def test_one_microsecond_before_expiry_confirm_succeeds(self, make_seat):
        seat = self._held_seat(make_seat, NOW + timedelta(microseconds=1))
        result = confirm(seat, "session-old", booking_id=1, now=NOW)
        assert result.status == SeatStatus.BOOKED

    def test_one_microsecond_before_expiry_reclaim_fails(self, make_seat):
        seat = self._held_seat(make_seat, NOW + timedelta(microseconds=1))
        with pytest.raises(SeatUnavailable):
            hold(seat, "session-new", NOW, HOLD_DURATION)

    def test_exactly_at_expiry_confirm_raises_hold_expired(self, make_seat):
        seat = self._held_seat(make_seat, NOW)
        with pytest.raises(HoldExpired):
            confirm(seat, "session-old", booking_id=1, now=NOW)

    def test_exactly_at_expiry_reclaim_succeeds(self, make_seat):
        seat = self._held_seat(make_seat, NOW)
        result = hold(seat, "session-new", NOW, HOLD_DURATION)
        assert result.status == SeatStatus.HELD
        assert result.held_by_session_id == "session-new"

    def test_one_microsecond_after_expiry_confirm_raises_hold_expired(self, make_seat):
        seat = self._held_seat(make_seat, NOW - timedelta(microseconds=1))
        with pytest.raises(HoldExpired):
            confirm(seat, "session-old", booking_id=1, now=NOW)

    def test_one_microsecond_after_expiry_reclaim_succeeds(self, make_seat):
        seat = self._held_seat(make_seat, NOW - timedelta(microseconds=1))
        result = hold(seat, "session-new", NOW, HOLD_DURATION)
        assert result.status == SeatStatus.HELD

    def test_expiry_boundary_is_consistent_across_confirm_and_reclaim(self, make_seat):
        # This is the test that catches a future edit that changes the
        # comparison operator in confirm() or hold() but not the other. At
        # the exact expiry instant, a seat must be simultaneously
        # unconfirmable by its holder AND reclaimable by someone else -- if
        # these two ever disagree, there is either a window where a seat is
        # neither confirmable nor reclaimable (silently lost inventory), or
        # a window where it is confirmable by two sessions at once (an
        # oversell). Both branches are asserted here, against the same
        # seat, so they can never quietly drift apart again.
        seat = self._held_seat(make_seat, NOW)

        with pytest.raises(HoldExpired):
            confirm(seat, "session-old", booking_id=1, now=NOW)

        result = hold(seat, "session-new", NOW, HOLD_DURATION)
        assert result.status == SeatStatus.HELD
        assert result.held_by_session_id == "session-new"


# ---------------------------------------------------------------------------
# is_hold_expired() -- the shared predicate itself
# ---------------------------------------------------------------------------


class TestIsHoldExpired:
    def test_none_expiry_is_not_expired(self, make_seat):
        seat = make_seat(status=SeatStatus.AVAILABLE, hold_expires_at=None)
        assert is_hold_expired(seat, NOW) is False

    def test_future_expiry_is_not_expired(self, make_seat):
        seat = make_seat(
            status=SeatStatus.HELD,
            held_by_session_id="s",
            hold_expires_at=NOW + timedelta(seconds=1),
        )
        assert is_hold_expired(seat, NOW) is False

    def test_past_or_equal_expiry_is_expired(self, make_seat):
        seat = make_seat(status=SeatStatus.HELD, held_by_session_id="s", hold_expires_at=NOW)
        assert is_hold_expired(seat, NOW) is True


# ---------------------------------------------------------------------------
# version increments
# ---------------------------------------------------------------------------


class TestVersionIncrements:
    def test_version_increments_across_full_lifecycle(self, make_seat):
        seat = make_seat(status=SeatStatus.AVAILABLE, version=0)

        held = hold(seat, "session-a", NOW, HOLD_DURATION)
        assert held.version == 1

        booked = confirm(held, "session-a", booking_id=1, now=NOW)
        assert booked.version == 2

        released = release(booked, NOW)
        assert released.version == 3
