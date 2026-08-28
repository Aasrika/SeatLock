"""Shared fixtures for app/domain unit tests."""

from __future__ import annotations

from typing import Any

import pytest

from app.domain.models import Seat, SeatStatus


@pytest.fixture
def make_seat() -> Any:
    """Factory fixture for building a Seat with sensible defaults.

    Every field can be overridden; unset fields default to a coherent
    AVAILABLE seat.
    """

    def _make_seat(**overrides: Any) -> Seat:
        defaults: dict[str, Any] = dict(
            id=1,
            event_id=1,
            status=SeatStatus.AVAILABLE,
            version=0,
            held_by_session_id=None,
            hold_expires_at=None,
            booking_id=None,
        )
        defaults.update(overrides)
        return Seat(**defaults)

    return _make_seat
