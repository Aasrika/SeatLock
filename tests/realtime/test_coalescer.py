"""app/realtime/coalescer.py -- pure logic, no containers, no event loop
(same "test the logic directly" convention as tests/domain/).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.realtime.coalescer import SeatState, SectionCoalescer

NOW = datetime(2026, 6, 1, tzinfo=UTC)
FUTURE = NOW + timedelta(minutes=8)
LATER = NOW + timedelta(minutes=16)


class _FakeClient:
    """Mirrors the frontend's version-monotonic discard rule exactly
    (see SeatState.rendered_equal's docstring) -- used here to prove the
    server-side coalescer produces a sequence a real client renders
    correctly, not just that the coalescer's own internal state looks
    right in isolation.
    """

    def __init__(self) -> None:
        self.last_version: dict[int, int] = {}
        self.rendered: dict[int, SeatState] = {}

    def apply(self, seat_id: int, state: SeatState) -> None:
        if seat_id in self.last_version and state.version <= self.last_version[seat_id]:
            return  # discarded: not newer than what we already have
        self.last_version[seat_id] = state.version
        self.rendered[seat_id] = state


class TestBasicCollapse:
    def test_single_update_is_forwarded(self):
        c = SectionCoalescer()
        c.record(1, SeatState("HELD", FUTURE, 2))
        diffs, raw = c.flush()
        assert raw == 1
        assert len(diffs) == 1
        assert diffs[0].seat_id == 1
        assert diffs[0].status == "HELD"
        assert diffs[0].version == 2

    def test_multiple_updates_in_one_window_collapse_to_net_state(self):
        c = SectionCoalescer()
        c.record(1, SeatState("HELD", FUTURE, 2))
        c.record(1, SeatState("HELD", LATER, 3))  # e.g. an extension
        c.record(1, SeatState("BOOKED", None, 4))
        diffs, raw = c.flush()
        assert raw == 3
        assert len(diffs) == 1  # ONE message, not three
        assert diffs[0].status == "BOOKED"
        assert diffs[0].hold_expires_at is None
        assert diffs[0].version == 4  # the latest, not the first or a stale one

    def test_unrelated_seats_in_the_same_window_both_forward(self):
        c = SectionCoalescer()
        c.record(1, SeatState("HELD", FUTURE, 2))
        c.record(2, SeatState("BOOKED", None, 5))
        diffs, raw = c.flush()
        assert raw == 2
        assert {d.seat_id for d in diffs} == {1, 2}


class TestSuppression:
    """A seat held then released inside one window emits its NET state
    (nothing, if unchanged from before the window) -- not one message
    per raw event.
    """

    def test_held_then_released_within_one_window_emits_nothing(self):
        c = SectionCoalescer()
        # Seed _last_state as AVAILABLE (as if a prior flush already
        # established this).
        c.record(1, SeatState("AVAILABLE", None, 10))
        c.flush()

        c.record(1, SeatState("HELD", FUTURE, 11))
        c.record(1, SeatState("AVAILABLE", None, 12))
        diffs, raw = c.flush()

        assert raw == 2
        assert diffs == []  # net-unchanged (AVAILABLE -> AVAILABLE): nothing sent

    def test_extension_alone_is_never_suppressed(self):
        """hold_expires_at changing with status unchanged MUST still
        broadcast -- the countdown depends on it. Comparing on status
        alone (rather than status + hold_expires_at) would wrongly
        suppress this.
        """
        c = SectionCoalescer()
        c.record(1, SeatState("HELD", FUTURE, 10))
        c.flush()

        c.record(1, SeatState("HELD", LATER, 11))  # extended, still HELD
        diffs, raw = c.flush()

        assert raw == 1
        assert len(diffs) == 1
        assert diffs[0].hold_expires_at == LATER
        assert diffs[0].version == 11

    def test_genuine_status_change_is_never_suppressed(self):
        c = SectionCoalescer()
        c.record(1, SeatState("AVAILABLE", None, 10))
        c.flush()

        c.record(1, SeatState("HELD", FUTURE, 11))
        diffs, raw = c.flush()

        assert len(diffs) == 1
        assert diffs[0].status == "HELD"


class TestSuppressionAcrossWindowsWithClientVersionDiscard:
    """The exact sequence the interaction between suppression and
    version-monotonic discard has to get right: held then released
    within one window (suppressed, nothing sent), then held again in
    the NEXT window. The client must end up correctly HELD, and must
    NOT discard the second hold as stale.
    """

    def test_suppressed_window_then_real_change_next_window(self):
        c = SectionCoalescer()
        client = _FakeClient()

        # Initial snapshot-equivalent: seat starts AVAILABLE, v10, known
        # to both the coalescer's baseline and the client. The first-
        # ever flush for a seat is never suppressed (no _last_state to
        # compare against yet), so this reaches the client normally.
        c.record(1, SeatState("AVAILABLE", None, 10))
        diffs, _ = c.flush()
        assert len(diffs) == 1
        for d in diffs:
            client.apply(d.seat_id, SeatState(d.status, d.hold_expires_at, d.version))
        assert client.rendered[1] == SeatState("AVAILABLE", None, 10)

        # Window 1: held then released -- net AVAILABLE, same as before
        # the window. Suppressed: the client receives nothing this tick.
        c.record(1, SeatState("HELD", FUTURE, 11))
        c.record(1, SeatState("AVAILABLE", None, 12))
        diffs, raw = c.flush()
        assert diffs == []
        assert raw == 2
        for d in diffs:
            client.apply(d.seat_id, SeatState(d.status, d.hold_expires_at, d.version))
        # Client is unchanged: still AVAILABLE at v10 (it never learned
        # about v11/v12 at all, which is fine -- nothing it renders
        # actually changed).
        assert client.rendered[1] == SeatState("AVAILABLE", None, 10)

        # Window 2: held again, for real this time.
        c.record(1, SeatState("HELD", LATER, 13))
        diffs, raw = c.flush()
        assert raw == 1
        assert len(diffs) == 1
        assert diffs[0].version == 13
        for d in diffs:
            client.apply(d.seat_id, SeatState(d.status, d.hold_expires_at, d.version))

        # The client must have applied v13, not discarded it as stale
        # relative to the v10 it was last holding.
        assert client.rendered[1] == SeatState("HELD", LATER, 13)
        assert client.last_version[1] == 13


class TestVersionOrderingAtTheClient:
    """Addition A: Redis pub/sub gives no ordering guarantee across the
    4 uvicorn workers publishing independently -- a client can receive
    an older write after a newer one. The client MUST discard any
    update whose version is <= the version it already holds.
    """

    def test_out_of_order_delivery_keeps_the_higher_version(self):
        client = _FakeClient()
        # Higher version arrives FIRST (out of order).
        client.apply(1, SeatState("HELD", FUTURE, 5))
        # Lower version arrives SECOND -- must be discarded.
        client.apply(1, SeatState("AVAILABLE", None, 3))

        assert client.rendered[1] == SeatState("HELD", FUTURE, 5)
        assert client.last_version[1] == 5

    def test_equal_version_is_also_discarded_not_reapplied(self):
        client = _FakeClient()
        client.apply(1, SeatState("HELD", FUTURE, 5))
        client.apply(1, SeatState("BOOKED", None, 5))  # same version, different payload
        assert client.rendered[1] == SeatState("HELD", FUTURE, 5)
