"""The coalescing buffer -- the main scaling mechanism for the realtime
seat map, not an optimisation bolted onto naive fanout.

Naive fanout is O(events x clients): every hold/release/expiry publishes
its own message, and every one of those gets serialized and sent to every
connected client individually. That is CPU-bound on per-message
serialization before it is ever bandwidth-bound -- 10k small sends/sec on
the same event loop that also serves booking requests degrades API
latency for everyone, not just WebSocket clients. Coalescing changes the
shape of the cost: raw seat-change events for one (event, section) pair
accumulate here for a short window (Settings.ws_coalesce_window_ms), and
exactly ONE combined diff is serialized and sent per section per window,
regardless of how many raw events happened inside it. The remaining cost
is O(clients) per tick -- proportional to how many people are watching,
never to how fast inventory is moving.

Within a window, redundant changes collapse: a seat held then released
inside the same tick nets to "nothing changed" and is not sent at all,
not sent as two messages.

Pure, no I/O, no asyncio -- deliberately, so it can be tested exhaustively
without a broker, a socket, or an event loop (see
tests/realtime/test_coalescer.py, mirroring tests/domain/'s
no-containers-needed convention for app/domain/). app/realtime/hub.py is
what actually calls this on a timer and owns the Redis/WebSocket side of
things.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class SeatState:
    """One seat's state as of a single raw update. `version` always rides
    along (the client needs it -- see module docstring's ordering
    discussion below) but is deliberately NOT part of the equality this
    module uses to decide whether to broadcast; see `rendered_equal`.
    """

    status: str
    hold_expires_at: datetime | None
    version: int

    def rendered_equal(self, other: SeatState) -> bool:
        """Compares ONLY the fields that change what a viewer's seat map
        actually renders: `status` (the seat's colour) and
        `hold_expires_at` (what a countdown displays). Deliberately
        EXCLUDES `version` and `held_by_session_id` (which isn't even
        carried on this type -- other viewers have no business knowing
        who holds a seat, only that it's held).

        This is an explicit decision, not an accident of which fields
        the dataclass happens to carry: `version` increments on every
        write, so an equality check that included it would defeat
        coalescing entirely -- nothing would ever compare equal, and
        every raw event would get its own broadcast regardless of
        whether anything a viewer can see actually changed. Comparing
        on the rendered fields only is what makes "held then released
        within one window nets to nothing" actually net to nothing.

        THE INTERACTION WITH VERSION-BASED ORDERING (worked through
        explicitly, not assumed): the client discards any update whose
        version is <= the version it already holds for that seat (Redis
        pub/sub gives no cross-publisher ordering guarantee across the
        4 uvicorn workers, so a client can otherwise receive an older
        write after a newer one -- see app/realtime/hub.py). Suppressing
        a render-equal update here does NOT create a version gap that
        breaks that rule: this coalescer always advances its own
        `_last_state` to the latest pending state on every flush,
        suppressed or not (see `flush()`), so the NEXT window's
        comparison is always against the true current state, and
        whatever eventually DOES get broadcast for a seat always
        carries that seat's latest known version -- never a version
        already superseded by something sent earlier. A client that
        received nothing during a suppressed window still has the
        correct (lower) version recorded for that seat, and the next
        real broadcast's version is unconditionally higher than that,
        so it is never wrongly discarded. Suppression and version-
        monotonic discard are orthogonal: one decides whether the
        SERVER sends anything at all; the other decides whether the
        CLIENT, having received something, applies it.
        """
        return self.status == other.status and self.hold_expires_at == other.hold_expires_at


@dataclass(frozen=True, slots=True)
class SeatDiff:
    """One seat's entry in an outbound broadcast. Deliberately narrower
    than SeatState -- section/row_label/seat_number are static and
    already known to the client from the snapshot; only what can change
    is sent here.
    """

    seat_id: int
    status: str
    hold_expires_at: datetime | None
    version: int


class SectionCoalescer:
    """One instance per (event_id, section) that has at least one local
    subscriber -- owned and driven by app/realtime/hub.py's flush ticker.
    """

    def __init__(self) -> None:
        self._last_state: dict[int, SeatState] = {}
        self._pending: dict[int, SeatState] = {}
        self._raw_event_count = 0

    def record(self, seat_id: int, state: SeatState) -> None:
        """Called once per raw update received from Redis, in whatever
        order they arrive within this window. Last write wins for a
        given seat_id within the window -- this IS the "net state"
        collapsing: two updates for the same seat before the next flush
        overwrite each other here, never both get forwarded.
        """
        self._pending[seat_id] = state
        self._raw_event_count += 1

    def flush(self) -> tuple[list[SeatDiff], int]:
        """Returns (diffs to broadcast, raw events absorbed this flush).

        `_last_state` is advanced to the full pending state for EVERY
        seat touched this window, whether or not it ends up in the
        returned diff list -- see `SeatState.rendered_equal`'s docstring
        for why this is required for the next window's comparison to be
        correct, independent of what was or wasn't actually sent.
        """
        diffs: list[SeatDiff] = []
        for seat_id, state in self._pending.items():
            previous = self._last_state.get(seat_id)
            if previous is None or not previous.rendered_equal(state):
                diffs.append(
                    SeatDiff(
                        seat_id=seat_id,
                        status=state.status,
                        hold_expires_at=state.hold_expires_at,
                        version=state.version,
                    )
                )
            self._last_state[seat_id] = state

        raw_count = self._raw_event_count
        self._pending = {}
        self._raw_event_count = 0
        return diffs, raw_count

    @property
    def is_dirty(self) -> bool:
        return bool(self._pending)
