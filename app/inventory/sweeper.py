"""The hold sweeper (SPEC.md section 5, invariant I3): reclaims seats
whose hold has expired, returning them to AVAILABLE so they can be
re-acquired by someone else.

Strategy-agnostic by construction. This module must behave IDENTICALLY
regardless of which SeatAcquisitionStrategy is configured -- same
interval, same batch size, same query shape, same SKIP LOCKED semantics
-- because it is a THIRD writer against the same `seats` table, alongside
whichever strategy is under test. If the sweeper's own behavior varied by
strategy, a benchmark comparing strategies would really be comparing
"strategy X plus a sweeper tuned for X" against "strategy Y plus a
sweeper tuned for Y," which is not the comparison SPEC.md section 4 asks
for. Structurally enforced here by never importing anything from
app.inventory.strategies and never reading Settings.strategy; the
benchmark harness (loadtest/recirculating_sweep.py) additionally asserts
sweeper configuration is identical across every strategy's runs, the same
way it already asserts everything else is.

Why SELECT ... FOR UPDATE SKIP LOCKED, not a plain UPDATE: a seat a
booker is actively acquiring holds a row lock for the duration of that
acquisition (pessimistic's SELECT ... FOR UPDATE for the whole
transaction; optimistic's brief implicit write-lock during its
conditional UPDATE). The sweeper must never block waiting for that lock
(it would then be adding its own contention on top of whatever the
booker is doing) and must never itself block a booker either. SKIP
LOCKED gives exactly this: a currently-locked row is silently excluded
from this pass's batch and picked up on a LATER pass instead, once
whatever was holding it has released it.

Every actual status change goes through state_machine.expire()
(CLAUDE.md rule 3) -- this reads candidate rows, converts each to a
domain Seat, calls expire(), and writes the result back via seat_apply().
Unlike the acquisition strategies (which recompute the resulting fields
directly in a single bulk UPDATE for performance, since they are racing
other acquisitions for the same row under real latency pressure), there
is no equivalent efficiency case here: batch_size rows processed one at a
time through a pure domain function is cheap, and going through the
domain layer for every write is the stricter, more literal compliance
with "no code outside state_machine.py sets a seat's status" -- there's
no reason not to take it when nothing forces a shortcut.

Scope note: SPEC.md section 5 also describes the sweeper "publishing
release events" for the realtime layer. That is NOT implemented here --
this pulls forward only the hold-reclamation behavior Phase 3's
recirculating benchmark needs and I3 requires; event publishing remains
future work for whichever phase actually builds app/realtime/.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain import state_machine
from app.domain.errors import DomainError
from app.infra.mappers import seat_apply, seat_to_domain
from app.infra.metrics import (
    sweeper_batch_duration_seconds,
    sweeper_lock_wait_seconds,
    sweeper_seats_expired_total,
)
from app.infra.tables import SeatRow


@dataclass
class SweepBatchResult:
    """The outcome of one sweep_once() call."""

    candidates_found: int
    seats_expired: int


async def sweep_once(session: AsyncSession, batch_size: int, now: datetime) -> SweepBatchResult:
    """One sweeper pass: reclaim up to `batch_size` expired holds.

    Equivalent SQL for the candidate read:

        SELECT * FROM seats
         WHERE status = 'HELD' AND hold_expires_at < :now
         ORDER BY hold_expires_at
         LIMIT :batch_size
           FOR UPDATE SKIP LOCKED;

    ORDER BY hold_expires_at (oldest-expired-first), not id: when more
    seats are currently expired than fit in one batch, this is what
    decides which ones get reclaimed FIRST. Oldest-first is this module's
    own choice (SPEC.md doesn't mandate an order) -- it makes I3 ("no
    seat stays HELD past hold_expires_at beyond one sweeper interval") a
    meaningfully stronger guarantee, since a persistent backlog always
    gets cleared oldest-to-newest rather than in arbitrary id order.
    """
    batch_start = time.monotonic()

    lock_start = time.monotonic()
    result = await session.execute(
        select(SeatRow)
        .where(SeatRow.status == "HELD", SeatRow.hold_expires_at < now)
        .order_by(SeatRow.hold_expires_at)
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    sweeper_lock_wait_seconds.observe(time.monotonic() - lock_start)

    rows = result.scalars().all()
    seats_expired = 0
    for row in rows:
        seat = seat_to_domain(row)
        try:
            expired_seat = state_machine.expire(seat, now)
        except DomainError:
            # Should not happen -- the WHERE clause above already
            # selects exactly expire()'s precondition, and the row is
            # locked (FOR UPDATE) from read to here, so nothing else can
            # have changed it in between. Defensive, not expected: skip
            # this one row rather than letting one unexpected state
            # abort the whole batch.
            continue
        seat_apply(row, expired_seat)
        seats_expired += 1

    await session.commit()

    sweeper_batch_duration_seconds.observe(time.monotonic() - batch_start)
    if seats_expired:
        sweeper_seats_expired_total.inc(seats_expired)

    return SweepBatchResult(candidates_found=len(rows), seats_expired=seats_expired)
