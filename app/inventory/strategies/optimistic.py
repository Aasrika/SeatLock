"""Strategy C: optimistic locking (SPEC.md section 4, Strategy C).

Per attempt:

    (a) SELECT id, status, version FROM seats WHERE id = ANY(:ids);
        -- no locking clause of any kind. No row lock is ever taken.

    (b) Validate the freshly-read snapshot through the domain state
        machine (app/domain/state_machine.py's hold()) exactly like every
        other strategy -- this is the ONLY place a Seat's status may
        change (CLAUDE.md rule 3). A rejection here (seat already BOOKED,
        or HELD and not yet expired) is a genuine business decision made
        against data we just read: it is NOT retried. Retrying wouldn't
        change the fact that the seat is unavailable right now.

    (c) UPDATE seats SET status='HELD', version = version + 1, ...
         WHERE id = ANY(:ids) AND status='AVAILABLE' AND version = :expected
        -- one conditional UPDATE, per-seat expected version (see "Why
        unnest(), not one statement per seat" below).

    (d) If the UPDATE's rowcount is less than the number of seats
        requested: something else changed at least one of those rows
        between (a) and (c). Roll back (undoing any rows THIS statement
        did manage to update -- an optimistic acquire never partially
        fulfils, same rule as every other strategy), count a conflict,
        sleep a full-jitter backoff, and retry from (a) with a FRESH
        read. Retrying with the SAME (now-stale) expected version could
        never succeed: the WHERE clause would just fail identically
        again, for the same reason it failed the first time.

    (e) Full rowcount match: commit.

No lock is ever taken in this strategy. A conflict is detected purely by
the UPDATE's WHERE clause matching zero rows for a seat whose version no
longer matches what we read -- there is no separate "check" step distinct
from the write. This is sound because a single UPDATE statement is atomic
per row: Postgres either finds a row where `id = X AND status='AVAILABLE'
AND version = expected` all still hold, in which case it updates that
exact row and nothing else could have changed it in between (the check and
the write happen as one indivisible operation against MVCC's current
snapshot), or it finds no such row, in which case nothing was written and
nothing needs to be undone for THAT row. There is no window between
"checked" and "wrote" where another writer could sneak in -- if there were,
this whole detection mechanism would be unsound, not just imprecise.

Why unnest(), not one statement per seat: SPEC.md's pseudocode uses one
shared expected version for the whole WHERE ... AND version = :expected --
that only works if every requested seat happens to share a version, which
won't be true in general (seat A might be at version 3, seat B at version
7). The real fix needs a PER-SEAT expected version, and there are two ways
to express that:

  1. One UPDATE statement per seat, in a loop, each with its own bound
     expected version, summing rowcounts. Rejected: an UPDATEd row is
     write-locked until COMMIT. Issuing N separate UPDATEs against the
     same transaction means the FIRST seat's row stays exclusively locked
     across every remaining round trip for seats 2..N -- "optimistic"
     locking would then be *partially* pessimistic, holding real row
     locks for however long the rest of the loop takes, with lock order
     determined by loop (i.e. seat_ids list) order rather than ORDER BY
     id. That is exactly the Phase 2 deadlock shape
     (tests/integration/test_pessimistic.py's TestDeadlockOrdering) coming
     back in: two concurrent multi-seat acquires whose seat_ids happen to
     list the same two seats in opposite order could deadlock on their
     own per-seat UPDATEs. The extra round trips (N network round trips
     instead of 1) are real too, but secondary -- the lock-order hazard is
     the reason this is wrong, not merely slow.
  2. ONE UPDATE statement, joining the target table against a virtual
     table of (id, expected_version) pairs built from two parallel arrays
     via Postgres's unnest():

         UPDATE seats AS s
            SET status = 'HELD', version = s.version + 1, ...
           FROM unnest(CAST(:ids AS bigint[]), CAST(:expected_versions AS integer[]))
                AS v(id, expected_version)
          WHERE s.id = v.id AND s.status = 'AVAILABLE'
                AND s.version = v.expected_version

     Chosen. One round trip regardless of how many seats are requested,
     and a fixed query shape (no dynamically-built VALUES list sized to
     seat count). rowcount is exactly how many of the requested seats
     matched, in one atomic statement.

Do NOT read this as "unnest() makes the whole multi-row UPDATE deadlock-
proof" -- it does not. A multi-row UPDATE can still deadlock against
ANOTHER concurrent multi-row UPDATE that touches an overlapping set of
rows in a different physical order: Postgres decides physical row-touch
order from its own query plan (e.g. index scan order over the unnest()
input), not from the order the caller happened to list seat_ids in, and
UPDATE ... FROM has no ORDER BY to force a consistent order the way
`SELECT ... FOR UPDATE ORDER BY id` does for pessimistic mode (a). Sorting
the input arrays before binding them does NOT guarantee Postgres executes
the underlying row updates in that order. So: a genuine 40P01 deadlock is
possible here, and is NOT a bug signal the way it is for pessimistic mode
(a) -- it is caught and treated as just another retryable conflict (see
the loop below and app/infra/metrics.py's deadlocks_total docstring for
the corrected, per-strategy meaning of that counter).

Full jitter, not fixed or "equal jitter" backoff: cites AWS's architecture
blog post "Exponential Backoff And Jitter" (Marc Brooker, 2015) by name.
That analysis shows that with NO jitter, every contender that just
conflicted retries after exactly the same delay, so an initial pile-up
tends to re-synchronise into another pile-up on the very next attempt --
the backoff schedule becomes a shared clock instead of a way to spread
contenders apart. Its "equal jitter" variant (half fixed + half random)
still keeps a rising floor that concentrates retries. "Full jitter"
(sleep for a uniformly random duration between 0 and the full exponential
backoff ceiling, `random.uniform(0, base * 2**attempt)`) has no such
floor, so contenders spread across the WHOLE window rather than
clustering near one edge of it -- empirically shown in that post to
produce both fewer total retries and less variance in completion time
than the alternatives, for exactly this kind of "many clients back off
after a shared conflict" scenario.

Retry budget is mandatory, not advisory: unbounded retries under
sustained contention are a self-inflicted DoS -- a client that never
gives up just adds load to an already-contended row forever, on top of
whatever the real contention already was. max_attempts defaults to 5
(Settings.optimistic_max_attempts); exhausting it is a clean domain
failure (optimistic_exhausted_total, AcquireResult(success=False, ...)),
never an infinite loop and never an unhandled crash.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta

from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain import state_machine
from app.domain.errors import DomainError
from app.infra.config import settings
from app.infra.mappers import seat_to_domain
from app.infra.metrics import (
    deadlocks_total,
    optimistic_attempts,
    optimistic_conflicts_total,
    optimistic_exhausted_total,
    optimistic_retries_total,
    oversell_blocked_total,
)
from app.infra.tables import HoldAuditRow, SeatRow
from app.inventory.strategies.base import AcquireResult

# Equivalent raw SQL for the conditional UPDATE -- see the module docstring
# ("Why unnest(), not one statement per seat") for the full rationale.
# CAST(...) is required, not decorative: asyncpg binds a Python list as a
# Postgres array only when the target type is unambiguous from context,
# and unnest() needs to know the element type of each array to build its
# two-column virtual table.
_CONDITIONAL_UPDATE_SQL = text(
    """
    UPDATE seats AS s
       SET status = 'HELD',
           version = s.version + 1,
           held_by_session_id = :holder,
           hold_expires_at = :hold_expires_at,
           updated_at = :now
      FROM unnest(CAST(:ids AS bigint[]), CAST(:expected_versions AS integer[]))
           AS v(id, expected_version)
     WHERE s.id = v.id AND s.status = 'AVAILABLE' AND s.version = v.expected_version
    """
)


class OptimisticStrategy:
    """Strategy C -- see module docstring."""

    def __init__(
        self,
        base_seconds: float | None = None,
        max_attempts: int | None = None,
        full_jitter: bool | None = None,
    ) -> None:
        self.base_seconds = (
            base_seconds if base_seconds is not None else settings.optimistic_backoff_base_seconds
        )
        self.max_attempts = (
            max_attempts if max_attempts is not None else settings.optimistic_max_attempts
        )
        self.full_jitter = (
            full_jitter if full_jitter is not None else settings.optimistic_full_jitter
        )

    async def acquire(
        self,
        session: AsyncSession,
        seat_ids: list[int],
        holder: str,
        hold_duration: timedelta,
        now: datetime,
        *,
        # Testing seam ONLY -- production callers never pass this. Lets
        # tests/integration/test_optimistic.py deterministically inject a
        # conflict (or a sustained one) between "we read" and "we write,"
        # a window that is otherwise a genuine timing race no amount of
        # asyncio.gather orchestration can reliably hit on demand. Modeled
        # on NAIVE_RACE_WINDOW_MS (app/inventory/strategies/naive.py) --
        # the same idea, a narrow, explicit, opt-in hook rather than a
        # flaky timing-dependent test.
        _test_hook_after_read: Callable[[], Awaitable[None]] | None = None,
    ) -> AcquireResult:
        for attempt in range(1, self.max_attempts + 1):
            # (a) fresh, unlocked read -- every attempt, no exceptions.
            result = await session.execute(select(SeatRow).where(SeatRow.id.in_(seat_ids)))
            rows = {row.id: row for row in result.scalars().all()}
            missing = [seat_id for seat_id in seat_ids if seat_id not in rows]
            if missing:
                await session.rollback()
                return AcquireResult(
                    success=False, acquired=[], failed=missing, reason="seat_not_found"
                )

            # (b) validate against exactly what we just read. A rejection
            # here is permanent -- not retried, see module docstring.
            expected_versions: dict[int, int] = {}
            for seat_id in seat_ids:
                seat = seat_to_domain(rows[seat_id])
                try:
                    state_machine.hold(seat, holder, now, hold_duration)
                except DomainError as exc:
                    oversell_blocked_total.labels(layer="application").inc()
                    await session.rollback()
                    return AcquireResult(
                        success=False, acquired=[], failed=[seat_id], reason=str(exc)
                    )
                expected_versions[seat_id] = seat.version

            if _test_hook_after_read is not None:
                await _test_hook_after_read()

            # (c) the conditional UPDATE, per-seat expected version.
            rowcount = await self._attempt_update(
                session, seat_ids, expected_versions, holder, hold_duration, now
            )

            if rowcount == len(seat_ids):
                # (e) full success.
                for seat_id in seat_ids:
                    session.add(HoldAuditRow(seat_id=seat_id, session_id=holder, acquired_at=now))
                await session.commit()
                optimistic_attempts.observe(attempt)
                return AcquireResult(success=True, acquired=seat_ids, failed=[])

            # (d) conflict: at least one row's version no longer matched
            # what we read. Roll back (undoes any rows THIS statement did
            # manage to update -- never partially fulfil), then retry with
            # a FRESH read -- see module docstring for why a stale version
            # can never succeed on retry.
            await session.rollback()
            optimistic_conflicts_total.inc()

            if attempt < self.max_attempts:
                optimistic_retries_total.inc()
                await self._backoff(attempt)

        optimistic_exhausted_total.inc()
        return AcquireResult(
            success=False,
            acquired=[],
            failed=seat_ids,
            reason=f"retry_budget_exhausted after {self.max_attempts} attempts",
        )

    async def _attempt_update(
        self,
        session: AsyncSession,
        seat_ids: list[int],
        expected_versions: dict[int, int],
        holder: str,
        hold_duration: timedelta,
        now: datetime,
    ) -> int:
        """The one conditional UPDATE. Returns rowcount -- the caller
        decides success/conflict from that; this method never rolls back
        or retries itself.

        A 40P01 deadlock here is caught and translated into "zero rows
        updated, treat as a conflict" rather than propagated -- see the
        module docstring's "Do NOT read this as ... deadlock-proof"
        section for why a multi-row UPDATE can still deadlock, and
        app/infra/metrics.py's deadlocks_total docstring for why that is
        expected-but-rare here, not a bug signal the way it is for
        pessimistic mode (a).
        """
        try:
            result = await session.execute(
                _CONDITIONAL_UPDATE_SQL,
                {
                    "holder": holder,
                    "hold_expires_at": now + hold_duration,
                    "now": now,
                    "ids": seat_ids,
                    "expected_versions": [expected_versions[seat_id] for seat_id in seat_ids],
                },
            )
        except DBAPIError as exc:
            if getattr(exc.orig, "sqlstate", None) == "40P01":  # deadlock_detected
                await session.rollback()
                deadlocks_total.inc()
                return 0  # caller treats this exactly like any other conflict
            raise
        return result.rowcount

    async def _backoff(self, attempt: int) -> None:
        """Sleep before the next attempt. attempt is 1-based (the attempt
        that JUST conflicted); the ceiling for the upcoming sleep uses
        `2 ** (attempt - 1)` so the very first retry (after attempt 1
        conflicts) sleeps up to `base`, not `2 * base`.

        full_jitter=True (default): AWS's full-jitter formula,
        `random.uniform(0, base * 2**(attempt-1))` -- see module docstring
        for why full jitter specifically, not fixed or equal jitter.
        full_jitter=False exists only for the Phase 3 jitter ablation
        (loadtest/run_benchmark.py): fixed exponential backoff with no
        random component at all, so the measured difference is
        attributable to jitter alone, nothing else changing between the
        two runs.
        """
        ceiling = self.base_seconds * (2 ** (attempt - 1))
        delay = random.uniform(0, ceiling) if self.full_jitter else ceiling
        await asyncio.sleep(delay)
