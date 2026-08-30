"""Strategy B: pessimistic locking (SPEC.md section 4, Strategy B).

Two selection modes:

  (a) SPECIFIC seats -- the caller already knows which ones (e.g. picked
      14C and 14D on a seat map):

          SELECT id, ... FROM seats
            WHERE id = ANY(:ids)
            ORDER BY id                -- mandatory, see below
            FOR UPDATE;

  (b) ANY N seats -- the caller wants "4 together" but doesn't care which:

          SELECT id, ... FROM seats
            WHERE event_id = :event_id AND status = 'AVAILABLE'
            ORDER BY id
            LIMIT :n
            FOR UPDATE SKIP LOCKED;

Why this closes the TOCTOU window naive.py leaves open: under READ
COMMITTED, if a FOR UPDATE has to block waiting for a row another
transaction currently holds, and that other transaction then commits,
Postgres does NOT hand the blocked transaction the stale snapshot it
started with. It re-reads the row's latest committed version and
re-evaluates the original WHERE clause against it -- this re-check is
called EvalPlanQual. So the "is this seat actually available" check
happens at the moment the lock is acquired, using current data, never
against a snapshot that might already be stale by the time the lock is
granted. That is the whole fix: naive.py's bug is checking against a
snapshot and then writing without re-checking; FOR UPDATE makes the check
and the lock acquisition the same event.

Why ORDER BY id is mandatory in (a): two transactions locking seats {5, 9}
and {9, 5} in opposite order can deadlock -- transaction 1 holds 5, wants
9; transaction 2 holds 9, wants 5; neither can proceed. Consistent lock
ordering (every transaction always locks in ascending id order) makes
that cycle structurally impossible: whichever transaction reaches the
lower id first will always reach the higher one uncontested, because
nobody ever approaches the pair in the other order. See
tests/integration/test_pessimistic.py for a demonstration that actually
reproduces the deadlock without the ordering, and shows it disappear with
it.

Why SKIP LOCKED is correct for (b) and WRONG for (a): (b) doesn't care
which seats it gets, only how many -- skipping a row someone else is
already touching and grabbing the next AVAILABLE one instead is exactly
right, and it's what lets N concurrent "any-4-seats" requests fan out
across different rows instead of all queueing for the same one. (a) DOES
care which seats it gets -- the customer picked 14C and 14D specifically.
Silently skipping 14C because another transaction happens to be touching
it, and returning only 14D, would return the wrong seats without ever
raising an error. For (a) we want to block (or fail cleanly via
lock_timeout, see below) until we can honestly answer whether 14C and 14D
specifically are available -- never substitute.

No I/O of any kind happens between acquiring the locks and COMMIT/
ROLLBACK in either mode. The locks are held for the rest of the
transaction; a payment-gateway call or any other external request made
while holding them would block every other contender for that seat for as
long as that call takes. This is exactly why holds exist as a separate,
lock-free phase ahead of payment confirmation (SPEC.md section 5) -- these
locks must never be held across anything slower than a handful of
database round trips.

lock_timeout (default 5s, configurable) is set via SET LOCAL at the start
of the transaction so a blocked acquire fails cleanly (Postgres
lock_not_available, 55P03) rather than hanging forever. Mapped to
StrategyUnavailable -> HTTP 503 by app/main.py's exception handler, not a
generic 500: the caller should retry, seat availability itself is still
unknown, this was an infrastructure timeout, not a business decision.

Deadlock (40P01) is handled the same way, but in mode (a) it should be
IMPOSSIBLE by construction -- deadlocks_total incrementing at all is a bug
signal (the ordering guarantee broke somehow), not a normal event, unlike
lock_timeouts_total, which is expected to be nonzero under real
contention.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import NoReturn

from sqlalchemy import and_, or_, select, text
from sqlalchemy import update as sa_update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain import state_machine
from app.domain.errors import DomainError
from app.infra.config import settings
from app.infra.mappers import seat_to_domain
from app.infra.metrics import (
    deadlocks_total,
    lock_timeouts_total,
    lock_wait_seconds,
    oversell_blocked_total,
    timed_checkout,
)
from app.infra.tables import HoldAuditRow, SeatRow
from app.inventory.strategies.base import AcquireResult, StrategyUnavailable


class PessimisticStrategy:
    """Strategy B -- see module docstring."""

    def __init__(self, lock_timeout_ms: int | None = None) -> None:
        self.lock_timeout_ms = (
            lock_timeout_ms if lock_timeout_ms is not None else settings.pessimistic_lock_timeout_ms
        )

    async def acquire(
        self,
        session: AsyncSession,
        seat_ids: list[int],
        holder: str,
        hold_duration: timedelta,
        now: datetime,
    ) -> AcquireResult:
        """Mode (a): specific seats."""
        await self._set_lock_timeout(session)

        lock_start = time.monotonic()
        try:
            result = await session.execute(
                select(SeatRow)
                .where(SeatRow.id.in_(seat_ids))
                .order_by(SeatRow.id)  # mandatory -- see module docstring
                .with_for_update()
            )
        except DBAPIError as exc:
            await session.rollback()
            self._raise_translated(exc)
        lock_wait_seconds.observe(time.monotonic() - lock_start)

        rows = {row.id: row for row in result.scalars().all()}
        missing = [seat_id for seat_id in seat_ids if seat_id not in rows]
        if missing:
            await session.rollback()
            return AcquireResult(
                success=False, acquired=[], failed=missing, reason="seat_not_found"
            )

        for seat_id in seat_ids:
            seat = seat_to_domain(rows[seat_id])
            try:
                state_machine.hold(seat, holder, now, hold_duration)
            except DomainError as exc:
                oversell_blocked_total.labels(layer="application").inc()
                await session.rollback()
                return AcquireResult(success=False, acquired=[], failed=[seat_id], reason=str(exc))

        await self._commit_holds(session, seat_ids, holder, hold_duration, now)
        return AcquireResult(success=True, acquired=seat_ids, failed=[])

    async def acquire_any_n(
        self,
        session: AsyncSession,
        event_id: int,
        count: int,
        holder: str,
        hold_duration: timedelta,
        now: datetime,
    ) -> AcquireResult:
        """Mode (b): "any N seats from this event, don't care which."

        Not part of the shared SeatAcquisitionStrategy Protocol -- no
        other strategy has an equivalent "any N" call shape, and this
        isn't exposed through POST /api/holds (which only ever knows
        specific seat_ids). Used directly by callers who want exactly
        this semantics, and by tests/integration/test_pessimistic.py.

        WHERE matches AVAILABLE seats *and* HELD-but-expired ones (Phase
        4 fix -- see this method's git history for the bug this
        replaced): a status filter written in SQL is itself a business
        rule about which seats can be acquired, and it must agree with
        the domain layer's own rule (state_machine.hold() already treats
        an expired HELD seat as reclaimable) or CLAUDE.md rule 3 is
        broken by a WHERE clause instead of a status assignment -- a
        quieter way to violate it than writing to seat.status directly,
        but a violation all the same. Before this fix, mode (b) could
        NEVER reclaim an expired hold itself; it depended entirely on
        the sweeper having already flipped the row to AVAILABLE. With a
        sweeper interval of seconds rather than milliseconds (Phase 4:
        lazy expiry at the query layer is the mechanism, the sweeper is
        cleanup), that gap meant every expired-but-unswept seat was
        wrongly unbookable for up to a full sweeper interval -- under
        flash-sale-scale load, thousands of false "insufficient
        availability" rejections against seats nobody actually holds.
        """
        await self._set_lock_timeout(session)

        lock_start = time.monotonic()
        try:
            result = await session.execute(
                select(SeatRow)
                .where(
                    SeatRow.event_id == event_id,
                    or_(
                        SeatRow.status == "AVAILABLE",
                        and_(SeatRow.status == "HELD", SeatRow.hold_expires_at <= now),
                    ),
                )
                .order_by(SeatRow.id)
                .limit(count)
                .with_for_update(skip_locked=True)
            )
        except DBAPIError as exc:
            await session.rollback()
            self._raise_translated(exc)
        lock_wait_seconds.observe(time.monotonic() - lock_start)

        rows = {row.id: row for row in result.scalars().all()}
        # Never partially fulfil: fewer than requested means "not enough
        # AVAILABLE-and-unlocked seats exist right now," not "give me
        # what you found." A user asking for 4 seats together does not
        # want 2 -- that would be a product decision made accidentally by
        # the database. Roll back before returning: SKIP LOCKED never
        # blocked anyone, but we still hold whatever locks we did acquire
        # until we release them, and those seats should go straight back
        # to being available to the next contender.
        if len(rows) < count:
            await session.rollback()
            return AcquireResult(
                success=False,
                acquired=[],
                failed=list(rows.keys()),
                reason=f"insufficient_availability: found {len(rows)} of {count} requested",
            )

        seat_ids = list(rows.keys())
        for seat_id in seat_ids:
            seat = seat_to_domain(rows[seat_id])
            try:
                state_machine.hold(seat, holder, now, hold_duration)
            except DomainError as exc:
                oversell_blocked_total.labels(layer="application").inc()
                await session.rollback()
                return AcquireResult(success=False, acquired=[], failed=[seat_id], reason=str(exc))

        await self._commit_holds(session, seat_ids, holder, hold_duration, now)
        return AcquireResult(success=True, acquired=seat_ids, failed=[])

    async def _set_lock_timeout(self, session: AsyncSession) -> None:
        # The first statement of the transaction -- whichever it is --
        # is what actually triggers the pool to hand out a connection, so
        # this is where pool_checkout_seconds is measured (see
        # app/infra/metrics.py's timed_checkout() for why it has to be a
        # pool event, not inferred from this statement's own latency).
        #
        # SET LOCAL does not accept a bound parameter here ($1) -- it is a
        # utility command, not a regular parameterised statement, and
        # Postgres requires a literal for it (confirmed directly: a bound
        # parameter raises "syntax error at or near '$1'"). Safe to
        # interpolate: lock_timeout_ms only ever comes from our own
        # config/constructor, never from request input.
        async with timed_checkout():
            await session.execute(text(f"SET LOCAL lock_timeout = '{int(self.lock_timeout_ms)}ms'"))

    async def _commit_holds(
        self,
        session: AsyncSession,
        seat_ids: list[int],
        holder: str,
        hold_duration: timedelta,
        now: datetime,
    ) -> None:
        # One bulk UPDATE covering every locked seat, matching SPEC.md's
        # literal pseudocode shape. SQLAlchemy Core's update() DOES honour
        # onupdate=func.now() automatically even outside the ORM, but
        # updated_at is set explicitly anyway: it costs nothing and
        # removes any doubt for a reviewer checking this statement in
        # isolation, without needing to know that fact about Core.
        #
        # Equivalent SQL:
        #   UPDATE seats
        #      SET status = 'HELD', version = version + 1,
        #          held_by_session_id = :holder,
        #          hold_expires_at = :hold_expires_at,
        #          updated_at = :now
        #    WHERE id = ANY(:ids);
        await session.execute(
            sa_update(SeatRow)
            .where(SeatRow.id.in_(seat_ids))
            .values(
                status="HELD",
                version=SeatRow.version + 1,
                held_by_session_id=holder,
                hold_expires_at=now + hold_duration,
                updated_at=now,
            )
        )
        for seat_id in seat_ids:
            session.add(HoldAuditRow(seat_id=seat_id, session_id=holder, acquired_at=now))
        await session.commit()

    @staticmethod
    def _raise_translated(exc: DBAPIError) -> NoReturn:
        """Turn a Postgres lock_timeout/deadlock into StrategyUnavailable
        (-> HTTP 503). Any other DBAPIError is genuinely unexpected here
        and re-raised as-is -- translating it would hide a real bug behind
        a generic 'infrastructure was briefly unavailable' story.

        Matched on SQLSTATE code (a string), NOT isinstance against the
        original asyncpg exception classes -- confirmed by direct
        inspection that SQLAlchemy's asyncpg dialect does not preserve the
        specific asyncpg exception subclass on .orig for PostgresError
        descendants. It walks up the MRO to the first generic bucket it
        recognises (IntegrityConstraintViolationError, PostgresError, ...)
        and re-wraps the message as plain text in one of ITS OWN generic
        Error/OperationalError/etc. classes -- `isinstance(exc.orig,
        asyncpg.exceptions.DeadlockDetectedError)` is always False. The one
        thing it does faithfully carry over is `.sqlstate`/`.pgcode`, which
        is also the standard, driver-independent way to identify a
        specific Postgres error condition -- matching on it here is not a
        workaround, it's the correct approach either way.
        """
        sqlstate = getattr(exc.orig, "sqlstate", None)
        if sqlstate == "55P03":  # lock_not_available
            lock_timeouts_total.inc()
            raise StrategyUnavailable("lock_timeout: gave up waiting for a lock") from exc
        if sqlstate == "40P01":  # deadlock_detected
            deadlocks_total.inc()
            raise StrategyUnavailable("deadlock detected while acquiring locks") from exc
        raise exc
