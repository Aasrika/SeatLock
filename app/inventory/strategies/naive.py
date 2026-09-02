"""Strategy A: the naive, deliberately broken seat-acquisition strategy.

    *** WARNING: NEVER USE THIS IN PRODUCTION. IT WILL OVERSELL. ***

This module exists to PROVE a bug, not avoid one -- CLAUDE.md rule 6: do
not "fix" this file. It is the control condition the rest of the benchmark
is measured against (SPEC.md section 4, Strategy A).

It implements a textbook time-of-check-to-time-of-use (TOCTOU) race:

    1. SELECT the requested seats' current state -- no locking clause.
    2. Decide, in this process's memory, whether every seat is AVAILABLE
       (or an expired HELD, which the domain state machine treats as
       reclaimable).
    3. UPDATE those seats to HELD -- unconditionally. The UPDATE carries no
       WHERE clause tying the write to the state that was actually read;
       it is scoped only by primary key.

Between steps 1 and 3, another concurrent request can run these same three
steps against the exact same seat. Under PostgreSQL's default READ
COMMITTED isolation, step 1's SELECT does not block anyone else's SELECT,
and step 3's UPDATE has no guard clause to detect that the row changed
underneath it. So two (or more) concurrent callers can each see AVAILABLE,
each decide independently to proceed, and each blindly overwrite the row.
Both report success. Both write a hold_audit row. The seat has been sold
twice -- and nothing here would notice, because each request only ever
looks at what *it* read, never at what is true in the database *now*.

Raising the isolation level alone would not fix this without also adding a
guard clause to the UPDATE (Strategy C, optimistic locking) or a lock
(Strategy B, pessimistic locking) -- both arrive in later phases.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain import state_machine
from app.domain.errors import DomainError
from app.domain.models import Seat
from app.infra.config import settings
from app.infra.mappers import seat_apply, seat_to_domain
from app.infra.tables import HoldAuditRow, SeatRow
from app.inventory.strategies.base import AcquireResult


class NaiveStrategy:
    """Strategy A -- see module docstring. Deliberately unsafe."""

    async def acquire(
        self,
        session: AsyncSession,
        seat_ids: list[int],
        holder: str,
        hold_duration: timedelta,
        now: datetime,
    ) -> AcquireResult:
        # Step 1: SELECT with no locking clause -- FOR UPDATE would defeat
        # the entire point of this strategy.
        result = await session.execute(select(SeatRow).where(SeatRow.id.in_(seat_ids)))
        rows = {row.id: row for row in result.scalars().all()}

        missing = [seat_id for seat_id in seat_ids if seat_id not in rows]
        if missing:
            return AcquireResult(
                success=False, acquired=[], failed=missing, reason="seat_not_found", attempts=1
            )

        # This delay exists ONLY to make the race reproducible on demand.
        # Natural scheduling jitter under real concurrent load is often
        # enough to trigger it anyway; NAIVE_RACE_WINDOW_MS defaulting to 0
        # means the bug is still real here, just probabilistic rather than
        # guaranteed. Widening it turns "might oversell under load" into
        # "will oversell on the next concurrent request," which is what a
        # deterministic test needs.
        if settings.naive_race_window_ms:
            await asyncio.sleep(settings.naive_race_window_ms / 1000)

        # Step 2: decide, using ONLY what was read in step 1 -- by now it
        # may already be stale.
        held_seats: dict[int, Seat] = {}
        for seat_id in seat_ids:
            seat = seat_to_domain(rows[seat_id])
            try:
                held_seats[seat_id] = state_machine.hold(seat, holder, now, hold_duration)
            except DomainError as exc:
                return AcquireResult(
                    success=False, acquired=[], failed=[seat_id], reason=str(exc), attempts=1
                )

        # Step 3: write back -- unconditionally. seat_apply() only sets
        # attributes on the already-loaded ORM row; SQLAlchemy's flush
        # emits `UPDATE seats SET ... WHERE id = :id`, scoped by primary
        # key alone. No `AND status = 'AVAILABLE'`, no version check.
        for seat_id, held_seat in held_seats.items():
            seat_apply(rows[seat_id], held_seat)
            session.add(HoldAuditRow(seat_id=seat_id, session_id=holder, acquired_at=now))

        await session.commit()

        return AcquireResult(success=True, acquired=list(held_seats.keys()), failed=[], attempts=1)
