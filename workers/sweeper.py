"""The hold sweeper (SPEC.md section 5, invariant I3): reclaims seats
whose hold has expired, returning them to AVAILABLE so they can be
re-acquired by someone else.

    python -m workers.sweeper

Consolidated here in Phase 4 from the split that existed during Phase 3's
benchmark-only pull-forward (app/inventory/sweeper.py held the query/
domain logic, workers/sweeper_worker.py held the "run forever" driver):
those were always one cohesive background-job concern, and workers/
(SPEC.md's own directory for "background jobs: hold sweeper, reconciler")
is where it belongs now that it is production code, not a benchmarking-
only shortcut. `sweep_once()` stays independently callable with no loop
required, exactly as before -- tests call it directly.

LAZY EXPIRY IS THE MECHANISM; THIS IS CLEANUP (Phase 4). Every read path
that reports or acts on seat availability (state_machine.hold()'s own
reclaim-if-expired branch; app/inventory/strategies/pessimistic.py's
acquire_any_n; app/api/routes/admin.py's seat-status-counts) already
treats a HELD row whose hold_expires_at has passed as available, whether
or not this sweeper has physically gotten to it yet. That is what makes
Settings.sweeper_interval_seconds safe at multiple SECONDS (SPEC.md's own
5-10s guidance) instead of milliseconds: I3 ("no seat stays HELD past
hold_expires_at beyond one sweeper interval") is about the ROW's status
column eventually converging to match reality, not about whether the seat
is reclaimable in the meantime -- it always is. This sweeper's actual job
is only to make the persisted row agree with what every reader already
treats as true, and to release the Redis mirror key so a cache-consulting
read doesn't need to fall through to Postgres forever for a seat nobody
holds anymore.

Strategy-agnostic by construction: never imports anything from
app.inventory.strategies, never reads Settings.strategy. Originally
enforced for Phase 3's cross-strategy benchmark comparison; still true and
still the right property in production, where the sweeper must behave
identically no matter which SeatAcquisitionStrategy is configured.

Why SELECT ... FOR UPDATE SKIP LOCKED, not a plain UPDATE: a seat a
booker is actively acquiring holds a row lock for the duration of that
acquisition. The sweeper must never block waiting for that lock, and must
never itself block a booker either. SKIP LOCKED excludes a currently-
locked row from this pass's batch; it is picked up on a LATER pass once
whatever was holding it has released it.

Batch size tradeoff (Settings.sweeper_batch_size, default 100): a larger
batch holds more row locks simultaneously for the duration of one pass,
which can queue bookers trying to acquire one of those same rows right
now; a smaller batch frees rows faster per row but may not keep up with a
large backlog, letting sweeper_backlog_gauge climb. There is no universally
correct number -- tune against that gauge in a real deployment, not by
guessing.

Every actual status change goes through state_machine.expire() (CLAUDE.md
rule 3) -- this reads candidate rows, converts each to a domain Seat,
calls expire(), and writes the result back via seat_apply(). A
concurrent-sweeper or sweeper-vs-booker race can still make one specific
row's expire() call illegal by the time it runs (e.g. a booker legitimately
reclaimed the same expired hold in the narrow window between this pass's
read and this call) -- IllegalTransition from that is EXPECTED, not an
error: SKIP LOCKED already makes it rare (the row is locked for the whole
window between read and write), so this is the safety net catching
whatever narrow race SKIP LOCKED doesn't cover, not the routine path.
Logged at debug and counted (sweeper_illegal_transition_total), never
raised -- one unexpected row must never abort an entire batch.

REDIS MIRROR DELETE ORDERING: strictly after the Postgres COMMIT, never
before or in the same transaction. A crash (or any failure) between the
commit and the delete leaves Postgres AVAILABLE and Redis still holding a
stale key -- a free seat reads as unavailable to anything consulting the
mirror, which is a LOST SALE, but a safe one: the reconciler repairs it
(app/infra/metrics.py's reconciliation_divergence_total{kind=
"redis_key_present_for_unheld_seat"}), and no customer is ever told a
seat is available when Postgres disagrees. The REVERSE ordering (delete
first, commit second) would let a seat read as available from Redis while
Postgres still says HELD -- a second customer could be sent into
checkout on a seat that isn't actually free, only to be rejected later at
the row lock or the partial unique index. Fail toward unavailability,
never toward false availability -- this ordering is why.

Scope note: SPEC.md section 5 also describes the sweeper "publishing
release events" for the realtime layer. That is NOT implemented here --
out of scope for Phase 4's hold/expiry/reconciliation work; event
publishing remains future work for whichever phase builds app/realtime/.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
import time
from dataclasses import dataclass
from datetime import UTC, datetime

import structlog
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain import state_machine
from app.domain.errors import IllegalTransition
from app.infra import hold_cache
from app.infra.config import settings
from app.infra.db import async_session_factory
from app.infra.mappers import seat_apply, seat_to_domain
from app.infra.metrics import (
    sweeper_backlog_gauge,
    sweeper_batch_duration_seconds,
    sweeper_illegal_transition_total,
    sweeper_lock_wait_seconds,
    sweeper_seats_expired_total,
)
from app.infra.redis import get_redis
from app.infra.tables import SeatRow
from app.realtime.pubsub import publish_seat_update

log = structlog.get_logger(__name__)


@dataclass
class SweepBatchResult:
    """The outcome of one sweep_once() call."""

    candidates_found: int
    seats_expired: int


async def measure_backlog(session: AsyncSession, now: datetime) -> int:
    """Count of seats CURRENTLY HELD with hold_expires_at already passed,
    system-wide -- independent of batch_size, and independent of whether
    a sweep pass is running at all right now.

    Called at the start of every sweep_once() pass (keeping the gauge
    fresh whenever the sweeper is alive), AND independently by
    workers/reconciler.py's own loop (a completely separate process, on
    its own schedule) -- not a hypothetical use, an actual second caller.
    Phase 8a's chaos suite found the gap this closes: with only the
    sweeper ever measuring it, killing the sweeper didn't just stop it
    draining the backlog, it froze the gauge at its last pre-kill value
    -- the one signal meant to reveal a stopped sweeper went blind at
    exactly the moment the sweeper died, not merely slow. Two independent
    writers to the same gauge is correct, not a race, because
    multiprocess_mode="mostrecent" (app/infra/metrics.py) means "whichever
    process last measured wins" -- there is no single source of truth to
    protect here, only a reading to keep fresh.
    """
    count = (
        await session.execute(
            select(func.count())
            .select_from(SeatRow)
            .where(SeatRow.status == "HELD", SeatRow.hold_expires_at <= now)
        )
    ).scalar_one()
    sweeper_backlog_gauge.set(count)
    return count


async def sweep_once(session: AsyncSession, batch_size: int, now: datetime) -> SweepBatchResult:
    """One sweeper pass: reclaim up to `batch_size` expired holds.

    Equivalent SQL for the candidate read:

        SELECT id FROM seats
         WHERE status = 'HELD' AND hold_expires_at <= :now
         ORDER BY id
         LIMIT :batch
           FOR UPDATE SKIP LOCKED;

    ORDER BY id here (Phase 4) rather than Phase 3's ORDER BY
    hold_expires_at: id order is what was specified for the production
    sweeper, and is still a well-defined, deterministic order for
    LIMIT'd row selection -- the oldest-expired-first ordering was this
    module's own earlier choice for a stronger I3 guarantee under a
    persistent backlog, not a SPEC.md requirement; either is defensible,
    and id order is what this phase specifies.
    """
    batch_start = time.monotonic()

    await measure_backlog(session, now)

    lock_start = time.monotonic()
    result = await session.execute(
        select(SeatRow)
        .where(SeatRow.status == "HELD", SeatRow.hold_expires_at <= now)
        .order_by(SeatRow.id)
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    sweeper_lock_wait_seconds.observe(time.monotonic() - lock_start)

    rows = result.scalars().all()
    expired_rows: list[SeatRow] = []
    for row in rows:
        seat = seat_to_domain(row)
        try:
            expired_seat = state_machine.expire(seat, now)
        except IllegalTransition:
            # Expected -- see module docstring. Skip this one row, keep
            # processing the rest of the batch.
            log.debug("sweeper.illegal_transition", seat_id=row.id)
            sweeper_illegal_transition_total.inc()
            continue
        seat_apply(row, expired_seat)
        expired_rows.append(row)

    await session.commit()

    # Redis mirror delete strictly AFTER the commit -- see module
    # docstring's ordering section. Each delete is independently best-
    # effort (app/infra/hold_cache.py never raises); one failing does not
    # stop the others.
    for row in expired_rows:
        await hold_cache.delete_hold_mirror(row.id)

    # Realtime fanout (Phase 7), same AFTER-commit ordering, same
    # reasoning: a lost publish is a stale seat map that self-corrects
    # on the client's next reconnect snapshot; a premature one would
    # show a seat AVAILABLE before Postgres has actually agreed it is.
    # event_id/section/version are already on `row` -- these are the
    # exact rows just expired, no follow-up query needed.
    redis_client = get_redis()
    for row in expired_rows:
        await publish_seat_update(
            redis_client,
            event_id=row.event_id,
            section=row.section,
            seat_id=row.id,
            status=row.status,
            hold_expires_at=row.hold_expires_at,
            version=row.version,
        )

    sweeper_batch_duration_seconds.observe(time.monotonic() - batch_start)
    seats_expired = len(expired_rows)
    if seats_expired:
        sweeper_seats_expired_total.inc(seats_expired)

    return SweepBatchResult(candidates_found=len(rows), seats_expired=seats_expired)


async def run_forever(interval_seconds: float, batch_size: int, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        now = datetime.now(UTC)
        async with async_session_factory() as session:
            result = await sweep_once(session, batch_size, now)
        if result.seats_expired:
            log.info(
                "sweeper.pass",
                seats_expired=result.seats_expired,
                candidates_found=result.candidates_found,
            )
        # wait_for(..., timeout=interval_seconds) rather than plain
        # sleep(): lets a signal-triggered stop_event interrupt the wait
        # immediately instead of finishing out the full interval first.
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)


async def main_async() -> None:
    """Graceful shutdown on SIGINT/SIGTERM: finishes whatever batch is
    currently in flight, then exits, rather than being killed mid-
    transaction (which Postgres would roll back safely regardless --
    finishing cleanly just avoids discarding a batch's work for no
    reason). NOTE: asyncio's ProactorEventLoop (the default on Windows)
    does not implement add_signal_handler for SIGINT/SIGTERM -- confirmed
    by direct testing, it raises NotImplementedError, suppressed below.
    On Windows this means the loop always runs to forced termination
    (e.g. the benchmark harness's stop_sweeper, matching stop_api's
    taskkill /F /T) rather than shutting down gracefully; harmless here
    (an in-flight sweep transaction is simply rolled back by Postgres)
    but worth stating plainly rather than silently having graceful
    shutdown not actually work on this platform.
    """
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _handle_signal() -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _handle_signal)

    log.info(
        "sweeper.starting",
        interval_seconds=settings.sweeper_interval_seconds,
        batch_size=settings.sweeper_batch_size,
    )
    await run_forever(settings.sweeper_interval_seconds, settings.sweeper_batch_size, stop_event)
    log.info("sweeper.stopped")


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
