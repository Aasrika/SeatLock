"""Phase 5 item 2's stale-key reaper (SPEC.md section 6): a client stuck
behind a crashed request must be able to retry.

    python -m workers.idempotency_reaper

MUST NOT blindly mark every stale IN_PROGRESS row FAILED. app/infra/
idempotency.py's begin_idempotent_request() necessarily commits the
initial IN_PROGRESS row in its OWN transaction (a concurrent second
request must be able to see it before the first has done any real work)
-- which means the completion marker (COMPLETED, with the response) is a
SEPARATE, later write. If a crash lands between "the booking committed"
and "the key was marked COMPLETED" (see BookingRow.idempotency_key's own
docstring for exactly how that can happen even though the booking write
and the completion marker are meant to share one transaction -- defence
in depth against exactly this, not the expected path), a stale scan that
only looks at idempotency_keys would see IN_PROGRESS and wrongly conclude
"nothing happened yet."  Blindly marking it FAILED would then let a
client retry, which would re-execute a request that ALREADY SUCCEEDED --
double-booking or double-confirming.

So: before flipping any stale IN_PROGRESS row, check whether a booking
already carries that (user_id, key) (BookingRow.idempotency_key,
updated by both app/booking/create.py and app/booking/confirm.py's
transactions, always holding whichever operation most recently touched
that booking) -- scoped by user_id, not idempotency_key alone, for the
same reason app/infra/idempotency.py's own lookups are (see
IdempotencyKeyRow's docstring): two different users can legitimately
submit the identical key string, and matching on the string alone here
would risk recovering the WRONG user's booking for a stale row.

    booking EXISTS -> the write succeeded; recover to COMPLETED with a
        response rebuilt from the booking's current state
        (idempotency_stale_keys_recovered_total).
    no booking -> the write never happened at all; mark FAILED so a
        retry is free to execute for real (idempotency_stale_keys_
        reaped_total).

Same shape as workers/sweeper.py: `reap_once()` independently callable
with no loop required, `run_forever`/`main_async`/`main` for the
standalone process.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.booking.responses import build_booking_response
from app.infra.config import settings
from app.infra.db import async_session_factory
from app.infra.metrics import (
    idempotency_stale_keys_reaped_total,
    idempotency_stale_keys_recovered_total,
)
from app.infra.tables import BookingRow, IdempotencyKeyRow

log = structlog.get_logger(__name__)


@dataclass
class ReapBatchResult:
    stale_found: int
    recovered: int
    reaped: int


async def reap_once(
    session: AsyncSession, timeout_seconds: float, now: datetime
) -> ReapBatchResult:
    """One reaper pass over up to `Settings.idempotency_reaper_batch_size`
    stale rows.

    FOR UPDATE SKIP LOCKED for the same reason workers/sweeper.py uses it
    on seats: a row this pass is about to decide on must not be one an
    in-flight begin_idempotent_request() (the FAILED-reclaim branch) is
    concurrently touching.
    """
    cutoff = now - timedelta(seconds=timeout_seconds)
    stale_rows = (
        (
            await session.execute(
                select(IdempotencyKeyRow)
                .where(
                    IdempotencyKeyRow.status == "IN_PROGRESS",
                    IdempotencyKeyRow.created_at <= cutoff,
                )
                .order_by(IdempotencyKeyRow.user_id, IdempotencyKeyRow.key)
                .limit(settings.idempotency_reaper_batch_size)
                .with_for_update(skip_locked=True)
            )
        )
        .scalars()
        .all()
    )

    recovered = 0
    reaped = 0
    for row in stale_rows:
        booking = (
            await session.execute(
                select(BookingRow).where(
                    BookingRow.user_id == row.user_id, BookingRow.idempotency_key == row.key
                )
            )
        ).scalar_one_or_none()

        if booking is not None:
            response = await build_booking_response(session, booking)
            row.status = "COMPLETED"
            row.response_status = 200
            row.response_body = response.model_dump(mode="json")
            recovered += 1
            log.info("idempotency_reaper.recovered", key=row.key, booking_id=booking.id)
        else:
            row.status = "FAILED"
            reaped += 1
            log.info("idempotency_reaper.reaped", key=row.key)

    await session.commit()

    if recovered:
        idempotency_stale_keys_recovered_total.inc(recovered)
    if reaped:
        idempotency_stale_keys_reaped_total.inc(reaped)

    return ReapBatchResult(stale_found=len(stale_rows), recovered=recovered, reaped=reaped)


async def run_forever(
    interval_seconds: float, timeout_seconds: float, stop_event: asyncio.Event
) -> None:
    while not stop_event.is_set():
        now = datetime.now(UTC)
        async with async_session_factory() as session:
            result = await reap_once(session, timeout_seconds, now)
        if result.stale_found:
            log.info(
                "idempotency_reaper.pass",
                stale_found=result.stale_found,
                recovered=result.recovered,
                reaped=result.reaped,
            )
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)


async def main_async() -> None:
    """See workers/sweeper.py's main_async for the Windows
    ProactorEventLoop signal-handling caveat -- identical here.
    """
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _handle_signal() -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _handle_signal)

    log.info(
        "idempotency_reaper.starting",
        interval_seconds=settings.idempotency_reaper_interval_seconds,
        timeout_seconds=settings.idempotency_stale_timeout_seconds,
    )
    await run_forever(
        settings.idempotency_reaper_interval_seconds,
        settings.idempotency_stale_timeout_seconds,
        stop_event,
    )
    log.info("idempotency_reaper.stopped")


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
