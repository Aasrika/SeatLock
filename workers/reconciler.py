"""The reconciler (SPEC.md section 5): periodically compares Redis's
hold-mirror keys against Postgres and repairs any divergence. Postgres is
the source of truth (CLAUDE.md rule 4) -- this ALWAYS repairs Redis to
match Postgres, never the reverse.

    python -m workers.reconciler

Three divergence kinds, counted and logged separately
(reconciliation_divergence_total{kind}):

  redis_key_missing_for_held_seat: Postgres says a seat is HELD, but no
    mirror key exists for it. Most likely cause: a hold succeeded in
    Postgres but its mirror SET failed (see hold_cache_errors_total).
    Repair: SET the mirror key from Postgres's own held_by_session_id and
    hold_expires_at.

  redis_key_present_for_unheld_seat: a mirror key exists for a seat
    Postgres no longer considers HELD (AVAILABLE, BOOKED, or reassigned
    to a different session -- that last case is its own kind below).
    Most likely cause: the sweeper's Redis DELETE failed after its
    Postgres commit succeeded (see workers/sweeper.py's ordering comment
    for why that ordering, despite this failure mode being possible, is
    still the correct one -- fail toward unavailability, never toward
    false availability). Repair: DELETE the stale key.

  redis_session_mismatch: a mirror key exists AND Postgres agrees the
    seat is HELD, but the SESSION IDs disagree. This is the one
    divergence kind where Redis serves an ACTIVELY WRONG answer, not
    merely a stale one -- it would tell the wrong session it holds a
    seat that actually belongs to someone else. Can only arise from a
    stale key surviving an expire-and-reacquire cycle: the old session's
    key never got deleted, and a new session then legitimately acquired
    the same seat. Repair: overwrite the mirror to match Postgres.

"The counter reconciliation_divergence_total is worth a resume line by
itself" (SPEC.md section 5, quoted directly): it says the system assumed
its own cache would drift and instrumented for it, rather than assuming
the cache is always right.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
from dataclasses import dataclass
from datetime import UTC, datetime

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.infra import hold_cache
from app.infra.config import settings
from app.infra.db import async_session_factory
from app.infra.metrics import reconciliation_divergence_total
from app.infra.redis import get_redis
from app.infra.tables import SeatRow

log = structlog.get_logger(__name__)

_KEY_PREFIX = "seat:"
_KEY_SUFFIX = ":hold"


def _seat_id_from_key(key: str) -> int | None:
    if not (key.startswith(_KEY_PREFIX) and key.endswith(_KEY_SUFFIX)):
        return None
    try:
        return int(key[len(_KEY_PREFIX) : -len(_KEY_SUFFIX)])
    except ValueError:
        return None


@dataclass
class ReconcileResult:
    redis_key_missing_for_held_seat: int = 0
    redis_key_present_for_unheld_seat: int = 0
    redis_session_mismatch: int = 0

    @property
    def total_divergences(self) -> int:
        return (
            self.redis_key_missing_for_held_seat
            + self.redis_key_present_for_unheld_seat
            + self.redis_session_mismatch
        )


async def reconcile_once(session: AsyncSession, now: datetime) -> ReconcileResult:
    """One reconciliation pass over every currently-HELD seat and every
    currently-mirrored Redis key. O(HELD seats + mirror keys), not
    O(all seats) -- fine at the scale a mirror cache exists to serve; a
    much larger deployment might shard this by event.
    """
    held_rows = (
        await session.execute(
            select(SeatRow.id, SeatRow.held_by_session_id, SeatRow.hold_expires_at).where(
                SeatRow.status == "HELD"
            )
        )
    ).all()
    held_by_seat: dict[int, tuple[str, datetime]] = {
        row.id: (row.held_by_session_id, row.hold_expires_at) for row in held_rows
    }

    redis_client = get_redis()
    mirrored: dict[int, str] = {}
    async for key in redis_client.scan_iter(match=f"{_KEY_PREFIX}*{_KEY_SUFFIX}"):
        seat_id = _seat_id_from_key(key)
        if seat_id is None:
            continue
        session_id = await redis_client.get(key)
        if session_id is not None:
            mirrored[seat_id] = session_id

    result = ReconcileResult()

    for seat_id in held_by_seat.keys() - mirrored.keys():
        held_session_id, hold_expires_at = held_by_seat[seat_id]
        log.warning(
            "reconciliation.divergence",
            kind="redis_key_missing_for_held_seat",
            seat_id=seat_id,
            postgres_state={"session_id": held_session_id, "hold_expires_at": str(hold_expires_at)},
            redis_state=None,
        )
        await hold_cache.set_hold_mirror(seat_id, held_session_id, hold_expires_at, now)
        reconciliation_divergence_total.labels(kind="redis_key_missing_for_held_seat").inc()
        result.redis_key_missing_for_held_seat += 1

    for seat_id in mirrored.keys() - held_by_seat.keys():
        log.warning(
            "reconciliation.divergence",
            kind="redis_key_present_for_unheld_seat",
            seat_id=seat_id,
            postgres_state=None,
            redis_state={"session_id": mirrored[seat_id]},
        )
        await hold_cache.delete_hold_mirror(seat_id)
        reconciliation_divergence_total.labels(kind="redis_key_present_for_unheld_seat").inc()
        result.redis_key_present_for_unheld_seat += 1

    for seat_id in held_by_seat.keys() & mirrored.keys():
        held_session_id, hold_expires_at = held_by_seat[seat_id]
        mirrored_session_id = mirrored[seat_id]
        if mirrored_session_id != held_session_id:
            log.warning(
                "reconciliation.divergence",
                kind="redis_session_mismatch",
                seat_id=seat_id,
                postgres_state={
                    "session_id": held_session_id,
                    "hold_expires_at": str(hold_expires_at),
                },
                redis_state={"session_id": mirrored_session_id},
            )
            await hold_cache.set_hold_mirror(seat_id, held_session_id, hold_expires_at, now)
            reconciliation_divergence_total.labels(kind="redis_session_mismatch").inc()
            result.redis_session_mismatch += 1

    return result


async def run_forever(interval_seconds: float, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        now = datetime.now(UTC)
        async with async_session_factory() as session:
            result = await reconcile_once(session, now)
        if result.total_divergences:
            log.info(
                "reconciler.pass",
                redis_key_missing_for_held_seat=result.redis_key_missing_for_held_seat,
                redis_key_present_for_unheld_seat=result.redis_key_present_for_unheld_seat,
                redis_session_mismatch=result.redis_session_mismatch,
            )
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)


async def main_async() -> None:
    """See workers/sweeper.py's main_async docstring for the identical
    graceful-shutdown/Windows-ProactorEventLoop reasoning -- not repeated
    here.
    """
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _handle_signal() -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _handle_signal)

    log.info("reconciler.starting", interval_seconds=settings.reconciler_interval_seconds)
    await run_forever(settings.reconciler_interval_seconds, stop_event)
    log.info("reconciler.stopped")


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
