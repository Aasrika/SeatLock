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

CONFIRM ON SECOND LOOK. Postgres and Redis cannot be read atomically
together -- there are two separate reads (a Postgres SELECT, a Redis
SCAN+GET), inherently non-simultaneous. That gap means a perfectly
healthy seat caught mid-transition (between its Postgres commit and its
Redis mirror write, or between the sweeper's commit and its Redis
delete) can look divergent on a single observation even though nothing
is actually wrong -- it is resolving itself in real time. That does not
threaten correctness (repairing a key that was about to be deleted
anyway is a no-op), but it does threaten the ALERTING signal:
reconciliation_divergence_total is the metric this system expects
someone to alert on, and a counter that fires on ordinary read-timing
noise gets its threshold raised by whoever is paged by it, until real
drift stops being visible too.

The fix: an observation of a non-atomic read across two independent
stores is a CANDIDATE, not a finding, until confirmed. This is the same
logic as running a concurrency test 20 times instead of once (see this
project's own test suite) -- a single sample proves nothing about a
timing-sensitive claim; a result that survives a second, independent
look is what actually counts as evidence. So: gather candidates first
(skipping any seat whose updated_at is within
Settings.reconciler_recent_change_grace_seconds of `now` -- a row
changing RIGHT NOW is presumptively a transition in flight, not drift,
and needs no further look at all this pass; it will be caught next pass
if it is real), wait Settings.reconciler_confirm_delay_seconds, then
re-read BOTH stores fresh for exactly those candidates. Still divergent
-> repair and count in reconciliation_divergence_total{kind}. Resolved
on its own -> count in reconciliation_transient_total{kind} instead, no
repair. The two counters share labels/kinds but are never collapsed --
a healthy system under real load is expected to show a nonzero transient
rate alongside a near-zero divergence rate; that is confirm-on-second-
look working, not a problem.
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
from app.infra.metrics import reconciliation_divergence_total, reconciliation_transient_total
from app.infra.redis import get_redis
from app.infra.tables import SeatRow
from workers.sweeper import measure_backlog

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
    # Candidates that resolved on their own by the confirm-on-second-look
    # re-read -- never repaired, never counted above. See module
    # docstring's "CONFIRM ON SECOND LOOK" section.
    transient_redis_key_missing_for_held_seat: int = 0
    transient_redis_key_present_for_unheld_seat: int = 0
    transient_redis_session_mismatch: int = 0

    @property
    def total_divergences(self) -> int:
        return (
            self.redis_key_missing_for_held_seat
            + self.redis_key_present_for_unheld_seat
            + self.redis_session_mismatch
        )

    @property
    def total_transient(self) -> int:
        return (
            self.transient_redis_key_missing_for_held_seat
            + self.transient_redis_key_present_for_unheld_seat
            + self.transient_redis_session_mismatch
        )


async def reconcile_once(
    session: AsyncSession,
    now: datetime,
    *,
    confirm_delay_seconds: float | None = None,
    recent_change_grace_seconds: float | None = None,
) -> ReconcileResult:
    """One reconciliation pass over every currently-HELD seat and every
    currently-mirrored Redis key. O(HELD seats + mirror keys), not
    O(all seats) -- fine at the scale a mirror cache exists to serve; a
    much larger deployment might shard this by event.

    confirm_delay_seconds / recent_change_grace_seconds default to
    Settings when not given -- overridable so tests can use a short delay
    instead of waiting the full production default.
    """
    confirm_delay = (
        settings.reconciler_confirm_delay_seconds
        if confirm_delay_seconds is None
        else confirm_delay_seconds
    )
    grace_seconds = (
        settings.reconciler_recent_change_grace_seconds
        if recent_change_grace_seconds is None
        else recent_change_grace_seconds
    )

    held_rows = (
        await session.execute(
            select(
                SeatRow.id,
                SeatRow.held_by_session_id,
                SeatRow.hold_expires_at,
                SeatRow.updated_at,
            ).where(SeatRow.status == "HELD")
        )
    ).all()
    held_by_seat: dict[int, tuple[str, datetime, datetime]] = {
        row.id: (row.held_by_session_id, row.hold_expires_at, row.updated_at) for row in held_rows
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

    missing_ids = held_by_seat.keys() - mirrored.keys()
    unheld_ids = mirrored.keys() - held_by_seat.keys()
    both_ids = held_by_seat.keys() & mirrored.keys()

    # updated_at for "present for unheld" candidates isn't in held_by_seat
    # (the seat isn't HELD) -- fetched separately so the SAME recency
    # grace period applies uniformly to every candidate kind, not just
    # the two kinds that happen to come from a HELD row.
    unheld_updated_at: dict[int, datetime] = {}
    if unheld_ids:
        rows = await session.execute(
            select(SeatRow.id, SeatRow.updated_at).where(SeatRow.id.in_(unheld_ids))
        )
        unheld_updated_at = dict(rows.all())

    def _recently_changed(updated_at: datetime | None) -> bool:
        # 0 <= diff, not just diff < grace_seconds: a NEGATIVE diff means
        # updated_at is after `now`, which should never happen with a
        # real wall clock (a row can't be updated in the future) --
        # treating it as "not recent" rather than matching the `< 0 <
        # grace_seconds` comparison by accident keeps this robust against
        # clock skew instead of silently filtering out every candidate.
        if updated_at is None:
            return False
        diff = (now - updated_at).total_seconds()
        return 0 <= diff < grace_seconds

    candidates: list[tuple[int, str]] = []
    for seat_id in missing_ids:
        if not _recently_changed(held_by_seat[seat_id][2]):
            candidates.append((seat_id, "redis_key_missing_for_held_seat"))
    for seat_id in unheld_ids:
        if not _recently_changed(unheld_updated_at.get(seat_id)):
            candidates.append((seat_id, "redis_key_present_for_unheld_seat"))
    for seat_id in both_ids:
        held_session_id, _, updated_at = held_by_seat[seat_id]
        if mirrored[seat_id] != held_session_id and not _recently_changed(updated_at):
            candidates.append((seat_id, "redis_session_mismatch"))

    result = ReconcileResult()
    if not candidates:
        return result

    # See module docstring's "CONFIRM ON SECOND LOOK" -- everything
    # gathered above is a candidate, not a finding, until it survives
    # this wait-and-recheck.
    await asyncio.sleep(confirm_delay)

    for seat_id, kind in candidates:
        row = (
            await session.execute(
                select(SeatRow.status, SeatRow.held_by_session_id, SeatRow.hold_expires_at).where(
                    SeatRow.id == seat_id
                )
            )
        ).one_or_none()
        current_status = row.status if row is not None else None
        current_holder = row.held_by_session_id if row is not None else None
        current_hold_expires_at = row.hold_expires_at if row is not None else None
        redis_session_id = await redis_client.get(f"{_KEY_PREFIX}{seat_id}{_KEY_SUFFIX}")

        if kind == "redis_key_missing_for_held_seat":
            still_divergent = current_status == "HELD" and redis_session_id is None
        elif kind == "redis_key_present_for_unheld_seat":
            still_divergent = current_status != "HELD" and redis_session_id is not None
        else:  # redis_session_mismatch
            still_divergent = (
                current_status == "HELD"
                and redis_session_id is not None
                and redis_session_id != current_holder
            )

        if not still_divergent:
            log.debug("reconciliation.transient", kind=kind, seat_id=seat_id)
            reconciliation_transient_total.labels(kind=kind).inc()
            if kind == "redis_key_missing_for_held_seat":
                result.transient_redis_key_missing_for_held_seat += 1
            elif kind == "redis_key_present_for_unheld_seat":
                result.transient_redis_key_present_for_unheld_seat += 1
            else:
                result.transient_redis_session_mismatch += 1
            continue

        log.warning(
            "reconciliation.divergence",
            kind=kind,
            seat_id=seat_id,
            postgres_state=(
                {"session_id": current_holder, "hold_expires_at": str(current_hold_expires_at)}
                if current_status == "HELD"
                else {"status": current_status}
            ),
            redis_state={"session_id": redis_session_id} if redis_session_id else None,
        )
        if kind == "redis_key_present_for_unheld_seat":
            await hold_cache.delete_hold_mirror(seat_id)
        else:
            await hold_cache.set_hold_mirror(seat_id, current_holder, current_hold_expires_at, now)

        reconciliation_divergence_total.labels(kind=kind).inc()
        if kind == "redis_key_missing_for_held_seat":
            result.redis_key_missing_for_held_seat += 1
        elif kind == "redis_key_present_for_unheld_seat":
            result.redis_key_present_for_unheld_seat += 1
        else:
            result.redis_session_mismatch += 1

    return result


async def run_forever(interval_seconds: float, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        now = datetime.now(UTC)
        async with async_session_factory() as session:
            result = await reconcile_once(session, now)
            # Phase 8a's chaos suite (loadtest/chaos/scenarios/
            # sweeper_killed.py) found sweeper_backlog_gauge's blind spot:
            # it was only ever written by workers/sweeper.py's own loop,
            # so killing the sweeper didn't just stop it draining the
            # backlog -- it stopped the MEASUREMENT too, freezing the one
            # signal meant to reveal the sweeper is gone. measure_backlog()
            # was already written to be meaningful called independently
            # (see its own docstring); calling it here, from a completely
            # separate process on its own schedule, means the gauge keeps
            # reflecting reality even if the sweeper itself is dead --
            # multiprocess_mode="mostrecent" is exactly "whichever process
            # last measured wins," which is what makes two independent
            # writers to the same gauge correct rather than a race.
            await measure_backlog(session, now)
        if result.total_divergences or result.total_transient:
            log.info(
                "reconciler.pass",
                redis_key_missing_for_held_seat=result.redis_key_missing_for_held_seat,
                redis_key_present_for_unheld_seat=result.redis_key_present_for_unheld_seat,
                redis_session_mismatch=result.redis_session_mismatch,
                transient_redis_key_missing_for_held_seat=(
                    result.transient_redis_key_missing_for_held_seat
                ),
                transient_redis_key_present_for_unheld_seat=(
                    result.transient_redis_key_present_for_unheld_seat
                ),
                transient_redis_session_mismatch=result.transient_redis_session_mismatch,
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
