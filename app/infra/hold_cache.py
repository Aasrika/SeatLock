"""Redis mirror of active holds -- cache only, never the source of truth
(CLAUDE.md rule 4). Every function here is best-effort: a Redis failure
is caught, logged, and counted (hold_cache_errors_total), never raised.
Redis being unavailable must degrade the system to correct-but-slower
(a hold still succeeds in Postgres; a cache-consulting read falls back to
Postgres), never to incorrect.

Key shape: `seat:{id}:hold` -> session_id, TTL set to match
hold_expires_at exactly (not hold_duration -- see set_hold_mirror's
docstring for why that distinction matters after an extension).

Write paths (SPEC.md section 5):
  - successful hold  -> set_hold_mirror   (app/api/routes/booking.py)
  - successful extend -> set_hold_mirror  (re-set with the NEW
    hold_expires_at -- see app/api/routes/booking.py's extend_hold)
  - expire (sweeper) -> delete_hold_mirror, strictly AFTER the Postgres
    commit (workers/sweeper.py's module docstring has the full ordering
    argument)
  - confirm/release  -> delete_hold_mirror (not wired to a caller yet --
    no confirm/release endpoint exists until Phase 5's booking-
    confirmation path; the function is ready for it)

Read path: check_seat_available() -- Redis first, Postgres fallback on a
miss OR any Redis error. Postgres always wins on conflict: if Redis says
"available" but Postgres disagrees, that would only happen through a bug
elsewhere (the mirror is only ever written alongside a successful
Postgres write), and this function does not special-case it -- the
reconciler (workers/reconciler.py) is what corrects mirror drift that
happens outside the normal request path.
"""

from __future__ import annotations

from datetime import datetime

from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.infra.metrics import hold_cache_errors_total
from app.infra.redis import get_redis
from app.infra.tables import SeatRow


def _key(seat_id: int) -> str:
    return f"seat:{seat_id}:hold"


async def set_hold_mirror(
    seat_id: int, session_id: str, hold_expires_at: datetime, now: datetime
) -> None:
    """SET seat:{id}:hold -> session_id, TTL = time remaining until
    hold_expires_at.

    TTL is derived from hold_expires_at, NOT from Settings.
    hold_duration_seconds -- after an extension those two values differ
    (hold_expires_at moves forward, the original duration doesn't), and
    computing TTL from duration would expire the mirror key BEFORE the
    real hold actually ends, a self-inflicted divergence for no reason.
    """
    ttl_seconds = max(1, int((hold_expires_at - now).total_seconds()))
    try:
        await get_redis().set(_key(seat_id), session_id, ex=ttl_seconds)
    except RedisError:
        hold_cache_errors_total.labels(operation="set").inc()


async def delete_hold_mirror(seat_id: int) -> None:
    try:
        await get_redis().delete(_key(seat_id))
    except RedisError:
        hold_cache_errors_total.labels(operation="delete").inc()


async def get_hold_mirror(seat_id: int) -> str | None:
    """Raw mirror lookup: the session_id currently mirrored for this seat,
    or None if there is no key OR Redis is unavailable. Callers cannot
    distinguish "genuinely no hold" from "Redis is down" from this return
    value alone -- that's deliberate; check_seat_available() is what
    actually implements the fall-back-to-Postgres contract this ambiguity
    requires.
    """
    try:
        return await get_redis().get(_key(seat_id))
    except RedisError:
        hold_cache_errors_total.labels(operation="get").inc()
        return None


async def check_seat_available(session: AsyncSession, seat_id: int, now: datetime) -> bool:
    """Is this seat available right now? Redis first (fast path), falling
    back to Postgres on a miss or any Redis error -- Postgres is always
    consulted when Redis cannot confidently answer, and Postgres's answer
    always wins.

    Lazy-expiry aware: a Postgres row that is HELD but past
    hold_expires_at counts as available, matching every other read path
    audited in Phase 4 (app/inventory/strategies/pessimistic.py's
    acquire_any_n, app/api/routes/admin.py's seat-status-counts).
    """
    mirrored_session_id = await get_hold_mirror(seat_id)
    if mirrored_session_id is not None:
        # Redis confidently says HELD -- no need to ask Postgres. If this
        # is ever wrong (e.g. stale key drift), the reconciler is what
        # corrects the mirror itself; this function does not second-guess
        # a hit, only a miss.
        return False

    row = (
        await session.execute(
            select(SeatRow.status, SeatRow.hold_expires_at).where(SeatRow.id == seat_id)
        )
    ).one_or_none()
    if row is None:
        return False
    status, hold_expires_at = row
    if status == "AVAILABLE":
        return True
    return status == "HELD" and hold_expires_at is not None and hold_expires_at <= now
