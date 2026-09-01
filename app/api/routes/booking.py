"""POST /api/holds, POST /api/holds/{seat_id}/extend -- thin router, no
business logic.

All decision-making for ACQUIRING a hold lives in the configured
SeatAcquisitionStrategy (app/inventory/strategies/) and the domain state
machine it calls into. Extending one is different: it never changes a
seat's status (it stays HELD throughout), so CLAUDE.md rule 3 ("no code
outside state_machine.py may set a seat's status") does not apply -- there
is no state_machine.py transition for it, deliberately, and this module
implements it directly as a conditional UPDATE.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy import update as sa_update
from sqlalchemy.ext.asyncio import AsyncSession

from app.infra import hold_cache
from app.infra.config import settings
from app.infra.db import get_session
from app.infra.redis import get_redis
from app.infra.tables import SeatRow
from app.inventory.strategies.base import SeatAcquisitionStrategy, get_strategy
from app.realtime.pubsub import publish_seat_update

router = APIRouter()


def _current_strategy() -> SeatAcquisitionStrategy:
    """FastAPI dependency: look up the configured strategy per request."""
    return get_strategy(settings.strategy)


class HoldRequest(BaseModel):
    event_id: int
    seat_ids: list[int]
    session_id: str


class HoldResponse(BaseModel):
    event_id: int
    seat_ids: list[int]
    session_id: str
    hold_expires_at: datetime


class HoldFailureResponse(BaseModel):
    failed: list[int]
    reason: str | None


@router.post("/holds", status_code=status.HTTP_201_CREATED, response_model=HoldResponse)
async def create_hold(
    body: HoldRequest,
    session: Annotated[AsyncSession, Depends(get_session)],
    strategy: Annotated[SeatAcquisitionStrategy, Depends(_current_strategy)],
) -> HoldResponse | JSONResponse:
    now = datetime.now(UTC)
    hold_duration = timedelta(seconds=settings.hold_duration_seconds)

    result = await strategy.acquire(session, body.seat_ids, body.session_id, hold_duration, now)

    if not result.success:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content=HoldFailureResponse(failed=result.failed, reason=result.reason).model_dump(),
        )

    hold_expires_at = now + hold_duration
    # Best-effort (app/infra/hold_cache.py never raises) -- a Redis
    # failure here must never turn an already-committed Postgres hold
    # into an HTTP error (CLAUDE.md rule 4: Redis is a cache, never the
    # source of truth).
    for seat_id in result.acquired:
        await hold_cache.set_hold_mirror(seat_id, body.session_id, hold_expires_at, now)

    # Realtime fanout (Phase 7): AFTER the strategy's own commit (every
    # strategy.acquire() implementation commits internally), never
    # before -- same ordering principle as the hold-mirror writes above.
    # section+version aren't part of AcquireResult (touching that shape
    # would mean touching all three strategies and their own test
    # suites for a value only the realtime layer needs) -- one cheap
    # follow-up SELECT instead.
    if result.acquired:
        acquired_rows = (
            await session.execute(
                select(SeatRow.id, SeatRow.section, SeatRow.version).where(
                    SeatRow.id.in_(result.acquired)
                )
            )
        ).all()
        redis_client = get_redis()
        for seat_id, section, version in acquired_rows:
            await publish_seat_update(
                redis_client,
                event_id=body.event_id,
                section=section,
                seat_id=seat_id,
                status="HELD",
                hold_expires_at=hold_expires_at,
                version=version,
            )

    return HoldResponse(
        event_id=body.event_id,
        seat_ids=result.acquired,
        session_id=body.session_id,
        hold_expires_at=hold_expires_at,
    )


class ExtendRequest(BaseModel):
    session_id: str


class ExtendResponse(BaseModel):
    seat_id: int
    session_id: str
    hold_expires_at: datetime


class ExtendFailureResponse(BaseModel):
    reason: str


async def extend_hold_at(
    session: AsyncSession,
    seat_id: int,
    session_id: str,
    now: datetime,
    new_hold_expires_at: datetime,
) -> bool:
    """The conditional UPDATE itself, with `now` and the new expiry taken
    as explicit parameters -- not read from the wall clock here -- so
    tests can construct the exact boundary (extend requested at exactly
    hold_expires_at) deterministically, the same reason every domain
    function takes `now` explicitly rather than calling datetime.now()
    internally. extend_hold() (the actual route, below) is the only
    production caller, and it always passes the real wall clock.

    SPEC.md section 5's boundary race: a user clicks "extend" at T+7:59
    while the sweeper fires at T+8:00. This is the same conditional-write
    pattern as optimistic locking (app/inventory/strategies/
    optimistic.py) applied to a different problem -- there, the WHERE
    clause's version check detects "someone else changed this row since
    I read it"; here, it detects "this hold is already gone by the time I
    tried to extend it." Both are a single atomic UPDATE whose WHERE
    clause IS the correctness check: if the row still matches, the write
    and the check happen as one indivisible operation and nothing could
    have changed it in between; if it doesn't, rowcount is 0 and nothing
    was written.

    `hold_expires_at > :now`, strictly greater-than -- matching
    `is_hold_expired`'s `<=` from the other side of the same boundary: a
    hold expiring at EXACTLY `now` must fail extension, not succeed.
    Extending it anyway would resurrect a hold the domain layer already
    considers gone the instant this request was received.

    Returns True on success, False if rowcount was 0 (hold already gone,
    held by a different session, or the seat doesn't exist) -- the
    caller's job to turn that into a clean 409, never "extend anyway."
    Do not paper over it (SPEC.md section 5's own wording): the
    frontend's correct response to a 409 here is to re-acquire, since the
    seat may already belong to someone else.
    """
    result = await session.execute(
        sa_update(SeatRow)
        .where(
            SeatRow.id == seat_id,
            SeatRow.status == "HELD",
            SeatRow.held_by_session_id == session_id,
            SeatRow.hold_expires_at > now,
        )
        .values(hold_expires_at=new_hold_expires_at, version=SeatRow.version + 1, updated_at=now)
        .returning(SeatRow.event_id, SeatRow.section, SeatRow.version)
    )
    row = result.first()

    if row is None:
        await session.rollback()
        return False

    await session.commit()
    # Refresh the mirror's TTL to match the NEW hold_expires_at, not the
    # original hold_duration -- see hold_cache.set_hold_mirror's
    # docstring for why using duration here would expire the mirror
    # before the real (extended) hold actually ends.
    await hold_cache.set_hold_mirror(seat_id, session_id, new_hold_expires_at, now)

    # Realtime fanout (Phase 7): AFTER commit. An extension doesn't
    # change status, but it DOES change hold_expires_at, which is
    # exactly what a viewer's countdown renders -- suppressing this
    # publish because "status didn't change" would leave every watching
    # client's timer silently wrong until the seat's next real status
    # change. event_id/section/version come from the UPDATE's own
    # RETURNING rather than a second query.
    event_id, section, version = row
    await publish_seat_update(
        get_redis(),
        event_id=event_id,
        section=section,
        seat_id=seat_id,
        status="HELD",
        hold_expires_at=new_hold_expires_at,
        version=version,
    )
    return True


@router.post(
    "/holds/{seat_id}/extend", status_code=status.HTTP_200_OK, response_model=ExtendResponse
)
async def extend_hold(
    seat_id: int,
    body: ExtendRequest,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> ExtendResponse | JSONResponse:
    """Thin route wrapper: real wall clock, translates extend_hold_at's
    boolean outcome into the HTTP response. See extend_hold_at for the
    actual conditional-write logic and its reasoning.
    """
    now = datetime.now(UTC)
    new_hold_expires_at = now + timedelta(seconds=settings.hold_duration_seconds)

    succeeded = await extend_hold_at(session, seat_id, body.session_id, now, new_hold_expires_at)

    if not succeeded:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content=ExtendFailureResponse(reason="hold_not_found_or_already_expired").model_dump(),
        )

    return ExtendResponse(
        seat_id=seat_id, session_id=body.session_id, hold_expires_at=new_hold_expires_at
    )
