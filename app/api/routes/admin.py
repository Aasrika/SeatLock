"""GET /api/admin/invariants and GET /api/admin/oversell-report.

Thin router: the checks themselves are app/domain/invariants.py's pure
functions; this module's job is only to load the seats/audit rows they
need and shape the response. /invariants is what the load harness
(loadtest/run_benchmark.py) polls DURING a run, not just after -- catching
a violation that self-heals before the run ends is the whole point
(SPEC.md section 10).
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.errors import InvariantViolation
from app.domain.invariants import (
    check_booking_linkage,
    check_conservation,
    check_no_double_booking,
    check_state_coherence,
)
from app.infra.db import get_session
from app.infra.mappers import seat_to_domain
from app.infra.tables import BookingSeatRow, EventRow, HoldAuditRow, SeatRow

router = APIRouter()


class InvariantResult(BaseModel):
    passed: bool
    detail: str | None = None


class InvariantsResponse(BaseModel):
    event_id: int
    checked_at: datetime
    results: dict[str, InvariantResult]
    all_passed: bool


async def _get_event_or_404(session: AsyncSession, event_id: int) -> EventRow:
    event = await session.get(EventRow, event_id)
    if event is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="event not found")
    return event


@router.get("/invariants", response_model=InvariantsResponse)
async def get_invariants(
    event_id: int, session: Annotated[AsyncSession, Depends(get_session)]
) -> InvariantsResponse:
    event = await _get_event_or_404(session, event_id)

    seat_rows = (
        (await session.execute(select(SeatRow).where(SeatRow.event_id == event_id))).scalars().all()
    )
    seats = [seat_to_domain(row) for row in seat_rows]

    active_seat_ids = set(
        (
            await session.execute(
                select(BookingSeatRow.seat_id)
                .join(SeatRow, SeatRow.id == BookingSeatRow.seat_id)
                .where(SeatRow.event_id == event_id, BookingSeatRow.released_at.is_(None))
            )
        )
        .scalars()
        .all()
    )

    # Every check runs independently -- one invariant failing must never
    # prevent the others from being reported.
    results: dict[str, InvariantResult] = {}

    def run_check(name: str, check: Callable[[], None]) -> None:
        try:
            check()
            results[name] = InvariantResult(passed=True)
        except InvariantViolation as exc:
            results[name] = InvariantResult(passed=False, detail=str(exc))

    run_check("conservation", lambda: check_conservation(seats, event.total_seats))
    run_check("no_double_booking", lambda: check_no_double_booking(seats))
    run_check("state_coherence", lambda: check_state_coherence(seats))
    run_check("booking_linkage", lambda: check_booking_linkage(seats, active_seat_ids))

    return InvariantsResponse(
        event_id=event_id,
        checked_at=datetime.now(UTC),
        results=results,
        all_passed=all(result.passed for result in results.values()),
    )


class SeatOversell(BaseModel):
    seat_id: int
    distinct_holders: int
    holders: list[str]


class OversellReportResponse(BaseModel):
    event_id: int
    seats: list[SeatOversell]
    # Two genuinely different numbers -- collapsing them into one
    # "total_oversell_count" hid the actual signal (see loadtest/results/
    # for the investigation): oversold_seats is capped at the number of
    # seats in contention (1 for a single-seat scenario, however many are
    # in play for a multi-seat one) and cannot show a distribution by
    # itself. excess_holders (sum over seats of holders - 1) has no such
    # ceiling and is what actually varies run to run under a real race.
    oversold_seats: int
    excess_holders: int


@router.get("/oversell-report", response_model=OversellReportResponse)
async def get_oversell_report(
    event_id: int, session: Annotated[AsyncSession, Depends(get_session)]
) -> OversellReportResponse:
    await _get_event_or_404(session, event_id)

    rows = (
        await session.execute(
            select(HoldAuditRow.seat_id, HoldAuditRow.session_id)
            .join(SeatRow, SeatRow.id == HoldAuditRow.seat_id)
            .where(SeatRow.event_id == event_id)
        )
    ).all()

    holders_by_seat: dict[int, set[str]] = {}
    for seat_id, session_id in rows:
        holders_by_seat.setdefault(seat_id, set()).add(session_id)

    seats = [
        SeatOversell(seat_id=seat_id, distinct_holders=len(holders), holders=sorted(holders))
        for seat_id, holders in sorted(holders_by_seat.items())
    ]
    oversold_seats = sum(1 for seat in seats if seat.distinct_holders > 1)
    excess_holders = sum(max(0, seat.distinct_holders - 1) for seat in seats)

    return OversellReportResponse(
        event_id=event_id,
        seats=seats,
        oversold_seats=oversold_seats,
        excess_holders=excess_holders,
    )
