"""POST /api/holds -- thin router, no business logic.

All decision-making lives in the configured SeatAcquisitionStrategy
(app/inventory/strategies/) and the domain state machine it calls into.
This module only parses the request, calls the strategy, and translates
the result into an HTTP response.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.infra.config import settings
from app.infra.db import get_session
from app.inventory.strategies.base import SeatAcquisitionStrategy, get_strategy

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

    return HoldResponse(
        event_id=body.event_id,
        seat_ids=result.acquired,
        session_id=body.session_id,
        hold_expires_at=now + hold_duration,
    )
