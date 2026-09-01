"""POST /api/bookings, POST /api/bookings/{id}/confirm -- thin router,
same philosophy as app/api/routes/booking.py: no business logic here,
just wiring. The actual work lives in app/booking/ (creation, confirm)
and app/infra/idempotency.py (the Idempotency-Key machinery both routes
share).

Both routes store EVERY response they produce, not only successful ones
-- SPEC.md section 6 / CLAUDE.md I4 says "the same Idempotency-Key with
the same request fingerprint always returns the same response," and a
clean 409 (e.g. seats not held) is as much "the response" as a 201 is. A
retry with the same key+fingerprint against a request that was cleanly
rejected must see that SAME rejection again, not a fresh (and possibly
different, if seat state has since changed) re-validation.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.booking.confirm import (
    BookingNotFound,
    ConfirmFailed,
    confirm_booking_transaction,
    load_booking,
)
from app.booking.create import BookingCreationFailed, CreateBookingParams, create_booking
from app.booking.responses import BookingResponse
from app.infra import idempotency
from app.infra.config import settings
from app.infra.db import get_session

router = APIRouter()


class CreateBookingRequest(BaseModel):
    event_id: int
    seat_ids: list[int]
    session_id: str
    user_id: int
    total_amount: Decimal
    currency: str


class ConfirmBookingRequest(BaseModel):
    session_id: str


async def _handle_idempotency_outcome(
    outcome: idempotency.IdempotencyOutcome,
) -> JSONResponse | None:
    """Returns a response to short-circuit with, or None if the caller
    should proceed (idempotency.New). Raises HTTPException for the 422
    conflict case -- that one has no stored response to replay, so there
    is nothing for the caller to do but reject outright.
    """
    if isinstance(outcome, idempotency.Replay):
        return JSONResponse(status_code=outcome.response_status, content=outcome.response_body)
    if isinstance(outcome, idempotency.Conflict):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Idempotency-Key reused with a different request body",
        )
    if isinstance(outcome, idempotency.InProgress):
        retry_after = str(max(1, round(outcome.retry_after_seconds)))
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            headers={"Retry-After": retry_after},
            content={"reason": "request_in_progress"},
        )
    return None


@router.post("/bookings", status_code=status.HTTP_201_CREATED, response_model=BookingResponse)
async def create_booking_route(
    body: CreateBookingRequest,
    request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
) -> BookingResponse | JSONResponse:
    now = datetime.now(UTC)
    fingerprint = idempotency.compute_fingerprint(
        request.method, request.url.path, body.model_dump(mode="json")
    )
    outcome = await idempotency.begin_idempotent_request(
        session,
        idempotency_key,
        body.user_id,
        fingerprint,
        now,
        settings.idempotency_key_ttl_seconds,
        stale_timeout_seconds=settings.idempotency_stale_timeout_seconds,
    )
    short_circuit = await _handle_idempotency_outcome(outcome)
    if short_circuit is not None:
        return short_circuit

    response: BookingResponse | None
    response_body: dict[str, Any]
    try:
        response = await create_booking(
            session,
            CreateBookingParams(
                event_id=body.event_id,
                seat_ids=body.seat_ids,
                session_id=body.session_id,
                user_id=body.user_id,
                total_amount=body.total_amount,
                currency=body.currency,
                idempotency_key=idempotency_key,
            ),
            now,
        )
        response_status = status.HTTP_201_CREATED
        response_body = response.model_dump(mode="json")
    except BookingCreationFailed as exc:
        response = None
        response_status = status.HTTP_409_CONFLICT
        response_body = {"reason": exc.reason, "failed_seat_ids": exc.failed_seat_ids}

    # Same session, same (not-yet-committed) transaction as create_booking's
    # own writes above -- see app/infra/idempotency.py's module docstring
    # for why this one shared commit is the entire point.
    await idempotency.complete_idempotent_request(
        session, idempotency_key, response_status, response_body
    )
    await session.commit()

    if response is None:
        return JSONResponse(status_code=response_status, content=response_body)
    return response


@router.post("/bookings/{booking_id}/confirm", response_model=BookingResponse)
async def confirm_booking_route(
    booking_id: int,
    body: ConfirmBookingRequest,
    request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
) -> BookingResponse | JSONResponse:
    now = datetime.now(UTC)

    # Read-only, ahead of the idempotency check: begin_idempotent_request
    # needs user_id, which for THIS route comes from the booking itself
    # (not the request body -- the client only supplies session_id here),
    # not from any write. A booking that doesn't exist yet has no user_id
    # to record and isn't a mutation to make idempotent in the first
    # place, so this 404 bypasses the idempotency machinery entirely
    # rather than trying to force it through begin_idempotent_request.
    booking = await load_booking(session, booking_id)
    if booking is None:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND, content={"reason": "booking_not_found"}
        )

    fingerprint = idempotency.compute_fingerprint(
        request.method, request.url.path, body.model_dump(mode="json")
    )
    outcome = await idempotency.begin_idempotent_request(
        session,
        idempotency_key,
        booking.user_id,
        fingerprint,
        now,
        settings.idempotency_key_ttl_seconds,
        stale_timeout_seconds=settings.idempotency_stale_timeout_seconds,
    )
    short_circuit = await _handle_idempotency_outcome(outcome)
    if short_circuit is not None:
        return short_circuit

    response: BookingResponse | None
    response_body: dict[str, Any]
    try:
        response = await confirm_booking_transaction(
            session, booking_id, body.session_id, idempotency_key, now
        )
        response_status = status.HTTP_200_OK
        response_body = response.model_dump(mode="json")
    except BookingNotFound:
        response = None
        response_status = status.HTTP_404_NOT_FOUND
        response_body = {"reason": "booking_not_found"}
    except ConfirmFailed as exc:
        response = None
        response_status = status.HTTP_409_CONFLICT
        response_body = {"reason": exc.reason}

    await idempotency.complete_idempotent_request(
        session, idempotency_key, response_status, response_body
    )
    await session.commit()

    if response is None:
        return JSONResponse(status_code=response_status, content=response_body)
    return response
