"""Phase 5 items 3/4 (SPEC.md section 7): applies payment_events'
durably-recorded effects to bookings, asynchronously -- the webhook
route (app/api/routes/webhooks.py) only ever inserts and acks fast; this
is what actually moves a booking through booking_state_machine.py.

    python -m workers.payment_worker

Only picks up rows with processing_status IS NULL -- events the webhook
route already flagged 'UNRESOLVED' (no resolvable booking_id) are
deliberately never queued here; see PaymentEventRow's own docstring for
why they still had to be durably inserted and 200'd anyway.

Every effect is guarded through the domain layer exactly like every
other seat/booking transition in this codebase (CLAUDE.md rule 3,
app/domain/booking_state_machine.py) -- an illegal transition (SPEC.md
section 7's out-of-order case: payment.succeeded arriving after
payment.refunded already moved a booking to REFUNDED) is logged and
recorded, never applied, and never raised past this worker: one bad
event must not abort an entire batch, same reasoning as workers/
sweeper.py's IllegalTransition handling.

THE LATE-SUCCESS CASE (item 4): payment.succeeded reaches a PENDING
booking whose seats are no longer validly held for it (the hold expired
and the seats were resold, or swept, before the payment cleared). The
booking moves to REFUND_REQUIRED; the seat is NOT touched -- it may
already legitimately belong to someone else's booking, and reclaiming
it would double-allocate exactly what this whole project exists to
prevent. late_payment_refund_required_total counts every occurrence;
nothing here ever tries to resolve REFUND_REQUIRED automatically (see
booking_state_machine.require_refund's own docstring -- a human/finance
process takes over from here).

Same fail-toward-the-recoverable-side principle as workers/sweeper.py's
Postgres-then-Redis delete ordering and Phase 4's lazy expiry: money is
reversible (refund it), a seat someone else now holds is not (undoing
that would be a second, DIFFERENT oversell, not a fix for this one).
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
from dataclasses import dataclass
from datetime import UTC, datetime

import structlog
from sqlalchemy import insert, select
from sqlalchemy import update as sa_update
from sqlalchemy.ext.asyncio import AsyncSession

from app.booking.confirm import attempt_confirm_write, load_booking
from app.domain.booking_state_machine import confirm_booking as domain_confirm_booking
from app.domain.booking_state_machine import refund_booking as domain_refund_booking
from app.domain.booking_state_machine import require_refund as domain_require_refund
from app.domain.errors import IllegalBookingTransition
from app.infra.config import settings
from app.infra.db import async_session_factory
from app.infra.mappers import booking_to_domain
from app.infra.metrics import late_payment_refund_required_total
from app.infra.tables import BookingRow, OutboxRow, PaymentEventRow, SeatRow

log = structlog.get_logger(__name__)


@dataclass
class ProcessBatchResult:
    processed: int
    applied: int
    rejected: int


async def _seats_still_valid_for_booking(
    session: AsyncSession, booking: BookingRow, now: datetime
) -> bool:
    """All of a PENDING booking's seats must still be HELD by that
    booking's own session, unexpired -- exactly what attempt_confirm_
    write's own WHERE clause requires. Checked separately (read-only)
    first so the late-success branch can be taken WITHOUT attempting (and
    rolling back) a write first -- a cleaner, if slightly redundant with
    attempt_confirm_write's own guard, way to decide which path to take.
    """
    seat_rows = (
        (await session.execute(select(SeatRow).where(SeatRow.id.in_(booking.seat_ids))))
        .scalars()
        .all()
    )
    if len(seat_rows) != len(booking.seat_ids):
        return False
    return all(
        row.status == "HELD"
        and row.held_by_session_id == booking.session_id
        and row.hold_expires_at is not None
        and row.hold_expires_at > now
        for row in seat_rows
    )


async def _apply_payment_succeeded(session: AsyncSession, booking_id: int, now: datetime) -> str:
    """Returns the terminal processing_status to record: 'APPLIED' or
    'REJECTED'. Never raises for a business-level outcome (illegal
    transition, late success) -- those are all legitimate results this
    function itself decides between, not errors.
    """
    booking = await load_booking(session, booking_id)
    if booking is None:
        return "REJECTED"  # defensive; booking_id was resolved at ingest time

    if booking.status == "CONFIRMED":
        # Already confirmed -- by the synchronous confirm route racing
        # this event, or by an earlier delivery of this same event
        # somehow reaching here twice. Idempotent no-op (I5: same effect
        # N times == once), not an illegal transition to log/count.
        return "APPLIED"

    if booking.status != "PENDING":
        # REFUNDED, REFUND_REQUIRED, or CANCELLED -- SPEC.md section 7's
        # out-of-order case: e.g. payment.refunded already moved this
        # booking past PENDING, and this payment.succeeded (a duplicate,
        # or delivered late/out of order) must not resurrect it. Calling
        # the domain function is what actually PROVES this is illegal
        # (rather than just asserting it from the status check above) --
        # it is the authoritative rule, this is just where it is invoked.
        try:
            domain_confirm_booking(booking_to_domain(booking), now)
        except IllegalBookingTransition as exc:
            log.info(
                "payment_worker.illegal_transition",
                booking_id=booking_id,
                from_status=booking.status,
                reason=str(exc),
            )
        return "REJECTED"

    if not await _seats_still_valid_for_booking(session, booking, now):
        # THE LATE-SUCCESS CASE -- see module docstring.
        try:
            domain_require_refund(booking_to_domain(booking), now)
        except IllegalBookingTransition:
            return "REJECTED"  # can't happen: status == PENDING was just checked
        result = await session.execute(
            sa_update(BookingRow)
            .where(BookingRow.id == booking_id, BookingRow.status == "PENDING")
            .values(status="REFUND_REQUIRED")
        )
        if result.rowcount != 1:
            return "REJECTED"  # raced with something else; next delivery (if any) retries
        await session.execute(
            insert(OutboxRow),
            {
                "aggregate_id": f"booking:{booking_id}",
                "event_type": "booking.refund_required",
                "payload": {
                    "booking_id": booking_id,
                    "reason": "late_payment_success_after_resale",
                },
                "created_at": now,
            },
        )
        late_payment_refund_required_total.inc()
        log.info("payment_worker.late_success_refund_required", booking_id=booking_id)
        return "APPLIED"

    # Seats still validly held for this booking -- confirm, via the SAME
    # write attempt_confirm_write's own docstring says is shared with the
    # synchronous confirm route. No idempotency_key: this confirmation
    # was triggered by a webhook, not a client request -- see that
    # function's own docstring for why the column is left untouched here.
    ok = await attempt_confirm_write(
        session, booking_id, list(booking.seat_ids), booking.session_id, now
    )
    if not ok:
        # Raced with something between the check above and this write
        # (e.g. the sweeper reclaimed it in between) -- re-check on the
        # NEXT delivery/reprocessing rather than deciding here; returning
        # REJECTED leaves this event's outcome visible without silently
        # retrying it automatically (this worker does not re-queue
        # events -- see module docstring's scope).
        return "REJECTED"
    return "APPLIED"


async def _apply_payment_refunded(session: AsyncSession, booking_id: int, now: datetime) -> str:
    booking = await load_booking(session, booking_id)
    if booking is None:
        return "REJECTED"

    try:
        refunded = domain_refund_booking(booking_to_domain(booking), now)
    except IllegalBookingTransition as exc:
        log.info(
            "payment_worker.illegal_transition",
            booking_id=booking_id,
            from_status=booking.status,
            reason=str(exc),
        )
        return "REJECTED"

    result = await session.execute(
        sa_update(BookingRow)
        .where(BookingRow.id == booking_id, BookingRow.status == "CONFIRMED")
        .values(status=refunded.status.value)
    )
    if result.rowcount != 1:
        return "REJECTED"

    # Release the seats back to AVAILABLE -- the domain-legal counterpart
    # to attempt_confirm_write's BOOKED transition. Uses
    # app/domain/state_machine.py's release() semantics directly (via an
    # equivalent conditional UPDATE) rather than calling it per-seat and
    # writing back, matching every other multi-seat write in this module.
    await session.execute(
        sa_update(SeatRow)
        .where(
            SeatRow.id.in_(booking.seat_ids),
            SeatRow.status == "BOOKED",
            SeatRow.booking_id == booking_id,
        )
        .values(status="AVAILABLE", version=SeatRow.version + 1, booking_id=None, updated_at=now)
    )
    await session.execute(
        insert(OutboxRow),
        {
            "aggregate_id": f"booking:{booking_id}",
            "event_type": "booking.refunded",
            "payload": {"booking_id": booking_id, "seat_ids": list(booking.seat_ids)},
            "created_at": now,
        },
    )
    return "APPLIED"


_HANDLERS = {
    "payment.succeeded": _apply_payment_succeeded,
    "payment.refunded": _apply_payment_refunded,
}


async def process_once(session: AsyncSession, batch_size: int, now: datetime) -> ProcessBatchResult:
    rows = (
        (
            await session.execute(
                select(PaymentEventRow)
                .where(PaymentEventRow.processing_status.is_(None))
                .order_by(PaymentEventRow.received_at)
                .limit(batch_size)
                .with_for_update(skip_locked=True)
            )
        )
        .scalars()
        .all()
    )

    applied = 0
    rejected = 0
    for event in rows:
        handler = _HANDLERS.get(event.event_type)
        if handler is None:
            log.info("payment_worker.unknown_event_type", event_type=event.event_type)
            event.processing_status = "REJECTED"
            rejected += 1
            continue

        # event.booking_id is guaranteed non-None: the webhook route only
        # ever leaves processing_status NULL (this query's own filter)
        # when booking_id resolved at ingest time (see PaymentEventRow's
        # docstring / app/payments/ingest.py).
        status_value = await handler(session, event.booking_id, now)
        event.processing_status = status_value
        event.processed_at = now
        if status_value == "APPLIED":
            applied += 1
        else:
            rejected += 1

    await session.commit()

    return ProcessBatchResult(processed=len(rows), applied=applied, rejected=rejected)


async def run_forever(interval_seconds: float, batch_size: int, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        now = datetime.now(UTC)
        async with async_session_factory() as session:
            result = await process_once(session, batch_size, now)
        if result.processed:
            log.info(
                "payment_worker.pass",
                processed=result.processed,
                applied=result.applied,
                rejected=result.rejected,
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
        "payment_worker.starting",
        interval_seconds=settings.payment_worker_interval_seconds,
        batch_size=settings.payment_worker_batch_size,
    )
    await run_forever(
        settings.payment_worker_interval_seconds, settings.payment_worker_batch_size, stop_event
    )
    log.info("payment_worker.stopped")


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
