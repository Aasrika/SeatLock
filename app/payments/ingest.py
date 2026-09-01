"""Webhook ingestion (SPEC.md section 7): authenticate, dedup-insert,
ack fast. Applying the event's EFFECT (moving a booking through
booking_state_machine.py) happens later, asynchronously, in
workers/payment_worker.py -- this module's only job is getting the
event durably and exactly-once into payment_events.

Insert first, process second: `provider_event_id` is payment_events'
own primary key (see that table's docstring), so "already seen" is a
plain unique-violation on the INSERT, not a separate SELECT-then-decide
that could itself race. A duplicate is NOT an error -- it is returned
as 200 identically to a fresh accept (webhook_duplicate_total is how
the difference is made observable instead), because a provider that
gets anything other than 2xx for what it thinks was a successful
delivery just retries again, and 500-on-duplicate is exactly the
retry-storm failure mode this design exists to avoid.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import insert, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.infra.metrics import (
    webhook_duplicate_total,
    webhook_events_total,
    webhook_signature_failures_total,
    webhook_unresolved_total,
)
from app.infra.tables import BookingRow, PaymentEventRow
from app.payments.signature import verify_signature


class MalformedPayload(Exception):
    """The signature verified (this IS the provider), but the JSON body
    doesn't have what an event needs (event_id, event_type). A genuine
    provider/integration bug, not a duplicate-delivery or unknown-
    booking situation -- the route translates this into 400, not 200:
    silently accepting a payload with no usable id would durably record
    nothing (there's no provider_event_id to dedup on) while telling the
    provider "done," which is worse than a loud rejection.
    """


@dataclass(frozen=True, slots=True)
class SignatureInvalid:
    pass


@dataclass(frozen=True, slots=True)
class Accepted:
    provider_event_id: str
    event_type: str


@dataclass(frozen=True, slots=True)
class Duplicate:
    provider_event_id: str
    event_type: str


@dataclass(frozen=True, slots=True)
class Unresolved:
    """Durably inserted, but the payload's booking_id didn't resolve to
    an existing booking (missing, malformed, or referencing an id that
    doesn't/no-longer exists) -- e.g. a test event, or an event replayed
    from a different environment's data. Still 200: rejecting a
    legitimate provider event because ITS reference doesn't match OUR
    data is the same retry-storm risk as rejecting a duplicate.
    """

    provider_event_id: str
    event_type: str


IngestOutcome = SignatureInvalid | Accepted | Duplicate | Unresolved


async def ingest_webhook(
    session: AsyncSession,
    raw_body: bytes,
    signature: str | None,
    secret: str,
    now: datetime,
) -> IngestOutcome:
    """Metrics are incremented HERE, not in the route -- same choice
    app/infra/idempotency.py's begin_idempotent_request makes for its
    own outcome counters: the outcome is decided in this function, so
    this is where it becomes observable, rather than requiring the route
    to re-derive "which counter does this outcome mean" from a value
    it's just passing through.
    """
    if not verify_signature(raw_body, signature, secret):
        webhook_signature_failures_total.inc()
        return SignatureInvalid()

    try:
        payload: dict[str, Any] = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise MalformedPayload("body is not valid JSON") from exc

    provider_event_id = payload.get("event_id")
    event_type = payload.get("event_type")
    if not isinstance(provider_event_id, str) or not isinstance(event_type, str):
        raise MalformedPayload("payload missing string event_id/event_type")

    booking_id: int | None = None
    raw_booking_id = payload.get("booking_id")
    if isinstance(raw_booking_id, int):
        exists = (
            await session.execute(select(BookingRow.id).where(BookingRow.id == raw_booking_id))
        ).scalar_one_or_none()
        if exists is not None:
            booking_id = raw_booking_id

    processing_status = None if booking_id is not None else "UNRESOLVED"

    try:
        await session.execute(
            insert(PaymentEventRow).values(
                provider_event_id=provider_event_id,
                booking_id=booking_id,
                event_type=event_type,
                payload=payload,
                received_at=now,
                processing_status=processing_status,
            )
        )
        await session.commit()
    except IntegrityError:
        await session.rollback()
        webhook_duplicate_total.inc()
        webhook_events_total.labels(type=event_type, outcome="duplicate").inc()
        return Duplicate(provider_event_id=provider_event_id, event_type=event_type)

    if booking_id is None:
        webhook_unresolved_total.inc()
        webhook_events_total.labels(type=event_type, outcome="unresolved").inc()
        return Unresolved(provider_event_id=provider_event_id, event_type=event_type)

    webhook_events_total.labels(type=event_type, outcome="accepted").inc()
    return Accepted(provider_event_id=provider_event_id, event_type=event_type)
