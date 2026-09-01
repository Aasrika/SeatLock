"""SPEC.md section 6: idempotency for any endpoint that creates or mutates
money-adjacent state (POST /api/bookings, POST /api/bookings/{id}/confirm).

Not FastAPI middleware, and not a black-box `Depends()` that wraps a whole
route -- both would fight the one requirement that actually matters here:

    THE COMPLETION MARKER MUST COMMIT IN THE SAME TRANSACTION AS THE
    BOOKING WRITE. If they are separate transactions, a crash between them
    leaves the booking committed with its idempotency_keys row still
    IN_PROGRESS, and a naive retry (or a reaper that blindly marks stale
    IN_PROGRESS rows FAILED) would re-execute the operation and double-book.

A transparent wrapper that runs "before" and "after" a route can't make
that guarantee without secretly reaching into the route's own session and
commit -- more magic than the guarantee is worth. Instead this module
exposes two explicit calls a route makes itself, both against the SAME
`AsyncSession`:

    outcome = await begin_idempotent_request(session, key, user_id,
                                              fingerprint, now, ttl_seconds)
    # ... New: do the actual business logic against `session`, no commit ...
    await complete_idempotent_request(session, key, user_id, status, body)
    await session.commit()   # ONE commit: the booking write AND this row

`begin_idempotent_request` itself still needs its own immediately-committed
transaction for the initial IN_PROGRESS insert -- a concurrent second
request with the same key must be able to see it via a unique-violation on
that INSERT, which can't happen before this insert's own commit. That
split is unavoidable and is exactly why workers/idempotency_reaper.py
cannot simply trust "IN_PROGRESS past timeout == abandoned": it must
belt-and-suspenders check whether a booking already carries the key before
concluding that (see that module's docstring).

SECURITY: Idempotency-Key is client-supplied, untrusted input. Every
query in this module filters on (user_id, key) together -- matching
idempotency_keys' own composite primary key (see IdempotencyKeyRow's
docstring) -- never on key alone. A lookup scoped by key alone would let
one user's request find (and, on a fingerprint coincidence, be served)
a DIFFERENT user's stored response merely by submitting a key that user
happened to use first. Same class of bug as a missing IDOR check on
GET /bookings/{id}: any lookup keyed on attacker-controlled input must
be scoped to the authenticated principal.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import insert, select
from sqlalchemy import update as sa_update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.infra.metrics import (
    idempotency_conflict_total,
    idempotency_in_progress_total,
    idempotent_replay_total,
)
from app.infra.tables import IdempotencyKeyRow


def compute_fingerprint(method: str, path: str, body: dict[str, Any]) -> str:
    """SHA-256 hex of method + path + a CANONICAL encoding of body.

    `body` is expected to already be the PARSED (and, for a Pydantic
    request model, validated/coerced) request body -- e.g.
    `request_model.model_dump(mode="json")` -- not raw bytes off the
    wire. Canonicalising the parsed form (sorted keys, no incidental
    whitespace) means two requests that are semantically identical but
    byte-different on the wire (different key order, extra whitespace)
    still fingerprint identically, which is the right behaviour for THIS
    hash: unlike the webhook signature check in app/payments/, nothing
    here is verifying the client's exact bytes were untampered with --
    only whether "this is the same logical request" for I4's purposes.
    """
    canonical_body = json.dumps(body, sort_keys=True, separators=(",", ":"))
    digest_input = f"{method}\n{path}\n{canonical_body}".encode()
    return hashlib.sha256(digest_input).hexdigest()


@dataclass(frozen=True, slots=True)
class New:
    """No prior attempt with this key -- or a prior attempt that was
    reaped as abandoned (FAILED) and has now been reclaimed for this
    retry. The caller should proceed with the real operation.
    """


@dataclass(frozen=True, slots=True)
class Replay:
    """COMPLETED, fingerprint matches -- return this verbatim, execute
    nothing.
    """

    response_status: int
    response_body: dict[str, Any]


@dataclass(frozen=True, slots=True)
class Conflict:
    """Same key, a DIFFERENT fingerprint -- a client bug (SPEC.md section
    6). The caller should respond 422.
    """


@dataclass(frozen=True, slots=True)
class InProgress:
    """The original request with this key has not finished yet. The
    caller should respond 409 with a Retry-After header.
    """

    retry_after_seconds: float


IdempotencyOutcome = New | Replay | Conflict | InProgress


async def begin_idempotent_request(
    session: AsyncSession,
    key: str,
    user_id: int,
    fingerprint: str,
    now: datetime,
    ttl_seconds: float,
    *,
    stale_timeout_seconds: float,
) -> IdempotencyOutcome:
    """Its own short transaction, committed immediately -- see module
    docstring for why this commit cannot be deferred to join the caller's
    later one.

    SECURITY: every query here filters on BOTH user_id and key, never key
    alone -- see IdempotencyKeyRow's own docstring. Idempotency-Key is
    client-supplied, untrusted input; a lookup scoped by key alone would
    let user B's request find (and, on a fingerprint coincidence, be
    served) user A's stored response for a key A happened to use first.
    Scoping by (user_id, key) means B's request against a key it does
    not own never finds a row at all -- it just proceeds as New(), the
    same as if the key had never been used by anyone.
    """
    try:
        await session.execute(
            insert(IdempotencyKeyRow).values(
                key=key,
                user_id=user_id,
                request_fingerprint=fingerprint,
                status="IN_PROGRESS",
                created_at=now,
                expires_at=now + timedelta(seconds=ttl_seconds),
            )
        )
        await session.commit()
        return New()
    except IntegrityError:
        await session.rollback()

    existing = (
        await session.execute(
            select(IdempotencyKeyRow).where(
                IdempotencyKeyRow.user_id == user_id, IdempotencyKeyRow.key == key
            )
        )
    ).scalar_one()

    if existing.request_fingerprint != fingerprint:
        idempotency_conflict_total.inc()
        return Conflict()

    if existing.status == "IN_PROGRESS":
        idempotency_in_progress_total.inc()
        elapsed = (now - existing.created_at).total_seconds()
        retry_after = max(1.0, stale_timeout_seconds - elapsed)
        return InProgress(retry_after_seconds=retry_after)

    if existing.status == "FAILED":
        # Reclaim: the reaper only ever marks a row FAILED when NO
        # booking carries its (user_id, key) (see idempotency_reaper.py)
        # -- there is genuinely nothing this retry could double-book by
        # re-executing, so flip the SAME row back to IN_PROGRESS rather
        # than erroring. The WHERE clause's own status='FAILED' guard is
        # what makes this safe under a concurrent second retry doing the
        # same thing: only one UPDATE can match, the other sees rowcount
        # 0 and falls back to reporting InProgress below, same as any
        # other genuinely concurrent duplicate submission.
        result = await session.execute(
            sa_update(IdempotencyKeyRow)
            .where(
                IdempotencyKeyRow.user_id == user_id,
                IdempotencyKeyRow.key == key,
                IdempotencyKeyRow.status == "FAILED",
            )
            .values(
                status="IN_PROGRESS",
                created_at=now,
                expires_at=now + timedelta(seconds=ttl_seconds),
            )
        )
        await session.commit()
        if result.rowcount == 1:
            return New()
        idempotency_in_progress_total.inc()
        return InProgress(retry_after_seconds=stale_timeout_seconds)

    # COMPLETED -- complete_idempotent_request() always sets both fields
    # together with this status, so neither is ever None here in practice;
    # the `or` fallbacks exist only to satisfy the Optional type, not
    # because either branch is expected to be taken.
    idempotent_replay_total.inc()
    return Replay(
        response_status=existing.response_status or 500,
        response_body=existing.response_body or {},
    )


async def complete_idempotent_request(
    session: AsyncSession,
    key: str,
    user_id: int,
    response_status: int,
    response_body: dict[str, Any],
) -> None:
    """Call using the SAME session as the operation's own writes, and let
    the CALLER commit once, covering both. Deliberately does not commit
    here itself -- see module docstring; this is the entire point of
    Phase 5 item 2.

    user_id is required, not optional -- see begin_idempotent_request's
    own comment: every query against this table must be scoped by
    (user_id, key), and an UPDATE keyed on `key` alone would silently
    complete/overwrite whichever user's row happens to have that key.
    """
    await session.execute(
        sa_update(IdempotencyKeyRow)
        .where(IdempotencyKeyRow.user_id == user_id, IdempotencyKeyRow.key == key)
        .values(status="COMPLETED", response_status=response_status, response_body=response_body)
    )
