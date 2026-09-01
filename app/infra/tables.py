"""SQLAlchemy 2.0 ORM tables for Seatlock's persistence layer.

This is a SEPARATE representation from app/domain/'s pure dataclasses --
they are related only through the explicit conversions in
app/infra/mappers.py. Nothing here may be imported by app/domain/.

The four Phase 0 tables (events, seats, bookings, booking_seats) plus
Phase 5's idempotency_keys, payment_events, and outbox.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# This naming convention must exist before the first migration is
# generated. Unnamed constraints get server-generated names (e.g.
# "seats_status_check1") that Alembic cannot reliably reference, which
# breaks downgrade and every future ALTER. Retrofitting this later requires
# manual rename migrations.
NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class EventRow(Base):
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    venue: Mapped[str] = mapped_column(String, nullable=False)
    starts_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    total_seats: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class SeatRow(Base):
    __tablename__ = "seats"

    # BIGINT identity chosen over UUID: monotonic ids give B-tree insert
    # locality on this hot inventory table, avoiding the page-split noise a
    # random UUID primary key would introduce -- noise that would
    # contaminate the Phase 3 concurrency benchmarks. If distributed id
    # generation is ever required (e.g. sharding inventory by event across
    # separate databases), UUIDv7 is the documented alternative: still
    # monotonic, so it doesn't reintroduce this problem the way UUIDv4 would.
    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    event_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("events.id", ondelete="CASCADE"), nullable=False
    )
    section: Mapped[str] = mapped_column(String, nullable=False)
    row_label: Mapped[str] = mapped_column(String, nullable=False)
    seat_number: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    held_by_session_id: Mapped[str | None] = mapped_column(String, nullable=True)
    hold_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # DENORMALISED cache of booking_seats, not the source of truth.
    # booking_seats is authoritative and carries the I1 partial-unique index
    # (see BookingSeatRow below); this column exists only so the hot
    # availability read (list seats for an event) can stay on a single
    # table instead of a join. Both MUST be written in the same
    # transaction -- see app.domain.invariants.check_booking_linkage, which
    # asserts they never diverge.
    #
    # ondelete="SET NULL": seats are never hard-deleted in this design, but
    # a booking's row could theoretically be removed (e.g. GDPR erasure,
    # far outside current scope); if it ever is, the seat must fall back to
    # an unlinked state rather than the FK silently blocking or cascading
    # away the seat itself.
    booking_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("bookings.id", ondelete="SET NULL"), nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        UniqueConstraint("event_id", "section", "row_label", "seat_number"),
        CheckConstraint("status IN ('AVAILABLE', 'HELD', 'BOOKED')", name="seat_status_valid"),
        # The sweeper's hot query: `WHERE status = 'HELD' AND hold_expires_at
        # < now()`. Partial so the index only covers HELD rows -- AVAILABLE
        # and BOOKED seats, the overwhelming majority most of the time,
        # never enter it.
        Index(None, "hold_expires_at", postgresql_where=text("status = 'HELD'")),
        Index(None, "event_id", "status"),
    )


class BookingRow(Base):
    __tablename__ = "bookings"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    event_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("events.id"), nullable=False)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    session_id: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    total_amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    # VARCHAR, not CHAR: Postgres blank-pads CHAR(n), which produces
    # surprising trailing-space behaviour on comparison and export.
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    # Which seats this booking claims -- set once, at creation, and never
    # revised. NOT expressed via SeatRow.booking_id: that column is
    # written only by app/domain/state_machine.py's hold()/confirm()
    # (deliberately None while HELD, set together with BOOKED), and
    # app/domain/invariants.py's check_state_coherence() has always
    # required exactly that (HELD -> booking_id None). A PENDING booking
    # created before confirm still needs to know which seats it claims,
    # but writing SeatRow.booking_id on a still-HELD seat to record that
    # would violate that invariant -- confirmed directly: an early
    # version of app/booking/create.py did exactly this and
    # check_state_coherence caught it immediately in
    # tests/integration/test_idempotency.py. This column is the fix:
    # app/booking/create.py and .../confirm.py read/write it instead of
    # touching seats at all until the seat is genuinely BOOKED.
    seat_ids: Mapped[list[int]] = mapped_column(ARRAY(BigInteger), nullable=False)
    # Filed in Phase 0 as "a lookup aid only" (see the old comment this one
    # replaces) -- turns out to be load-bearing for crash recovery instead
    # (Phase 5): it always holds the Idempotency-Key of whichever operation
    # (create or confirm) most recently wrote this row, overwritten by each
    # subsequent operation. app/infra/idempotency.py's own idempotency_keys
    # table is the actual dedup/response-cache mechanism and the source of
    # truth for a key's status; THIS column is what workers/
    # idempotency_reaper.py joins back through when it finds a stale
    # IN_PROGRESS row -- if a booking already exists carrying that key, the
    # booking write itself succeeded and only the completion marker was
    # lost (e.g. a crash after commit), so the reaper recovers COMPLETED
    # from the booking's own current state rather than wrongly marking a
    # successful operation FAILED (which would let a client retry and
    # double-book). See idempotency_reaper.py's module docstring.
    idempotency_key: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    confirmed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        CheckConstraint(
            "status IN ('PENDING', 'CONFIRMED', 'CANCELLED', 'REFUNDED', 'REFUND_REQUIRED')",
            name="booking_status_valid",
        ),
        # Unique (Phase 5, was a non-unique lookup index in Phase 0): now
        # that idempotency_reaper.py relies on "at most one booking per
        # key" to recover cleanly, two bookings silently sharing a key
        # would make that recovery lookup ambiguous. Partial, WHERE NOT
        # NULL: Postgres's plain unique indexes already never treat two
        # NULLs as colliding, so this WHERE clause changes nothing about
        # actual behaviour -- it is here so a future reader doesn't need
        # to already know that Postgres default in order to see that
        # bookings created outside the idempotency path (idempotency_key
        # left NULL) are deliberately, not accidentally, exempt here.
        Index(
            None,
            "idempotency_key",
            unique=True,
            postgresql_where=text("idempotency_key IS NOT NULL"),
        ),
    )


class BookingSeatRow(Base):
    __tablename__ = "booking_seats"

    booking_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("bookings.id", ondelete="CASCADE"), primary_key=True
    )
    # RESTRICT, not CASCADE: seats are never hard-deleted in this design
    # (their lifecycle is expressed through `status`, not row deletion).
    # RESTRICT documents that expectation and fails loudly at the DB level
    # if anything ever tries to delete a seat that still has booking_seats
    # history attached, instead of silently cascading the deletion away.
    seat_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("seats.id", ondelete="RESTRICT"), primary_key=True
    )
    released_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        # DB-level last line of defence for I1 (no oversell). A seat can
        # accumulate many historical booking_seats rows over time, but at
        # most one may have released_at IS NULL at once. Postgres can only
        # express this as a partial index, not a table-level UNIQUE
        # constraint, since constraints cannot carry a WHERE clause -- that
        # is why this is an Index(unique=True) rather than a
        # UniqueConstraint.
        Index(
            None,
            "seat_id",
            unique=True,
            postgresql_where=text("released_at IS NULL"),
        ),
    )


class HoldAuditRow(Base):
    """Diagnostic-only: one row per *successful* acquire(), regardless of
    strategy or whether the acquisition should have succeeded.

    This is not part of SPEC.md's core schema -- it exists so Phase 1's
    naive strategy's oversell can be proven after the fact. `seats` only
    ever shows the last write for a given seat, which hides a transient
    double-acquisition (two sessions both briefly believing they hold the
    same seat). hold_audit is append-only, so
    `GET /api/admin/oversell-report` can find every session that ever
    successfully held a given seat and flag any seat with more than one.
    """

    __tablename__ = "hold_audit"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    seat_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("seats.id", ondelete="CASCADE"), nullable=False
    )
    session_id: Mapped[str] = mapped_column(String, nullable=False)
    acquired_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (Index(None, "seat_id"),)


class IdempotencyKeyRow(Base):
    """SPEC.md section 6. The dedup/response-cache mechanism for any
    endpoint that mutates money-adjacent state (POST /api/bookings, POST
    /api/bookings/{id}/confirm) -- see app/infra/idempotency.py for the
    four-case flow this table drives, and workers/idempotency_reaper.py
    for what happens to a row that never leaves IN_PROGRESS.
    """

    __tablename__ = "idempotency_keys"

    key: Mapped[str] = mapped_column(String, primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    request_fingerprint: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    response_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    response_body: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    # Not a TTL on the KEY's dedup guarantee (that lasts as long as the row
    # exists) -- this is when the row becomes eligible for cleanup/deletion
    # by some future retention job. No such job exists yet in this phase;
    # the column ships now because SPEC.md section 3 specifies it as part
    # of this table's shape.
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        CheckConstraint(
            "status IN ('IN_PROGRESS', 'COMPLETED', 'FAILED')", name="idempotency_key_status_valid"
        ),
        # workers/idempotency_reaper.py's own query: find IN_PROGRESS rows
        # older than a timeout. Partial so the index only covers rows that
        # could possibly be stale -- COMPLETED/FAILED rows, the eventual
        # majority, never enter it.
        Index(None, "created_at", postgresql_where=text("status = 'IN_PROGRESS'")),
    )


class PaymentEventRow(Base):
    """SPEC.md section 7. provider_event_id is the primary key on purpose
    (not a separate surrogate id) -- the dedup guarantee this table exists
    for IS "insert once per provider_event_id, ever", so making that value
    the actual primary key means Postgres's own unique-violation error is
    the dedup check; there is no separate query to get it wrong or skip.
    """

    __tablename__ = "payment_events"

    provider_event_id: Mapped[str] = mapped_column(String, primary_key=True)
    # Nullable, ON DELETE SET NULL: a webhook whose payload names a
    # booking_id that does not (or no longer) exists must still be
    # insertable -- see app/payments/webhook.py's UNRESOLVED handling.
    # Rejecting the INSERT outright would mean returning something other
    # than 200 for a legitimate provider event, which is exactly the
    # retry-storm failure mode this table's whole design exists to avoid.
    booking_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("bookings.id", ondelete="SET NULL"), nullable=True
    )
    event_type: Mapped[str] = mapped_column(String, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # NULL until the webhook route's own INSERT decides it: 'UNRESOLVED'
    # immediately (booking_id didn't resolve, nothing for a worker to act
    # on), otherwise NULL/pending until workers/payment_worker.py processes
    # it and sets a terminal value ('APPLIED', 'REJECTED', 'ERROR').
    processing_status: Mapped[str | None] = mapped_column(String(16), nullable=True)

    __table_args__ = (
        Index(None, "booking_id"),
        # workers/payment_worker.py's queue query: unprocessed events,
        # oldest first. Partial for the same reason as the idempotency
        # reaper's index above -- most rows are done and should never be
        # rescanned.
        Index(None, "received_at", postgresql_where=text("processed_at IS NULL")),
    )


class OutboxRow(Base):
    """SPEC.md section 3 ("outbox (Phase 5+)"). Transactional-outbox
    pattern: a row is written here in the SAME transaction as whatever
    booking-status change it describes (confirm, late-success
    refund-required, webhook-driven refund -- see app/booking/confirm.py
    and workers/payment_worker.py), so the event is durable the instant
    the state change is, with no separate commit to lose.

    No publisher/consumer exists yet in this phase -- that is
    app/realtime/'s future job (SPEC.md section 5 mentions the sweeper
    "publishing release events" with the same scope note: out of scope
    until whichever phase builds that layer). Shipping the durable-write
    half now, unconsumed, is still correct: published_at simply never
    leaves NULL until a consumer exists to set it.
    """

    __tablename__ = "outbox"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    # Free-text "type:id" (e.g. "booking:123"), not a FK -- the outbox is
    # deliberately generic across aggregate types, and a future non-booking
    # event source should not require a schema change here to participate.
    aggregate_id: Mapped[str] = mapped_column(String, nullable=False)
    event_type: Mapped[str] = mapped_column(String, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (Index(None, "created_at", postgresql_where=text("published_at IS NULL")),)
