"""SQLAlchemy 2.0 ORM tables for Seatlock's persistence layer.

This is a SEPARATE representation from app/domain/'s pure dataclasses --
they are related only through the explicit conversions in
app/infra/mappers.py. Nothing here may be imported by app/domain/.

Only the four Phase 0 tables: events, seats, bookings, booking_seats.
idempotency_keys, payment_events, and outbox belong to Phase 5 -- do not
add them here.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

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
    idempotency_key: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    confirmed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        CheckConstraint(
            "status IN ('PENDING', 'CONFIRMED', 'CANCELLED', 'REFUNDED')",
            name="booking_status_valid",
        ),
        # Non-unique -- the real dedup guarantee lives in Phase 5's
        # idempotency_keys table. This is a lookup aid only.
        Index(None, "idempotency_key"),
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
