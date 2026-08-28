"""Integration tests for the Phase 0 schema: migrations, constraints, and
mapper round-trips against a real Postgres via testcontainers.

Ordering note: TestMigrationLifecycle's two tests run schema-level DDL --
the only tests in this module allowed to -- and are defined first so
pytest's default top-to-bottom-in-file execution order runs them before the
constraint/round-trip tests below, which assume a fully migrated, empty
schema. The downgrade+upgrade test restores the schema to `head` before it
finishes, so nothing later is affected. Every other test gets its own
connection-level transaction that is rolled back afterward (see `db_conn`),
so they never leak data into one another regardless of order.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from sqlalchemy import insert, inspect, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession, create_async_engine
from testcontainers.community.postgres import PostgresContainer

import app.infra.config as infra_config
from app.infra.mappers import seat_to_domain
from app.infra.tables import BookingRow, BookingSeatRow, EventRow, SeatRow

REPO_ROOT = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 6, 1, tzinfo=UTC)

# Async tests and fixtures share one event loop for the whole session (see
# pyproject.toml's asyncio_default_{fixture,test}_loop_scope = "session").
# That's required here: the `engine` fixture below is session-scoped, and an
# async engine created in one event loop cannot be reused from a different
# one -- asyncpg surfaces that mismatch as opaque "another operation is in
# progress" / "attached to a different loop" errors, not a clear one.


@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer(
        "postgres:16",
        username="seatlock_test",
        password="seatlock_test",
        dbname="seatlock_test",
        driver=None,
    ) as container:
        yield container


@pytest.fixture(scope="session")
def database_url(postgres_container: PostgresContainer) -> str:
    return postgres_container.get_connection_url(driver="asyncpg")


@pytest.fixture(scope="session")
def alembic_config(database_url: str) -> Config:
    # migrations/env.py always reads app.infra.config.settings.database_url
    # by design (see its comment: exactly one place credentials are
    # configured). To point migrations at the ephemeral test container
    # instead of the local dev database, patch that one setting directly for
    # the session rather than adding a second code path just for tests.
    original_url = infra_config.settings.database_url
    infra_config.settings.database_url = database_url
    yield Config(str(REPO_ROOT / "alembic.ini"))
    infra_config.settings.database_url = original_url


@pytest.fixture(scope="session")
def migrated_schema(alembic_config: Config) -> None:
    """Apply migrations once, up front, for the constraint/round-trip tests
    below. Kept separate from TestMigrationLifecycle, which exercises
    upgrade/downgrade explicitly and asserts on it directly.
    """
    command.upgrade(alembic_config, "head")


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def engine(database_url: str, migrated_schema: None):
    eng = create_async_engine(database_url)
    yield eng
    await eng.dispose()


@pytest_asyncio.fixture
async def db_conn(engine: AsyncEngine):
    """A connection with its own outer transaction, rolled back after each
    test so data-inserting tests never leak state into each other.
    """
    async with engine.connect() as conn:
        trans = await conn.begin()
        try:
            yield conn
        finally:
            await trans.rollback()


async def _insert_event(conn: AsyncConnection) -> int:
    result = await conn.execute(
        insert(EventRow).returning(EventRow.id),
        {"name": "Test Event", "venue": "Test Venue", "starts_at": NOW, "total_seats": 100},
    )
    return result.scalar_one()


async def _insert_seat(conn: AsyncConnection, event_id: int, **overrides: Any) -> int:
    values = {
        "event_id": event_id,
        "section": "A",
        "row_label": "1",
        "seat_number": 1,
        "status": "AVAILABLE",
        "version": 0,
    }
    values.update(overrides)
    result = await conn.execute(insert(SeatRow).returning(SeatRow.id), values)
    return result.scalar_one()


async def _insert_booking(conn: AsyncConnection, event_id: int, **overrides: Any) -> int:
    values = {
        "event_id": event_id,
        "user_id": 1,
        "session_id": "session-a",
        "status": "PENDING",
        "total_amount": Decimal("42.00"),
        "currency": "USD",
    }
    values.update(overrides)
    result = await conn.execute(insert(BookingRow).returning(BookingRow.id), values)
    return result.scalar_one()


@pytest_asyncio.fixture
async def event_id(db_conn: AsyncConnection) -> int:
    return await _insert_event(db_conn)


class TestMigrationLifecycle:
    """Schema-level DDL tests -- see module docstring for ordering."""

    async def test_migration_applies_cleanly_from_scratch(self, engine: AsyncEngine):
        # `migrated_schema` (an `engine` dependency) already ran `upgrade
        # head`; if that had raised, we would never get here. This asserts
        # it actually left the real tables behind, not just that nothing
        # threw.
        async with engine.connect() as conn:
            tables = await conn.run_sync(lambda c: set(inspect(c).get_table_names()))
        assert {"events", "seats", "bookings", "booking_seats"} <= tables

    def test_downgrade_then_upgrade_works(self, alembic_config: Config, database_url: str):
        # Deliberately a plain sync test, not async: alembic's upgrade/
        # downgrade commands run our async env.py via their own internal
        # asyncio.run() call. Calling that from inside a coroutine already
        # running on pytest-asyncio's event loop (as every other test in
        # this module does, via the shared session-scoped `engine`) is a
        # nested-event-loop conflict. A plain sync test has no ambient loop,
        # so asyncio.run() here is safe -- and inspection below uses its own
        # short-lived engine/loop rather than touching the shared one.
        def table_names() -> set[str]:
            async def _get() -> set[str]:
                temp_engine = create_async_engine(database_url)
                try:
                    async with temp_engine.connect() as conn:
                        return await conn.run_sync(lambda c: set(inspect(c).get_table_names()))
                finally:
                    await temp_engine.dispose()

            return asyncio.run(_get())

        command.downgrade(alembic_config, "base")
        # alembic's own bookkeeping table is expected to survive a downgrade
        # to "base" -- it's what makes "base" a known state at all.
        assert table_names() - {"alembic_version"} == set()

        command.upgrade(alembic_config, "head")
        assert {"events", "seats", "bookings", "booking_seats"} <= table_names()


class TestPartialUniqueIndexOnBookingSeats:
    """I1's DB-level last line of defence: at most one active (released_at
    IS NULL) booking_seats row per seat.
    """

    async def test_two_active_rows_for_same_seat_raise_integrity_error(
        self, db_conn: AsyncConnection, event_id: int
    ):
        seat_id = await _insert_seat(db_conn, event_id)
        booking_a = await _insert_booking(db_conn, event_id)
        booking_b = await _insert_booking(db_conn, event_id)

        await db_conn.execute(insert(BookingSeatRow), {"booking_id": booking_a, "seat_id": seat_id})

        with pytest.raises(IntegrityError):
            async with db_conn.begin_nested():
                await db_conn.execute(
                    insert(BookingSeatRow), {"booking_id": booking_b, "seat_id": seat_id}
                )

    async def test_released_row_does_not_block_a_new_active_row(
        self, db_conn: AsyncConnection, event_id: int
    ):
        seat_id = await _insert_seat(db_conn, event_id)
        booking_a = await _insert_booking(db_conn, event_id)
        booking_b = await _insert_booking(db_conn, event_id)
        booking_c = await _insert_booking(db_conn, event_id)

        # First active row.
        await db_conn.execute(insert(BookingSeatRow), {"booking_id": booking_a, "seat_id": seat_id})
        # A second active row for the same seat is rejected...
        with pytest.raises(IntegrityError):
            async with db_conn.begin_nested():
                await db_conn.execute(
                    insert(BookingSeatRow), {"booking_id": booking_b, "seat_id": seat_id}
                )
        # ...but a *released* row for the same seat is not -- proving the
        # index is genuinely partial, not a total unique index in disguise.
        await db_conn.execute(
            insert(BookingSeatRow),
            {"booking_id": booking_c, "seat_id": seat_id, "released_at": NOW},
        )

    async def test_cancelled_booking_without_release_blocks_seat(
        self, db_conn: AsyncConnection, event_id: int
    ):
        # The partial index keys off released_at, NOT booking status -- if
        # a booking is cancelled but its booking_seats row is never marked
        # released, the seat stays blocked. This is exactly why
        # app/infra/tables.py's comment on SeatRow.booking_id says
        # booking_seats and the cancellation must be updated together.
        seat_id = await _insert_seat(db_conn, event_id)
        booking_a = await _insert_booking(db_conn, event_id)
        booking_b = await _insert_booking(db_conn, event_id)

        await db_conn.execute(insert(BookingSeatRow), {"booking_id": booking_a, "seat_id": seat_id})
        # "Cancel" the booking at the bookings-row level only -- deliberately
        # not touching booking_seats.released_at.
        await db_conn.execute(
            update(BookingRow).where(BookingRow.id == booking_a).values(status="CANCELLED")
        )

        with pytest.raises(IntegrityError):
            async with db_conn.begin_nested():
                await db_conn.execute(
                    insert(BookingSeatRow), {"booking_id": booking_b, "seat_id": seat_id}
                )


class TestStatusCheckConstraints:
    async def test_bogus_seat_status_is_rejected(self, db_conn: AsyncConnection, event_id: int):
        with pytest.raises(IntegrityError):
            async with db_conn.begin_nested():
                await _insert_seat(db_conn, event_id, status="BOGUS")


class TestMapperRoundTrip:
    async def test_seat_round_trips_through_db(self, db_conn: AsyncConnection, event_id: int):
        async with AsyncSession(bind=db_conn, expire_on_commit=False) as session:
            row = SeatRow(
                event_id=event_id,
                section="A",
                row_label="1",
                seat_number=1,
                status="HELD",
                version=3,
                held_by_session_id="session-x",
                hold_expires_at=NOW,
                booking_id=None,
            )
            session.add(row)
            await session.flush()
            original = seat_to_domain(row)

            await session.refresh(row)  # force a genuine re-SELECT from Postgres
            reloaded = seat_to_domain(row)

        assert reloaded == original


class TestConstraintNaming:
    """A future change to app/infra/tables.py's naming convention should
    break this test, not silently ship unnamed constraints.
    """

    async def test_constraint_names_match_naming_convention(self, engine: AsyncEngine):
        def _inspect(sync_conn: Any) -> dict[str, Any]:
            insp = inspect(sync_conn)
            return {
                "seats_pk": insp.get_pk_constraint("seats")["name"],
                "seats_checks": {c["name"] for c in insp.get_check_constraints("seats")},
                "seats_fks": {fk["name"] for fk in insp.get_foreign_keys("seats")},
                "seats_indexes": {ix["name"] for ix in insp.get_indexes("seats")},
                "seats_unique": {uq["name"] for uq in insp.get_unique_constraints("seats")},
                "bookings_pk": insp.get_pk_constraint("bookings")["name"],
                "bookings_checks": {c["name"] for c in insp.get_check_constraints("bookings")},
                "booking_seats_pk": insp.get_pk_constraint("booking_seats")["name"],
                "booking_seats_indexes": {ix["name"] for ix in insp.get_indexes("booking_seats")},
            }

        async with engine.connect() as conn:
            info = await conn.run_sync(_inspect)

        assert info["seats_pk"] == "pk_seats"
        assert info["seats_checks"] == {"ck_seats_seat_status_valid"}
        assert info["seats_fks"] == {"fk_seats_event_id_events", "fk_seats_booking_id_bookings"}
        # SQLAlchemy's Postgres reflection also surfaces the unique
        # constraint's backing index here, in addition to reporting it via
        # get_unique_constraints() below -- both must be checked.
        assert info["seats_indexes"] == {
            "ix_seats_event_id_status",
            "ix_seats_hold_expires_at",
            "uq_seats_event_id_section_row_label_seat_number",
        }
        assert info["seats_unique"] == {"uq_seats_event_id_section_row_label_seat_number"}
        assert info["bookings_pk"] == "pk_bookings"
        assert info["bookings_checks"] == {"ck_bookings_booking_status_valid"}
        assert info["booking_seats_pk"] == "pk_booking_seats"
        assert info["booking_seats_indexes"] == {"ix_booking_seats_seat_id"}
