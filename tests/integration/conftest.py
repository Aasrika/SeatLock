"""Shared testcontainers/Alembic/engine fixtures for tests/integration/.

Async tests and fixtures across this whole directory share one event loop
for the session (see pyproject.toml's asyncio_default_{fixture,test}_loop_
scope = "session"). That's required: the `engine` fixture below is
session-scoped, and an async engine created in one event loop cannot be
reused from a different one -- asyncpg surfaces that mismatch as opaque
"another operation is in progress" / "attached to a different loop"
errors, not a clear one.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from testcontainers.community.postgres import PostgresContainer

import app.infra.config as infra_config

REPO_ROOT = Path(__file__).resolve().parents[2]


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
    """Apply migrations once, up front, for every test below. Schema-level
    DDL tests that need to upgrade/downgrade explicitly (see
    test_schema.py::TestMigrationLifecycle) do so themselves afterward and
    restore `head` before finishing.
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

    Not suitable for tests that need genuinely independent, concurrently
    committing sessions (e.g. proving a real race) -- those should build
    their own sessions from `engine` directly.
    """
    async with engine.connect() as conn:
        trans = await conn.begin()
        try:
            yield conn
        finally:
            await trans.rollback()
