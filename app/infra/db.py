"""Async SQLAlchemy engine, session factory, and FastAPI session dependency."""

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.infra.config import settings


def build_engine(
    database_url: str, *, pool_size: int, max_overflow: int, idle_in_transaction_timeout_ms: int
) -> AsyncEngine:
    """Extracted from module-level construction so tests/integration/
    test_db.py can build an engine against a real (testcontainers) Postgres with
    these exact settings, instead of duplicating the connect_args logic
    or trying to reuse the module-level `engine` below (already bound to
    settings.database_url at whatever it was when this module was first
    imported, before any test fixture gets a chance to point it at a
    test container).
    """
    return create_async_engine(
        database_url,
        pool_pre_ping=True,
        pool_size=pool_size,
        max_overflow=max_overflow,
        # asyncpg's server_settings issues these as session-level `SET`s
        # at connection startup -- scoped to every connection THIS engine
        # opens, not a cluster-wide postgresql.conf change (Alembic,
        # testcontainers, and any other client connecting directly to
        # Postgres are unaffected). See Settings.idle_in_transaction_
        # session_timeout_ms's comment for why this exists and how the
        # value was chosen.
        connect_args={
            "server_settings": {
                "idle_in_transaction_session_timeout": str(idle_in_transaction_timeout_ms)
            }
        },
    )


engine = build_engine(
    settings.database_url,
    pool_size=settings.pool_size,
    max_overflow=settings.max_overflow,
    idle_in_transaction_timeout_ms=settings.idle_in_transaction_session_timeout_ms,
)

async_session_factory = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency that yields a request-scoped async session."""
    async with async_session_factory() as session:
        yield session
