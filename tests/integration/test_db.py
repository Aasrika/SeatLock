"""Phase 8a follow-up: idle_in_transaction_session_timeout must actually
reach Postgres, not just look right in connect_args -- see
app/infra/config.py's comment on Settings.idle_in_transaction_session_
timeout_ms for the full reasoning (a worker hard-killed mid-transaction
otherwise leaves Postgres holding every lock it acquired until the OS's
TCP keepalive defaults notice, on the order of two hours).

This is a construction-time-and-behavior check against a real Postgres,
not a chaos test: it asserts the *setting* is wired through and takes
effect on a real session. loadtest/chaos/scenarios/
api_worker_killed_holding_lock.py is what proves it actually bounds lock
release time end to end, against a process that is genuinely killed or
suspended.
"""

from __future__ import annotations

from sqlalchemy import text

from app.infra.config import settings
from app.infra.db import build_engine


async def test_idle_in_transaction_session_timeout_is_set_on_real_connections(
    database_url: str, migrated_schema: None
) -> None:
    engine = build_engine(
        database_url,
        pool_size=settings.pool_size,
        max_overflow=settings.max_overflow,
        idle_in_transaction_timeout_ms=settings.idle_in_transaction_session_timeout_ms,
    )
    try:
        async with engine.connect() as conn:
            result = await conn.execute(text("SHOW idle_in_transaction_session_timeout"))
            reported = result.scalar_one()
    finally:
        await engine.dispose()

    # Postgres reports it back as "<N>s"/"<N>ms" per its own GUC display
    # convention, not the bare millisecond integer we configured --
    # comparing the underlying seconds avoids depending on which unit
    # Postgres chose to render.
    assert reported.endswith(("s", "ms"))
    seconds = (
        float(reported[:-2]) / 1000 if reported.endswith("ms") else float(reported[:-1])
    )
    assert seconds == settings.idle_in_transaction_session_timeout_ms / 1000
