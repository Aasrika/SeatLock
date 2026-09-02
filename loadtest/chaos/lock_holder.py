"""Standalone lock-holder for loadtest/chaos/scenarios/
api_worker_killed_holding_lock.py -- deliberately NOT part of the
FastAPI app or any SeatAcquisitionStrategy.

app/inventory/strategies/pessimistic.py's whole design is "no I/O of any
kind happens between acquiring the locks and COMMIT/ROLLBACK" (see its
own module docstring) -- an artificial sleep while holding a row lock is
EXACTLY what that file exists to prevent in the real acquire path, so it
does not belong there, even behind a settings flag defaulting to off.
This script reproduces "a process holding a transaction dies without a
clean disconnect" directly, as its own connection, using the app's own
idle_in_transaction_session_timeout setting (so it is bound by the exact
same configuration production connections are) -- without adding any
new code path to the app itself.

    python -m loadtest.chaos.lock_holder <seat_id> [hold_seconds]

Connects directly to Postgres (asyncpg, bypassing the API and every
SeatAcquisitionStrategy), opens a transaction, SELECTs the given seat row
FOR UPDATE, prints "lock acquired" (the scenario script's cue that the
lock is actually held, not just that the subprocess has started), then
sleeps -- holding the row lock -- until hold_seconds elapses or the
process is killed, whichever comes first.

If this process is never killed and hold_seconds is long enough, IT
would eventually be terminated by Postgres itself once
idle_in_transaction_session_timeout elapses (asyncpg surfaces that as a
connection error) -- a real, observable side effect of the same setting
under test, not a bug in this script.
"""

from __future__ import annotations

import asyncio
import sys

import asyncpg

from app.infra.config import settings


async def _hold(seat_id: int, hold_seconds: float) -> None:
    # asyncpg's own connect() wants a "postgresql://" DSN, not
    # SQLAlchemy's "postgresql+asyncpg://" driver-qualified one.
    dsn = settings.database_url.replace("postgresql+asyncpg://", "postgresql://", 1)
    conn = await asyncpg.connect(
        dsn,
        server_settings={
            "idle_in_transaction_session_timeout": str(
                settings.idle_in_transaction_session_timeout_ms
            )
        },
    )
    try:
        async with conn.transaction():
            await conn.fetchrow("SELECT id FROM seats WHERE id = $1 FOR UPDATE", seat_id)
            print("lock acquired", flush=True)
            await asyncio.sleep(hold_seconds)
    finally:
        await conn.close()


def main() -> None:
    seat_id = int(sys.argv[1])
    hold_seconds = float(sys.argv[2]) if len(sys.argv) > 2 else 120.0
    asyncio.run(_hold(seat_id, hold_seconds))


if __name__ == "__main__":
    main()
