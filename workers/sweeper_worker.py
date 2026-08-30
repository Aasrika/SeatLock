"""Standalone process: runs app/inventory/sweeper.py's sweep_once() in a
loop, every Settings.sweeper_interval_seconds.

    python -m workers.sweeper_worker

Uses app.infra.db's shared engine/session factory -- same database, same
connection pool configuration as the API -- but is a genuinely separate
OS process (SPEC.md's workers/ directory: "background jobs: hold
sweeper, reconciler"). This is a background job sharing the monolith's
one database and one domain layer, not a separate service (CLAUDE.md
rule 1): it has no API of its own, no independent transactional
boundary, and no logic beyond what app/inventory/sweeper.py already
defines -- this file is only the "run it forever, on a timer" driver.

PROMETHEUS_MULTIPROC_DIR must be set to the SAME directory as whichever
API instance this sweeper runs alongside (same env var, same value) --
app/infra/metrics.py's Counter/Histogram objects then write this
process's samples into that shared directory, and that API's own
GET /metrics scrape aggregates them in automatically. No separate scrape
endpoint for the sweeper is needed or provided.

Graceful shutdown on SIGINT/SIGTERM: finishes whatever batch is currently
in flight, then exits, rather than being killed mid-transaction (which
Postgres would roll back safely regardless -- finishing cleanly just
avoids discarding a batch's work for no reason). NOTE: asyncio's
ProactorEventLoop (the default on Windows) does not implement
add_signal_handler for SIGINT/SIGTERM -- confirmed by direct testing, it
raises NotImplementedError, suppressed below. On Windows this means the
loop always runs to forced termination (e.g. the benchmark harness's
stop_sweeper, matching stop_api's taskkill /F /T) rather than shutting
down gracefully; harmless here (an in-flight sweep transaction is simply
rolled back by Postgres) but worth stating plainly rather than silently
having graceful shutdown not actually work on this platform.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
from datetime import UTC, datetime

from app.infra.config import settings
from app.infra.db import async_session_factory
from app.inventory.sweeper import sweep_once


async def run_forever(interval_seconds: float, batch_size: int, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        now = datetime.now(UTC)
        async with async_session_factory() as session:
            result = await sweep_once(session, batch_size, now)
        if result.seats_expired:
            print(
                f"[sweeper] expired {result.seats_expired} of {result.candidates_found} candidates"
            )
        # wait_for(..., timeout=interval_seconds) rather than plain
        # sleep(): lets a signal-triggered stop_event interrupt the wait
        # immediately instead of finishing out the full interval first.
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)


async def main_async() -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _handle_signal() -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _handle_signal)

    print(
        f"[sweeper] starting: interval={settings.sweeper_interval_seconds}s "
        f"batch_size={settings.sweeper_batch_size}"
    )
    await run_forever(settings.sweeper_interval_seconds, settings.sweeper_batch_size, stop_event)
    print("[sweeper] stopped")


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
