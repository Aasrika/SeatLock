"""Phase 8a chaos suite entry point -- `make chaos` runs this.

Self-contained by design (unlike loadtest/run_benchmark.py's default
mode, which assumes `make run-api` is already running externally): every
scenario injects a failure directly into the API process, one of its
background workers, or a docker-compose-managed dependency, so this
script owns starting and stopping ALL of them itself. That also means
each scenario gets a FRESH API + sweeper + reconciler + payment_worker +
seat pool, rather than sharing state across scenarios in one long-lived
process -- api_worker_killed permanently reduces that run's worker count,
sweeper_killed's replacement sweeper may carry different internal state,
and letting either bleed into the NEXT scenario would confound its own
result. The cost is real: six full docker-compose-plus-five-Python-
processes cycles instead of one. Correctness of each scenario's read on
its own hypothesis is worth more here than wall-clock time.

Usage:
    python -m loadtest.chaos.run_all
    python -m loadtest.chaos.run_all --only redis_killed,redis_paused
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from sqlalchemy import select, text

from app.infra.config import settings
from app.infra.db import async_session_factory, engine
from app.infra.tables import SeatRow
from loadtest import run_benchmark as rb
from loadtest.chaos import actions
from loadtest.chaos.harness import ChaosInfra, ScenarioReport
from loadtest.chaos.scenarios import (
    api_worker_killed,
    postgres_restarted,
    redis_killed,
    redis_killed_restarted_empty,
    redis_paused,
    sweeper_killed,
)
from loadtest.recirculating_pilot import start_sweeper
from scripts.seed import seed

REPO_ROOT = Path(__file__).resolve().parents[2]
BASE_URL = "http://localhost:8000"
EVENT_NAME = "Chaos Suite"
SEAT_COUNT = 40
VUS = 20

# Short relative to production (8 minutes / 5-10s -- see Settings' own
# comments) so a ~90s scenario run actually observes several hold-expiry
# and sweep cycles instead of zero. Benchmarking-only overrides, exactly
# like Phase 3's recirculating suite -- never the product defaults.
HOLD_DURATION_SECONDS = 4.0
SWEEPER_INTERVAL_SECONDS = 2.0
SWEEPER_BATCH_SIZE = 100
RECONCILER_INTERVAL_SECONDS = 5.0
RECONCILER_CONFIRM_DELAY_SECONDS = 1.0
PROMETHEUS_MULTIPROC_DIR = ".prometheus-multiproc-chaos"

SCENARIOS: dict[str, Any] = {
    "redis_killed": redis_killed,
    "redis_paused": redis_paused,
    "redis_killed_restarted_empty": redis_killed_restarted_empty,
    "sweeper_killed": sweeper_killed,
    "api_worker_killed": api_worker_killed,
    "postgres_restarted": postgres_restarted,
}


def _run_subprocess(cmd: list[str], **kwargs: Any) -> None:
    subprocess.run(cmd, cwd=REPO_ROOT, check=True, **kwargs)  # noqa: S603


def ensure_dependencies_up() -> None:
    """`docker compose up -d` is idempotent -- safe to call before every
    scenario as a belt-and-braces restore in case a previous scenario's
    own recover() left something paused or stopped (it shouldn't, but a
    chaos suite that silently limps into its next scenario on broken
    infrastructure would produce meaningless results for that scenario).
    """
    _run_subprocess(["docker", "compose", "up", "-d"])
    for service in ("postgres", "redis"):
        actions.wait_for_container_running(service, timeout_seconds=30.0)
        # docker_pause/docker_unpause is idempotent against an already-
        # running container; harmless if the previous scenario already
        # unpaused cleanly -- "not paused" is the common case, not an
        # error worth surfacing.
        with contextlib.suppress(subprocess.CalledProcessError):
            actions.docker_unpause(service)


def upgrade_schema() -> None:
    _run_subprocess([sys.executable, "-m", "alembic", "upgrade", "head"])


async def reset_and_seed_chaos_event() -> tuple[int, list[int]]:
    async with async_session_factory() as session:
        await session.execute(
            text(
                "TRUNCATE TABLE hold_audit, booking_seats, bookings, seats, events "
                "RESTART IDENTITY CASCADE"
            )
        )
        await session.commit()

    event_id = await seed(EVENT_NAME, SEAT_COUNT)

    async with async_session_factory() as session:
        seat_ids = (
            (
                await session.execute(
                    select(SeatRow.id).where(SeatRow.event_id == event_id).order_by(SeatRow.id)
                )
            )
            .scalars()
            .all()
        )
    return event_id, list(seat_ids)


def start_reconciler(
    *, interval_seconds: float, confirm_delay_seconds: float
) -> subprocess.Popen[bytes]:
    env = {
        **os.environ,
        "RECONCILER_INTERVAL_SECONDS": str(interval_seconds),
        "RECONCILER_CONFIRM_DELAY_SECONDS": str(confirm_delay_seconds),
    }
    proc = subprocess.Popen([sys.executable, "-m", "workers.reconciler"], env=env)  # noqa: S603
    time.sleep(0.5)
    return proc


def start_payment_worker() -> subprocess.Popen[bytes]:
    proc = subprocess.Popen(  # noqa: S603
        [sys.executable, "-m", "workers.payment_worker"], env=dict(os.environ)
    )
    time.sleep(0.5)
    return proc


def start_infra(event_id: int) -> ChaosInfra:
    rb._clear_prometheus_multiproc_dir(PROMETHEUS_MULTIPROC_DIR)
    api_proc = rb.start_api(
        strategy="optimistic",
        workers=4,
        pool_size=settings.pool_size,
        max_overflow=settings.max_overflow,
        base_url=BASE_URL,
        prometheus_multiproc_dir=PROMETHEUS_MULTIPROC_DIR,
        hold_duration_seconds=HOLD_DURATION_SECONDS,
    )
    sweeper_proc = start_sweeper(
        interval_seconds=SWEEPER_INTERVAL_SECONDS,
        batch_size=SWEEPER_BATCH_SIZE,
        prometheus_multiproc_dir=PROMETHEUS_MULTIPROC_DIR,
    )
    reconciler_proc = start_reconciler(
        interval_seconds=RECONCILER_INTERVAL_SECONDS,
        confirm_delay_seconds=RECONCILER_CONFIRM_DELAY_SECONDS,
    )
    payment_worker_proc = start_payment_worker()
    return ChaosInfra(
        api_proc=api_proc,
        sweeper_proc=sweeper_proc,
        reconciler_proc=reconciler_proc,
        payment_worker_proc=payment_worker_proc,
        prometheus_multiproc_dir=PROMETHEUS_MULTIPROC_DIR,
        hold_duration_seconds=HOLD_DURATION_SECONDS,
        sweeper_interval_seconds=SWEEPER_INTERVAL_SECONDS,
        sweeper_batch_size=SWEEPER_BATCH_SIZE,
        reconciler_interval_seconds=RECONCILER_INTERVAL_SECONDS,
    )


def stop_infra(infra: ChaosInfra) -> None:
    for proc in (
        infra.payment_worker_proc,
        infra.reconciler_proc,
        infra.sweeper_proc,
        infra.api_proc,
    ):
        with_suppress_stop(proc)


def with_suppress_stop(proc: subprocess.Popen[bytes]) -> None:
    try:
        rb.stop_api(proc)
    except Exception as exc:  # noqa: BLE001 -- teardown must not abort the whole suite
        print(f"warning: failed to stop pid={proc.pid}: {exc}", file=sys.stderr)


async def run_one_async(name: str) -> ScenarioReport:
    module = SCENARIOS[name]
    print(f"\n=== {name} ===", flush=True)

    ensure_dependencies_up()
    # engine.dispose() before reusing the pool for anything -- belt and
    # braces alongside pool_pre_ping=True (app/infra/db.py) after a
    # scenario that may have just restarted Postgres out from under an
    # already-open connection. Cheap; only matters right after
    # postgres_restarted, harmless every other time.
    await engine.dispose()
    event_id, seat_ids = await reset_and_seed_chaos_event()
    infra = start_infra(event_id)
    try:
        rb.check_api_is_up(BASE_URL)
        report = module.run(base_url=BASE_URL, event_id=event_id, seat_ids=seat_ids, infra=infra)
    finally:
        stop_infra(infra)
        # A hard taskkill (api_worker_killed's own injection, or the
        # normal /F /T stop above) doesn't guarantee Windows has released
        # a killed worker's memory-mapped metrics .db file the instant it
        # leaves the process list -- confirmed directly: this run crashed
        # here once, past _clear_prometheus_multiproc_dir's own 9s retry
        # budget, immediately after api_worker_killed's hard kill. A
        # short fixed pause between scenarios is cheap against each
        # scenario's multi-minute runtime and gives that race more room
        # to resolve before the NEXT scenario's start_infra() tries to
        # clear the same directory.
        await asyncio.sleep(2.0)
        ensure_dependencies_up()  # undo anything the scenario's own recover() missed

    verdict = "PASSED" if report.passed else "FAILED"
    print(f"--- {name}: {verdict} ---")
    for finding in report.findings:
        print(f"  - {finding}")
    return report


async def async_main(names: list[str]) -> list[ScenarioReport]:
    # ONE event loop for the whole suite, not one asyncio.run() per
    # scenario -- app/infra/db.py's `engine` (and this module's own
    # reset_and_seed_chaos_event, which reuses it) is a module-level
    # asyncpg connection pool. asyncpg connections are bound to the event
    # loop that opened them; a second asyncio.run() call creates a NEW
    # loop, and handing that loop a connection opened under the first
    # one surfaces as a raw AttributeError deep in asyncio's Proactor
    # transport (confirmed directly while building this suite -- not a
    # Postgres-restart bug, a loop-mismatch bug that looks like one).
    return [await run_one_async(name) for name in names]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--only",
        type=str,
        default=None,
        help=f"comma-separated subset of {sorted(SCENARIOS)} (default: all, in order)",
    )
    args = parser.parse_args()

    names = list(SCENARIOS) if args.only is None else [s.strip() for s in args.only.split(",")]
    unknown = set(names) - set(SCENARIOS)
    if unknown:
        raise SystemExit(
            f"Unknown scenario(s): {sorted(unknown)} -- choices are {sorted(SCENARIOS)}"
        )

    upgrade_schema()

    reports = asyncio.run(async_main(names))

    print("\n=== Summary ===")
    for report in reports:
        print(f"{report.scenario}: {'PASSED' if report.passed else 'FAILED'}")

    if any(not report.passed for report in reports):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
