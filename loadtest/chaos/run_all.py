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
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import psutil
from sqlalchemy import select, text

from app.infra.config import settings
from app.infra.db import async_session_factory, engine
from app.infra.tables import SeatRow
from loadtest import run_benchmark as rb
from loadtest.chaos import actions
from loadtest.chaos.harness import ChaosInfra, ScenarioReport
from loadtest.chaos.scenarios import (
    api_worker_killed,
    api_worker_killed_holding_lock,
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
    "api_worker_killed_holding_lock": api_worker_killed_holding_lock,
    "postgres_restarted": postgres_restarted,
    # LAST, deliberately: confirmed directly, repeatedly, that
    # api_worker_killed's hard kill of one uvicorn worker can leave an
    # orphaned replacement worker process behind despite three layers of
    # cleanup (with_suppress_stop's pre-kill child capture, a pid-scoped
    # sweep for orphans with no live parent to walk from, and repeating
    # that sweep for several seconds to catch a late-arriving one) --
    # see docs/chaos-results.md for the full account. Ordering this
    # scenario last means nothing else in a full `make chaos` run ever
    # needs start_infra() to succeed after it, which is the actual
    # failure mode every occurrence has been: the NEXT scenario's fresh
    # PROMETHEUS_MULTIPROC_DIR clear failing on a file an orphan still
    # holds open. This does not fix the orphaning itself -- it makes the
    # one thing this suite is actually deliverable on (`make chaos`
    # completing all scenarios) robust by construction instead.
    "api_worker_killed": api_worker_killed,
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
    *, interval_seconds: float, confirm_delay_seconds: float, prometheus_multiproc_dir: str
) -> subprocess.Popen[bytes]:
    # PROMETHEUS_MULTIPROC_DIR must match api_proc's -- prometheus_client's
    # multiprocess mode aggregates by reading every per-PID .db file in
    # ONE shared directory (app/infra/metrics.py's own module docstring).
    # Without this, the reconciler's own measure_backlog() call (added
    # specifically so sweeper_backlog_gauge survives the sweeper's death
    # -- see workers/reconciler.py's comment) writes into whatever
    # directory Settings.prometheus_multiproc_dir defaults to, NOT the
    # chaos suite's dedicated one -- the API's /metrics scrape never sees
    # it, and the gauge looks frozen at 0.0 even though the reconciler is
    # measuring correctly. Confirmed directly: this omission is exactly
    # why sweeper_killed's re-run first failed after the fix was added
    # everywhere except here.
    env = {
        **os.environ,
        "RECONCILER_INTERVAL_SECONDS": str(interval_seconds),
        "RECONCILER_CONFIRM_DELAY_SECONDS": str(confirm_delay_seconds),
        "PROMETHEUS_MULTIPROC_DIR": prometheus_multiproc_dir,
    }
    proc = subprocess.Popen([sys.executable, "-m", "workers.reconciler"], env=env)  # noqa: S603
    time.sleep(0.5)
    return proc


def start_payment_worker(*, prometheus_multiproc_dir: str) -> subprocess.Popen[bytes]:
    env = {**os.environ, "PROMETHEUS_MULTIPROC_DIR": prometheus_multiproc_dir}
    proc = subprocess.Popen([sys.executable, "-m", "workers.payment_worker"], env=env)  # noqa: S603
    time.sleep(0.5)
    return proc


# Every api_proc.pid this run has ever spawned, across all scenarios --
# see _kill_orphaned_uvicorn_workers's own comment for why this is
# needed: an orphaned worker's `spawn_main(parent_pid=N, ...)` command
# line records its ORIGINAL parent, and cross-checking N against this
# set is what lets that cleanup sweep kill only OUR OWN orphans, never
# an unrelated Python multiprocessing worker elsewhere on the machine.
_known_api_master_pids: set[int] = set()


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
    _known_api_master_pids.add(api_proc.pid)
    sweeper_proc = start_sweeper(
        interval_seconds=SWEEPER_INTERVAL_SECONDS,
        batch_size=SWEEPER_BATCH_SIZE,
        prometheus_multiproc_dir=PROMETHEUS_MULTIPROC_DIR,
    )
    reconciler_proc = start_reconciler(
        interval_seconds=RECONCILER_INTERVAL_SECONDS,
        confirm_delay_seconds=RECONCILER_CONFIRM_DELAY_SECONDS,
        prometheus_multiproc_dir=PROMETHEUS_MULTIPROC_DIR,
    )
    payment_worker_proc = start_payment_worker(prometheus_multiproc_dir=PROMETHEUS_MULTIPROC_DIR)
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
    # A single sweep isn't always enough: confirmed directly that a
    # replacement worker can appear microseconds AFTER one sweep already
    # ran -- uvicorn's own supervisor, reacting to api_worker_killed's
    # hard kill of one of its workers, can spawn a replacement in a
    # window that races this exact teardown. Repeating the sweep for a
    # few seconds catches a late-arriving one before the NEXT scenario's
    # start_infra() tries to clear the same shared metrics directory.
    for _ in range(6):
        _kill_orphaned_uvicorn_workers()
        time.sleep(1.0)


_SPAWN_MAIN_PARENT_PID_RE = re.compile(r"parent_pid=(\d+)")


def _kill_orphaned_uvicorn_workers() -> None:
    """Belt-and-braces final sweep, NOT relying on walking from
    api_proc.pid at teardown time: confirmed directly, more than once,
    that capturing its children BEFORE killing it (with_suppress_stop's
    own approach) is not sufficient -- the uvicorn MASTER can die on its
    own sometime after api_worker_killed's scenario hard-kills one of its
    workers (observed directly: its recorded parent pid was already gone
    by the time teardown ran), leaving no live pid to walk children FROM
    at all.

    Every uvicorn --workers child's command line is Python's own
    `multiprocessing.spawn`'s `spawn_main(parent_pid=N, ...)`, which
    records its ORIGINAL parent pid literally in the command line even
    after that parent is long gone. Scanning for that pattern and
    cross-checking N against _known_api_master_pids (every api_proc.pid
    THIS run has ever spawned) is what finds these orphans with no
    process-tree relationship left to find them through -- scoped to our
    own pids specifically, not a blind kill of anything matching the
    same generic multiprocessing command line elsewhere on the machine.
    """
    for proc in psutil.process_iter(["pid", "cmdline"]):
        try:
            cmdline = " ".join(proc.info["cmdline"] or [])
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
        if "multiprocessing.spawn" not in cmdline or "spawn_main" not in cmdline:
            continue
        match = _SPAWN_MAIN_PARENT_PID_RE.search(cmdline)
        if match and int(match.group(1)) in _known_api_master_pids:
            print(f"warning: killing orphaned uvicorn worker pid={proc.pid}", file=sys.stderr)
            actions.kill_pid(proc.pid)


def with_suppress_stop(proc: subprocess.Popen[bytes]) -> None:
    # Capture children BEFORE killing the parent -- once the parent pid
    # is gone, psutil can no longer walk from it to find them. Confirmed
    # directly, twice: uvicorn's --workers 4 spawns its actual worker
    # processes via Python's multiprocessing (spawn_main), and
    # `taskkill /F /T` (rb.stop_api's own mechanism) does not reliably
    # catch all of them on Windows -- this run's api_proc left 4 orphaned
    # worker processes running (correct ppid, still alive) well past
    # taskkill's own tree-walk, each still holding its own metrics .db
    # file open, which is exactly what made the NEXT scenario's
    # _clear_prometheus_multiproc_dir fail. No amount of sleeping between
    # scenarios fixes an orphan that taskkill never actually reached --
    # explicitly finding and killing every descendant via psutil (which
    # walks LIVE parent/child links at call time, not taskkill's own
    # possibly-stale enumeration) is what actually guarantees no orphan
    # survives, regardless of timing.
    children = []
    with contextlib.suppress(psutil.NoSuchProcess):
        children = psutil.Process(proc.pid).children(recursive=True)

    try:
        rb.stop_api(proc)
    except Exception as exc:  # noqa: BLE001 -- teardown must not abort the whole suite
        print(f"warning: failed to stop pid={proc.pid}: {exc}", file=sys.stderr)

    for child in children:
        if child.is_running():
            print(f"warning: killing orphaned child pid={child.pid} of {proc.pid}", file=sys.stderr)
            actions.kill_pid(child.pid)


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
        # leaves the process list -- confirmed directly, twice: this run
        # crashed here past _clear_prometheus_multiproc_dir's own 9s
        # retry budget, both times immediately after api_worker_killed's
        # hard kill (once at 2.0s of pause here, still not enough under a
        # full seven-scenario run's heavier system load). A fixed pause
        # between scenarios is cheap against each scenario's multi-minute
        # runtime; widened from 2.0s to 5.0s after the second occurrence.
        # If this recurs even at 5.0s, the robust fix is polling
        # actions.pid_is_running() for the previous scenario's exact PIDs
        # instead of guessing a duration -- not done here because a
        # third occurrence hasn't been observed yet to justify it.
        await asyncio.sleep(5.0)
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

    # Final sweep, even though nothing else in THIS run needs it (that is
    # exactly why api_worker_killed sorts last in SCENARIOS -- see its
    # own comment there): an orphaned worker from that scenario would
    # otherwise outlive this script entirely, leaking a process no next
    # `make chaos` invocation would ever clean up on its own.
    _kill_orphaned_uvicorn_workers()

    print("\n=== Summary ===")
    for report in reports:
        print(f"{report.scenario}: {'PASSED' if report.passed else 'FAILED'}")

    if any(not report.passed for report in reports):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
