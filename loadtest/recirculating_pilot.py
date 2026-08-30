"""Phase 3 recirculating-contention pilot.

Before running the full recirculating sweep (a ~20 minute matrix), verify
the workload it depends on -- short holds + the sweeper reclaiming
them -- actually recirculates inventory, rather than just exhausting once
and staying that way (which would produce a second artifact instead of
fixing the first one; see loadtest/diagnose_exhaustion.py and the
conversation this pilot follows from).

For each (strategy, contention ratio) cell, starts an API instance with a
SHORT, overridden HOLD_DURATION_SECONDS and a sweeper worker with a
SHORT, overridden SWEEPER_INTERVAL_SECONDS together, seeds seat_count
seats, runs contention_sweep.js in a thread executor, and CONCURRENTLY
polls seat status counts every --poll-interval throughout the run.

Polling queries the database DIRECTLY, via a small dedicated connection
pool, NOT GET /api/admin/seat-status-counts. Confirmed empirically (first
version of this script): polling that endpoint over HTTP, through the
API's own connection pool, under the SAME 200-VU load being observed,
starves on that saturated pool and only achieves about one sample every
~2s (the HTTP client's timeout) rather than the intended 50ms --
Postgres itself has plenty of spare capacity (only ~60 of its default
100 connections are in use by the API workers), the API's OWN pool is
what's exhausted. A dedicated connection sidesteps that entirely and
also avoids adding the poll itself to the very load being measured. The
admin endpoint still exists (app/api/routes/admin.py) for other,
non-saturating uses -- it just isn't the right tool for this specific
job.

From the samples, reports:
  1. Recirculation cycles observed: the number of times available_count
     fell to 0 and later became > 0 again -- each such recovery is one
     full exhaust-then-recirculate cycle.
  2. Fraction of the run where >= 1 seat was AVAILABLE.
  3. Standard deviation of available_count across all samples.

Both API and sweeper configuration (hold duration, sweeper interval,
batch size) are asserted identical across every cell here, the same way
the real sweep asserts it -- this parameter-selection step is exactly
where an accidental strategy-specific override would first sneak in
unnoticed.

Runs BOTH pessimistic and optimistic at each ratio (not just one): the
question isn't only "does inventory recirculate," it's "does it
recirculate comparably for both strategies," since the whole point of
the eventual sweep is comparing them under IDENTICAL contention
conditions.

hold_duration_seconds and sweeper_interval_seconds default to a starting
guess (2.0s / 0.2s) -- these are BENCHMARKING values, chosen empirically
from what this pilot reports, never product defaults (see
Settings.hold_duration_seconds / .sweeper_interval_seconds for those).

Usage:
    python -m loadtest.recirculating_pilot
    python -m loadtest.recirculating_pilot --ratios 5,100 \
        --hold-duration-seconds 1.0 --sweeper-interval-seconds 0.1
"""

from __future__ import annotations

import argparse
import asyncio
import os
import statistics
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.infra.config import settings
from app.infra.tables import SeatRow
from loadtest import run_benchmark as rb

RESULTS_DIR = rb.RESULTS_DIR


def start_sweeper(
    *, interval_seconds: float, batch_size: int, prometheus_multiproc_dir: str
) -> subprocess.Popen[bytes]:
    """Start workers/sweeper_worker.py as its own OS process. Reuses
    rb.stop_api to tear it down -- that helper is generic over "a Python
    subprocess this harness started," not specific to uvicorn.
    """
    env = {
        **os.environ,
        "SWEEPER_INTERVAL_SECONDS": str(interval_seconds),
        "SWEEPER_BATCH_SIZE": str(batch_size),
        "PROMETHEUS_MULTIPROC_DIR": prometheus_multiproc_dir,
    }
    proc = subprocess.Popen(  # noqa: S603
        [sys.executable, "-m", "workers.sweeper_worker"], env=env
    )
    # No /health endpoint to poll -- the worker's first sweep_once() call
    # happens immediately on startup (see run_forever's while loop), well
    # under a second after the process spawns. A short fixed wait is
    # simpler than adding a synchronization signal for a benchmarking-only
    # subprocess.
    time.sleep(0.5)
    return proc


@dataclass
class AvailableCountSample:
    elapsed_seconds: float
    available: int
    held: int
    booked: int


def _run_k6_blocking(
    k6_bin: str,
    *,
    event_id: int,
    seat_ids: list[int],
    vus: int,
    duration: str,
    warmup_vus: int,
    warmup_duration: str,
    base_url: str,
    summary_path: Any,
) -> None:
    """Plain blocking call -- run via loop.run_in_executor (see
    run_pilot_cell), not awaited directly, so the async polling loop
    below can run concurrently with it in the same event loop.
    """
    env = {
        **os.environ,
        "BASE_URL": base_url,
        "EVENT_ID": str(event_id),
        "SEAT_IDS": ",".join(str(s) for s in seat_ids),
        "VUS": str(vus),
        "DURATION": duration,
        "WARMUP_VUS": str(warmup_vus),
        "WARMUP_DURATION": warmup_duration,
        "SUMMARY_PATH": str(summary_path),
    }
    subprocess.run(  # noqa: S603
        [k6_bin, "run", str(rb.SCENARIOS["contention_sweep"])], env=env, check=False
    )


async def poll_available_count_async(
    session_factory: async_sessionmaker, event_id: int, poll_interval_seconds: float, is_done
) -> list[AvailableCountSample]:
    """See module docstring for why this queries the database directly
    rather than the admin HTTP endpoint.
    """
    samples: list[AvailableCountSample] = []
    start = time.monotonic()
    while not is_done():
        async with session_factory() as session:
            rows = (
                await session.execute(
                    select(SeatRow.status, func.count())
                    .where(SeatRow.event_id == event_id)
                    .group_by(SeatRow.status)
                )
            ).all()
        counts = dict(rows)
        samples.append(
            AvailableCountSample(
                elapsed_seconds=time.monotonic() - start,
                available=counts.get("AVAILABLE", 0),
                held=counts.get("HELD", 0),
                booked=counts.get("BOOKED", 0),
            )
        )
        await asyncio.sleep(poll_interval_seconds)
    return samples


def _analyze_recirculation(samples: list[AvailableCountSample]) -> dict[str, Any]:
    if not samples:
        return {
            "recirculation_cycles": None,
            "fraction_with_available_seat": None,
            "available_count_stdev": None,
        }

    cycles = 0
    was_zero = samples[0].available == 0
    for sample in samples[1:]:
        if was_zero and sample.available > 0:
            cycles += 1
            was_zero = False
        elif sample.available == 0:
            was_zero = True

    available_counts = [s.available for s in samples]
    fraction_with_available = sum(1 for c in available_counts if c > 0) / len(available_counts)

    return {
        "recirculation_cycles": cycles,
        "fraction_with_available_seat": fraction_with_available,
        "available_count_stdev": (
            statistics.stdev(available_counts) if len(available_counts) > 1 else 0.0
        ),
        "available_count_mean": statistics.mean(available_counts),
        "sample_count": len(samples),
    }


async def run_pilot_cell(
    *,
    k6_bin: str,
    strategy: str,
    ratio: int,
    vus: int,
    duration: str,
    duration_seconds: float,
    warmup_vus: int,
    warmup_duration: str,
    hold_duration_seconds: float,
    sweeper_interval_seconds: float,
    sweeper_batch_size: int,
    poll_interval_seconds: float,
    workers: int,
    pool_size: int,
    max_overflow: int,
    base_url: str,
    prometheus_multiproc_dir: str,
    run_id: str,
) -> dict[str, Any]:
    seat_count = max(1, round(vus / ratio))

    api_proc = rb.start_api(
        strategy=strategy,
        workers=workers,
        pool_size=pool_size,
        max_overflow=max_overflow,
        base_url=base_url,
        prometheus_multiproc_dir=prometheus_multiproc_dir,
        hold_duration_seconds=hold_duration_seconds,
    )
    sweeper_proc = start_sweeper(
        interval_seconds=sweeper_interval_seconds,
        batch_size=sweeper_batch_size,
        prometheus_multiproc_dir=prometheus_multiproc_dir,
    )
    # Dedicated, small, separate from the API's own pool -- see module
    # docstring's "Polling queries the database DIRECTLY" section.
    poll_engine = create_async_engine(settings.database_url, pool_size=2, max_overflow=2)
    poll_session_factory = async_sessionmaker(bind=poll_engine, expire_on_commit=False)
    try:
        label = f"{strategy}-ratio{ratio}"
        event_id, seat_ids = await rb.reset_and_seed(f"pilot {label}", seat_count)

        summary_path = RESULTS_DIR / f"{run_id}-{label}-k6.json"
        loop = asyncio.get_running_loop()
        k6_future = loop.run_in_executor(
            None,
            lambda: _run_k6_blocking(
                k6_bin,
                event_id=event_id,
                seat_ids=seat_ids,
                vus=vus,
                duration=duration,
                warmup_vus=warmup_vus,
                warmup_duration=warmup_duration,
                base_url=base_url,
                summary_path=summary_path,
            ),
        )
        samples = await poll_available_count_async(
            poll_session_factory, event_id, poll_interval_seconds, k6_future.done
        )
        await k6_future  # propagate any exception; confirm full completion

        analysis = _analyze_recirculation(samples)
        return {
            "strategy": strategy,
            "contention_ratio_target": ratio,
            "seat_count": seat_count,
            "hold_duration_seconds": hold_duration_seconds,
            "sweeper_interval_seconds": sweeper_interval_seconds,
            "sweeper_batch_size": sweeper_batch_size,
            "duration_seconds": duration_seconds,
            **analysis,
            "samples": [asdict(s) for s in samples],
        }
    finally:
        await poll_engine.dispose()
        rb.stop_api(sweeper_proc)
        rb.stop_api(api_proc)


def _fmt(value: float | None, digits: int = 3) -> str:
    return "—" if value is None else f"{value:.{digits}f}"


def render_markdown(results: list[dict[str, Any]]) -> str:
    lines = [
        "# Phase 3 recirculating-contention pilot",
        "",
        "Not the sweep -- verifies the workload before running it. See "
        "loadtest/recirculating_pilot.py's module docstring.",
        "",
        "| Strategy | Ratio | Seats | Hold (s) | Sweeper interval (s) | Cycles observed | "
        "Fraction with >=1 available | Available count (mean / stdev) | Samples |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for r in results:
        lines.append(
            f"| {r['strategy']} | {r['contention_ratio_target']} | {r['seat_count']} | "
            f"{r['hold_duration_seconds']} | {r['sweeper_interval_seconds']} | "
            f"{r['recirculation_cycles']} | {_fmt(r['fraction_with_available_seat'])} | "
            f"{_fmt(r.get('available_count_mean'))} / {_fmt(r['available_count_stdev'])} | "
            f"{r.get('sample_count', 0)} |"
        )
    return "\n".join(lines) + "\n"


async def main_async(args: argparse.Namespace) -> None:
    import json
    import shutil

    ratios = sorted(int(r.strip()) for r in args.ratios.split(",") if r.strip())
    k6_bin = shutil.which(args.k6_bin)
    if k6_bin is None:
        raise SystemExit(f"'{args.k6_bin}' not found on PATH.")

    duration_seconds = rb._parse_duration_seconds(args.duration)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-pilot"

    results: list[dict[str, Any]] = []
    for ratio in ratios:
        for strategy in ("pessimistic", "optimistic"):
            print(f"--- pilot: strategy={strategy} ratio={ratio} ---")
            result = await run_pilot_cell(
                k6_bin=k6_bin,
                strategy=strategy,
                ratio=ratio,
                vus=args.vus,
                duration=args.duration,
                duration_seconds=duration_seconds,
                warmup_vus=args.warmup_vus,
                warmup_duration=args.warmup_duration,
                hold_duration_seconds=args.hold_duration_seconds,
                sweeper_interval_seconds=args.sweeper_interval_seconds,
                sweeper_batch_size=settings.sweeper_batch_size,
                poll_interval_seconds=args.poll_interval,
                workers=args.workers,
                pool_size=settings.pool_size,
                max_overflow=settings.max_overflow,
                base_url=args.base_url,
                prometheus_multiproc_dir=settings.prometheus_multiproc_dir,
                run_id=run_id,
            )
            results.append(result)

    # Configuration identity, same reasoning as the real sweep -- even a
    # two-cell pilot must not silently vary hold duration or sweeper
    # settings between strategies.
    fields = ("hold_duration_seconds", "sweeper_interval_seconds", "sweeper_batch_size")
    reference = {name: results[0][name] for name in fields}
    for r in results:
        config = {name: r[name] for name in fields}
        if config != reference:
            raise SystemExit(
                f"Pilot configuration mismatch: {r['strategy']}/{r['contention_ratio_target']} "
                f"has {config}, expected {reference}."
            )

    json_path = RESULTS_DIR / f"{run_id}.json"
    json_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    md_path = RESULTS_DIR / f"{run_id}.md"
    rendered = render_markdown(results)
    md_path.write_text(rendered, encoding="utf-8")

    print(f"\nWrote {json_path}")
    print(f"Wrote {md_path}")
    print()
    print(rendered)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ratios", default="5,100", help="Two (or more) contention ratios to pilot."
    )
    parser.add_argument("--vus", type=int, default=200)
    parser.add_argument("--duration", default="15s")
    parser.add_argument("--warmup-vus", type=int, default=10)
    parser.add_argument("--warmup-duration", default="3s")
    parser.add_argument("--hold-duration-seconds", type=float, default=2.0)
    parser.add_argument("--sweeper-interval-seconds", type=float, default=0.2)
    parser.add_argument(
        "--poll-interval", type=float, default=0.05, help="How often to sample seat-status-counts."
    )
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--k6-bin", default="k6")
    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
