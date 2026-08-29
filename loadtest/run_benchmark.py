"""Orchestrates a full naive-strategy benchmark run.

For each of N repetitions (default 5 -- a single run proving nothing is
the failure mode SPEC.md warns about):

    1. Reset the database and seed a fresh --contention (10-seat) event.
    2. Start a k6 scenario (last_seat.js or flash_sale.js) against it. Both
       scripts run a warmup phase (hitting GET /health, never touching seat
       state) before the measured burst -- see loadtest/last_seat.js's
       docstring for why, and loadtest/results/ for the experiment that
       investigated it.
    3. While k6 runs, poll GET /api/admin/invariants every 500ms and
       record any violation with a timestamp -- catching a violation that
       self-heals before the run ends is the point (SPEC.md section 10).
       This is genuinely concurrent: k6 runs as its own OS process (started
       non-blocking via Popen), and this polling loop runs in the main
       thread of *this* process while that k6 process is executing, not a
       simulation of concurrency.
    4. Wait for k6 to fully exit (`proc.wait()`, after the polling loop
       above already observed `proc.poll() is not None`) -- THEN, and only
       then, pull GET /api/admin/oversell-report. There is no race here
       between "k6 still sending requests" and "reading the oversell
       report": run_k6() does not return until the k6 process has fully
       terminated, and fetch_oversell_report() is only ever called on
       run_k6()'s return value in main_async() below.

Then writes loadtest/results/<timestamp>.json (raw per-run + aggregate,
including the full run configuration -- worker count, pool settings,
NAIVE_RACE_WINDOW_MS, VU count, scenario, warmup -- a benchmark whose
configuration isn't recorded alongside its numbers is not reproducible)
and loadtest/results/<timestamp>.md (a markdown table, README-ready).

Assumes the API is already running (see `make run-api`) and reachable at
--base-url; this script does not start it. Requires the `k6` binary on
PATH.

Usage:
    python -m loadtest.run_benchmark
    python -m loadtest.run_benchmark --scenario last_seat --runs 10
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select, text

from app.infra.config import settings
from app.infra.db import async_session_factory
from app.infra.tables import SeatRow
from scripts.seed import CONTENTION_SEAT_COUNT, seed

LOADTEST_DIR = Path(__file__).resolve().parent
RESULTS_DIR = LOADTEST_DIR / "results"
POLL_INTERVAL_SECONDS = 0.5

SCENARIOS = {
    "last_seat": LOADTEST_DIR / "last_seat.js",
    "flash_sale": LOADTEST_DIR / "flash_sale.js",
}
# Match each script's own default VUS/DURATION exactly (see the .js files)
# so Python always passes an explicit value and therefore always knows the
# real duration to divide by for throughput -- never left to a JS default
# that this side has to guess at.
DEFAULT_VUS = {"last_seat": 500, "flash_sale": 200}
DEFAULT_DURATION = {"last_seat": "10s", "flash_sale": "30s"}


def _parse_duration_seconds(duration: str) -> float:
    """Parse k6-style durations ("10s", "1m30s") into seconds. Only the
    units our own scripts actually use."""
    total = 0.0
    for amount, unit in re.findall(r"(\d+(?:\.\d+)?)(h|m|s)", duration):
        total += float(amount) * {"h": 3600, "m": 60, "s": 1}[unit]
    return total


@dataclass
class RunResult:
    run: int
    # --- configuration recorded alongside the numbers it produced ---
    scenario: str
    vus: int
    duration_seconds: float
    warmup_applied: bool
    warmup_vus: int
    warmup_duration_seconds: float
    workers: int
    pool_size: int
    max_overflow: int
    naive_race_window_ms: int
    seat_count: int
    # --- outcome, four categories, never merged ---
    successes: int = 0
    expected_409s: int = 0
    unexpected_app_errors: int = 0
    transport_failures: int = 0
    # --- oversell, split per Issue 6 ---
    oversold_seats: int = 0
    excess_holders: int = 0
    # --- derived, comparable across seat-pool sizes ---
    contention_ratio: float | None = None
    oversold_seats_fraction: float | None = None
    # --- latency / throughput, measured phase only ---
    p50_ms: float | None = None
    p95_ms: float | None = None
    p99_ms: float | None = None
    throughput_rps: float | None = None
    invariant_violations: list[dict[str, Any]] = field(default_factory=list)


async def reset_and_seed(event_name: str) -> tuple[int, list[int]]:
    """Wipe every Phase-0/1 table and seed a fresh --contention event.

    TRUNCATE ... RESTART IDENTITY so ids are predictable and don't drift
    run over run; CASCADE handles the FK ordering for us.
    """
    async with async_session_factory() as session:
        await session.execute(
            text(
                "TRUNCATE TABLE hold_audit, booking_seats, bookings, seats, events "
                "RESTART IDENTITY CASCADE"
            )
        )
        await session.commit()

    event_id = await seed(event_name, CONTENTION_SEAT_COUNT)

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


def _http_get_json(url: str, timeout: float = 2.0) -> dict[str, Any] | None:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:  # noqa: S310
            return json.loads(resp.read())
    except (urllib.error.URLError, TimeoutError, OSError):
        return None


def check_api_is_up(base_url: str) -> None:
    if _http_get_json(f"{base_url}/health") is None:
        raise SystemExit(
            f"Cannot reach {base_url}/health -- is the API running? Start it with "
            "`make run-api` first."
        )


def check_naive_race_window_is_zero() -> None:
    """The empirical benchmark must run at NAIVE_RACE_WINDOW_MS=0 -- we need
    evidence the race occurs under natural timing, not because we widened
    the window (that's what the deterministic regression test is for; see
    tests/integration/test_naive_strategy.py).

    Caveat, made explicit rather than papered over: this process cannot
    reach into the already-running API server process and change ITS
    environment. What we can do is read Settings.naive_race_window_ms in
    *this* process (which loads the same .env the server does, under
    normal usage) and refuse to proceed if it's nonzero, on the assumption
    both processes share that .env. If you started the server with an
    explicit environment override this process can't see, this check
    cannot catch that -- it is a strong hint, not a guarantee.
    """
    if settings.naive_race_window_ms != 0:
        raise SystemExit(
            "NAIVE_RACE_WINDOW_MS is nonzero "
            f"({settings.naive_race_window_ms}) in this process's config. The empirical "
            "benchmark must run with NAIVE_RACE_WINDOW_MS=0 so any oversell reflects natural "
            "timing, not a widened race window. Unset it (or set it to 0) in .env and restart "
            "the API server, then re-run this script."
        )


def poll_invariants_until_done(
    proc: subprocess.Popen[bytes], base_url: str, event_id: int
) -> list[dict[str, Any]]:
    violations: list[dict[str, Any]] = []
    while proc.poll() is None:
        result = _http_get_json(f"{base_url}/api/admin/invariants?event_id={event_id}")
        if result is not None and not result.get("all_passed", True):
            violations.append(
                {"timestamp": datetime.now(UTC).isoformat(), "results": result["results"]}
            )
        time.sleep(POLL_INTERVAL_SECONDS)
    return violations


def run_k6(
    k6_bin: str,
    scenario: str,
    *,
    base_url: str,
    event_id: int,
    seat_ids: list[int],
    vus: int,
    duration: str,
    warmup_vus: int,
    warmup_duration: str,
    summary_path: Path,
) -> list[dict[str, Any]]:
    script = SCENARIOS[scenario]
    env: dict[str, str] = {
        "BASE_URL": base_url,
        "EVENT_ID": str(event_id),
        "VUS": str(vus),
        "DURATION": duration,
        "WARMUP_VUS": str(warmup_vus),
        "WARMUP_DURATION": warmup_duration,
        "SUMMARY_PATH": str(summary_path),
    }
    if scenario == "last_seat":
        env["SEAT_ID"] = str(seat_ids[0])
    else:
        env["SEAT_IDS"] = ",".join(str(s) for s in seat_ids)

    full_env = {**os.environ, **env}
    cmd = [k6_bin, "run", str(script)]
    # Non-blocking: the k6 process runs concurrently with the poll loop
    # below in this same Python process. proc.wait() (inside
    # poll_invariants_until_done's loop condition, then explicitly again
    # after) is what guarantees we don't return until k6 has fully exited.
    proc = subprocess.Popen(cmd, env=full_env)  # noqa: S603
    violations = poll_invariants_until_done(proc, base_url, event_id)
    returncode = proc.wait()
    if returncode != 0:
        print(f"warning: k6 exited with code {returncode}", file=sys.stderr)
    return violations


def parse_k6_summary(summary_path: Path) -> dict[str, Any]:
    """Parse handleSummary()'s actual JSON payload.

    Confirmed by direct inspection (not guessed, not carried over from
    --summary-export's different, flatter, deprecated schema): every metric
    is `data.metrics.<name>.values.<stat>`, e.g.
    `data.metrics.measured_duration_ms.values["p(95)"]`,
    `data.metrics.status_409.values.count`. A metric with zero samples is
    simply absent from the object -- e.g. status_transport_error won't
    appear at all in a run with no transport failures -- so every lookup
    below defaults missing counters to 0 rather than erroring.
    """
    data = json.loads(summary_path.read_text(encoding="utf-8"))
    metrics = data.get("metrics", {})

    def value(metric: str, key: str) -> float | None:
        return metrics.get(metric, {}).get("values", {}).get(key)

    def count(metric: str) -> int:
        return int(value(metric, "count") or 0)

    return {
        "successes": count("status_2xx"),
        "expected_409s": count("status_409"),
        "unexpected_app_errors": count("status_other"),
        "transport_failures": count("status_transport_error"),
        "p50_ms": value("measured_duration_ms", "med"),
        "p95_ms": value("measured_duration_ms", "p(95)"),
        "p99_ms": value("measured_duration_ms", "p(99)"),
    }


def fetch_oversell_report(base_url: str, event_id: int) -> dict[str, Any]:
    result = _http_get_json(f"{base_url}/api/admin/oversell-report?event_id={event_id}")
    return result or {"oversold_seats": 0, "excess_holders": 0, "seats": []}


def aggregate(runs: list[RunResult]) -> dict[str, Any]:
    def mean(values: list[float]) -> float | None:
        return statistics.mean(values) if values else None

    def collect(attr: str) -> list[float]:
        return [v for r in runs if (v := getattr(r, attr)) is not None]

    return {
        "total_oversold_seats": sum(r.oversold_seats for r in runs),
        "total_excess_holders": sum(r.excess_holders for r in runs),
        "runs_with_oversold_seat": sum(1 for r in runs if r.oversold_seats > 0),
        "runs_with_excess_holders": sum(1 for r in runs if r.excess_holders > 0),
        "runs_with_invariant_violation": sum(1 for r in runs if r.invariant_violations),
        "p50_ms_mean": mean(collect("p50_ms")),
        "p95_ms_mean": mean(collect("p95_ms")),
        "p99_ms_mean": mean(collect("p99_ms")),
        "throughput_rps_mean": mean(collect("throughput_rps")),
        "contention_ratio_mean": mean(collect("contention_ratio")),
        "oversold_seats_fraction_mean": mean(collect("oversold_seats_fraction")),
    }


def _fmt(value: float | None, digits: int = 1) -> str:
    return "—" if value is None else f"{value:.{digits}f}"


def render_markdown(scenario: str, runs: list[RunResult], agg: dict[str, Any]) -> str:
    cfg = runs[0]
    lines = [
        f"# Benchmark: {settings.strategy} strategy, {scenario} scenario",
        "",
        "## Configuration",
        f"- Workers: {cfg.workers}, pool_size: {cfg.pool_size}, max_overflow: {cfg.max_overflow}",
        f"- NAIVE_RACE_WINDOW_MS: {cfg.naive_race_window_ms} (must be 0 for this to be evidence "
        "of a natural-timing oversell)",
        f"- VUs: {cfg.vus}, duration: {cfg.duration_seconds:.0f}s, "
        f"warmup: {cfg.warmup_vus} VUs for {cfg.warmup_duration_seconds:.0f}s",
        f"- Seats in contention: {cfg.seat_count}",
        "",
    ]
    if scenario == "last_seat":
        lines.append(
            "**Worst-case demonstration**: every VU targets one seat, so `oversold_seats` is "
            "mathematically capped at 1 and cannot show a distribution -- **excess_holders** "
            "(sum over seats of holders - 1) is the headline figure here."
        )
    else:
        lines.append(
            "**Headline scenario**: the full seat pool is in contention, so `oversold_seats` "
            "can show a real distribution across runs -- unlike last_seat.js, where it is "
            "capped at 1."
        )
    lines.append("")
    lines.append(
        "| Run | Successes | Expected 409s | Unexpected errors | Transport failures | "
        "Oversold seats | Excess holders | Contention ratio | Throughput (req/s) | p50 (ms) | "
        "p95 (ms) | p99 (ms) | Invariant violations |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for r in runs:
        lines.append(
            f"| {r.run} | {r.successes} | {r.expected_409s} | {r.unexpected_app_errors} | "
            f"{r.transport_failures} | {r.oversold_seats} | {r.excess_holders} | "
            f"{_fmt(r.contention_ratio)} | {_fmt(r.throughput_rps)} | {_fmt(r.p50_ms)} | "
            f"{_fmt(r.p95_ms)} | {_fmt(r.p99_ms)} | {len(r.invariant_violations)} |"
        )
    lines.append("")
    lines.append(
        f"{agg['runs_with_oversold_seat']}/{len(runs)} runs produced at least one oversold "
        f"seat; {agg['runs_with_excess_holders']}/{len(runs)} runs produced at least one "
        f"excess holder; {agg['runs_with_invariant_violation']}/{len(runs)} runs recorded at "
        "least one invariant violation while the load was running.\n\n"
        f"Total excess_holders across all runs: {agg['total_excess_holders']} "
        f"(mean p95: {_fmt(agg['p95_ms_mean'])}ms, mean throughput: "
        f"{_fmt(agg['throughput_rps_mean'])} req/s)."
    )
    return "\n".join(lines) + "\n"


async def main_async(args: argparse.Namespace) -> None:
    check_api_is_up(args.base_url)
    check_naive_race_window_is_zero()

    k6_bin = shutil.which(args.k6_bin)
    if k6_bin is None:
        raise SystemExit(
            f"'{args.k6_bin}' not found on PATH. Install k6 "
            "(https://k6.io/docs/get-started/installation/) and retry."
        )

    vus = args.vus if args.vus is not None else DEFAULT_VUS[args.scenario]
    duration = args.duration if args.duration is not None else DEFAULT_DURATION[args.scenario]
    duration_seconds = _parse_duration_seconds(duration)
    warmup_duration_seconds = _parse_duration_seconds(args.warmup_duration)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

    runs: list[RunResult] = []
    for run_number in range(1, args.runs + 1):
        print(f"--- run {run_number}/{args.runs} ---")
        event_id, seat_ids = await reset_and_seed(f"Benchmark run {run_number}")
        seat_count = len(seat_ids)

        summary_path = RESULTS_DIR / f"{run_id}-run{run_number}-k6.json"
        violations = run_k6(
            k6_bin,
            args.scenario,
            base_url=args.base_url,
            event_id=event_id,
            seat_ids=seat_ids,
            vus=vus,
            duration=duration,
            warmup_vus=args.warmup_vus,
            warmup_duration=args.warmup_duration,
            summary_path=summary_path,
        )

        # k6 has fully exited by this point (run_k6 only returns after
        # proc.wait()) -- only now do we read anything that depends on
        # every request having already landed.
        k6_metrics = parse_k6_summary(summary_path)
        oversell_report = fetch_oversell_report(args.base_url, event_id)

        total_requests = (
            k6_metrics["successes"]
            + k6_metrics["expected_409s"]
            + k6_metrics["unexpected_app_errors"]
            + k6_metrics["transport_failures"]
        )
        oversold_seats = oversell_report.get("oversold_seats") or 0

        runs.append(
            RunResult(
                run=run_number,
                scenario=args.scenario,
                vus=vus,
                duration_seconds=duration_seconds,
                warmup_applied=True,
                warmup_vus=args.warmup_vus,
                warmup_duration_seconds=warmup_duration_seconds,
                workers=args.workers,
                pool_size=settings.pool_size,
                max_overflow=settings.max_overflow,
                naive_race_window_ms=settings.naive_race_window_ms,
                seat_count=seat_count,
                invariant_violations=violations,
                contention_ratio=(total_requests / seat_count) if seat_count else None,
                oversold_seats_fraction=(oversold_seats / seat_count) if seat_count else None,
                oversold_seats=oversold_seats,
                excess_holders=oversell_report.get("excess_holders") or 0,
                throughput_rps=(total_requests / duration_seconds) if duration_seconds else None,
                **k6_metrics,
            )
        )

    agg = aggregate(runs)

    output = {
        "strategy": settings.strategy,
        "scenario": args.scenario,
        "runs": [asdict(r) for r in runs],
        "aggregate": agg,
    }

    json_path = RESULTS_DIR / f"{run_id}.json"
    json_path.write_text(json.dumps(output, indent=2), encoding="utf-8")

    md_path = RESULTS_DIR / f"{run_id}.md"
    rendered = render_markdown(args.scenario, runs, agg)
    md_path.write_text(rendered, encoding="utf-8")

    print(f"\nWrote {json_path}")
    print(f"Wrote {md_path}")
    print()
    print(rendered)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=5, help="Number of repetitions.")
    parser.add_argument(
        "--scenario",
        choices=sorted(SCENARIOS),
        default="flash_sale",
        help="k6 scenario to run (flash_sale is the headline benchmark; last_seat is the "
        "worst-case demonstration -- see README).",
    )
    parser.add_argument("--base-url", default="http://localhost:8000", help="Running API base URL.")
    parser.add_argument(
        "--vus", type=int, default=None, help="Override the scenario's default VUs."
    )
    parser.add_argument(
        "--duration", default=None, help="Override the scenario's default measured duration."
    )
    parser.add_argument("--warmup-vus", type=int, default=20, help="VUs during the warmup phase.")
    parser.add_argument("--warmup-duration", default="10s", help="Warmup phase duration.")
    parser.add_argument(
        "--workers",
        type=int,
        default=settings.uvicorn_workers,
        help="Worker count the API server was actually started with (recorded in results only "
        "-- this process cannot introspect a separate running process, so it must be told).",
    )
    parser.add_argument("--k6-bin", default="k6", help="k6 executable name or path.")
    args = parser.parse_args()

    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
