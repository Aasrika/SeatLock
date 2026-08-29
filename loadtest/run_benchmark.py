"""Orchestrates a full benchmark run for one strategy, or a side-by-side
comparison of several.

Single-strategy mode (default) assumes the API is already running (see
`make run-api`) and reachable at --base-url; this script does not start
it. For each of N repetitions (default 5 -- a single run proving nothing
is the failure mode SPEC.md warns about):

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
       run_k6()'s return value in _collect_runs() below.

Comparison mode (--strategies naive,pessimistic) is different: THIS script
starts and stops the API itself, once per strategy, so STRATEGY can vary
between them while every other setting (scenario, VUs, workers, pool_size,
NAIVE_RACE_WINDOW_MS=0) stays identical -- asserted afterward
(_assert_identical_configuration), not just trusted, because a comparison
where configuration silently drifted between strategies would be actively
misleading rather than merely incomplete.

Both modes write loadtest/results/<timestamp>.json (raw per-run +
aggregate, including the full run configuration -- worker count, pool
settings, NAIVE_RACE_WINDOW_MS, VU count, scenario, warmup -- a benchmark
whose configuration isn't recorded alongside its numbers is not
reproducible) and loadtest/results/<timestamp>.md (a markdown table,
README-ready).

Requires the `k6` binary on PATH.

Usage:
    python -m loadtest.run_benchmark
    python -m loadtest.run_benchmark --scenario last_seat --runs 10
    python -m loadtest.run_benchmark --strategies naive,pessimistic
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.parse
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


def _fetch_metrics_text(base_url: str) -> str | None:
    try:
        with urllib.request.urlopen(f"{base_url}/metrics", timeout=5) as resp:  # noqa: S310
            return resp.read().decode()
    except (urllib.error.URLError, TimeoutError, OSError):
        return None


def _parse_histogram_p99_seconds(metrics_text: str, metric_name: str) -> float | None:
    """Approximate p99 from a Prometheus histogram's cumulative bucket
    counts: the boundary of the first bucket whose cumulative count
    reaches 99% of all observations. This is a bucket-boundary estimate,
    not a true interpolated quantile -- good enough for a benchmark
    report, not a substitute for histogram_quantile() against a real
    Prometheus time series.
    """
    buckets: list[tuple[float, float]] = []
    for line in metrics_text.splitlines():
        if not line.startswith(f"{metric_name}_bucket"):
            continue
        le_str = line.split('le="')[1].split('"')[0]
        count_str = line.rsplit(" ", 1)[1]
        le = float("inf") if le_str == "+Inf" else float(le_str)
        buckets.append((le, float(count_str)))
    if not buckets:
        return None
    buckets.sort(key=lambda b: b[0])
    total = buckets[-1][1]
    if total == 0:
        return None
    target = total * 0.99
    for le, cumulative in buckets:
        if cumulative >= target:
            return None if le == float("inf") else le
    return None


def _clear_prometheus_multiproc_dir(path: str) -> None:
    """Same reasoning as the Makefile's run-api target: clear stale
    per-process metric files exactly once, before any worker starts --
    otherwise switching strategies would mix one strategy's lock_wait_
    seconds samples into the next one's scrape.
    """
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    for db_file in p.glob("*.db"):
        with contextlib.suppress(FileNotFoundError):
            db_file.unlink()


def start_api(
    *,
    strategy: str,
    workers: int,
    pool_size: int,
    max_overflow: int,
    base_url: str,
    prometheus_multiproc_dir: str,
    health_timeout_seconds: float = 30.0,
) -> subprocess.Popen[bytes]:
    """Start uvicorn as a subprocess configured for `strategy`, wait for
    /health, and return the process. Only used by comparison mode --
    single-strategy mode assumes the API is already running externally.
    """
    _clear_prometheus_multiproc_dir(prometheus_multiproc_dir)

    port = urllib.parse.urlparse(base_url).port or 8000
    env = {
        **os.environ,
        "STRATEGY": strategy,
        # Forced regardless of whatever this process's own .env says --
        # comparison mode must never accidentally benchmark a widened
        # race window for one strategy and not another.
        "NAIVE_RACE_WINDOW_MS": "0",
        "UVICORN_WORKERS": str(workers),
        "POOL_SIZE": str(pool_size),
        "MAX_OVERFLOW": str(max_overflow),
        "PROMETHEUS_MULTIPROC_DIR": prometheus_multiproc_dir,
    }
    cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "app.main:app",
        "--host",
        "0.0.0.0",
        "--port",
        str(port),
        "--workers",
        str(workers),
    ]
    proc = subprocess.Popen(cmd, env=env)  # noqa: S603

    deadline = time.monotonic() + health_timeout_seconds
    while time.monotonic() < deadline:
        if _http_get_json(f"{base_url}/health") is not None:
            return proc
        time.sleep(0.5)

    stop_api(proc)
    raise SystemExit(
        f"API (strategy={strategy!r}) did not become healthy within {health_timeout_seconds:.0f}s."
    )


def stop_api(proc: subprocess.Popen[bytes]) -> None:
    """Stop an API started by start_api(), including its uvicorn worker
    subprocesses -- a plain proc.terminate() only signals the top-level
    process; with --workers > 1, uvicorn's worker children have been seen
    to survive that (encountered by hand during Phase 1 diagnostics) and
    keep the port bound.
    """
    if sys.platform == "win32":
        subprocess.run(  # noqa: S603, S607
            ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
            capture_output=True,
            check=False,
        )
        proc.wait(timeout=10)
    else:
        # Not exercised in this environment (Windows) -- best-effort POSIX
        # path: uvicorn's own supervisor catches SIGTERM and cascades
        # graceful shutdown to its worker children.
        import signal

        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)


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


async def _collect_runs(
    *,
    k6_bin: str,
    scenario: str,
    base_url: str,
    vus: int,
    duration: str,
    duration_seconds: float,
    warmup_vus: int,
    warmup_duration: str,
    workers: int,
    naive_race_window_ms: int,
    run_count: int,
    run_id: str,
    label: str,
) -> list[RunResult]:
    """The per-repetition loop shared by single-strategy mode and each
    strategy's slice of comparison mode: reset+seed, run k6, collect.
    """
    warmup_duration_seconds = _parse_duration_seconds(warmup_duration)
    runs: list[RunResult] = []
    for run_number in range(1, run_count + 1):
        print(f"--- {label} run {run_number}/{run_count} ---")
        event_id, seat_ids = await reset_and_seed(f"{label} run {run_number}")
        seat_count = len(seat_ids)

        summary_path = RESULTS_DIR / f"{run_id}-{label}-run{run_number}-k6.json"
        violations = run_k6(
            k6_bin,
            scenario,
            base_url=base_url,
            event_id=event_id,
            seat_ids=seat_ids,
            vus=vus,
            duration=duration,
            warmup_vus=warmup_vus,
            warmup_duration=warmup_duration,
            summary_path=summary_path,
        )

        # k6 has fully exited by this point (run_k6 only returns after
        # proc.wait()) -- only now do we read anything that depends on
        # every request having already landed.
        k6_metrics = parse_k6_summary(summary_path)
        oversell_report = fetch_oversell_report(base_url, event_id)

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
                scenario=scenario,
                vus=vus,
                duration_seconds=duration_seconds,
                warmup_applied=True,
                warmup_vus=warmup_vus,
                warmup_duration_seconds=warmup_duration_seconds,
                workers=workers,
                pool_size=settings.pool_size,
                max_overflow=settings.max_overflow,
                naive_race_window_ms=naive_race_window_ms,
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
    return runs


def _assert_identical_configuration(all_results: dict[str, list[RunResult]]) -> None:
    """Configuration must be IDENTICAL across strategies for the
    comparison to be valid -- only the strategy may vary. Asserted here
    rather than trusted: a comparison where pool_size or VUs silently
    drifted between strategies would be actively misleading, not merely
    incomplete.
    """
    fields = (
        "scenario",
        "vus",
        "duration_seconds",
        "workers",
        "pool_size",
        "max_overflow",
        "naive_race_window_ms",
        "seat_count",
    )
    reference: dict[str, Any] | None = None
    reference_strategy = ""
    for strategy, runs in all_results.items():
        for run in runs:
            config = {name: getattr(run, name) for name in fields}
            if reference is None:
                reference, reference_strategy = config, strategy
            elif config != reference:
                raise SystemExit(
                    f"Configuration mismatch: strategy={strategy!r} run={run.run} has "
                    f"{config}, but strategy={reference_strategy!r} established {reference}. "
                    "The comparison is only valid if configuration is identical across "
                    "strategies -- refusing to produce a comparison table."
                )


def render_comparison_markdown(
    scenario: str,
    all_results: dict[str, list[RunResult]],
    lock_wait_p99_ms: dict[str, float | None],
) -> str:
    cfg = next(iter(all_results.values()))[0]
    lines = [
        f"# Benchmark comparison: {', '.join(all_results)} ({scenario} scenario)",
        "",
        "## Configuration (identical across strategies -- see _assert_identical_configuration)",
        f"- Workers: {cfg.workers}, pool_size: {cfg.pool_size}, max_overflow: {cfg.max_overflow}",
        f"- NAIVE_RACE_WINDOW_MS: {cfg.naive_race_window_ms}",
        f"- VUs: {cfg.vus}, duration: {cfg.duration_seconds:.0f}s, "
        f"warmup: {cfg.warmup_vus} VUs for {cfg.warmup_duration_seconds:.0f}s",
        f"- Seats in contention: {cfg.seat_count}",
        "",
        "| Strategy | Oversold seats (total) | Excess holders (total) | Throughput (req/s, "
        "mean) | p50 (ms, mean) | p95 (ms, mean) | p99 (ms, mean) | lock_wait p99 (ms) | "
        "Error rate (mean) |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for strategy, runs in all_results.items():
        agg = aggregate(runs)
        total_requests = sum(
            r.successes + r.expected_409s + r.unexpected_app_errors + r.transport_failures
            for r in runs
        )
        total_errors = sum(r.unexpected_app_errors + r.transport_failures for r in runs)
        error_rate = (total_errors / total_requests) if total_requests else None
        lock_wait = lock_wait_p99_ms.get(strategy)
        lock_wait_str = "—" if lock_wait is None else f"{lock_wait * 1000:.1f}"
        lines.append(
            f"| {strategy} | {agg['total_oversold_seats']} | {agg['total_excess_holders']} | "
            f"{_fmt(agg['throughput_rps_mean'])} | {_fmt(agg['p50_ms_mean'])} | "
            f"{_fmt(agg['p95_ms_mean'])} | {_fmt(agg['p99_ms_mean'])} | {lock_wait_str} | "
            f"{_fmt(error_rate, 3)} |"
        )
    lines.append("")
    lines.append(
        'lock_wait p99 is absent ("—") for strategies that never take a row lock (e.g. '
        "naive) -- it comes from that strategy's own /metrics scrape, not from k6."
    )
    return "\n".join(lines) + "\n"


async def run_comparison(args: argparse.Namespace) -> None:
    strategies = [s.strip() for s in args.strategies.split(",") if s.strip()]
    if len(strategies) < 2:
        raise SystemExit("--strategies needs at least 2 comma-separated strategy names to compare")

    k6_bin = shutil.which(args.k6_bin)
    if k6_bin is None:
        raise SystemExit(
            f"'{args.k6_bin}' not found on PATH. Install k6 "
            "(https://k6.io/docs/get-started/installation/) and retry."
        )

    vus = args.vus if args.vus is not None else DEFAULT_VUS[args.scenario]
    duration = args.duration if args.duration is not None else DEFAULT_DURATION[args.scenario]
    duration_seconds = _parse_duration_seconds(duration)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

    all_results: dict[str, list[RunResult]] = {}
    lock_wait_p99_ms: dict[str, float | None] = {}

    for strategy in strategies:
        print(f"=== strategy: {strategy} ===")
        proc = start_api(
            strategy=strategy,
            workers=args.workers,
            pool_size=settings.pool_size,
            max_overflow=settings.max_overflow,
            base_url=args.base_url,
            prometheus_multiproc_dir=settings.prometheus_multiproc_dir,
        )
        try:
            all_results[strategy] = await _collect_runs(
                k6_bin=k6_bin,
                scenario=args.scenario,
                base_url=args.base_url,
                vus=vus,
                duration=duration,
                duration_seconds=duration_seconds,
                warmup_vus=args.warmup_vus,
                warmup_duration=args.warmup_duration,
                workers=args.workers,
                naive_race_window_ms=0,  # forced in start_api's env, see above
                run_count=args.runs,
                run_id=run_id,
                label=strategy,
            )
            metrics_text = _fetch_metrics_text(args.base_url)
            lock_wait_p99_ms[strategy] = (
                _parse_histogram_p99_seconds(metrics_text, "lock_wait_seconds")
                if metrics_text
                else None
            )
        finally:
            stop_api(proc)

    _assert_identical_configuration(all_results)

    output = {
        "scenario": args.scenario,
        "strategies": {
            strategy: {
                "runs": [asdict(r) for r in runs],
                "aggregate": aggregate(runs),
                "lock_wait_p99_ms": lock_wait_p99_ms.get(strategy),
            }
            for strategy, runs in all_results.items()
        },
    }

    json_path = RESULTS_DIR / f"{run_id}-comparison.json"
    json_path.write_text(json.dumps(output, indent=2), encoding="utf-8")

    md_path = RESULTS_DIR / f"{run_id}-comparison.md"
    rendered = render_comparison_markdown(args.scenario, all_results, lock_wait_p99_ms)
    md_path.write_text(rendered, encoding="utf-8")

    print(f"\nWrote {json_path}")
    print(f"Wrote {md_path}")
    print()
    print(rendered)


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

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

    runs = await _collect_runs(
        k6_bin=k6_bin,
        scenario=args.scenario,
        base_url=args.base_url,
        vus=vus,
        duration=duration,
        duration_seconds=duration_seconds,
        warmup_vus=args.warmup_vus,
        warmup_duration=args.warmup_duration,
        workers=args.workers,
        naive_race_window_ms=settings.naive_race_window_ms,
        run_count=args.runs,
        run_id=run_id,
        label=args.scenario,
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
        help="Worker count. In single-strategy mode, recorded in results only (this process "
        "cannot introspect a separate running process, so it must be told); in --strategies "
        "comparison mode, also the actual value used to start each strategy's API instance.",
    )
    parser.add_argument("--k6-bin", default="k6", help="k6 executable name or path.")
    parser.add_argument(
        "--strategies",
        default=None,
        help="Comma-separated strategy names (e.g. 'naive,pessimistic') to run a side-by-side "
        "comparison instead of a single-strategy benchmark. This mode starts and stops the API "
        "itself, once per strategy -- --base-url must be free for it to bind, not already "
        "serving a separately-managed instance.",
    )
    args = parser.parse_args()

    if args.strategies is not None:
        asyncio.run(run_comparison(args))
    else:
        asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
