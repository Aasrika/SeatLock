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

Sweep mode (--sweep) is Phase 3's phase deliverable: every strategy in
--strategies (default all three) run at every contention ratio in
--contention-ratios (seat_count = round(vus / ratio) -- see
contention_sweep.js for why contention is varied by seat count, not VU
count), INTERLEAVED so machine drift over the sweep's wall-clock duration
cannot correlate with which strategy happens to run later (see
run_sweep_cell/_run_sweep_matrix's docstrings). Followed automatically by
a refinement pass (a handful of extra ratios inside whichever adjacent
pair of coarse ratios is where optimistic's mean valid throughput first
drops below pessimistic's) and a jitter ablation (optimistic only, the
two highest ratios, full jitter vs. fixed backoff).

Both single-strategy/comparison modes write loadtest/results/
<timestamp>.json (raw per-run + aggregate, including the full run
configuration -- worker count, pool settings, NAIVE_RACE_WINDOW_MS, VU
count, scenario, warmup -- a benchmark whose configuration isn't recorded
alongside its numbers is not reproducible) and loadtest/results/
<timestamp>.md (a markdown table, README-ready). Sweep mode writes
loadtest/results/<timestamp>-sweep.json and -sweep-summary.md instead --
the latter is raw generated data tables only; the actual committed
analysis lives in docs/benchmarks/phase3-crossover.md.

Requires the `k6` binary on PATH.

Usage:
    python -m loadtest.run_benchmark
    python -m loadtest.run_benchmark --scenario last_seat --runs 10
    python -m loadtest.run_benchmark --strategies naive,pessimistic
    python -m loadtest.run_benchmark --sweep
    python -m loadtest.run_benchmark --sweep --contention-ratios 2,10,50 --sweep-reps 3
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
    "contention_sweep": LOADTEST_DIR / "contention_sweep.js",
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


async def reset_and_seed(
    event_name: str, seat_count: int = CONTENTION_SEAT_COUNT
) -> tuple[int, list[int]]:
    """Wipe every Phase-0/1 table and seed a fresh event with `seat_count`
    seats (default CONTENTION_SEAT_COUNT, matching single-strategy and
    comparison mode's fixed 10-seat pool). The sweep (run_sweep) is the
    one caller that varies seat_count deliberately -- see its module-level
    docstring / contention_sweep.js's header comment for why contention is
    varied by seat count, never VU count.

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

    event_id = await seed(event_name, seat_count)

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


def _parse_counter_value(metrics_text: str, metric_name: str) -> float:
    """Parse a label-less Counter's current value directly out of raw
    /metrics text. metric_name is the FULL name as constructed in
    app/infra/metrics.py (e.g. "optimistic_conflicts_total") --
    prometheus_client strips and re-adds the "_total" suffix internally,
    but the rendered line's name always matches what was passed to
    Counter(name=...), so no suffix juggling is needed here.
    """
    prefix = f"{metric_name} "
    for line in metrics_text.splitlines():
        if line.startswith(prefix):
            return float(line.rsplit(" ", 1)[1])
    return 0.0


def _parse_histogram_sum_count(metrics_text: str, metric_name: str) -> tuple[float, float]:
    """Parse a label-less Histogram's cumulative (_sum, _count) directly
    out of raw /metrics text -- sum / count gives a mean, the figure the
    sweep reports for optimistic_attempts (see render_sweep_markdown).
    """
    total_sum, total_count = 0.0, 0.0
    for line in metrics_text.splitlines():
        if line.startswith(f"{metric_name}_sum "):
            total_sum = float(line.rsplit(" ", 1)[1])
        elif line.startswith(f"{metric_name}_count "):
            total_count = float(line.rsplit(" ", 1)[1])
    return total_sum, total_count


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

    Retries each unlink on WinError 32 ("being used by another process")
    -- encountered directly running the Phase 3 sweeps: unlike comparison
    mode (which restarts the API once per STRATEGY, a handful of times
    total), the sweeps restart once per CELL, dozens of times in quick
    succession. stop_api's taskkill /F /T kills the whole process tree
    and proc.wait() confirms the top-level process has exited, but
    Windows does not guarantee a --workers child's memory-mapped .db file
    handle is released in the same instant the process disappears from
    the process list -- there is a short teardown window where the file
    still shows as in-use. 2s of retry (10 x 0.2s) was enough for the
    coarse sweep (one extra process: the API); the recirculating sweep
    adds a SECOND process sharing this directory (workers/sweeper_worker.py)
    and hit this same race with that budget exhausted -- confirmed
    directly, not assumed. 30 x 0.3s (9s) gives real margin without
    masking a genuinely stuck file (this still raises, loudly, if 9s
    isn't enough -- it does not retry forever).
    """
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    max_attempts = 30
    for db_file in p.glob("*.db"):
        for attempt in range(max_attempts):
            try:
                db_file.unlink()
                break
            except FileNotFoundError:
                break
            except PermissionError:
                if attempt == max_attempts - 1:
                    raise
                time.sleep(0.3)


def start_api(
    *,
    strategy: str,
    workers: int,
    pool_size: int,
    max_overflow: int,
    base_url: str,
    prometheus_multiproc_dir: str,
    optimistic_full_jitter: bool | None = None,
    hold_duration_seconds: float | None = None,
    health_timeout_seconds: float = 30.0,
) -> subprocess.Popen[bytes]:
    """Start uvicorn as a subprocess configured for `strategy`, wait for
    /health, and return the process. Only used by comparison/sweep modes
    -- single-strategy mode assumes the API is already running externally.

    optimistic_full_jitter is only meaningful for strategy="optimistic";
    passed through as an explicit env var override so the jitter ablation
    (run_jitter_ablation) can start two otherwise-identical API instances
    that differ in exactly that one setting.

    hold_duration_seconds overrides Settings.hold_duration_seconds (8
    minutes in production) -- only ever set by the recirculating-
    contention benchmark (loadtest/recirculating_pilot.py /
    recirculating_sweep.py), which needs short holds (~1-2s) so inventory
    actually cycles within a load-test burst. This is a BENCHMARKING
    configuration, never a product one -- see those scripts' own
    docstrings for how the value is chosen (empirically, via a pilot),
    and docs/benchmarks/phase3-crossover.md for the value actually used.
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
    if hold_duration_seconds is not None:
        env["HOLD_DURATION_SECONDS"] = str(hold_duration_seconds)
    if optimistic_full_jitter is not None:
        env["OPTIMISTIC_FULL_JITTER"] = "true" if optimistic_full_jitter else "false"
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


# ============================================================================
# Contention sweep (Phase 3's phase deliverable) -- SPEC.md section 4's
# crossover analysis, produced empirically rather than assumed.
#
# Contention ratio is TARGETED (not measured) by computing
# seat_count = round(VUs / target_ratio) up front, then seeding exactly
# that many seats -- see contention_sweep.js's header comment for the full
# "why seat count, not VU count" rationale. The OBSERVED ratio
# (total_requests / seat_count, the same quantity RunResult.contention_
# ratio already computes for comparison mode) is recorded on every
# SweepRunResult too, since actual throughput -- and therefore actual
# total requests -- varies by strategy even at a fixed target.
#
# Runs are INTERLEAVED, not grouped by strategy: for a fixed contention
# ratio, every strategy runs once (cycling through all of them), then the
# whole cycle repeats for the next repetition, before moving to the next
# ratio. This means the API is restarted between EVERY SINGLE RUN in the
# matrix, not once per strategy -- the whole point is that machine drift
# over the run's wall-clock duration (thermal throttling, other processes,
# anything) must not correlate with WHICH STRATEGY happens to run later,
# or the crossover point would be measuring drift, not contention.
# started_at (wall-clock ISO timestamp) is recorded per run specifically
# so that correlation can be checked for after the fact.
# ============================================================================


@dataclass
class SweepRunResult:
    strategy: str
    contention_ratio_target: int
    repetition: int
    started_at: str
    # --- configuration recorded alongside the numbers it produced ---
    seat_count: int
    vus: int
    duration_seconds: float
    workers: int
    pool_size: int
    max_overflow: int
    naive_race_window_ms: int
    optimistic_full_jitter: bool | None = None  # only meaningful for optimistic
    # --- outcome, four categories, never merged ---
    successes: int = 0
    expected_409s: int = 0
    unexpected_app_errors: int = 0
    transport_failures: int = 0
    # --- oversell ---
    oversold_seats: int = 0
    excess_holders: int = 0
    # --- throughput: total/raw-successes/valid-successes (ruling 4) ---
    # total_request_rps counts every request the API processed, success or
    # correctly-rejected 409 -- the metric that actually discriminates
    # between pessimistic and optimistic (neither is capped by seat_count
    # the way successes are in this acquire-only workload). raw_successes_
    # rps and valid_successes_rps are ruling 4's own pair: identical for
    # pessimistic/optimistic (excess_holders is always 0 for both); their
    # GAP for naive specifically is the overselling, in rate terms. See
    # docs/benchmarks/phase3-crossover.md's "Why there is no single valid
    # throughput number" section for why total_request_rps, not either of
    # these, is what this sweep uses to look for a crossover.
    total_request_rps: float | None = None
    raw_successes_rps: float | None = None
    valid_successes_rps: float | None = None
    contention_ratio_observed: float | None = None
    # --- latency, measured phase only ---
    p50_ms: float | None = None
    p95_ms: float | None = None
    p99_ms: float | None = None
    # --- strategy-specific, from /metrics, absent (None) where N/A ---
    lock_wait_p99_ms: float | None = None  # pessimistic only
    optimistic_conflicts: float | None = None
    optimistic_retries: float | None = None
    optimistic_exhausted: float | None = None
    optimistic_attempts_mean: float | None = None


async def run_sweep_cell(
    *,
    k6_bin: str,
    strategy: str,
    contention_ratio_target: int,
    repetition: int,
    vus: int,
    duration: str,
    duration_seconds: float,
    warmup_vus: int,
    warmup_duration: str,
    workers: int,
    pool_size: int,
    max_overflow: int,
    base_url: str,
    prometheus_multiproc_dir: str,
    run_id: str,
    optimistic_full_jitter: bool | None = None,
) -> SweepRunResult:
    """One cell of the sweep matrix: start this strategy's API fresh,
    seed exactly the seat count this ratio needs, run one k6 burst,
    collect k6 + oversell-report + /metrics, tear the API back down.
    """
    seat_count = max(1, round(vus / contention_ratio_target))
    started_at = datetime.now(UTC).isoformat()

    proc = start_api(
        strategy=strategy,
        workers=workers,
        pool_size=pool_size,
        max_overflow=max_overflow,
        base_url=base_url,
        prometheus_multiproc_dir=prometheus_multiproc_dir,
        optimistic_full_jitter=optimistic_full_jitter,
    )
    try:
        label = f"{strategy}-ratio{contention_ratio_target}-rep{repetition}"
        event_id, seat_ids = await reset_and_seed(f"sweep {label}", seat_count)

        summary_path = RESULTS_DIR / f"{run_id}-{label}-k6.json"
        run_k6(
            k6_bin,
            "contention_sweep",
            base_url=base_url,
            event_id=event_id,
            seat_ids=seat_ids,
            vus=vus,
            duration=duration,
            warmup_vus=warmup_vus,
            warmup_duration=warmup_duration,
            summary_path=summary_path,
        )

        k6_metrics = parse_k6_summary(summary_path)
        oversell_report = fetch_oversell_report(base_url, event_id)
        metrics_text = _fetch_metrics_text(base_url)

        total_requests = (
            k6_metrics["successes"]
            + k6_metrics["expected_409s"]
            + k6_metrics["unexpected_app_errors"]
            + k6_metrics["transport_failures"]
        )
        excess_holders = oversell_report.get("excess_holders") or 0
        oversold_seats = oversell_report.get("oversold_seats") or 0

        lock_wait_p99_ms = None
        optimistic_conflicts = optimistic_retries = optimistic_exhausted = None
        optimistic_attempts_mean = None
        if metrics_text is not None:
            if strategy == "pessimistic":
                p99 = _parse_histogram_p99_seconds(metrics_text, "lock_wait_seconds")
                lock_wait_p99_ms = None if p99 is None else p99 * 1000
            elif strategy == "optimistic":
                optimistic_conflicts = _parse_counter_value(
                    metrics_text, "optimistic_conflicts_total"
                )
                optimistic_retries = _parse_counter_value(metrics_text, "optimistic_retries_total")
                optimistic_exhausted = _parse_counter_value(
                    metrics_text, "optimistic_exhausted_total"
                )
                att_sum, att_count = _parse_histogram_sum_count(metrics_text, "optimistic_attempts")
                optimistic_attempts_mean = (att_sum / att_count) if att_count else None

        return SweepRunResult(
            strategy=strategy,
            contention_ratio_target=contention_ratio_target,
            repetition=repetition,
            started_at=started_at,
            seat_count=seat_count,
            vus=vus,
            duration_seconds=duration_seconds,
            workers=workers,
            pool_size=pool_size,
            max_overflow=max_overflow,
            naive_race_window_ms=0,
            optimistic_full_jitter=optimistic_full_jitter if strategy == "optimistic" else None,
            oversold_seats=oversold_seats,
            excess_holders=excess_holders,
            total_request_rps=(total_requests / duration_seconds) if duration_seconds else None,
            raw_successes_rps=(
                (k6_metrics["successes"] / duration_seconds) if duration_seconds else None
            ),
            valid_successes_rps=(
                ((k6_metrics["successes"] - excess_holders) / duration_seconds)
                if duration_seconds
                else None
            ),
            contention_ratio_observed=(total_requests / seat_count) if seat_count else None,
            lock_wait_p99_ms=lock_wait_p99_ms,
            optimistic_conflicts=optimistic_conflicts,
            optimistic_retries=optimistic_retries,
            optimistic_exhausted=optimistic_exhausted,
            optimistic_attempts_mean=optimistic_attempts_mean,
            **k6_metrics,
        )
    finally:
        stop_api(proc)


async def _run_sweep_matrix(
    *,
    k6_bin: str,
    strategies: list[str],
    contention_ratios: list[int],
    reps: int,
    vus: int,
    duration: str,
    duration_seconds: float,
    warmup_vus: int,
    warmup_duration: str,
    workers: int,
    pool_size: int,
    max_overflow: int,
    base_url: str,
    prometheus_multiproc_dir: str,
    run_id: str,
) -> dict[tuple[str, int], list[SweepRunResult]]:
    """Runs the full (ratio x rep x strategy) matrix, INTERLEAVED per
    ruling 3 -- see this section's module-level comment above for why.
    """
    results: dict[tuple[str, int], list[SweepRunResult]] = {
        (strategy, ratio): [] for strategy in strategies for ratio in contention_ratios
    }
    total_cells = len(strategies) * len(contention_ratios) * reps
    cell_number = 0
    for ratio in contention_ratios:
        for rep in range(1, reps + 1):
            for strategy in strategies:
                cell_number += 1
                print(
                    f"--- sweep cell {cell_number}/{total_cells}: strategy={strategy} "
                    f"ratio={ratio} rep={rep} ---"
                )
                result = await run_sweep_cell(
                    k6_bin=k6_bin,
                    strategy=strategy,
                    contention_ratio_target=ratio,
                    repetition=rep,
                    vus=vus,
                    duration=duration,
                    duration_seconds=duration_seconds,
                    warmup_vus=warmup_vus,
                    warmup_duration=warmup_duration,
                    workers=workers,
                    pool_size=pool_size,
                    max_overflow=max_overflow,
                    base_url=base_url,
                    prometheus_multiproc_dir=prometheus_multiproc_dir,
                    run_id=run_id,
                )
                results[(strategy, ratio)].append(result)
    return results


def _assert_sweep_configuration(results: dict[tuple[str, int], list[SweepRunResult]]) -> None:
    """Everything except strategy, seat_count, contention_ratio_target
    (and, for the jitter ablation, optimistic_full_jitter) must be
    identical across every cell -- asserted, not trusted, same reasoning
    as _assert_identical_configuration.
    """
    fields = ("vus", "duration_seconds", "workers", "pool_size", "max_overflow")
    reference: dict[str, Any] | None = None
    reference_key: tuple[str, int] | None = None
    for key, runs in results.items():
        for run in runs:
            config = {name: getattr(run, name) for name in fields}
            if reference is None:
                reference, reference_key = config, key
            elif config != reference:
                raise SystemExit(
                    f"Sweep configuration mismatch: {key} rep={run.repetition} has {config}, "
                    f"but {reference_key} established {reference}. Refusing to produce a "
                    "sweep table."
                )


def _sweep_cell_stats(runs: list[SweepRunResult], attr: str) -> dict[str, float | None]:
    values = [v for r in runs if (v := getattr(r, attr)) is not None]
    if not values:
        return {"mean": None, "min": None, "max": None, "stdev": None}
    return {
        "mean": statistics.mean(values),
        "min": min(values),
        "max": max(values),
        "stdev": statistics.stdev(values) if len(values) > 1 else 0.0,
    }


def _find_crossover_interval(
    results: dict[tuple[str, int], list[SweepRunResult]], contention_ratios: list[int]
) -> tuple[int, int] | None:
    """First (lo, hi) pair of adjacent tested ratios where mean optimistic
    total_request_rps is still >= mean pessimistic total_request_rps at
    lo, but has dropped below it by hi.

    Deliberately NOT valid_successes_rps (ruling 4's successes-minus-
    excess_holders metric): in an acquire-only workload (no confirm/
    release), valid_successes_rps is mathematically capped at
    seat_count / duration for EVERY strategy, and is IDENTICAL for
    pessimistic and optimistic at every ratio by construction (both have
    excess_holders == 0, always) -- using it here would make this function
    return None trivially, always, regardless of any real difference
    between the two strategies. total_request_rps (every request the API
    actually processed, success or correctly-rejected 409) is the metric
    that actually varies between pessimistic and optimistic due to lock
    contention vs. retry overhead, and is what SPEC.md section 4's
    crossover is actually describing. See docs/benchmarks/
    phase3-crossover.md's "Why there is no single valid throughput number"
    section for the full reasoning, including how this was caught.

    Returns None if no such crossing exists anywhere in the tested range
    (optimistic leads throughout, or trails throughout) -- callers must
    report that honestly rather than inventing a bracket.
    """
    ratios = sorted(contention_ratios)
    prev_optimistic_ahead: bool | None = None
    prev_ratio: int | None = None
    for ratio in ratios:
        opt_stats = _sweep_cell_stats(results.get(("optimistic", ratio), []), "total_request_rps")
        pes_stats = _sweep_cell_stats(results.get(("pessimistic", ratio), []), "total_request_rps")
        opt_mean, pes_mean = opt_stats["mean"], pes_stats["mean"]
        if opt_mean is None or pes_mean is None:
            continue
        optimistic_ahead = opt_mean >= pes_mean
        if prev_optimistic_ahead is True and optimistic_ahead is False and prev_ratio is not None:
            return (prev_ratio, ratio)
        prev_optimistic_ahead = optimistic_ahead
        prev_ratio = ratio
    return None


def _pick_refinement_ratios(lo: int, hi: int, count: int = 4) -> list[int]:
    """count evenly-spaced integer ratios strictly between lo and hi,
    deduplicated (a narrow lo/hi gap can otherwise produce repeats).
    """
    if hi - lo <= 1:
        return []
    picks = sorted({round(lo + (hi - lo) * i / (count + 1)) for i in range(1, count + 1)})
    return [r for r in picks if lo < r < hi]


async def run_jitter_ablation(
    *,
    k6_bin: str,
    ratios: list[int],
    reps: int,
    vus: int,
    duration: str,
    duration_seconds: float,
    warmup_vus: int,
    warmup_duration: str,
    workers: int,
    pool_size: int,
    max_overflow: int,
    base_url: str,
    prometheus_multiproc_dir: str,
    run_id: str,
) -> dict[tuple[int, bool], list[SweepRunResult]]:
    """Optimistic only, at the two (or however many are passed) highest
    contention ratios, once with full jitter and once with fixed backoff
    -- makes jitter's effect a MEASURED result (ruling 5 / Phase 3 plan
    item 5), not an asserted one. Interleaved the same way as the main
    sweep, for the same reason.
    """
    results: dict[tuple[int, bool], list[SweepRunResult]] = {
        (ratio, jitter): [] for ratio in ratios for jitter in (True, False)
    }
    total_cells = len(ratios) * 2 * reps
    cell_number = 0
    for ratio in ratios:
        for rep in range(1, reps + 1):
            for full_jitter in (True, False):
                cell_number += 1
                print(
                    f"--- jitter ablation cell {cell_number}/{total_cells}: ratio={ratio} "
                    f"full_jitter={full_jitter} rep={rep} ---"
                )
                result = await run_sweep_cell(
                    k6_bin=k6_bin,
                    strategy="optimistic",
                    contention_ratio_target=ratio,
                    repetition=rep,
                    vus=vus,
                    duration=duration,
                    duration_seconds=duration_seconds,
                    warmup_vus=warmup_vus,
                    warmup_duration=warmup_duration,
                    workers=workers,
                    pool_size=pool_size,
                    max_overflow=max_overflow,
                    base_url=base_url,
                    prometheus_multiproc_dir=prometheus_multiproc_dir,
                    run_id=run_id,
                    optimistic_full_jitter=full_jitter,
                )
                results[(ratio, full_jitter)].append(result)
    return results


def render_sweep_markdown(
    coarse: dict[tuple[str, int], list[SweepRunResult]],
    contention_ratios: list[int],
    strategies: list[str],
    refined: dict[tuple[str, int], list[SweepRunResult]] | None,
    crossover_interval: tuple[int, int] | None,
    jitter_ablation: dict[tuple[int, bool], list[SweepRunResult]] | None,
) -> str:
    def cell_row(strategy: str, ratio: int, runs: list[SweepRunResult]) -> str:
        if not runs:
            return f"| {strategy} | {ratio} | — | — | — | — | — | — | — | — |"
        total = _sweep_cell_stats(runs, "total_request_rps")
        raw_succ = _sweep_cell_stats(runs, "raw_successes_rps")
        valid_succ = _sweep_cell_stats(runs, "valid_successes_rps")
        p99 = _sweep_cell_stats(runs, "p99_ms")
        total_oversold = sum(r.oversold_seats for r in runs)
        total_excess = sum(r.excess_holders for r in runs)
        extra = ""
        if strategy == "pessimistic":
            lw = _sweep_cell_stats(runs, "lock_wait_p99_ms")["mean"]
            extra = _fmt(lw)
        elif strategy == "optimistic":
            conf = _sweep_cell_stats(runs, "optimistic_conflicts")["mean"]
            att = _sweep_cell_stats(runs, "optimistic_attempts_mean")["mean"]
            extra = f"conf={_fmt(conf)}/att={_fmt(att, 2)}"
        return (
            f"| {strategy} | {ratio} | {total_oversold} | {total_excess} | "
            f"{_fmt(total['mean'])} (min {_fmt(total['min'])}/max {_fmt(total['max'])}) | "
            f"{_fmt(raw_succ['mean'])} | {_fmt(valid_succ['mean'])} | "
            f"{_fmt(p99['mean'])} (min {_fmt(p99['min'])}/max {_fmt(p99['max'])}) | "
            f"{len(runs)} | {extra} |"
        )

    lines = [
        "# Phase 3 contention sweep -- raw data tables",
        "",
        "Auto-generated by loadtest/run_benchmark.py's --sweep mode. See "
        "docs/benchmarks/phase3-crossover.md for the synthesized, hand-written analysis "
        "this data backs, including why total_request_rps (not raw/valid successes) is what "
        "the crossover search below actually compares.",
        "",
        "## Coarse sweep",
        "",
        "| Strategy | Contention ratio (target) | Oversold seats (total) | Excess holders "
        "(total) | Total request rate (req/s, mean / min / max) | Raw successes (req/s, "
        "mean) | Valid successes (req/s, mean) | p99 (ms, mean / min / max) | Reps | "
        "Strategy-specific |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for ratio in contention_ratios:
        for strategy in strategies:
            lines.append(cell_row(strategy, ratio, coarse.get((strategy, ratio), [])))
    lines.append("")
    lines.append(
        "**Raw successes** = successes / duration. **Valid successes** = (successes - "
        "excess_holders) / duration. Identical to each other for pessimistic and optimistic "
        "(excess_holders is always 0 for both); for naive, the gap between them at a given "
        "ratio IS the overselling, in rate terms (ruling 4). Neither is the metric used for "
        "crossover detection below -- **total request rate** is, since in this acquire-only "
        "workload valid successes is capped at seat_count / duration for every strategy and "
        "cannot discriminate between the two correct ones at all."
    )
    lines.append("")
    if crossover_interval is not None:
        lines.append(
            f"**Crossover interval (coarse pass)**: optimistic's mean total request rate was "
            f">= pessimistic's at ratio {crossover_interval[0]}, and < pessimistic's by ratio "
            f"{crossover_interval[1]}. See the refinement pass below for a narrower bound."
        )
    else:
        lines.append(
            "**No crossover found in the coarse pass's tested range** -- optimistic's mean "
            "total request rate was either ahead of or behind pessimistic's across every "
            "ratio tested, not crossing between them. See docs/benchmarks/"
            "phase3-crossover.md for what that means for this specific run."
        )

    if refined is not None:
        lines.append("")
        lines.append("## Refinement pass")
        lines.append("")
        lines.append(
            "| Strategy | Contention ratio (target) | Oversold seats (total) | Excess "
            "holders (total) | Total request rate (req/s, mean / min / max) | Raw successes "
            "(req/s, mean) | Valid successes (req/s, mean) | p99 (ms, mean / min / max) | "
            "Reps | Strategy-specific |"
        )
        lines.append("|---|---|---|---|---|---|---|---|---|---|")
        refined_ratios = sorted({ratio for (_, ratio) in refined})
        for ratio in refined_ratios:
            for strategy in strategies:
                lines.append(cell_row(strategy, ratio, refined.get((strategy, ratio), [])))

    if jitter_ablation is not None:
        lines.append("")
        lines.append("## Jitter ablation (optimistic only)")
        lines.append("")
        lines.append(
            "| Contention ratio | Full jitter | Total request rate (req/s, mean ± stdev) | "
            "p99 (ms, mean) | Conflicts (mean) | Retries (mean) | Reps |"
        )
        lines.append("|---|---|---|---|---|---|---|")
        for (ratio, full_jitter), runs in sorted(jitter_ablation.items()):
            total = _sweep_cell_stats(runs, "total_request_rps")
            p99 = _sweep_cell_stats(runs, "p99_ms")
            conf = _sweep_cell_stats(runs, "optimistic_conflicts")
            retr = _sweep_cell_stats(runs, "optimistic_retries")
            lines.append(
                f"| {ratio} | {full_jitter} | {_fmt(total['mean'])} (±{_fmt(total['stdev'])}) "
                f"| {_fmt(p99['mean'])} | {_fmt(conf['mean'], 2)} | {_fmt(retr['mean'], 2)} | "
                f"{len(runs)} |"
            )

    return "\n".join(lines) + "\n"


async def run_sweep(args: argparse.Namespace) -> None:
    strategies = (
        [s.strip() for s in args.strategies.split(",") if s.strip()]
        if args.strategies
        else ["naive", "pessimistic", "optimistic"]
    )
    contention_ratios = sorted(
        int(r.strip()) for r in args.contention_ratios.split(",") if r.strip()
    )

    k6_bin = shutil.which(args.k6_bin)
    if k6_bin is None:
        raise SystemExit(
            f"'{args.k6_bin}' not found on PATH. Install k6 "
            "(https://k6.io/docs/get-started/installation/) and retry."
        )

    duration_seconds = _parse_duration_seconds(args.sweep_duration)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

    common = dict(
        k6_bin=k6_bin,
        vus=args.sweep_vus,
        duration=args.sweep_duration,
        duration_seconds=duration_seconds,
        warmup_vus=args.sweep_warmup_vus,
        warmup_duration=args.sweep_warmup_duration,
        workers=args.workers,
        pool_size=settings.pool_size,
        max_overflow=settings.max_overflow,
        base_url=args.base_url,
        prometheus_multiproc_dir=settings.prometheus_multiproc_dir,
        run_id=run_id,
    )

    print(f"=== coarse sweep: strategies={strategies} ratios={contention_ratios} ===")
    coarse = await _run_sweep_matrix(
        strategies=strategies, contention_ratios=contention_ratios, reps=args.sweep_reps, **common
    )
    _assert_sweep_configuration(coarse)

    crossover_interval = _find_crossover_interval(coarse, contention_ratios)

    refined: dict[tuple[str, int], list[SweepRunResult]] | None = None
    if not args.skip_refinement and crossover_interval is not None:
        refine_ratios = _pick_refinement_ratios(*crossover_interval)
        if refine_ratios:
            print(f"=== refinement pass: ratios={refine_ratios} (inside {crossover_interval}) ===")
            refined = await _run_sweep_matrix(
                strategies=strategies,
                contention_ratios=refine_ratios,
                reps=args.sweep_reps,
                **common,
            )
            _assert_sweep_configuration(refined)

    jitter_ablation: dict[tuple[int, bool], list[SweepRunResult]] | None = None
    if not args.skip_jitter_ablation:
        ablation_ratios = (
            contention_ratios[-2:] if len(contention_ratios) >= 2 else contention_ratios
        )
        print(f"=== jitter ablation: ratios={ablation_ratios} ===")
        jitter_ablation = await run_jitter_ablation(
            ratios=ablation_ratios, reps=args.sweep_reps, **common
        )

    def _serialize(d: dict) -> dict:
        return {"|".join(str(p) for p in key): [asdict(r) for r in runs] for key, runs in d.items()}

    output = {
        "strategies": strategies,
        "contention_ratios": contention_ratios,
        "crossover_interval": crossover_interval,
        "coarse": _serialize(coarse),
        "refined": _serialize(refined) if refined is not None else None,
        "jitter_ablation": _serialize(jitter_ablation) if jitter_ablation is not None else None,
    }

    json_path = RESULTS_DIR / f"{run_id}-sweep.json"
    json_path.write_text(json.dumps(output, indent=2), encoding="utf-8")

    md_path = RESULTS_DIR / f"{run_id}-sweep-summary.md"
    rendered = render_sweep_markdown(
        coarse, contention_ratios, strategies, refined, crossover_interval, jitter_ablation
    )
    md_path.write_text(rendered, encoding="utf-8")

    print(f"\nWrote {json_path}")
    print(f"Wrote {md_path}")
    print()
    print(rendered)


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
        help="Comma-separated strategy names (e.g. 'naive,pessimistic'). For --strategies "
        "alone (no --sweep), runs a side-by-side comparison instead of a single-strategy "
        "benchmark, and needs >= 2 names. For --sweep, selects which strategies the sweep "
        "matrix covers (default: all three). Either mode starts and stops the API itself, "
        "once per run -- --base-url must be free for it to bind, not already serving a "
        "separately-managed instance.",
    )
    parser.add_argument(
        "--sweep",
        action="store_true",
        help="Run the Phase 3 contention sweep (SPEC.md section 4's crossover analysis) "
        "instead of a single-strategy or comparison benchmark: every strategy in "
        "--strategies (default all three) at every ratio in --contention-ratios, "
        "INTERLEAVED (see run_sweep's docstring for why), followed by a refinement pass "
        "around the detected crossover and a jitter ablation.",
    )
    parser.add_argument(
        "--contention-ratios",
        default="2,5,10,20,50,100",
        help="Comma-separated target contention ratios (requests-offered per seat) for the "
        "coarse sweep pass. Seat count is DERIVED from this (seat_count = round(vus / ratio)) "
        "-- see contention_sweep.js for why contention is varied by seat count, not VU count.",
    )
    parser.add_argument(
        "--sweep-reps",
        type=int,
        default=3,
        help="Repetitions per (strategy, ratio) cell. Minimum 3 -- a single run per cell "
        "cannot support any claim about a crossover point. If the full matrix takes too "
        "long, cut --contention-ratios, not this.",
    )
    parser.add_argument(
        "--sweep-vus", type=int, default=200, help="Fixed VUs for every sweep cell."
    )
    parser.add_argument(
        "--sweep-duration",
        default="10s",
        help="Fixed measured-phase duration for every sweep cell.",
    )
    parser.add_argument(
        "--sweep-warmup-vus", type=int, default=20, help="Fixed warmup VUs for every sweep cell."
    )
    parser.add_argument(
        "--sweep-warmup-duration", default="5s", help="Fixed warmup duration for every sweep cell."
    )
    parser.add_argument(
        "--skip-refinement",
        action="store_true",
        help="Skip the second-stage refinement pass around the detected crossover interval.",
    )
    parser.add_argument(
        "--skip-jitter-ablation",
        action="store_true",
        help="Skip the full-jitter-vs-fixed-backoff ablation at the two highest ratios.",
    )
    args = parser.parse_args()

    if args.sweep:
        if args.sweep_reps < 3:
            raise SystemExit(
                "--sweep-reps must be >= 3 -- a single run per cell cannot support any claim "
                "about a crossover point. Cut --contention-ratios instead if the full matrix "
                "takes too long."
            )
        asyncio.run(run_sweep(args))
    elif args.strategies is not None:
        asyncio.run(run_comparison(args))
    else:
        asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
