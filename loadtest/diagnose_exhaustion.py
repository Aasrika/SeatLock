"""Phase 3 diagnostic -- NOT the sweep itself, and not a replacement for
docs/benchmarks/phase3-crossover.md.

The coarse sweep's "no crossover through ratio 100" finding rests on an
unexamined assumption: that total_request_rps, measured over a whole
10-second burst, is actually comparing the two strategies' behavior UNDER
CONTENTION. It might not be. This is an acquire-only workload (no
confirm/release) against a small, fixed seat pool -- once every seat is
HELD, the run keeps sending requests for whatever's left of the 10s
window, and pessimistic and optimistic do very different amounts of work
per REJECTION once inventory is gone:

  - optimistic: an unlocked SELECT sees the seat already HELD/BOOKED ->
    the domain state machine rejects it. No lock taken, no UPDATE issued.
  - pessimistic: SELECT ... FOR UPDATE acquires the row lock FIRST, THEN
    discovers the seat is unavailable, THEN rolls back and releases it.

If most of a run's measured window falls AFTER exhaustion, total_
request_rps mostly measures rejection cost, not concurrency-control cost
-- which would produce exactly the monotone, no-crossover result the
coarse sweep found, at every ratio, regardless of whether a genuine
crossover exists during real contention.

This script re-runs pessimistic and optimistic (naive excluded -- see
below) at each coarse-sweep contention ratio using k6's raw, per-request
JSON output (`k6 run --out json=<path>`, confirmed by direct inspection:
one line per Point, `data.tags.phase` distinguishes warmup from measured,
`data.time` is an ISO8601 timestamp Python 3.11's datetime.fromisoformat
parses directly) instead of the aggregate-only summary the main sweep
uses. From that, per run:

  1. TIME TO EXHAUSTION: elapsed time from the first measured-phase
     request to the LAST successful (status_2xx) response. Neither
     strategy ever oversells (excess_holders == 0 for both, confirmed by
     the coarse sweep) -- every success takes exactly one distinct seat,
     so the last success IS the moment the last seat became HELD.
  2. PHASE-SEGMENTED METRICS: every measured-phase request is classified
     CONTESTED (timestamp <= exhaustion time) or EXHAUSTED (after), and
     count/throughput/p50/p95/p99 are reported separately per phase.

Naive is excluded: its oversell means "last success" is not "last seat
filled" (many successes can land on the same already-taken seat), so this
exhaustion definition doesn't hold for it -- and it isn't part of the
pessimistic-vs-optimistic crossover question this diagnostic exists to
check anyway.

This is deliberately smaller than the real sweep: --reps defaults to 1
(exploratory, not a claim -- rerun with more if a ratio's result looks
borderline) and it writes its own results file, never phase3-crossover.md.

Usage:
    python -m loadtest.diagnose_exhaustion
    python -m loadtest.diagnose_exhaustion --contention-ratios 5,50 --reps 3
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from loadtest import run_benchmark as rb

RESULTS_DIR = rb.RESULTS_DIR
_MEASURED_STATUS_METRICS = (
    "status_2xx",
    "status_409",
    "status_other",
    "status_transport_error",
)


def _parse_k6_time(ts: str) -> float:
    """k6's raw JSON timestamps are ISO8601 with local-offset timezone and
    sub-microsecond fractional seconds -- confirmed by direct inspection
    (e.g. "2026-08-30T10:36:12.7812387+05:30"). Python 3.11's
    datetime.fromisoformat parses this directly (silently truncating
    fractional precision below microseconds), so no manual truncation is
    needed here -- confirmed empirically before relying on it.
    """
    return datetime.fromisoformat(ts).timestamp()


@dataclass
class PhaseStats:
    duration_seconds: float
    successes: int = 0
    expected_409s: int = 0
    unexpected_app_errors: int = 0
    transport_failures: int = 0
    total_request_rps: float | None = None
    p50_ms: float | None = None
    p95_ms: float | None = None
    p99_ms: float | None = None


@dataclass
class ExhaustionDiagnostic:
    strategy: str
    contention_ratio_target: int
    repetition: int
    seat_count: int
    vus: int
    configured_duration_seconds: float
    time_to_exhaustion_seconds: float | None
    time_to_exhaustion_fraction: float | None
    oversold_seats: int = 0
    excess_holders: int = 0
    contested: PhaseStats | None = None
    exhausted: PhaseStats | None = None


def _percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    idx = min(len(values) - 1, max(0, int(round(p * (len(values) - 1)))))
    return values[idx]


def run_k6_with_raw_json(
    k6_bin: str,
    *,
    base_url: str,
    event_id: int,
    seat_ids: list[int],
    vus: int,
    duration: str,
    warmup_vus: int,
    warmup_duration: str,
    raw_output_path: Path,
) -> None:
    script = rb.SCENARIOS["contention_sweep"]
    env = {
        **os.environ,
        "BASE_URL": base_url,
        "EVENT_ID": str(event_id),
        "SEAT_IDS": ",".join(str(s) for s in seat_ids),
        "VUS": str(vus),
        "DURATION": duration,
        "WARMUP_VUS": str(warmup_vus),
        "WARMUP_DURATION": warmup_duration,
    }
    cmd = [k6_bin, "run", "--out", f"json={raw_output_path}", str(script)]
    proc = subprocess.Popen(cmd, env=env)  # noqa: S603
    returncode = proc.wait()
    if returncode != 0:
        print(f"warning: k6 exited with code {returncode}", file=sys.stderr)


def _analyze_raw_json(
    path: Path, configured_duration_seconds: float
) -> tuple[float | None, PhaseStats | None, PhaseStats | None]:
    """Returns (time_to_exhaustion_seconds, contested_stats, exhausted_stats)."""
    status_points: list[tuple[float, str]] = []  # (relative_time, category)
    duration_points: list[tuple[float, float]] = []  # (relative_time, value_ms)
    raw_times: list[float] = []

    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if obj.get("type") != "Point":
                continue
            data = obj.get("data", {})
            tags = data.get("tags") or {}
            if tags.get("phase") != "measured":
                continue
            metric = obj.get("metric")
            if metric not in _MEASURED_STATUS_METRICS and metric != "measured_duration_ms":
                continue
            t = _parse_k6_time(data["time"])
            raw_times.append(t)
            if metric in _MEASURED_STATUS_METRICS:
                status_points.append((t, metric))
            else:
                duration_points.append((t, data["value"]))

    if not raw_times:
        return None, None, None

    t0 = min(raw_times)
    status_points = [(t - t0, cat) for t, cat in status_points]
    duration_points = [(t - t0, v) for t, v in duration_points]

    success_times = [t for t, cat in status_points if cat == "status_2xx"]
    if not success_times:
        return None, None, None
    exhaustion_time = max(success_times)

    # Clean partition, no double-counting at the boundary: CONTESTED is
    # everything up to and including the last success; EXHAUSTED is
    # strictly after it. phase_duration for EXHAUSTED uses the
    # *configured* burst duration, not the last observed timestamp --
    # matching how the main sweep computes throughput (divide by the
    # configured duration, not an observed span, since k6's graceful stop
    # can let a few in-flight iterations finish slightly past the nominal
    # window).
    def _phase_stats(predicate, phase_duration: float) -> PhaseStats:
        counts = dict.fromkeys(_MEASURED_STATUS_METRICS, 0)
        for t, cat in status_points:
            if predicate(t):
                counts[cat] += 1
        total = sum(counts.values())
        durations = [v for t, v in duration_points if predicate(t)]
        return PhaseStats(
            duration_seconds=phase_duration,
            successes=counts["status_2xx"],
            expected_409s=counts["status_409"],
            unexpected_app_errors=counts["status_other"],
            transport_failures=counts["status_transport_error"],
            total_request_rps=(total / phase_duration) if phase_duration > 0 else None,
            p50_ms=_percentile(durations, 0.50),
            p95_ms=_percentile(durations, 0.95),
            p99_ms=_percentile(durations, 0.99),
        )

    contested = _phase_stats(lambda t: t <= exhaustion_time, exhaustion_time)
    exhausted = _phase_stats(
        lambda t: t > exhaustion_time, max(0.0, configured_duration_seconds - exhaustion_time)
    )
    return exhaustion_time, contested, exhausted


async def run_one_cell(
    *,
    k6_bin: str,
    strategy: str,
    ratio: int,
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
) -> ExhaustionDiagnostic:
    seat_count = max(1, round(vus / ratio))
    proc = rb.start_api(
        strategy=strategy,
        workers=workers,
        pool_size=pool_size,
        max_overflow=max_overflow,
        base_url=base_url,
        prometheus_multiproc_dir=prometheus_multiproc_dir,
    )
    try:
        label = f"{strategy}-ratio{ratio}-rep{repetition}"
        event_id, seat_ids = await rb.reset_and_seed(f"diagnose {label}", seat_count)
        raw_path = RESULTS_DIR / f"{run_id}-{label}-raw.json"
        run_k6_with_raw_json(
            k6_bin,
            base_url=base_url,
            event_id=event_id,
            seat_ids=seat_ids,
            vus=vus,
            duration=duration,
            warmup_vus=warmup_vus,
            warmup_duration=warmup_duration,
            raw_output_path=raw_path,
        )
        oversell_report = rb.fetch_oversell_report(base_url, event_id)
        exhaustion_time, contested, exhausted = _analyze_raw_json(raw_path, duration_seconds)
        return ExhaustionDiagnostic(
            strategy=strategy,
            contention_ratio_target=ratio,
            repetition=repetition,
            seat_count=seat_count,
            vus=vus,
            configured_duration_seconds=duration_seconds,
            time_to_exhaustion_seconds=exhaustion_time,
            time_to_exhaustion_fraction=(
                (exhaustion_time / duration_seconds) if exhaustion_time is not None else None
            ),
            oversold_seats=oversell_report.get("oversold_seats") or 0,
            excess_holders=oversell_report.get("excess_holders") or 0,
            contested=contested,
            exhausted=exhausted,
        )
    finally:
        rb.stop_api(proc)


def _fmt(value: float | None, digits: int = 1) -> str:
    return "—" if value is None else f"{value:.{digits}f}"


def render_markdown(results: dict[tuple[str, int], list[ExhaustionDiagnostic]]) -> str:
    lines = [
        "# Phase 3 exhaustion diagnostic (raw)",
        "",
        "Not the benchmark -- see loadtest/diagnose_exhaustion.py's module docstring for "
        "what this checks and why. naive excluded (see docstring).",
        "",
        "| Strategy | Ratio | Seats | Rep | Time to exhaustion (s) | Fraction of run | "
        "Contested: total req/s | Contested: p99 (ms) | Exhausted: total req/s | "
        "Exhausted: p99 (ms) |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for (strategy, ratio), runs in sorted(results.items()):
        for r in runs:
            c, e = r.contested, r.exhausted
            lines.append(
                f"| {strategy} | {ratio} | {r.seat_count} | {r.repetition} | "
                f"{_fmt(r.time_to_exhaustion_seconds, 3)} | "
                f"{_fmt(r.time_to_exhaustion_fraction, 3)} | "
                f"{_fmt(c.total_request_rps if c else None)} | "
                f"{_fmt(c.p99_ms if c else None)} | "
                f"{_fmt(e.total_request_rps if e else None)} | "
                f"{_fmt(e.p99_ms if e else None)} |"
            )
    return "\n".join(lines) + "\n"


async def main_async(args: argparse.Namespace) -> None:
    import shutil

    from app.infra.config import settings

    strategies = ["pessimistic", "optimistic"]
    contention_ratios = sorted(
        int(r.strip()) for r in args.contention_ratios.split(",") if r.strip()
    )

    k6_bin = shutil.which(args.k6_bin)
    if k6_bin is None:
        raise SystemExit(f"'{args.k6_bin}' not found on PATH.")

    duration_seconds = rb._parse_duration_seconds(args.duration)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-diag"

    results: dict[tuple[str, int], list[ExhaustionDiagnostic]] = {}
    total_cells = len(strategies) * len(contention_ratios) * args.reps
    cell_number = 0
    for ratio in contention_ratios:
        for rep in range(1, args.reps + 1):
            for strategy in strategies:
                cell_number += 1
                print(
                    f"--- diag cell {cell_number}/{total_cells}: {strategy} "
                    f"ratio={ratio} rep={rep} ---"
                )
                result = await run_one_cell(
                    k6_bin=k6_bin,
                    strategy=strategy,
                    ratio=ratio,
                    repetition=rep,
                    vus=args.vus,
                    duration=args.duration,
                    duration_seconds=duration_seconds,
                    warmup_vus=args.warmup_vus,
                    warmup_duration=args.warmup_duration,
                    workers=args.workers,
                    pool_size=settings.pool_size,
                    max_overflow=settings.max_overflow,
                    base_url=args.base_url,
                    prometheus_multiproc_dir=settings.prometheus_multiproc_dir,
                    run_id=run_id,
                )
                results.setdefault((strategy, ratio), []).append(result)

    json_path = RESULTS_DIR / f"{run_id}.json"
    json_path.write_text(
        json.dumps(
            {f"{s}|{r}": [asdict(x) for x in runs] for (s, r), runs in results.items()}, indent=2
        ),
        encoding="utf-8",
    )
    md_path = RESULTS_DIR / f"{run_id}.md"
    rendered = render_markdown(results)
    md_path.write_text(rendered, encoding="utf-8")

    print(f"\nWrote {json_path}")
    print(f"Wrote {md_path}")
    print()
    print(rendered)


def main() -> None:
    import asyncio

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contention-ratios", default="2,5,10,20,50,100")
    parser.add_argument("--reps", type=int, default=1)
    parser.add_argument("--vus", type=int, default=200)
    parser.add_argument("--duration", default="10s")
    parser.add_argument("--warmup-vus", type=int, default=20)
    parser.add_argument("--warmup-duration", default="5s")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--k6-bin", default="k6")
    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
