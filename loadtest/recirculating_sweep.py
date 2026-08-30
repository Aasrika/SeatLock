"""Phase 3 recirculating-contention sweep -- the corrected replacement
for the coarse sweep's acquire-only design.

Why this exists: the original coarse sweep (loadtest/run_benchmark.py
--sweep) found "no crossover through ratio 100," but
loadtest/diagnose_exhaustion.py showed that finding was dominated by a
workload artifact -- an acquire-only burst against a small fixed seat
pool exhausts almost immediately, so >=90% of every measured window was
spent in post-exhaustion REJECTION cost (cheaper for optimistic, which
takes no lock, than for pessimistic, which locks, discovers unavailable,
then releases), not genuine contention. This sweep fixes that by using
short holds (Settings-overriding HOLD_DURATION_SECONDS) plus
workers/sweeper_worker.py (Settings-overriding SWEEPER_INTERVAL_SECONDS)
so inventory recirculates for the WHOLE measured window instead of
exhausting once.

VALIDITY THRESHOLD -- set here, in advance, before this sweep's results
exist: a (strategy, ratio) cell counts toward the crossover analysis only
if its measured fraction_available (fraction of the measured window with
>= 1 seat AVAILABLE, averaged across repetitions) is >= VALIDITY_THRESHOLD
(0.6). Deciding this before running is what stops the criterion being
fitted to whichever ratios happen to produce an interesting answer.
Every cell's measured fraction_available is recorded and reported
regardless of whether it clears the bar -- excluded cells are visible,
with the reason, never silently dropped.

SEAT FLOOR -- confirmed empirically (see the pilot results in
conversation) that below ~10 seats, inventory can't sustain a
persistently-contested state regardless of hold duration or sweeper
interval tuning: with only 2-4 units of inventory, "available" is an
inherently narrow, transient target under real load. Ratios that would
need fewer than SEAT_FLOOR seats at --base-vus instead get MORE VUs
(compute_seat_count_and_vus, loadtest/recirculating_pilot.py), preserving
the exact target ratio rather than shrinking inventory further. Where
that hits the client-side ceiling documented in the Phase 1 connection-
refused investigation (confirmed directly: ratio 100 at 10 seats / 1000
VUs produced 1800-2400 transport failures per run; ratio 50 at 10 seats /
500 VUs produced zero), that is a MEASUREMENT LIMIT to document, not a
result to route around by shrinking seats.

STEADY-STATE METRICS -- the first TRANSIENT_SECONDS of every measured
phase are discarded before computing throughput/p50/p95/p99. This value
was determined empirically, not picked: the pilot's fine-grained
available-count samples showed the initial full seat pool (seeded
AVAILABLE at t=0) gets completely consumed for the first time
0.80-0.94s into the measured phase at ratios 5 and 10 (ratio 2's pool
never fully exhausted in 15s, consistent with near-total availability
throughout -- there is no distinct transient to discard there either).
1.0s (a small margin above the observed range) is used as a single,
ratio-independent cutoff applied uniformly, rather than a per-ratio value
-- picking a different cutoff per ratio after seeing each ratio's own
data would reopen exactly the "fitted after the fact" problem the
validity threshold exists to avoid.

Interleaved per ratio (cycle through strategies, repeat the cycle) -- see
loadtest/run_benchmark.py's sweep section for why: machine drift over the
sweep's wall-clock duration must not correlate with which strategy runs
later.

Minimum 3 repetitions per cell, enforced -- a single run cannot support
any claim.

Usage:
    python -m loadtest.recirculating_sweep
    python -m loadtest.recirculating_sweep --ratios 2,5,10 --reps 3
"""

from __future__ import annotations

import argparse
import asyncio
import os
import statistics
import subprocess
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.infra.config import settings
from app.infra.db import async_session_factory
from app.infra.tables import HoldAuditRow, SeatRow
from loadtest import diagnose_exhaustion as de
from loadtest import recirculating_pilot as rp
from loadtest import run_benchmark as rb

RESULTS_DIR = rb.RESULTS_DIR


async def check_recirculating_oversell(
    event_id: int, hold_duration_seconds: float, tolerance_seconds: float
) -> tuple[int, int]:
    """Genuine, TIME-AWARE oversell check -- NOT rb.fetch_oversell_report.

    That endpoint's "more than one distinct holder ever recorded for a
    seat" definition is only valid for an ACQUIRE-ONCE workload, where a
    seat, once won, is never released again -- true of every prior
    benchmark (Phase 1/2, the original coarse sweep). In a recirculating
    workload, the SAME seat is legitimately held by many DIFFERENT
    sessions over one run, sequentially, as holds expire and get
    reclaimed -- confirmed directly: an early smoke test of this script
    reported pessimistic "oversold_seats=30" using that endpoint, which
    is structurally impossible for a strategy that never oversells. A
    GENUINE oversell (I1 violation) here means two DIFFERENT sessions'
    hold windows actually OVERLAPPED in time, not merely that a seat had
    more than one holder across the whole run.

    hold_audit records only acquired_at (not hold_expires_at) per entry,
    but every hold in one cell used the SAME hold_duration_seconds (fixed
    for that cell's whole run) -- so hold i's window is
    [acquired_at_i, acquired_at_i + hold_duration_seconds), and it
    overlaps the NEXT hold on the same seat when
    acquired_at_{i+1} < acquired_at_i + hold_duration_seconds - TOLERANCE.

    tolerance_seconds exists because acquired_at is NOT the moment a
    hold's row write actually committed -- app/api/routes/booking.py
    captures `now` at the very top of the route handler, BEFORE
    strategy.acquire() does any database I/O. Confirmed directly (this
    bug was caught building this function): a fresh single-cell run
    showed pessimistic -- which cannot oversell by construction -- with 3
    "overlaps," all with gaps of 0.975-0.993s against a 1.0s
    hold_duration. Mechanism: a request's `now` is captured before its DB
    round-trip; if the sweeper (or another strategy path) flips a seat to
    AVAILABLE WHILE that request is still in flight (pool checkout, query
    execution), the request's actual read/write correctly sees AVAILABLE
    and succeeds, but its recorded acquired_at can trail the true reclaim
    moment by however long ITS OWN processing took.

    That "however long" is NOT a fixed number -- this project's own
    benchmarks have shown p99 request latency ranging from tens to
    thousands of ms depending on contention, and hold_duration_seconds is
    only ~1s, so a fixed tolerance risks being too small under heavy
    contention (missing this artifact, flagging false "overlaps") or too
    large under light contention (masking a genuine one). The caller
    passes this cell's OWN observed steady-state p99 latency
    (RecirculatingRunResult.steady.p99_ms / 1000) as tolerance_seconds --
    a direct, data-driven bound on how long this specific cell's own
    request processing actually took, rather than a guessed constant.

    Returns (overlapping_seats, overlap_count) -- overlapping_seats is
    how many distinct seats had at least one genuine overlap;
    overlap_count is the total number of overlapping pairs found, the
    magnitude measure (analogous to excess_holders elsewhere in this
    project, just overlap-based instead of distinct-holder-based).
    """
    async with async_session_factory() as session:
        rows = (
            await session.execute(
                select(HoldAuditRow.seat_id, HoldAuditRow.acquired_at)
                .join(SeatRow, SeatRow.id == HoldAuditRow.seat_id)
                .where(SeatRow.event_id == event_id)
                .order_by(HoldAuditRow.seat_id, HoldAuditRow.acquired_at)
            )
        ).all()

    hold_delta = timedelta(seconds=max(0.0, hold_duration_seconds - tolerance_seconds))
    by_seat: dict[int, list[datetime]] = {}
    for seat_id, acquired_at in rows:
        by_seat.setdefault(seat_id, []).append(acquired_at)

    overlapping_seats = 0
    overlap_count = 0
    for times in by_seat.values():
        seat_had_overlap = False
        for prev, curr in zip(times, times[1:], strict=False):
            if curr < prev + hold_delta:
                overlap_count += 1
                seat_had_overlap = True
        if seat_had_overlap:
            overlapping_seats += 1

    return overlapping_seats, overlap_count


def _run_k6_raw_json_blocking(
    k6_bin: str,
    *,
    event_id: int,
    seat_ids: list[int],
    vus: int,
    duration: str,
    warmup_vus: int,
    warmup_duration: str,
    base_url: str,
    raw_output_path: Any,
) -> None:
    """Blocking (run via loop.run_in_executor, see run_recirculating_cell)
    -- writes k6's raw per-request JSON output (`--out json=`), NOT the
    aggregate handleSummary() output recirculating_pilot.py's
    _run_k6_blocking writes. Steady-state throughput/p50/p95/p99 need
    per-request timestamps to window out the discarded transient
    (de.parse_raw_k6_points / de.phase_stats_over) -- the aggregate
    summary can't be split by time window at all.
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
    }
    subprocess.run(  # noqa: S603
        [k6_bin, "run", "--out", f"json={raw_output_path}", str(rb.SCENARIOS["contention_sweep"])],
        env=env,
        check=False,
    )


VALIDITY_THRESHOLD = 0.6
SEAT_FLOOR = 10
TRANSIENT_SECONDS = 1.0


@dataclass
class RecirculatingRunResult:
    strategy: str
    contention_ratio_target: int
    repetition: int
    started_at: str
    seat_count: int
    vus: int
    hold_duration_seconds: float
    sweeper_interval_seconds: float
    sweeper_batch_size: int
    duration_seconds: float
    transient_seconds: float
    fraction_available: float | None
    recirculation_cycles: int | None
    steady: de.PhaseStats | None
    # From check_recirculating_oversell -- overlap-based, NOT
    # rb.fetch_oversell_report's distinct-holders-ever count, which is
    # meaningless once seats legitimately get held by different sessions
    # sequentially over the course of one run. See that function's
    # docstring.
    oversold_seats: int = 0
    excess_holders: int = 0
    lock_wait_p99_ms: float | None = None
    optimistic_conflicts: float | None = None
    optimistic_retries: float | None = None
    optimistic_attempts_mean: float | None = None
    sweeper_seats_expired_total: float | None = None
    sweeper_batch_duration_seconds_sum: float | None = None
    sweeper_share_of_run: float | None = None


async def run_recirculating_cell(
    *,
    k6_bin: str,
    strategy: str,
    ratio: int,
    repetition: int,
    base_vus: int,
    seat_floor: int,
    duration: str,
    duration_seconds: float,
    warmup_vus: int,
    warmup_duration: str,
    warmup_duration_seconds: float,
    hold_duration_seconds: float,
    sweeper_interval_seconds: float,
    sweeper_batch_size: int,
    transient_seconds: float,
    poll_interval_seconds: float,
    workers: int,
    pool_size: int,
    max_overflow: int,
    base_url: str,
    prometheus_multiproc_dir: str,
    run_id: str,
) -> RecirculatingRunResult:
    seat_count, vus = rp.compute_seat_count_and_vus(ratio, base_vus, seat_floor)
    started_at = datetime.now(UTC).isoformat()

    api_proc = rb.start_api(
        strategy=strategy,
        workers=workers,
        pool_size=pool_size,
        max_overflow=max_overflow,
        base_url=base_url,
        prometheus_multiproc_dir=prometheus_multiproc_dir,
        hold_duration_seconds=hold_duration_seconds,
    )
    sweeper_proc = rp.start_sweeper(
        interval_seconds=sweeper_interval_seconds,
        batch_size=sweeper_batch_size,
        prometheus_multiproc_dir=prometheus_multiproc_dir,
    )
    poll_engine = create_async_engine(settings.database_url, pool_size=2, max_overflow=2)
    poll_session_factory = async_sessionmaker(bind=poll_engine, expire_on_commit=False)
    try:
        label = f"{strategy}-ratio{ratio}-rep{repetition}"
        event_id, seat_ids = await rb.reset_and_seed(f"recirc {label}", seat_count)

        raw_path = RESULTS_DIR / f"{run_id}-{label}-raw.json"
        loop = asyncio.get_running_loop()
        k6_future = loop.run_in_executor(
            None,
            lambda: _run_k6_raw_json_blocking(
                k6_bin,
                event_id=event_id,
                seat_ids=seat_ids,
                vus=vus,
                duration=duration,
                warmup_vus=warmup_vus,
                warmup_duration=warmup_duration,
                base_url=base_url,
                raw_output_path=raw_path,
            ),
        )
        samples = await rp.poll_available_count_async(
            poll_session_factory, event_id, poll_interval_seconds, k6_future.done
        )
        await k6_future

        recirc = rp._analyze_recirculation(
            samples,
            window_start_seconds=warmup_duration_seconds,
            window_end_seconds=warmup_duration_seconds + duration_seconds,
        )

        status_points, duration_points = de.parse_raw_k6_points(raw_path)
        steady_state_duration = max(0.0, duration_seconds - transient_seconds)
        steady = de.phase_stats_over(
            status_points, duration_points, lambda t: t >= transient_seconds, steady_state_duration
        )

        # Fall back to hold_duration_seconds itself (a generous bound)
        # if no steady-state requests were observed to derive a p99 from
        # -- better than an arbitrary small constant that could
        # misclassify a genuine overlap as noise, or vice versa.
        tolerance_seconds = (
            steady.p99_ms / 1000 if steady.p99_ms is not None else hold_duration_seconds
        )
        oversold_seats, overlap_count = await check_recirculating_oversell(
            event_id, hold_duration_seconds, tolerance_seconds
        )
        metrics_text = rb._fetch_metrics_text(base_url)

        lock_wait_p99_ms = None
        optimistic_conflicts = optimistic_retries = optimistic_attempts_mean = None
        sweeper_expired = sweeper_batch_sum = None
        if metrics_text is not None:
            if strategy == "pessimistic":
                p99 = rb._parse_histogram_p99_seconds(metrics_text, "lock_wait_seconds")
                lock_wait_p99_ms = None if p99 is None else p99 * 1000
            elif strategy == "optimistic":
                optimistic_conflicts = rb._parse_counter_value(
                    metrics_text, "optimistic_conflicts_total"
                )
                optimistic_retries = rb._parse_counter_value(
                    metrics_text, "optimistic_retries_total"
                )
                att_sum, att_count = rb._parse_histogram_sum_count(
                    metrics_text, "optimistic_attempts"
                )
                optimistic_attempts_mean = (att_sum / att_count) if att_count else None
            sweeper_expired = rb._parse_counter_value(metrics_text, "sweeper_seats_expired_total")
            sweeper_batch_sum, _ = rb._parse_histogram_sum_count(
                metrics_text, "sweeper_batch_duration_seconds"
            )

        return RecirculatingRunResult(
            strategy=strategy,
            contention_ratio_target=ratio,
            repetition=repetition,
            started_at=started_at,
            seat_count=seat_count,
            vus=vus,
            hold_duration_seconds=hold_duration_seconds,
            sweeper_interval_seconds=sweeper_interval_seconds,
            sweeper_batch_size=sweeper_batch_size,
            duration_seconds=duration_seconds,
            transient_seconds=transient_seconds,
            fraction_available=recirc["fraction_with_available_seat"],
            recirculation_cycles=recirc["recirculation_cycles"],
            steady=steady,
            oversold_seats=oversold_seats,
            excess_holders=overlap_count,
            lock_wait_p99_ms=lock_wait_p99_ms,
            optimistic_conflicts=optimistic_conflicts,
            optimistic_retries=optimistic_retries,
            optimistic_attempts_mean=optimistic_attempts_mean,
            sweeper_seats_expired_total=sweeper_expired,
            sweeper_batch_duration_seconds_sum=sweeper_batch_sum,
            sweeper_share_of_run=(
                (sweeper_batch_sum / duration_seconds)
                if sweeper_batch_sum is not None and duration_seconds
                else None
            ),
        )
    finally:
        await poll_engine.dispose()
        rb.stop_api(sweeper_proc)
        rb.stop_api(api_proc)


async def run_recirculating_matrix(
    *, k6_bin: str, strategies: list[str], ratios: list[int], reps: int, **common: Any
) -> dict[tuple[str, int], list[RecirculatingRunResult]]:
    """Interleaved: for each ratio, cycle through strategies, then repeat
    the cycle, before moving to the next ratio -- see module docstring.
    """
    results: dict[tuple[str, int], list[RecirculatingRunResult]] = {
        (s, r): [] for s in strategies for r in ratios
    }
    total_cells = len(strategies) * len(ratios) * reps
    n = 0
    for ratio in ratios:
        for rep in range(1, reps + 1):
            for strategy in strategies:
                n += 1
                print(f"--- recirc cell {n}/{total_cells}: {strategy} ratio={ratio} rep={rep} ---")
                result = await run_recirculating_cell(
                    k6_bin=k6_bin, strategy=strategy, ratio=ratio, repetition=rep, **common
                )
                results[(strategy, ratio)].append(result)
    return results


def cell_mean_fraction_available(runs: list[RecirculatingRunResult]) -> float | None:
    values = [r.fraction_available for r in runs if r.fraction_available is not None]
    return statistics.mean(values) if values else None


def cell_included(runs: list[RecirculatingRunResult]) -> tuple[bool, str | None]:
    """A cell counts toward the crossover analysis only if BOTH: its mean
    fraction_available across repetitions clears VALIDITY_THRESHOLD, AND
    no repetition saw transport failures in its steady-state window (a
    client-side ceiling invalidates the measurement regardless of how
    available inventory looked).
    """
    total_transport_failures = sum(
        r.steady.transport_failures for r in runs if r.steady is not None
    )
    if total_transport_failures > 0:
        return False, f"transport_failures={total_transport_failures} (client-side ceiling)"
    mean_fraction = cell_mean_fraction_available(runs)
    if mean_fraction is None:
        return False, "no recirculation samples collected"
    if mean_fraction < VALIDITY_THRESHOLD:
        return (
            False,
            f"mean fraction_available {mean_fraction:.3f} < {VALIDITY_THRESHOLD} threshold",
        )
    return True, None


def _fmt(value: float | None, digits: int = 1) -> str:
    return "—" if value is None else f"{value:.{digits}f}"


def render_markdown(
    results: dict[tuple[str, int], list[RecirculatingRunResult]],
    strategies: list[str],
    ratios: list[int],
) -> str:
    lines = [
        "# Phase 3 recirculating-contention sweep -- raw data tables",
        "",
        f"Validity threshold: mean fraction_available >= {VALIDITY_THRESHOLD}, decided before "
        "this sweep ran. Transient discarded: "
        f"{TRANSIENT_SECONDS}s (empirically determined -- see module docstring). Seat floor: "
        f"{SEAT_FLOOR}. Oversold/overlap counts are TIME-AWARE (genuine concurrent double-holds "
        "via check_recirculating_oversell), not the distinct-holders-ever count "
        "GET /api/admin/oversell-report uses elsewhere -- that definition doesn't apply once "
        "seats legitimately recirculate within one run.",
        "",
        "| Strategy | Ratio | Seats | VUs | Included | Exclusion reason | Mean "
        "fraction_available | Steady-state total req/s (mean) | p99 (ms, mean) | Overlapping "
        "seats (total) | Overlap count (total) | Sweeper share of run (mean) | Reps |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for ratio in ratios:
        for strategy in strategies:
            runs = results.get((strategy, ratio), [])
            if not runs:
                continue
            included, reason = cell_included(runs)
            mean_fraction = cell_mean_fraction_available(runs)
            total_req = [r.steady.total_request_rps for r in runs if r.steady is not None]
            p99s = [r.steady.p99_ms for r in runs if r.steady is not None and r.steady.p99_ms]
            sweeper_shares = [
                r.sweeper_share_of_run for r in runs if r.sweeper_share_of_run is not None
            ]
            total_oversold = sum(r.oversold_seats for r in runs)
            total_excess = sum(r.excess_holders for r in runs)
            lines.append(
                f"| {strategy} | {ratio} | {runs[0].seat_count} | {runs[0].vus} | "
                f"{'YES' if included else 'NO'} | {reason or '—'} | {_fmt(mean_fraction, 3)} | "
                f"{_fmt(statistics.mean(total_req)) if total_req else '—'} | "
                f"{_fmt(statistics.mean(p99s)) if p99s else '—'} | {total_oversold} | "
                f"{total_excess} | "
                f"{_fmt(statistics.mean(sweeper_shares), 3) if sweeper_shares else '—'} | "
                f"{len(runs)} |"
            )
    return "\n".join(lines) + "\n"


async def main_async(args: argparse.Namespace) -> None:
    import json
    import shutil

    ratios = sorted(int(r.strip()) for r in args.ratios.split(",") if r.strip())
    strategies = [s.strip() for s in args.strategies.split(",") if s.strip()]
    k6_bin = shutil.which(args.k6_bin)
    if k6_bin is None:
        raise SystemExit(f"'{args.k6_bin}' not found on PATH.")
    if args.reps < 3:
        raise SystemExit("--reps must be >= 3 -- a single run per cell cannot support any claim.")

    duration_seconds = rb._parse_duration_seconds(args.duration)
    warmup_duration_seconds = rb._parse_duration_seconds(args.warmup_duration)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-recirc"

    common = dict(
        base_vus=args.vus,
        seat_floor=args.seat_floor,
        duration=args.duration,
        duration_seconds=duration_seconds,
        warmup_vus=args.warmup_vus,
        warmup_duration=args.warmup_duration,
        warmup_duration_seconds=warmup_duration_seconds,
        hold_duration_seconds=args.hold_duration_seconds,
        sweeper_interval_seconds=args.sweeper_interval_seconds,
        sweeper_batch_size=settings.sweeper_batch_size,
        transient_seconds=args.transient_seconds,
        poll_interval_seconds=args.poll_interval,
        workers=args.workers,
        pool_size=settings.pool_size,
        max_overflow=settings.max_overflow,
        base_url=args.base_url,
        prometheus_multiproc_dir=settings.prometheus_multiproc_dir,
        run_id=run_id,
    )

    print(f"=== recirculating sweep: strategies={strategies} ratios={ratios} reps={args.reps} ===")
    results = await run_recirculating_matrix(
        k6_bin=k6_bin, strategies=strategies, ratios=ratios, reps=args.reps, **common
    )

    def _serialize(d: dict) -> dict:
        return {"|".join(str(p) for p in key): [asdict(r) for r in runs] for key, runs in d.items()}

    output = {
        "strategies": strategies,
        "ratios": ratios,
        "validity_threshold": VALIDITY_THRESHOLD,
        "seat_floor": args.seat_floor,
        "transient_seconds": args.transient_seconds,
        "results": _serialize(results),
    }
    json_path = RESULTS_DIR / f"{run_id}.json"
    json_path.write_text(json.dumps(output, indent=2), encoding="utf-8")

    md_path = RESULTS_DIR / f"{run_id}-summary.md"
    rendered = render_markdown(results, strategies, ratios)
    md_path.write_text(rendered, encoding="utf-8")

    print(f"\nWrote {json_path}")
    print(f"Wrote {md_path}")
    print()
    print(rendered)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ratios", default="2,5,10,20")
    parser.add_argument("--strategies", default="naive,pessimistic,optimistic")
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--vus", type=int, default=200, help="Base VU count.")
    parser.add_argument("--seat-floor", type=int, default=SEAT_FLOOR)
    parser.add_argument("--duration", default="15s")
    parser.add_argument("--warmup-vus", type=int, default=10)
    parser.add_argument("--warmup-duration", default="3s")
    parser.add_argument("--hold-duration-seconds", type=float, default=1.0)
    parser.add_argument("--sweeper-interval-seconds", type=float, default=0.1)
    parser.add_argument("--transient-seconds", type=float, default=TRANSIENT_SECONDS)
    parser.add_argument("--poll-interval", type=float, default=0.05)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--k6-bin", default="k6")
    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
