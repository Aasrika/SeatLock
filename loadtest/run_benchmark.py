"""Orchestrates a full naive-strategy benchmark run.

For each of N repetitions (default 5 -- a single run proving nothing is
the failure mode SPEC.md warns about):

    1. Reset the database and seed a fresh --contention (10-seat) event.
    2. Start a k6 scenario (last_seat.js or flash_sale.js) against it.
    3. While k6 runs, poll GET /api/admin/invariants every 500ms and
       record any violation with a timestamp -- catching a violation that
       self-heals before the run ends is the point (SPEC.md section 10).
    4. On completion, pull GET /api/admin/oversell-report.

Then writes loadtest/results/<timestamp>.json (raw per-run + aggregate)
and loadtest/results/<timestamp>.md (a markdown table, README-ready).

Assumes the API is already running (see `make run-api`) and reachable at
--base-url; this script does not start it. Requires the `k6` binary on
PATH.

Usage:
    python -m loadtest.run_benchmark
    python -m loadtest.run_benchmark --scenario flash_sale --runs 10
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
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


@dataclass
class RunResult:
    run: int
    oversell_count: int
    invariant_violations: list[dict[str, Any]] = field(default_factory=list)
    p50_ms: float | None = None
    p95_ms: float | None = None
    p99_ms: float | None = None
    throughput_rps: float | None = None
    error_rate: float | None = None


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
    vus: int | None,
    duration: str | None,
    summary_path: Path,
) -> list[dict[str, Any]]:
    script = SCENARIOS[scenario]
    env: dict[str, str] = {
        "BASE_URL": base_url,
        "EVENT_ID": str(event_id),
    }
    if scenario == "last_seat":
        env["SEAT_ID"] = str(seat_ids[0])
    else:
        env["SEAT_IDS"] = ",".join(str(s) for s in seat_ids)
    if vus is not None:
        env["VUS"] = str(vus)
    if duration is not None:
        env["DURATION"] = duration

    full_env = {**os.environ, **env}
    cmd = [k6_bin, "run", f"--summary-export={summary_path}", str(script)]
    proc = subprocess.Popen(cmd, env=full_env)  # noqa: S603
    violations = poll_invariants_until_done(proc, base_url, event_id)
    returncode = proc.wait()
    if returncode != 0:
        print(f"warning: k6 exited with code {returncode}", file=sys.stderr)
    return violations


def parse_k6_summary(summary_path: Path) -> dict[str, float | None]:
    data = json.loads(summary_path.read_text(encoding="utf-8"))
    metrics = data.get("metrics", {})

    def value(metric: str, key: str) -> float | None:
        return metrics.get(metric, {}).get("values", {}).get(key)

    total_2xx = value("status_2xx", "count") or 0
    total_409 = value("status_409", "count") or 0
    total_other = value("status_other", "count") or 0
    total = total_2xx + total_409 + total_other

    return {
        "p50_ms": value("http_req_duration", "med"),
        "p95_ms": value("http_req_duration", "p(95)"),
        "p99_ms": value("http_req_duration", "p(99)"),
        "throughput_rps": value("http_reqs", "rate"),
        "error_rate": (total_other / total) if total else None,
    }


def fetch_oversell_report(base_url: str, event_id: int) -> dict[str, Any]:
    result = _http_get_json(f"{base_url}/api/admin/oversell-report?event_id={event_id}")
    return result or {"total_oversell_count": None, "seats": []}


def aggregate(runs: list[RunResult]) -> dict[str, Any]:
    def mean(values: list[float]) -> float | None:
        return statistics.mean(values) if values else None

    p50s = [r.p50_ms for r in runs if r.p50_ms is not None]
    p95s = [r.p95_ms for r in runs if r.p95_ms is not None]
    p99s = [r.p99_ms for r in runs if r.p99_ms is not None]
    throughputs = [r.throughput_rps for r in runs if r.throughput_rps is not None]
    error_rates = [r.error_rate for r in runs if r.error_rate is not None]

    return {
        "total_oversells": sum(r.oversell_count for r in runs),
        "runs_with_oversell": sum(1 for r in runs if r.oversell_count > 0),
        "runs_with_invariant_violation": sum(1 for r in runs if r.invariant_violations),
        "p50_ms_mean": mean(p50s),
        "p95_ms_mean": mean(p95s),
        "p99_ms_mean": mean(p99s),
        "throughput_rps_mean": mean(throughputs),
        "error_rate_mean": mean(error_rates),
    }


def _fmt(value: float | None, digits: int = 1) -> str:
    return "—" if value is None else f"{value:.{digits}f}"


def render_markdown(
    strategy: str, scenario: str, runs: list[RunResult], agg: dict[str, Any]
) -> str:
    lines = [
        f"# Benchmark: {strategy} strategy, {scenario} scenario",
        "",
        "| Run | Oversells | Invariant violations | Throughput (req/s) | p50 (ms) | "
        "p95 (ms) | p99 (ms) | Retry rate | Error rate |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for r in runs:
        lines.append(
            f"| {r.run} | {r.oversell_count} | {len(r.invariant_violations)} | "
            f"{_fmt(r.throughput_rps)} | {_fmt(r.p50_ms)} | {_fmt(r.p95_ms)} | "
            f"{_fmt(r.p99_ms)} | — | {_fmt(r.error_rate, 3)} |"
        )
    lines.append(
        f"| **Aggregate** | **{agg['total_oversells']}** | "
        f"**{agg['runs_with_invariant_violation']}** | "
        f"**{_fmt(agg['throughput_rps_mean'])}** | **{_fmt(agg['p50_ms_mean'])}** | "
        f"**{_fmt(agg['p95_ms_mean'])}** | **{_fmt(agg['p99_ms_mean'])}** | — | "
        f"**{_fmt(agg['error_rate_mean'], 3)}** |"
    )
    lines.append("")
    lines.append(
        f"{agg['runs_with_oversell']}/{len(runs)} runs produced at least one oversold seat; "
        f"{agg['runs_with_invariant_violation']}/{len(runs)} runs recorded at least one "
        "invariant violation while the load was running."
    )
    return "\n".join(lines) + "\n"


async def main_async(args: argparse.Namespace) -> None:
    check_api_is_up(args.base_url)

    k6_bin = shutil.which(args.k6_bin)
    if k6_bin is None:
        raise SystemExit(
            f"'{args.k6_bin}' not found on PATH. Install k6 "
            "(https://k6.io/docs/get-started/installation/) and retry."
        )

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

    runs: list[RunResult] = []
    for run_number in range(1, args.runs + 1):
        print(f"--- run {run_number}/{args.runs} ---")
        event_id, seat_ids = await reset_and_seed(f"Benchmark run {run_number}")

        summary_path = RESULTS_DIR / f"{run_id}-run{run_number}-k6.json"
        violations = run_k6(
            k6_bin,
            args.scenario,
            base_url=args.base_url,
            event_id=event_id,
            seat_ids=seat_ids,
            vus=args.vus,
            duration=args.duration,
            summary_path=summary_path,
        )

        k6_metrics = parse_k6_summary(summary_path)
        oversell_report = fetch_oversell_report(args.base_url, event_id)

        runs.append(
            RunResult(
                run=run_number,
                oversell_count=oversell_report.get("total_oversell_count") or 0,
                invariant_violations=violations,
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
    md_path.write_text(
        render_markdown(settings.strategy, args.scenario, runs, agg), encoding="utf-8"
    )

    print(f"\nWrote {json_path}")
    print(f"Wrote {md_path}")
    print()
    print(render_markdown(settings.strategy, args.scenario, runs, agg))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=5, help="Number of repetitions.")
    parser.add_argument(
        "--scenario", choices=sorted(SCENARIOS), default="last_seat", help="k6 scenario to run."
    )
    parser.add_argument("--base-url", default="http://localhost:8000", help="Running API base URL.")
    parser.add_argument(
        "--vus", type=int, default=None, help="Override the scenario's default VUs."
    )
    parser.add_argument(
        "--duration", default=None, help="Override the scenario's default duration."
    )
    parser.add_argument("--k6-bin", default="k6", help="k6 executable name or path.")
    args = parser.parse_args()

    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
