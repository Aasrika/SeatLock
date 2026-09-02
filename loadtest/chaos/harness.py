"""The chaos-scenario engine (Phase 8a, SPEC.md section 10 Layer 5).

Every scenario in loadtest/chaos/scenarios/ follows the same five-step
discipline and the enforcement lives HERE, not copy-pasted per scenario:

    1. STEADY STATE  -- load runs untouched for a while before injection.
    2. HYPOTHESIS    -- stated in the calling scenario's own docstring;
                         this module has no opinion on what SHOULD happen,
                         only on recording what DID.
    3. INJECT        -- `inject()` is called at `inject_at_s` (measured
                         from when the k6 process starts, warmup included
                         -- so scenarios must give load time to ramp up
                         before that offset).
    4. ASSERT         -- `/api/admin/invariants` is polled every
                         `poll_interval_seconds` for the ENTIRE run
                         (steady state through post-recovery), not just at
                         the end. A single failing poll is recorded in
                         `invariant_violations` even if every later poll
                         passes -- SPEC.md section 10: "catching a
                         violation that self-heals before the test
                         finishes is exactly the kind of bug that reaches
                         production otherwise." Scenario scripts treat any
                         non-empty `invariant_violations` as a hard
                         failure (see their own assertions).
    5. RECOVER        -- `recover()` is called at `recover_at_s`, and
                         polling continues for `post_recover_poll_seconds`
                         AFTER k6 itself exits, so the scenario has data
                         to assert "returned to steady state within a
                         bounded time" against, not just "load stopped."

HTTP outcome categories come from k6's own `--out json=<path>` stream
(k6 tags every `http_reqs` sample with `status` and, because
steady_load.js sets it explicitly, `name` in {hold, booking_create,
confirm}) -- this is what lets the timeline show *booking* throughput
specifically, not just "requests per second of any kind," without
reinventing per-request instrumentation k6 already provides. Parsed in
full, once, after k6 exits -- see _parse_k6_outcome_events's docstring
for why NOT as a live tail during the run.
"""

from __future__ import annotations

import asyncio
import bisect
import json
import os
import subprocess
import sys
import threading
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypeVar

from loadtest.run_benchmark import _fetch_metrics_text, _http_get_json, _parse_counter_value

CHAOS_DIR = Path(__file__).resolve().parent
RESULTS_DIR = CHAOS_DIR / "results"
STEADY_LOAD_SCRIPT = CHAOS_DIR / "steady_load.js"


# loadtest.run_benchmark's _parse_counter_value is documented (its own
# docstring) as being for a LABEL-LESS Counter only: it matches an exact
# "<name> " line prefix, which never appears for a labeled series --
# those render as "<name>{label="value"} <number>", with "{" immediately
# after the name, no space. Every counter this harness actually reads
# (hold_cache_errors_total's "operation" label, reconciliation_
# divergence_total's "kind" label) IS labeled -- reusing that parser
# directly here would silently read 0.0 for both, always, regardless of
# the real value (confirmed directly while building this harness: it
# did exactly that). _parse_counter_total sums across every label
# combination instead, which is what "did this counter rise" needs
# here -- this harness is asking a yes/it-rose-by-N question per
# scenario, not reporting a per-label breakdown.
def _parse_counter_total(metrics_text: str, metric_name: str) -> float:
    total = 0.0
    for line in metrics_text.splitlines():
        if line.startswith(f"{metric_name}{{") or line.startswith(f"{metric_name} "):
            total += float(line.rsplit(" ", 1)[1])
    return total


# Gauges this harness reads (sweeper_backlog, both used via gauge_names=)
# are label-less, so run_benchmark's exact-prefix parser is correct
# as-is -- named separately here only so callers don't have to know
# that distinction.
_parse_gauge_value = _parse_counter_value

_T = TypeVar("_T")


def run_sync_in_thread(coro_fn: Callable[[], Awaitable[_T]]) -> _T:
    """Run an async callable to completion from plain sync code that may
    ITSELF already be executing inside a running event loop --
    run_all.py's async_main() is one long-lived loop for the whole
    suite (see its own comment on why), and every scenario's inject()/
    recover() is called synchronously from within it. asyncio.run()
    would raise "cannot be called from a running event loop" if called
    directly here; a dedicated thread gets coro_fn a genuinely fresh
    loop of its own (asyncio.run() is fine there), and .join() blocks
    the caller until it finishes -- the same shape every scenario's
    inject()/recover()/run() is already expected to have (synchronous,
    blocking, no concurrency needed with anything else in this
    single-purpose orchestration script).
    """
    box: list[_T] = []
    thread = threading.Thread(target=lambda: box.append(asyncio.run(coro_fn())))
    thread.start()
    thread.join()
    return box[0]


@dataclass
class TimelineEntry:
    elapsed_s: float
    timestamp: str
    phase: str  # "steady_state" | "injected" | "post_recover"
    invariants_all_passed: bool
    invariants: dict[str, dict[str, Any]]
    # kind ("hold" | "booking_create" | "confirm") -> status category ->
    # count of requests of that kind/category observed in THIS poll
    # window (a delta, not a running total).
    outcome_deltas: dict[str, dict[str, int]]
    metrics: dict[str, float]


@dataclass
class ChaosRunResult:
    scenario: str
    hypothesis: str
    base_url: str
    event_id: int
    started_at: str
    finished_at: str
    inject_at_s: float
    recover_at_s: float
    timeline: list[TimelineEntry] = field(default_factory=list)
    k6_summary: dict[str, Any] = field(default_factory=dict)
    k6_returncode: int | None = None

    @property
    def invariant_violations(self) -> list[TimelineEntry]:
        return [e for e in self.timeline if not e.invariants_all_passed]

    def entries_before(self, elapsed_s: float) -> list[TimelineEntry]:
        return [e for e in self.timeline if e.elapsed_s < elapsed_s]

    def entries_between(self, start_s: float, end_s: float) -> list[TimelineEntry]:
        return [e for e in self.timeline if start_s <= e.elapsed_s < end_s]

    def entries_after(self, elapsed_s: float) -> list[TimelineEntry]:
        return [e for e in self.timeline if e.elapsed_s >= elapsed_s]

    def total_outcomes(self, entries: Sequence[TimelineEntry], kind: str, category: str) -> int:
        return sum(e.outcome_deltas.get(kind, {}).get(category, 0) for e in entries)


@dataclass
class _OutcomeEvent:
    elapsed_s: float
    kind: str
    category: str


def _parse_k6_outcome_events(path: Path, k6_started_at: datetime) -> list[_OutcomeEvent]:
    """Parse k6's full --out json=... NDJSON file AFTER it has exited,
    keeping only http_reqs samples tagged with one of steady_load.js's
    three kinds (hold/booking_create/confirm) -- warmup's /health hits,
    for instance, are dropped: the timeline is about the booking hot
    path, not the whole script.

    Deliberately NOT a live tail during the run (an earlier version of
    this module did that): k6's JSON writer buffers its output, so
    "lines newly visible on disk at poll time T" lags noticeably behind
    "requests that actually happened at time T" -- confirmed directly
    while building this harness, a scenario's own narrow inject/recover
    window came back with zero observed outcomes that DID happen,
    misattributed to a later poll once the buffered lines finally
    surfaced. Parsing once, in full, after k6 exits, and bucketing each
    event by its OWN recorded timestamp (not by when this process
    happened to read it) removes that lag entirely -- correct is worth
    more here than "live."
    """
    events: list[_OutcomeEvent] = []
    if not path.exists():
        return events
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if obj.get("type") != "Point" or obj.get("metric") != "http_reqs":
                continue
            data = obj.get("data", {})
            tags = data.get("tags", {})
            kind = tags.get("name")
            if kind not in ("hold", "booking_create", "confirm"):
                continue
            time_str = data.get("time")
            if not time_str:
                continue
            try:
                event_dt = datetime.fromisoformat(time_str)
            except ValueError:
                continue
            elapsed = (event_dt - k6_started_at).total_seconds()
            category = _status_category(tags.get("status", "0"))
            events.append(_OutcomeEvent(elapsed_s=elapsed, kind=kind, category=category))
    return events


def _merge_outcomes_into_timeline(
    timeline: list[TimelineEntry], events: list[_OutcomeEvent]
) -> None:
    """Attribute each event to the first timeline poll whose elapsed_s is
    >= the event's own elapsed_s -- i.e. "the poll that would have
    observed this, had it been instantaneous" -- falling back to the
    LAST entry for anything that happened after the final poll (e.g. a
    request that landed in the gap between the last poll and k6 exiting).
    """
    if not timeline:
        return
    boundaries = [entry.elapsed_s for entry in timeline]
    for event in events:
        idx = bisect.bisect_left(boundaries, event.elapsed_s)
        entry = timeline[idx] if idx < len(timeline) else timeline[-1]
        entry.outcome_deltas.setdefault(event.kind, {}).setdefault(event.category, 0)
        entry.outcome_deltas[event.kind][event.category] += 1


def _status_category(status_str: str) -> str:
    try:
        status = int(status_str)
    except ValueError:
        return "transport_error"
    if status == 0:
        return "transport_error"
    if 200 <= status < 300:
        return "2xx"
    if status == 409:
        return "409"
    if status == 500:
        return "500"
    if status == 503:
        return "503"
    return "other"


def run_chaos_scenario(
    *,
    scenario: str,
    hypothesis: str,
    base_url: str,
    event_id: int,
    k6_env: dict[str, str],
    inject_at_s: float,
    inject: Callable[[], None],
    recover_at_s: float,
    recover: Callable[[], None],
    post_recover_poll_seconds: float = 30.0,
    metric_names: Sequence[str] = (),
    gauge_names: Sequence[str] = (),
    poll_interval_seconds: float = 0.25,
) -> ChaosRunResult:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    raw_json_path = RESULTS_DIR / f"{run_id}-{scenario}-k6-raw.json"
    summary_path = RESULTS_DIR / f"{run_id}-{scenario}-k6-summary.json"

    full_env = {**os.environ, **k6_env, "SUMMARY_PATH": str(summary_path)}
    cmd = ["k6", "run", "--out", f"json={raw_json_path}", str(STEADY_LOAD_SCRIPT)]

    # started_at/start are captured on either side of the SAME Popen call
    # so the wall-clock reference (used to convert k6's own event
    # timestamps into elapsed seconds, post-hoc) and the monotonic
    # reference (used for the live poll loop's elapsed seconds) start as
    # close together as this process can make them -- any remaining skew
    # is sub-second process-startup jitter, negligible against this
    # harness's 15-30s inject/recover windows.
    started_at = datetime.now(UTC)
    proc = subprocess.Popen(cmd, env=full_env)  # noqa: S603
    start = time.monotonic()

    result = ChaosRunResult(
        scenario=scenario,
        hypothesis=hypothesis,
        base_url=base_url,
        event_id=event_id,
        started_at=started_at.isoformat(),
        finished_at="",
        inject_at_s=inject_at_s,
        recover_at_s=recover_at_s,
    )

    injected = False
    recovered = False

    def poll_once(elapsed: float, phase: str) -> None:
        invariants = _http_get_json(f"{base_url}/api/admin/invariants?event_id={event_id}") or {
            "all_passed": False,
            "results": {
                "unreachable": {"passed": False, "detail": "invariants endpoint unreachable"}
            },
        }
        metrics_text = _fetch_metrics_text(base_url) or ""
        metrics = {name: _parse_counter_total(metrics_text, name) for name in metric_names}
        metrics.update({name: _parse_gauge_value(metrics_text, name) for name in gauge_names})
        result.timeline.append(
            TimelineEntry(
                elapsed_s=round(elapsed, 3),
                timestamp=datetime.now(UTC).isoformat(),
                phase=phase,
                invariants_all_passed=bool(invariants.get("all_passed", False)),
                invariants=invariants.get("results", {}),
                outcome_deltas={},  # filled in by _merge_outcomes_into_timeline below
                metrics=metrics,
            )
        )

    # --- steady state -> inject -> recover, while k6 runs -------------
    while proc.poll() is None:
        elapsed = time.monotonic() - start
        if not injected and elapsed >= inject_at_s:
            inject()
            injected = True
        if injected and not recovered and elapsed >= recover_at_s:
            recover()
            recovered = True
        phase = "post_recover" if recovered else ("injected" if injected else "steady_state")
        poll_once(elapsed, phase)
        time.sleep(poll_interval_seconds)

    # k6 may exit before recover_at_s elapses (e.g. a short DURATION) --
    # the failure must still be lifted, and this must still show up in
    # the timeline as its own poll, before the post-recovery window.
    if not injected:
        inject()
        injected = True
    if not recovered:
        recover()
        recovered = True
        poll_once(time.monotonic() - start, "post_recover")

    returncode = proc.wait()
    if returncode != 0:
        print(f"warning: k6 exited with code {returncode}", file=sys.stderr)
    result.k6_returncode = returncode

    # --- recovery window: k6 has stopped, but "recovered" means the
    # SYSTEM returned to steady state, not merely "load stopped" -- keep
    # polling so scenarios can assert a bounded recovery time.
    recovery_deadline = time.monotonic() + post_recover_poll_seconds
    while time.monotonic() < recovery_deadline:
        poll_once(time.monotonic() - start, "post_recover")
        time.sleep(poll_interval_seconds)

    if summary_path.exists():
        result.k6_summary = json.loads(summary_path.read_text(encoding="utf-8"))
    result.finished_at = datetime.now(UTC).isoformat()

    events = _parse_k6_outcome_events(raw_json_path, started_at)
    _merge_outcomes_into_timeline(result.timeline, events)

    _write_outputs(result, run_id)
    return result


@dataclass
class ChaosInfra:
    """The long-lived subprocesses run_all.py starts once per scenario
    (fresh each time -- see its own module docstring for why) and hands
    to that scenario's run(). Mutable on purpose: a scenario that kills
    and restarts one of these (sweeper_killed) reassigns the field to the
    new Popen so run_all.py's teardown tears down the CURRENT process,
    not the one that no longer exists.
    """

    api_proc: subprocess.Popen[bytes]
    sweeper_proc: subprocess.Popen[bytes]
    reconciler_proc: subprocess.Popen[bytes]
    payment_worker_proc: subprocess.Popen[bytes]
    prometheus_multiproc_dir: str
    hold_duration_seconds: float
    sweeper_interval_seconds: float
    sweeper_batch_size: int
    reconciler_interval_seconds: float


@dataclass
class ScenarioReport:
    """What each scenario module's run() returns to run_all.py --
    separate from ChaosRunResult (the mechanical timeline) because a
    scenario's verdict depends on hypothesis-specific thresholds only the
    scenario itself knows (e.g. "I3 recovers within one sweeper interval"
    is meaningless to the generic harness).

    result is None for a scenario that doesn't run k6/poll invariants at
    all (e.g. api_worker_killed_holding_lock.py -- a narrow, deterministic
    probe of one Postgres setting, not a load scenario) -- there is no
    timeline to attach in that case.
    """

    scenario: str
    passed: bool
    findings: list[str]
    result: ChaosRunResult | None


def default_k6_env(
    *, base_url: str, event_id: int, seat_ids: list[int], vus: int = 20, duration: str = "70s"
) -> dict[str, str]:
    return {
        "BASE_URL": base_url,
        "EVENT_ID": str(event_id),
        "SEAT_IDS": ",".join(str(s) for s in seat_ids),
        "VUS": str(vus),
        "DURATION": duration,
        "WARMUP_VUS": "5",
        "WARMUP_DURATION": "5s",
        "BOOKING_FRACTION": "0.2",
    }


def _write_outputs(result: ChaosRunResult, run_id: str) -> None:
    json_path = RESULTS_DIR / f"{run_id}-{result.scenario}.json"
    md_path = RESULTS_DIR / f"{run_id}-{result.scenario}.md"

    payload = asdict(result)
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    md_path.write_text(_render_markdown(result), encoding="utf-8")


def _render_markdown(result: ChaosRunResult) -> str:
    lines = [
        f"# Chaos scenario: {result.scenario}",
        "",
        f"**Hypothesis:** {result.hypothesis}",
        "",
        f"- Started: {result.started_at}",
        f"- Finished: {result.finished_at}",
        f"- Injected at: {result.inject_at_s:.1f}s",
        f"- Recovered at: {result.recover_at_s:.1f}s",
        f"- k6 exit code: {result.k6_returncode}",
        f"- Invariant violations recorded: {len(result.invariant_violations)}",
        "",
        "| elapsed_s | phase | invariants | hold 2xx/409/other | "
        "booking 2xx/other | confirm 2xx/500/503/other |",
        "|---:|---|---|---|---|---|",
    ]
    for e in result.timeline:
        ok = "OK" if e.invariants_all_passed else "**VIOLATION**"
        hold = e.outcome_deltas.get("hold", {})
        booking = e.outcome_deltas.get("booking_create", {})
        confirm = e.outcome_deltas.get("confirm", {})
        lines.append(
            f"| {e.elapsed_s:.2f} | {e.phase} | {ok} | "
            f"{hold.get('2xx', 0)}/{hold.get('409', 0)}/"
            f"{sum(v for k, v in hold.items() if k not in ('2xx', '409'))} | "
            f"{booking.get('2xx', 0)}/"
            f"{sum(v for k, v in booking.items() if k != '2xx')} | "
            f"{confirm.get('2xx', 0)}/{confirm.get('500', 0)}/{confirm.get('503', 0)}/"
            f"{sum(v for k, v in confirm.items() if k not in ('2xx', '500', '503'))} |"
        )
    return "\n".join(lines) + "\n"
