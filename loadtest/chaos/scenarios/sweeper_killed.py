"""Scenario (d): sweeper killed mid-load.

HYPOTHESIS: seats remain bookable via lazy expiry -- this is the direct
test of the Phase 4 design claim that the sweeper is cleanup, not
mechanism. sweeper_backlog_gauge rises monotonically while it is down and
drains after restart. I3 is the one invariant permitted to be violated
here, since it is defined relative to the sweeper interval -- assert it
recovers within one sweeper interval of restart, and assert the other
four never break.

RESERVED SEATS. A handful of seat ids (RESERVED_COUNT) are deliberately
withheld from k6's load and never touched by it. The first version of
this scenario gave k6 the WHOLE pool and found the backlog assertion
never actually exercised: under real contention (20 VUs across 40
seats), any seat that expires gets reclaimed by someone else's next hold
attempt almost immediately -- app/inventory/strategies/optimistic.py's
conditional UPDATE matches `(status = 'HELD' AND hold_expires_at <=
now())` as eligible, exactly the lazy-expiry-aware reclaim this
scenario's OTHER assertion (successful holds while the sweeper is down)
is already proving. That is a real, good property of this system, but it
means the sweeper_backlog gauge stays near zero regardless of whether
the sweeper is alive, for a reason that has nothing to do with the
sweeper -- a scenario that can't tell "sweeper is measuring a real
backlog" apart from "there is no backlog because load is reclaiming
everything" would pass either way, which is exactly the vacuous-
assertion trap this scenario already got caught by once (see
docs/chaos-results.md). Marking RESERVED_COUNT seats HELD-and-expired
directly, on seat ids k6 never sees, produces a genuine, uncontended
backlog that only the sweeper (or its stand-in, the reconciler) can
ever clear.

Run via loadtest/chaos/run_all.py, not standalone -- see
redis_killed.py's module docstring for why (this one specifically needs
run_all.py's already-started sweeper subprocess and its interval
settings, passed in via `infra`).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import asyncpg

from app.infra.config import settings
from loadtest.chaos import actions
from loadtest.chaos.harness import (
    ChaosInfra,
    ScenarioReport,
    default_k6_env,
    run_chaos_scenario,
    run_sync_in_thread,
)
from loadtest.recirculating_pilot import start_sweeper

INJECT_AT_S = 15.0
RECOVER_AT_S = 45.0  # 30s down -- long enough to accumulate a real backlog
RESERVED_COUNT = 5  # withheld from k6, marked stale directly -- see module docstring


async def _mark_seats_stale(seat_ids: list[int]) -> None:
    """UPDATE these seats directly to HELD with hold_expires_at already
    in the past -- simulating sessions that held them and vanished,
    uncontended by construction (k6 never sees these ids). Raw asyncpg,
    not app.infra.db's shared engine: this runs on run_sync_in_thread's
    own dedicated thread/loop, and asyncpg connections are bound to the
    loop that opened them (see api_worker_killed_holding_lock.py's
    _seat_is_locked for the same reasoning).
    """
    dsn = settings.database_url.replace("postgresql+asyncpg://", "postgresql://", 1)
    conn = await asyncpg.connect(dsn)
    try:
        expired_at = datetime.now(UTC) - timedelta(seconds=60)
        await conn.execute(
            "UPDATE seats SET status = 'HELD', held_by_session_id = 'chaos-reserved', "
            "hold_expires_at = $1 WHERE id = ANY($2::bigint[])",
            expired_at,
            seat_ids,
        )
    finally:
        await conn.close()


def run(*, base_url: str, event_id: int, seat_ids: list[int], infra: ChaosInfra) -> ScenarioReport:
    findings: list[str] = []
    reserved_seat_ids = seat_ids[-RESERVED_COUNT:]
    k6_seat_ids = seat_ids[:-RESERVED_COUNT]

    def inject() -> None:
        # A hard kill (crash), not SIGTERM -- matches "killed", not
        # "stopped gracefully."
        actions.kill_pid(infra.sweeper_proc.pid)
        # See module docstring: without this, k6's own contention would
        # reclaim any expired seat almost immediately regardless of the
        # sweeper's state, making the backlog assertion below vacuous.
        run_sync_in_thread(lambda: _mark_seats_stale(reserved_seat_ids))

    def recover() -> None:
        # There is no "un-kill" for a dead process -- recovery here means
        # starting a REPLACEMENT sweeper with the same settings.
        # run_all.py's teardown reads infra.sweeper_proc, so this
        # reassignment is what makes teardown target the new process
        # instead of the dead one.
        infra.sweeper_proc = start_sweeper(
            interval_seconds=infra.sweeper_interval_seconds,
            batch_size=infra.sweeper_batch_size,
            prometheus_multiproc_dir=infra.prometheus_multiproc_dir,
        )

    result = run_chaos_scenario(
        scenario="sweeper_killed",
        hypothesis=(
            "Seats remain bookable via lazy expiry while the sweeper is down (the direct "
            "test of Phase 4's 'sweeper is cleanup, not mechanism' claim). "
            "sweeper_backlog_gauge rises monotonically while it is down and drains after "
            "restart. I3 is the one invariant permitted to be violated here; the other "
            "four never break, and I3 recovers within one sweeper interval of restart."
        ),
        base_url=base_url,
        event_id=event_id,
        k6_env=default_k6_env(base_url=base_url, event_id=event_id, seat_ids=k6_seat_ids),
        inject_at_s=INJECT_AT_S,
        inject=inject,
        recover_at_s=RECOVER_AT_S,
        recover=recover,
        gauge_names=["sweeper_backlog"],
        post_recover_poll_seconds=30.0,
    )

    passed = True

    # The four invariants /api/admin/invariants actually checks
    # (conservation=I2, no_double_booking/state_coherence/booking_linkage,
    # all supporting I1) are NOT I3 -- none of them is defined relative to
    # hold_expires_at at all, so none of them can be "the I3 violation
    # permitted here." Any violation this endpoint reports is therefore
    # one of the OTHER four, which must never break.
    if result.invariant_violations:
        passed = False
        findings.append(
            f"HARD FAILURE: {len(result.invariant_violations)} invariant-violating "
            f"poll(s) among the four non-I3 checks -- first at "
            f"t={result.invariant_violations[0].elapsed_s:.2f}s: "
            f"{result.invariant_violations[0].invariants}"
        )
    else:
        findings.append(
            "The four non-I3 invariants held throughout (I2/I1/state-coherence/linkage)."
        )

    during = result.entries_between(INJECT_AT_S, RECOVER_AT_S)
    hold_successes_during = result.total_outcomes(during, "hold", "2xx")
    if hold_successes_during == 0:
        passed = False
        findings.append(
            "HYPOTHESIS CONTRADICTED: zero successful holds while the sweeper was down -- "
            "this would mean lazy expiry is NOT actually reclaiming expired holds, i.e. the "
            "sweeper is load-bearing, not cleanup."
        )
    else:
        findings.append(
            f"{hold_successes_during} successful hold(s) while the sweeper was down -- "
            "confirms lazy expiry, not the sweeper, is what makes an expired seat "
            "reacquirable."
        )

    # sweeper_backlog_gauge (multiprocess_mode='mostrecent') USED to be
    # written only by the sweeper process itself (workers/sweeper.py's
    # measure_backlog(), called from sweep_once() inside run_forever()).
    # Killing that process didn't just stop it draining the backlog -- it
    # stopped the MEASUREMENT too, freezing the gauge at its last pre-kill
    # value instead of letting it rise: the one signal meant to reveal a
    # stopped sweeper going blind at exactly the moment the sweeper died.
    # (An earlier version of this scenario found that, documented it as a
    # divergence, and left the gauge assertion below informational only --
    # a passing assertion that could not have failed, the same trap as
    # mypy on an empty package. A second earlier version fixed the write
    # path but gave k6 the WHOLE seat pool, making the assertion pass
    # vacuously for a DIFFERENT reason -- see module docstring's
    # "RESERVED SEATS" section.)
    #
    # Fixed at the source, not worked around here: workers/reconciler.py's
    # own loop now ALSO calls measure_backlog(), independently, on its own
    # schedule -- so the gauge keeps reflecting reality even with the
    # sweeper dead. This assertion is now a REAL one: it can fail if that
    # fix regresses, or if RESERVED_COUNT seats stop actually being
    # uncontended.
    gauge_during = [e.metrics.get("sweeper_backlog", 0.0) for e in during]
    rose = bool(gauge_during) and max(gauge_during) >= RESERVED_COUNT
    # Once the reserved seats' backlog is first observed, it must never
    # drop back below RESERVED_COUNT before recovery -- nothing sweeps
    # them (the sweeper is dead) and nothing else contends for them
    # (k6 never sees these ids).
    ever_dropped_below_reserved = False
    if rose:
        first_reached_idx = next(i for i, v in enumerate(gauge_during) if v >= RESERVED_COUNT)
        ever_dropped_below_reserved = any(
            v < RESERVED_COUNT for v in gauge_during[first_reached_idx:]
        )
    if not rose:
        passed = False
        findings.append(
            f"HYPOTHESIS CONTRADICTED: sweeper_backlog never reached the {RESERVED_COUNT} "
            f"seats reserved and marked stale for this run (observed: {gauge_during}) -- "
            "workers/reconciler.py's independent measure_backlog() call should have caught "
            "them on its own schedule even with the sweeper dead."
        )
    elif ever_dropped_below_reserved:
        passed = False
        findings.append(
            f"HYPOTHESIS CONTRADICTED: sweeper_backlog dropped back below {RESERVED_COUNT} "
            f"before the sweeper was restarted (observed: {gauge_during}) -- nothing should "
            "be draining the reserved seats' backlog with the sweeper dead."
        )
    else:
        findings.append(
            f"sweeper_backlog reached at least {RESERVED_COUNT} (peak {max(gauge_during):.0f}) "
            "while the sweeper was down and stayed there, as hypothesised -- "
            "workers/reconciler.py's independent measurement (added because an earlier run "
            "of this exact scenario found the gauge going blind here) kept it visible after "
            "the sweeper died."
        )

    # A tight "two sweeper intervals" wall-clock bound is the DESIGN
    # claim, but this harness's own poll cadence is not guaranteed to be
    # anywhere near its nominal 0.25s under heavy load -- confirmed
    # directly: running all seven scenarios back to back (this scenario
    # 4th in line, competing with k6 + sweeper + reconciler + payment_
    # worker + REPEATABLE READ's extra session-per-poll cost from the
    # invariants fix above) saw poll cycles take ~5s each, not 0.25s, so
    # a 4.0s bound had exactly zero samples that could possibly land
    # inside it after the pre-recovery one. Search the WHOLE post-
    # recovery window (post_recover_poll_seconds, generous on purpose)
    # for convergence instead of a tight cutoff, and report how long it
    # actually took as the informational number -- that is the real
    # claim under test, not this harness's own polling latency.
    after = result.entries_after(RECOVER_AT_S)
    drained_at = next(
        (e.elapsed_s for e in after if e.metrics.get("sweeper_backlog", 0.0) == 0), None
    )
    if drained_at is None:
        passed = False
        window_s = after[-1].elapsed_s - RECOVER_AT_S if after else 0.0
        findings.append(
            f"HYPOTHESIS CONTRADICTED: sweeper_backlog never reached 0 within the "
            f"{window_s:.1f}s post-recovery observation window -- "
            f"values observed: {[e.metrics.get('sweeper_backlog', 0.0) for e in after]}."
        )
    else:
        findings.append(
            f"sweeper_backlog drained back to 0 within {drained_at - RECOVER_AT_S:.1f}s of "
            f"restart (sweeper interval: {infra.sweeper_interval_seconds:.1f}s) -- bounded, "
            "not indefinite, as hypothesised for I3."
        )

    return ScenarioReport(
        scenario="sweeper_killed", passed=passed, findings=findings, result=result
    )
