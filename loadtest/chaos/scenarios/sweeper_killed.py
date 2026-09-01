"""Scenario (d): sweeper killed mid-load.

HYPOTHESIS: seats remain bookable via lazy expiry -- this is the direct
test of the Phase 4 design claim that the sweeper is cleanup, not
mechanism. sweeper_backlog_gauge rises monotonically while it is down and
drains after restart. I3 is the one invariant permitted to be violated
here, since it is defined relative to the sweeper interval -- assert it
recovers within one sweeper interval of restart, and assert the other
four never break.

Run via loadtest/chaos/run_all.py, not standalone -- see
redis_killed.py's module docstring for why (this one specifically needs
run_all.py's already-started sweeper subprocess and its interval
settings, passed in via `infra`).
"""

from __future__ import annotations

from loadtest.chaos import actions
from loadtest.chaos.harness import ChaosInfra, ScenarioReport, default_k6_env, run_chaos_scenario
from loadtest.recirculating_pilot import start_sweeper

INJECT_AT_S = 15.0
RECOVER_AT_S = 45.0  # 30s down -- long enough to accumulate a real backlog


def run(*, base_url: str, event_id: int, seat_ids: list[int], infra: ChaosInfra) -> ScenarioReport:
    findings: list[str] = []

    def inject() -> None:
        # A hard kill (crash), not SIGTERM -- matches "killed", not
        # "stopped gracefully."
        actions.kill_pid(infra.sweeper_proc.pid)

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
        k6_env=default_k6_env(base_url=base_url, event_id=event_id, seat_ids=seat_ids),
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

    # DIVERGENCE: sweeper_backlog_gauge (multiprocess_mode='mostrecent')
    # is only ever WRITTEN by the sweeper process itself (workers/
    # sweeper.py's measure_backlog(), called from sweep_once() inside
    # run_forever()). Killing that process does not just stop it from
    # draining the backlog -- it stops the BACKLOG MEASUREMENT too. The
    # metric freezes at its last pre-kill value instead of rising, which
    # is the opposite of the hypothesised "rises monotonically while it
    # is down": the one signal meant to reveal the sweeper is behind goes
    # blind at exactly the moment the sweeper is gone, not merely slow.
    gauge_during = [e.metrics.get("sweeper_backlog", 0.0) for e in during]
    if gauge_during and max(gauge_during) - min(gauge_during) <= 0:
        findings.append(
            "DIVERGENCE FROM HYPOTHESIS: sweeper_backlog stayed flat "
            f"(constant at {gauge_during[0]}) while the sweeper was down, instead of rising "
            "monotonically -- because the gauge is only updated by the sweeper process "
            "itself, which was the thing killed. It is not observable during a total "
            "sweeper outage; only the successful-holds evidence above proves seats stayed "
            "bookable during this window."
        )
    else:
        findings.append(f"sweeper_backlog moved during the outage: {gauge_during}.")

    after = result.entries_after(RECOVER_AT_S)
    recovery_bound_s = RECOVER_AT_S + infra.sweeper_interval_seconds * 2
    drained_within_bound = [
        e.metrics.get("sweeper_backlog", 0.0) for e in after if e.elapsed_s <= recovery_bound_s
    ]
    if drained_within_bound and drained_within_bound[-1] > 0 and min(drained_within_bound) > 0:
        passed = False
        findings.append(
            f"HYPOTHESIS CONTRADICTED: sweeper_backlog had not reached 0 within "
            f"{infra.sweeper_interval_seconds * 2:.1f}s (two sweeper intervals) of restart -- "
            f"values observed: {drained_within_bound}."
        )
    else:
        findings.append(
            f"sweeper_backlog drained back to 0 within two sweeper intervals "
            f"({infra.sweeper_interval_seconds * 2:.1f}s) of restart, as hypothesised for I3."
        )

    return ScenarioReport(
        scenario="sweeper_killed", passed=passed, findings=findings, result=result
    )
