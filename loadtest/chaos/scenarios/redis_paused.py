"""Scenario (b): `docker pause redis` mid-load -- a HANG, not a crash.

HYPOTHESIS: this is the harsher case. A hung dependency holds sockets
open, so without a configured timeout every Redis-touching request
blocks and the event loop fills with waiting coroutines, degrading
BOOKING throughput because of a cache. If Redis has no socket/command
timeout configured, this test finds it -- app/infra/config.py's
redis_socket_timeout_seconds / redis_socket_connect_timeout_seconds
(added specifically because of this scenario) bound that wait to ~2s per
Redis touch instead of forever. Assert booking throughput stays above a
floor while Redis is paused.

Run this via loadtest/chaos/run_all.py, not standalone -- see
redis_killed.py's module docstring for why.
"""

from __future__ import annotations

from loadtest.chaos import actions
from loadtest.chaos.harness import ChaosInfra, ScenarioReport, default_k6_env, run_chaos_scenario

INJECT_AT_S = 15.0
RECOVER_AT_S = 30.0
# A successful hold is what "booking throughput" means here: create_hold
# is the one request in the booking flow that touches Redis INLINE, twice
# (hold_cache.set_hold_mirror, then pubsub.publish_seat_update) -- see
# app/infra/config.py's comment. This floor is deliberately generous: with
# the 2s timeouts in place, a paused Redis costs each iteration roughly
# 2-4s instead of succeeding in milliseconds, so throughput should drop
# hard but never to zero over a 15s window with 20 VUs.
MIN_HOLD_SUCCESSES_DURING_PAUSE = 1


def run(*, base_url: str, event_id: int, seat_ids: list[int], infra: ChaosInfra) -> ScenarioReport:
    findings: list[str] = []

    def inject() -> None:
        actions.docker_pause("redis")

    def recover() -> None:
        actions.docker_unpause("redis")

    k6_env = default_k6_env(base_url=base_url, event_id=event_id, seat_ids=seat_ids)
    k6_env["BOOKING_FRACTION"] = "0.4"  # more confirm-path signal for this scenario specifically

    result = run_chaos_scenario(
        scenario="redis_paused",
        hypothesis=(
            "A hung dependency holds sockets open, so without a configured timeout every "
            "Redis-touching request blocks and the event loop fills with waiting "
            "coroutines, degrading BOOKING throughput because of a cache. Booking "
            "throughput stays above a floor while Redis is paused."
        ),
        base_url=base_url,
        event_id=event_id,
        k6_env=k6_env,
        inject_at_s=INJECT_AT_S,
        inject=inject,
        recover_at_s=RECOVER_AT_S,
        recover=recover,
        metric_names=["hold_cache_errors_total"],
    )

    passed = True

    if result.invariant_violations:
        passed = False
        findings.append(
            f"HARD FAILURE: {len(result.invariant_violations)} invariant-violating "
            f"poll(s) -- first at t={result.invariant_violations[0].elapsed_s:.2f}s: "
            f"{result.invariant_violations[0].invariants}"
        )

    during = result.entries_between(INJECT_AT_S, RECOVER_AT_S)
    hold_successes_during = result.total_outcomes(during, "hold", "2xx")
    confirm_successes_during = result.total_outcomes(during, "confirm", "2xx")

    if hold_successes_during < MIN_HOLD_SUCCESSES_DURING_PAUSE:
        passed = False
        findings.append(
            f"HYPOTHESIS CONTRADICTED: only {hold_successes_during} successful hold(s) "
            f"while Redis was paused (floor: {MIN_HOLD_SUCCESSES_DURING_PAUSE}) -- this is "
            "the signature of an UNBOUNDED wait: without redis_socket_timeout_seconds, "
            "every Redis-touching request would hang for the pause's full duration and "
            "throughput would flatline to exactly zero."
        )
    else:
        findings.append(
            f"Booking throughput stayed above the floor during the pause: "
            f"{hold_successes_during} successful hold(s), {confirm_successes_during} "
            "successful confirm(s) -- bounded by the 2.0s Redis socket/connect "
            "timeouts (app/infra/config.py), not unbounded."
        )

    after = result.entries_after(RECOVER_AT_S)
    hold_successes_after = result.total_outcomes(after, "hold", "2xx")
    findings.append(f"Successful holds after unpause: {hold_successes_after}.")

    return ScenarioReport(scenario="redis_paused", passed=passed, findings=findings, result=result)
