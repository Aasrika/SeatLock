"""Scenario (a): `docker kill redis` mid-load.

HYPOTHESIS: availability reads fall back to Postgres and slow down; holds
and confirms continue to succeed; all five invariants hold;
hold_cache_errors_total rises. Correct-but-slower, never incorrect.

Run this via loadtest/chaos/run_all.py, not standalone -- it assumes the
API, sweeper, reconciler, and payment_worker are already running against
a freshly seeded event (see run_all.py's orchestration).
"""

from __future__ import annotations

from loadtest.chaos import actions
from loadtest.chaos.harness import ChaosInfra, ScenarioReport, default_k6_env, run_chaos_scenario

INJECT_AT_S = 15.0
RECOVER_AT_S = 30.0


def run(*, base_url: str, event_id: int, seat_ids: list[int], infra: ChaosInfra) -> ScenarioReport:
    findings: list[str] = []

    def inject() -> None:
        actions.docker_kill("redis")

    def recover() -> None:
        actions.docker_start("redis")
        actions.wait_for_container_running("redis")

    result = run_chaos_scenario(
        scenario="redis_killed",
        hypothesis=(
            "Availability reads fall back to Postgres and slow down; holds and "
            "confirms continue to succeed; all five invariants hold; "
            "hold_cache_errors_total rises. Correct-but-slower, never incorrect."
        ),
        base_url=base_url,
        event_id=event_id,
        k6_env=default_k6_env(base_url=base_url, event_id=event_id, seat_ids=seat_ids),
        inject_at_s=INJECT_AT_S,
        inject=inject,
        recover_at_s=RECOVER_AT_S,
        recover=recover,
        metric_names=["hold_cache_errors_total"],
    )

    passed = True

    # ASSERT throughout: a violation at ANY poll, anywhere in the run
    # (steady state, injected, or post-recovery), is a hard failure --
    # not just "at the end."
    if result.invariant_violations:
        passed = False
        findings.append(
            f"HARD FAILURE: {len(result.invariant_violations)} invariant-violating "
            f"poll(s) recorded -- first at t={result.invariant_violations[0].elapsed_s:.2f}s: "
            f"{result.invariant_violations[0].invariants}"
        )

    during = result.entries_between(INJECT_AT_S, RECOVER_AT_S)
    hold_successes_during = result.total_outcomes(during, "hold", "2xx")
    if hold_successes_during == 0:
        passed = False
        findings.append(
            "HYPOTHESIS CONTRADICTED: zero successful holds while Redis was killed -- "
            "holds should continue to succeed (Postgres is the source of truth)."
        )
    else:
        findings.append(
            f"Holds continued to succeed while Redis was down: {hold_successes_during} "
            f"successful hold(s) between t={INJECT_AT_S:.0f}s and t={RECOVER_AT_S:.0f}s."
        )

    errors_before = max(
        (e.metrics.get("hold_cache_errors_total", 0.0) for e in result.entries_before(INJECT_AT_S)),
        default=0.0,
    )
    errors_during = max(
        (e.metrics.get("hold_cache_errors_total", 0.0) for e in during), default=0.0
    )
    if errors_during <= errors_before:
        findings.append(
            "NOTE: hold_cache_errors_total did not rise during the outage "
            f"(before={errors_before}, during={errors_during}) -- see the dead-code finding "
            "below for why this can happen even though holds kept succeeding."
        )
    else:
        findings.append(
            f"hold_cache_errors_total rose from {errors_before} to {errors_during} as hypothesised."
        )

    # DIVERGENCE: check_seat_available()/get_hold_mirror()'s read path
    # (app/infra/hold_cache.py) is never called by any route -- confirmed
    # by direct code inspection before this scenario was written, not
    # guessed from the results. The hypothesis above assumes a live
    # cache-read fallback; no such call site exists on the booking hot
    # path today, so there is nothing to "fall back to Postgres and slow
    # down" -- the only live Redis touches are the best-effort mirror SET
    # and pub/sub PUBLISH, both already tolerant of RedisError with no
    # read involved at all.
    findings.append(
        "DIVERGENCE FROM HYPOTHESIS: app/infra/hold_cache.py's check_seat_available() "
        "(the Redis-first, Postgres-fallback availability read) is defined but never "
        "called by any route -- the only live Redis touches on the booking hot path are "
        "the best-effort hold-mirror SET (create_hold/extend_hold) and the pub/sub PUBLISH "
        "(realtime fanout), both of which already catch RedisError with no read/fallback "
        "involved. Killing Redis therefore does NOT exercise a read-fallback-and-slow-down "
        "path, because that path is not wired into any request today. Result: a STRONGER "
        "outcome than hypothesised (near-zero degradation, not just bounded degradation) "
        "but for a structural reason (unused code), not because the fallback was proven to "
        "work under fire."
    )

    return ScenarioReport(scenario="redis_killed", passed=passed, findings=findings, result=result)
