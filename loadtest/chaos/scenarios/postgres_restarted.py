"""Scenario (f): Postgres restarted mid-load.

HYPOTHESIS: requests fail during the outage -- with 503, NOT 500 -- the
pool recovers without an API restart, and invariants hold on both sides
of the gap. If any request returns 500, that is a finding to fix, not to
document.

`docker compose restart postgres` (not kill+start) models the failure
mode a real deployment actually sees during a Postgres failover or
maintenance restart, not a permanent outage.

Run via loadtest/chaos/run_all.py, not standalone -- see
redis_killed.py's module docstring for why.
"""

from __future__ import annotations

from loadtest.chaos import actions
from loadtest.chaos.harness import ChaosInfra, ScenarioReport, default_k6_env, run_chaos_scenario

INJECT_AT_S = 15.0
RECOVER_AT_S = INJECT_AT_S + 1.0  # the restart itself IS the recovery action; see inject()


def run(*, base_url: str, event_id: int, seat_ids: list[int], infra: ChaosInfra) -> ScenarioReport:
    findings: list[str] = []

    def inject() -> None:
        # Blocking -- docker compose restart does not return until the
        # container has restarted. The harness's poll loop simply pauses
        # for that duration, which is fine: it's a faithful sample of
        # "what does a client see between the last successful poll before
        # this call and the first one after."
        actions.docker_restart("postgres", timeout_seconds=10)

    def recover() -> None:
        pass  # nothing further to do -- inject() already performed the restart

    result = run_chaos_scenario(
        scenario="postgres_restarted",
        hypothesis=(
            "Requests fail during the outage with 503, NOT 500; the pool recovers without "
            "an API restart; invariants hold on both sides of the gap."
        ),
        base_url=base_url,
        event_id=event_id,
        k6_env=default_k6_env(base_url=base_url, event_id=event_id, seat_ids=seat_ids),
        inject_at_s=INJECT_AT_S,
        inject=inject,
        recover_at_s=RECOVER_AT_S,
        recover=recover,
        post_recover_poll_seconds=25.0,
    )

    passed = True

    if result.invariant_violations:
        passed = False
        findings.append(
            f"HARD FAILURE: {len(result.invariant_violations)} invariant-violating "
            f"poll(s) -- first at t={result.invariant_violations[0].elapsed_s:.2f}s: "
            f"{result.invariant_violations[0].invariants}"
        )
    else:
        findings.append("Invariants held on both sides of the restart.")

    total_500 = sum(
        result.total_outcomes(result.timeline, kind, "500")
        for kind in ("hold", "booking_create", "confirm")
    )
    if total_500 > 0:
        passed = False
        findings.append(
            f"HYPOTHESIS CONTRADICTED (finding to fix, not document): {total_500} request(s) "
            "returned 500 during this run. See app/main.py's _database_unavailable_handler "
            "(DBAPIError -> 503) -- if this fires, either that handler regressed or a "
            "different exception type is escaping uncaught."
        )
    else:
        findings.append(
            "Zero 500s across the whole run -- app/main.py's DBAPIError -> 503 handler "
            "(added ahead of running this scenario -- see its own comment) held."
        )

    total_503 = sum(
        result.total_outcomes(result.timeline, kind, "503")
        for kind in ("hold", "booking_create", "confirm")
    )
    findings.append(
        f"{total_503} request(s) returned 503 during the run (expected to concentrate "
        f"around t={INJECT_AT_S:.0f}s; 0 is possible if the restart completed faster than "
        "any in-flight request landed on the broken pool)."
    )

    after = result.entries_after(RECOVER_AT_S + 2.0)
    successes_after = sum(
        result.total_outcomes(after, kind, "2xx") for kind in ("hold", "booking_create", "confirm")
    )
    if successes_after == 0:
        passed = False
        findings.append(
            "HYPOTHESIS CONTRADICTED: no successful requests resumed after the restart -- "
            "the pool did not recover without an API restart."
        )
    else:
        findings.append(
            f"{successes_after} successful request(s) resumed after the restart with no API "
            "process restart -- SQLAlchemy's pool recovered on its own, as hypothesised."
        )

    return ScenarioReport(
        scenario="postgres_restarted", passed=passed, findings=findings, result=result
    )
