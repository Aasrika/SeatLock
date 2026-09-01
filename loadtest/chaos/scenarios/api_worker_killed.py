"""Scenario (e): one of the 4 uvicorn workers killed mid-transaction.

HYPOTHESIS: in-flight transactions on that worker roll back; no partial
state; no seat left HELD by a session that no longer exists beyond hold
expiry; surviving workers absorb the load; invariants hold.

There is no "recover" action here -- a killed OS process does not come
back, and uvicorn's own multi-worker supervisor (not this codebase) is
what would or would not respawn it. recover() therefore only records
whether that happened; it is a data point, not part of the hypothesis
(which only claims survivors absorb the load, not self-healing worker
count).

Run via loadtest/chaos/run_all.py, not standalone -- see
redis_killed.py's module docstring for why (this one specifically needs
run_all.py's already-started API subprocess, passed in via `infra`).
"""

from __future__ import annotations

from loadtest.chaos import actions
from loadtest.chaos.harness import ChaosInfra, ScenarioReport, default_k6_env, run_chaos_scenario

INJECT_AT_S = 15.0
RECOVER_AT_S = INJECT_AT_S + 2.0  # nothing to reverse; see module docstring


def run(*, base_url: str, event_id: int, seat_ids: list[int], infra: ChaosInfra) -> ScenarioReport:
    findings: list[str] = []
    killed_pid: int | None = None
    worker_count_before = actions.count_live_children(infra.api_proc.pid)

    def inject() -> None:
        nonlocal killed_pid
        killed_pid = actions.find_one_uvicorn_worker_pid(infra.api_proc.pid)
        actions.kill_pid(killed_pid)

    def recover() -> None:
        # No corrective action: see module docstring. Just record whether
        # uvicorn respawned the worker on its own.
        pass

    result = run_chaos_scenario(
        scenario="api_worker_killed",
        hypothesis=(
            "In-flight transactions on the killed worker roll back cleanly; no partial "
            "state; no seat left HELD by a session that no longer exists beyond hold "
            "expiry; surviving workers absorb the load; invariants hold throughout."
        ),
        base_url=base_url,
        event_id=event_id,
        k6_env=default_k6_env(base_url=base_url, event_id=event_id, seat_ids=seat_ids),
        inject_at_s=INJECT_AT_S,
        inject=inject,
        recover_at_s=RECOVER_AT_S,
        recover=recover,
    )

    passed = True

    if result.invariant_violations:
        passed = False
        findings.append(
            f"HARD FAILURE: {len(result.invariant_violations)} invariant-violating "
            f"poll(s) -- first at t={result.invariant_violations[0].elapsed_s:.2f}s: "
            f"{result.invariant_violations[0].invariants}. A partial-state leak from the "
            "killed worker's in-flight transaction would show up here (conservation/"
            "state-coherence), since Postgres's own atomicity is not enough to protect "
            "against an application-level bug in what gets committed."
        )
    else:
        findings.append(
            "All four checked invariants held throughout -- consistent with Postgres's "
            "transactional guarantee: a hard-killed connection cannot have partially "
            "committed, so an in-flight hold/booking either fully landed before the kill "
            "or never landed at all."
        )

    if killed_pid is not None:
        findings.append(f"Killed uvicorn worker pid={killed_pid} at t={INJECT_AT_S:.0f}s.")
        worker_count_after = actions.count_live_children(infra.api_proc.pid)
        findings.append(
            f"Live worker child count: {worker_count_before} before -> {worker_count_after} "
            f"after (best-effort psutil count; a rise back to {worker_count_before} would "
            "mean uvicorn's own supervisor respawned the killed worker -- not something this "
            "scenario's hypothesis depends on either way, since it only claims survivors "
            "absorb the load)."
        )

    after = result.entries_after(INJECT_AT_S)
    total_successes_after = (
        result.total_outcomes(after, "hold", "2xx")
        + result.total_outcomes(after, "booking_create", "2xx")
        + result.total_outcomes(after, "confirm", "2xx")
    )
    if total_successes_after == 0:
        passed = False
        findings.append(
            "HYPOTHESIS CONTRADICTED: zero successful requests of any kind after the "
            "worker was killed -- surviving workers did not absorb the load."
        )
    else:
        findings.append(
            f"{total_successes_after} successful request(s) across hold/booking/confirm "
            "after the kill -- surviving workers absorbed the load, as hypothesised."
        )

    return ScenarioReport(
        scenario="api_worker_killed", passed=passed, findings=findings, result=result
    )
