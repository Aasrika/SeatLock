"""Scenario (c): Redis killed AND restarted EMPTY.

Persistence was disabled in Phase 0 specifically so this is a real data
loss, not a recovery from disk (docker-compose.yml: `--save "" --
appendonly no`, no volume) -- `docker start` after `docker kill` brings
Redis back with zero keys, by construction.

HYPOTHESIS: Postgres is unaffected; the reconciler repairs drift on its
next pass; reconciliation_divergence_total rises by a countable amount
and then stops; no invariant violation at any point.

Run via loadtest/chaos/run_all.py, not standalone -- see
redis_killed.py's module docstring for why.
"""

from __future__ import annotations

from loadtest.chaos import actions
from loadtest.chaos.harness import ChaosInfra, ScenarioReport, default_k6_env, run_chaos_scenario

INJECT_AT_S = 15.0
RECOVER_AT_S = 20.0  # brief outage -- the point is the EMPTY restart, not a long one
# Compared over the last PLATEAU_WINDOW_S of the run: if divergence is
# still climbing there, the reconciler has not caught up (or isn't
# running), which contradicts "rises ... and then stops."
PLATEAU_WINDOW_S = 10.0


def run(*, base_url: str, event_id: int, seat_ids: list[int], infra: ChaosInfra) -> ScenarioReport:
    findings: list[str] = []

    def inject() -> None:
        actions.docker_kill("redis")

    def recover() -> None:
        # Redis has --save ""/--appendonly no and no volume (docker-
        # compose.yml) -- docker start after docker kill comes back with
        # zero keys. Nothing extra needed to make this "restarted empty";
        # it is empty by construction.
        actions.docker_start("redis")
        actions.wait_for_container_running("redis")

    result = run_chaos_scenario(
        scenario="redis_killed_restarted_empty",
        hypothesis=(
            "Postgres is unaffected; the reconciler repairs drift on its next pass; "
            "reconciliation_divergence_total rises by a countable amount and then stops; "
            "no invariant violation at any point."
        ),
        base_url=base_url,
        event_id=event_id,
        k6_env=default_k6_env(base_url=base_url, event_id=event_id, seat_ids=seat_ids),
        inject_at_s=INJECT_AT_S,
        inject=inject,
        recover_at_s=RECOVER_AT_S,
        recover=recover,
        metric_names=["reconciliation_divergence_total"],
        post_recover_poll_seconds=45.0,  # room for several reconciler passes
    )

    passed = True

    if result.invariant_violations:
        passed = False
        findings.append(
            f"HARD FAILURE: {len(result.invariant_violations)} invariant-violating "
            f"poll(s) -- first at t={result.invariant_violations[0].elapsed_s:.2f}s: "
            f"{result.invariant_violations[0].invariants}"
        )

    def divergence_at(entries: list) -> float:
        return max(
            (e.metrics.get("reconciliation_divergence_total", 0.0) for e in entries), default=0.0
        )

    before = divergence_at(result.entries_before(INJECT_AT_S))
    final = divergence_at(result.timeline)
    last_window_start = result.timeline[-1].elapsed_s - PLATEAU_WINDOW_S if result.timeline else 0.0
    plateau_start_value = divergence_at(result.entries_before(last_window_start))

    if final <= before:
        findings.append(
            f"NOTE: reconciliation_divergence_total did not rise (before={before}, "
            f"final={final}). Either the empty-restart window ({INJECT_AT_S:.0f}s-"
            f"{RECOVER_AT_S:.0f}s) was too brief to leave any HELD seat mirrorless by the "
            "time the reconciler next scanned, or divergence had already been repaired "
            "before the first post-outage poll captured it -- not a contradiction of the "
            "hypothesis by itself, but recorded as a divergence from the expected shape."
        )
    else:
        findings.append(
            f"reconciliation_divergence_total rose from {before} to {final} as hypothesised."
        )

    if final > plateau_start_value:
        passed = False
        findings.append(
            f"HYPOTHESIS CONTRADICTED: divergence was still rising in the last "
            f"{PLATEAU_WINDOW_S:.0f}s of the run ({plateau_start_value} -> {final}) -- "
            "the reconciler has not caught up within this run's window."
        )
    else:
        findings.append(
            f"Divergence plateaued at {final} for at least the last {PLATEAU_WINDOW_S:.0f}s "
            "of the run -- the reconciler caught up and stopped, as hypothesised."
        )

    return ScenarioReport(
        scenario="redis_killed_restarted_empty", passed=passed, findings=findings, result=result
    )
