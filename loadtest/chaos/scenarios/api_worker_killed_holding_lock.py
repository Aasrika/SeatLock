"""Scenario (e) variant: a worker holding row locks dies mid-transaction,
with nothing to release them but Postgres's own timeout.

The original api_worker_killed.py scenario passed cleanly -- but only
because every real transaction in this codebase is short (a row lock
held for single-digit milliseconds), so a random kill under k6-driven
load essentially never catches a worker actually holding a lock. That
scenario says nothing about what happens if it did.

HYPOTHESIS (as originally written): a killed worker holding row locks
does NOT release them promptly -- there is no clean disconnect, the
socket simply stops, and the Postgres backend sits idle in transaction
holding every lock it acquired, until idle_in_transaction_session_timeout
(app/infra/config.py, added because of this exact scenario) releases it
-- bounded, not indefinite.

RUNNING THIS FOUND THE HYPOTHESIS NEEDS SPLITTING IN TWO, the same way
Phase 8a already split "Redis killed" from "Redis paused": a hard KILL
of a healthy process is not actually the unresponsive-socket case. The
OS cleans up a killed process's file descriptors on the way out --
including its open Postgres socket -- and sends the peer a normal
close. Postgres notices on its very next read attempt and aborts the
transaction, releasing the lock almost immediately. Confirmed directly
below (sub-test A): a hard-killed lock holder released its row lock in
well under a second, nowhere near idle_in_transaction_session_timeout.
That is a GOOD result (crash recovery doesn't need a multi-second wait),
but it does not exercise the setting this scenario exists to test.

The failure mode idle_in_transaction_session_timeout actually protects
against -- "the socket simply stops," no FIN, no RST, nothing -- needs a
process that is unresponsive WITHOUT dying: a network partition, a
frozen host, or (the process-level analogue actually reproducible on one
machine) a SUSPENDED process. Sub-test B suspends the lock holder
(psutil, NtSuspendProcess/SIGSTOP -- see actions.suspend_pid) instead of
killing it: the socket stays open and registered with the OS, but
nothing is left running to ever use it, which IS indistinguishable from
Postgres's side from a truly dead peer. That is the sub-test whose
release time should be bounded by idle_in_transaction_session_timeout,
not by an OS-level clean disconnect.

Deliberately NOT routed through the API or any SeatAcquisitionStrategy
-- see loadtest/chaos/lock_holder.py's own module docstring for why
(app/inventory/strategies/pessimistic.py's design explicitly forbids any
I/O between lock acquisition and commit; simulating a long hold there,
even behind a flag, would violate that file's own invariant). Both
sub-tests run a dedicated subprocess that opens a raw connection, holds
a row lock, and is killed or suspended -- then probe Postgres DIRECTLY
(SELECT ... FOR UPDATE NOWAIT) to measure exactly when the lock is
released, independent of any HTTP-layer retry/backoff that would
otherwise blur the measurement. No k6 load runs alongside either; this
is a narrow, deterministic test of one mechanism, not a load scenario.

Run via loadtest/chaos/run_all.py, not standalone -- see
redis_killed.py's module docstring for why (this one uses run_all.py's
already-seeded event for two seat ids to lock, though it needs none of
run_all.py's API/worker infra otherwise).
"""

from __future__ import annotations

import asyncio
import queue
import subprocess
import sys
import threading
import time

import asyncpg

from app.infra.config import settings
from loadtest.chaos import actions
from loadtest.chaos.harness import ChaosInfra, ScenarioReport

LOCK_ACQUIRED_TIMEOUT_S = 10.0  # how long to wait for the subprocess's "lock acquired" line
HOLD_SECONDS = 120.0  # far longer than either sub-test below -- killed/suspended long before this
POLL_INTERVAL_S = 0.5
MAX_WAIT_S = 30.0
MARGIN_S = 5.0  # slack for polling granularity + Postgres's own check-interval latency


async def _seat_is_locked(seat_id: int) -> bool:
    """True if SELECT ... FOR UPDATE NOWAIT on this seat fails (55P03,
    lock_not_available) -- i.e. someone else still holds it right now.

    Raw asyncpg, deliberately not app.infra.db's shared engine: this
    scenario's run() executes on its own dedicated thread with its own
    event loop (see run()'s own comment for why), and asyncpg connections
    are bound to the loop that opened them -- reusing the shared,
    already-pooled engine here would risk handing this thread's loop a
    connection opened under run_all.py's main loop, the exact
    loop-mismatch bug documented in run_all.py's async_main(). A fresh,
    self-contained connection per probe sidesteps that entirely.
    """
    dsn = settings.database_url.replace("postgresql+asyncpg://", "postgresql://", 1)
    conn = await asyncpg.connect(dsn)
    try:
        async with conn.transaction():
            try:
                await conn.fetchrow(
                    "SELECT id FROM seats WHERE id = $1 FOR UPDATE NOWAIT", seat_id
                )
            except asyncpg.exceptions.LockNotAvailableError:
                return True
            return False
    finally:
        await conn.close()


async def _start_lock_holder(seat_id: int) -> subprocess.Popen[str] | str:
    """Returns the running subprocess once it confirms the lock is held,
    or an error string on failure (kept as a plain string, not an
    exception, so callers can fold it into findings without a try/except
    at every call site).
    """
    # Popen, not asyncio.create_subprocess_exec: this is a one-shot
    # orchestration script running one sub-test at a time, not a server
    # with concurrent requests to keep responsive -- there is nothing
    # else this event loop needs to be doing while a subprocess starts.
    proc = subprocess.Popen(  # noqa: S603, ASYNC220
        [sys.executable, "-m", "loadtest.chaos.lock_holder", str(seat_id), str(HOLD_SECONDS)],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout is not None
    line_queue: queue.Queue[str] = queue.Queue()
    threading.Thread(target=lambda: line_queue.put(proc.stdout.readline()), daemon=True).start()

    try:
        line = line_queue.get(timeout=LOCK_ACQUIRED_TIMEOUT_S)
    except queue.Empty:
        actions.kill_pid(proc.pid)
        return (
            f"lock_holder never reported acquiring the lock within "
            f"{LOCK_ACQUIRED_TIMEOUT_S:.0f}s"
        )
    if "lock acquired" not in line:
        actions.kill_pid(proc.pid)
        return f"unexpected lock_holder output: {line!r}"
    return proc


async def _wait_for_release(seat_id: int) -> float | None:
    """Seconds until the lock is free, or None if MAX_WAIT_S elapses
    first. Caller measures elapsed from whenever it wants (acquisition
    time, kill/suspend time) -- this just returns the wall-clock moment
    release was observed.
    """
    deadline = time.monotonic() + MAX_WAIT_S
    while time.monotonic() < deadline:
        if not await _seat_is_locked(seat_id):
            return time.monotonic()
        await asyncio.sleep(POLL_INTERVAL_S)
    return None


async def _run_kill_subtest(seat_id: int) -> list[str]:
    findings = ["--- sub-test A: hard KILL (a healthy process crashing) ---"]

    started = await _start_lock_holder(seat_id)
    if isinstance(started, str):
        findings.append(f"HARNESS FAILURE: {started}")
        return findings
    proc = started

    acquired_at = time.monotonic()
    findings.append(f"lock_holder (pid={proc.pid}) acquired the row lock on seat {seat_id}.")
    actions.kill_pid(proc.pid)
    findings.append(f"Hard-killed pid={proc.pid} at t={time.monotonic() - acquired_at:.2f}s.")

    released_at = await _wait_for_release(seat_id)
    if released_at is None:
        findings.append(
            f"NOTE: lock still held {MAX_WAIT_S:.0f}s after a hard kill -- unexpected; a "
            "killed process's socket should be closed by the OS almost immediately."
        )
    else:
        elapsed = released_at - acquired_at
        findings.append(
            f"Lock released {elapsed:.2f}s after acquisition. Released via a normal OS-level "
            "socket close, NOT idle_in_transaction_session_timeout "
            f"({settings.idle_in_transaction_session_timeout_ms / 1000:.1f}s configured) -- "
            "a hard kill of a healthy process is not the failure mode that setting exists "
            "for. See sub-test B for that."
        )
    return findings


async def _run_suspend_subtest(seat_id: int) -> tuple[list[str], bool]:
    findings = ["--- sub-test B: SUSPEND (the actual 'socket simply stops' case) ---"]
    passed = True

    started = await _start_lock_holder(seat_id)
    if isinstance(started, str):
        return [*findings, f"HARNESS FAILURE: {started}"], False
    proc = started

    acquired_at = time.monotonic()
    findings.append(f"lock_holder (pid={proc.pid}) acquired the row lock on seat {seat_id}.")
    # Freeze, don't kill -- the socket stays open and registered with the
    # OS, but nothing is left running to ever service it. See
    # actions.suspend_pid's docstring for why this, not kill_pid, is what
    # actually reproduces an unresponsive peer.
    actions.suspend_pid(proc.pid)
    findings.append(
        f"Suspended (not killed) pid={proc.pid} at t={time.monotonic() - acquired_at:.2f}s."
    )

    released_at = await _wait_for_release(seat_id)
    configured_s = settings.idle_in_transaction_session_timeout_ms / 1000

    if released_at is None:
        passed = False
        findings.append(
            f"HYPOTHESIS CONTRADICTED: the lock was still held {MAX_WAIT_S:.0f}s after "
            "suspending the process -- idle_in_transaction_session_timeout did not release "
            "it at all."
        )
    else:
        elapsed = released_at - acquired_at
        findings.append(
            f"Lock released {elapsed:.2f}s after acquisition "
            f"(configured idle_in_transaction_session_timeout: {configured_s:.1f}s)."
        )
        if elapsed > configured_s + MARGIN_S:
            passed = False
            findings.append(
                f"HYPOTHESIS CONTRADICTED: release took {elapsed:.2f}s, more than "
                f"{MARGIN_S:.0f}s past the configured {configured_s:.1f}s timeout -- the "
                "setting is not bounding release time as expected."
            )
        elif elapsed < configured_s * 0.5:
            passed = False
            findings.append(
                f"SUSPICIOUS: release took only {elapsed:.2f}s, well under half the "
                f"configured {configured_s:.1f}s timeout -- the NOWAIT probe may not be "
                "correctly detecting the lock (a false pass, not a real fast release)."
            )
        else:
            findings.append(
                "Release time is consistent with idle_in_transaction_session_timeout "
                "actually being what released it -- not instant, not indefinite, bounded, "
                "for the case (a suspended/unresponsive peer) it exists to handle."
            )

    # Best-effort cleanup: a suspended process left frozen would otherwise
    # sit forever (its own HOLD_SECONDS sleep never advances while
    # suspended) -- kill it outright now that the scenario's measurement
    # is done. taskkill /F works on a suspended process just fine on
    # Windows; no need to resume it first.
    actions.kill_pid(proc.pid)

    return findings, passed


async def _run_async(seat_ids: list[int]) -> ScenarioReport:
    kill_findings = await _run_kill_subtest(seat_ids[0])
    suspend_findings, passed = await _run_suspend_subtest(seat_ids[1])

    return ScenarioReport(
        scenario="api_worker_killed_holding_lock",
        passed=passed,
        findings=[*kill_findings, *suspend_findings],
        result=None,
    )


def run(*, base_url: str, event_id: int, seat_ids: list[int], infra: ChaosInfra) -> ScenarioReport:
    # No k6 load, no invariant polling -- see this module's own docstring
    # for why. Uses only Postgres connections (two seat ids from
    # run_all.py's already-seeded event); the API/sweeper/reconciler/
    # payment_worker infra it's handed is unused here, kept only for a
    # uniform run() signature across every scenario module.
    del base_url, event_id, infra

    # run() is called synchronously from run_all.py's run_one_async,
    # which is itself already executing inside async_main()'s single,
    # long-lived event loop (see that module's own comment on why there
    # is only ever one). Calling asyncio.run() directly here would raise
    # "cannot be called from a running event loop." A dedicated thread
    # gets this scenario's own logic a genuinely fresh loop (asyncio.run()
    # is fine there -- no loop already running on that thread), and
    # .join() blocks the caller until it finishes, matching every other
    # scenario's synchronous, blocking run() exactly.
    box: list[ScenarioReport] = []
    thread = threading.Thread(target=lambda: box.append(asyncio.run(_run_async(seat_ids[:2]))))
    thread.start()
    thread.join()
    return box[0]
