"""Failure injection primitives for the chaos suite -- one function per
verb, each returning (or taking) enough state to be reversed cleanly.

Windows note (SPEC.md section 10 / this phase's task): `tc netem` is not
available here. Every injection below is `docker kill` / `docker pause` /
`docker restart`, or an OS-level process kill for the one scenario
(api_worker_killed) that isn't about a container at all -- all portable.

Containers are never referenced by a hardcoded name (docker-compose.yml
does not set container_name:, and Compose's own naming includes the
project directory name, which is not fixed) -- always resolved via
`docker compose ps -q <service>` at call time.
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

import psutil

REPO_ROOT = Path(__file__).resolve().parents[2]


def _compose(*args: str) -> str:
    """Run `docker compose <args>` from the repo root (where
    docker-compose.yml lives) and return stripped stdout. Raises on a
    non-zero exit -- a failed injection must not silently no-op.
    """
    result = subprocess.run(  # noqa: S603
        ["docker", "compose", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def container_id(service: str) -> str:
    container = _compose("ps", "-q", service)
    if not container:
        raise RuntimeError(
            f"docker compose ps -q {service} returned nothing -- is `docker compose up -d` running?"
        )
    return container


def docker_kill(service: str) -> None:
    """SIGKILL the container process -- no graceful shutdown, no chance to
    flush anything. This is what scenario (a)/(c) mean by "killed": an
    abrupt process death, distinct from (b)'s pause (a hang).
    """
    _compose("kill", service)


def docker_pause(service: str) -> None:
    """Freeze every process in the container via the cgroup freezer. The
    container's network namespace stays up at the kernel level (see
    app/infra/config.py's comment on redis_socket_timeout_seconds) -- new
    TCP connections can still complete their handshake, but nothing
    inside the container ever responds. This is the HANG case, not a
    crash -- distinct from docker_kill.
    """
    _compose("pause", service)


def docker_unpause(service: str) -> None:
    _compose("unpause", service)


def docker_restart(service: str, *, timeout_seconds: int = 30) -> None:
    """Graceful-then-forced restart via Compose. Used for the Postgres
    scenario -- a restart, not a kill, because the hypothesis under test
    is "the pool recovers without an API restart," which is best modeled
    by the exact failure mode a real deployment sees during a Postgres
    failover or maintenance restart.
    """
    _compose("restart", "--timeout", str(timeout_seconds), service)


def docker_start(service: str) -> None:
    """Bring a killed (docker_kill'd) container back up. For Redis
    specifically (docker-compose.yml: --save "" --appendonly no, no
    volume), this is what makes "killed and restarted EMPTY" (scenario c)
    literally true -- there is no data to reload, by construction.
    """
    _compose("start", service)


def wait_for_container_running(service: str, *, timeout_seconds: float = 15.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        status = _compose("ps", "--format", "{{.State}}", service)
        if status.strip().lower() == "running":
            return
        time.sleep(0.25)
    raise RuntimeError(f"{service} did not report 'running' within {timeout_seconds}s")


def find_one_uvicorn_worker_pid(master_pid: int) -> int:
    """Find exactly one of the `--workers N` child processes of the
    uvicorn master process started by loadtest.run_benchmark.start_api
    (or the Makefile's run-api target, if master_pid is discovered some
    other way). Picks the first live child found -- which one is killed
    doesn't matter for the hypothesis (surviving workers absorb the
    load); only that it is a worker, not the master itself.

    Raises if there are no children, e.g. the master hasn't finished
    forking its workers yet -- callers should retry briefly rather than
    treat that as "there is nothing to kill."
    """
    master = psutil.Process(master_pid)
    children = [child for child in master.children(recursive=True) if child.is_running()]
    if not children:
        raise RuntimeError(
            f"uvicorn master pid={master_pid} has no live child worker processes yet"
        )
    return children[0].pid


def kill_pid(pid: int) -> None:
    """Hard-kill one process by PID -- SIGKILL-equivalent, no graceful
    shutdown, matching "kill one of the 4 uvicorn workers" (a crash, not
    a controlled stop) rather than sending it SIGTERM and waiting.
    """
    if sys.platform == "win32":
        subprocess.run(  # noqa: S603, S607
            ["taskkill", "/F", "/PID", str(pid)], capture_output=True, check=False
        )
    else:
        psutil.Process(pid).kill()


def pid_is_running(pid: int) -> bool:
    return psutil.pid_exists(pid)


def suspend_pid(pid: int) -> None:
    """Freeze a process WITHOUT killing it (NtSuspendProcess on Windows,
    SIGSTOP on POSIX, both via psutil) -- the process-level analogue of
    docker_pause, and the one that actually reproduces "the socket
    simply stops, no clean disconnect": killing a process (even hard)
    lets the OS clean up its open file descriptors, including sockets,
    on the way out -- the kernel sends the peer a normal close, and
    Postgres notices almost immediately. A SUSPENDED process's socket is
    still open and registered with the OS, but nothing is left running
    to ever use it -- indistinguishable, from Postgres's side, from a
    network partition or a frozen host. Confirmed directly while
    building loadtest/chaos/scenarios/api_worker_killed_holding_lock.py:
    killing the lock-holder released its row lock in well under a
    second, not anywhere near idle_in_transaction_session_timeout --
    that first result is what led to adding this function.
    """
    psutil.Process(pid).suspend()


def count_live_children(master_pid: int) -> int:
    """How many live child processes `master_pid` currently has -- used
    as a best-effort "worker count" data point (before/after a kill),
    not as a correctness signal in its own right.
    """
    try:
        master = psutil.Process(master_pid)
    except psutil.NoSuchProcess:
        return 0
    return len([child for child in master.children(recursive=True) if child.is_running()])
