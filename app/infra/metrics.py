"""Prometheus metrics, multiprocess-mode aware.

With uvicorn running multiple worker processes sharing one listening
socket (see Makefile's run-api, UVICORN_WORKERS), an in-process
CollectorRegistry would only ever reflect whichever single worker happens
to answer a given GET /metrics scrape. lock_wait_seconds p99 would be
computed from roughly 1/workers of all requests and would visibly change
between successive scrapes of the *same* running system, with no error to
reveal that anything was wrong -- just quietly misleading numbers.

Multiprocess mode (PROMETHEUS_MULTIPROC_DIR) has every worker write its
own per-process metric files to a shared directory; GET /metrics
aggregates across all of them via MultiProcessCollector, so the numbers
reflect the whole fleet regardless of which worker happens to answer the
scrape request itself.

Limitation, not a bug: multiprocess mode does not support every metric
type identically. Gauges need an explicit multiprocess_mode ('all',
'liveall', 'min', 'max', 'sum', 'mostrecent') because "the current value"
isn't well-defined across N processes the way a sum or count is for
Counters and Histograms. We don't define any Gauges here; if one is ever
added, its multiprocess_mode must be chosen deliberately, not left to
whatever the library defaults to.
"""

from __future__ import annotations

import contextvars
import os
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from app.infra.config import settings

# Must happen before prometheus_client -- or any of its submodules -- is
# imported ANYWHERE in this process. prometheus_client.values decides
# in-process vs. multiprocess value storage by checking this env var at
# THAT SUBMODULE'S IMPORT TIME, not dynamically later and not per-metric.
# If prometheus_client had already been imported first (directly or
# transitively, by anything) without this set, every Counter/Histogram
# created afterward would silently be in-process-only -- no error, just
# empty output from render_metrics_text() below, because nothing was ever
# configured to write the per-process files multiprocess aggregation reads
# from. Every uvicorn worker re-imports this module fresh (Windows uses
# spawn, not fork, for --workers), so doing this here, this early, means
# it's correct in every worker even if nothing exported it beforehand.
os.environ.setdefault("PROMETHEUS_MULTIPROC_DIR", settings.prometheus_multiproc_dir)

from prometheus_client import (  # noqa: E402
    CollectorRegistry,
    Counter,
    Histogram,
    generate_latest,
    multiprocess,
)
from sqlalchemy import event  # noqa: E402

from app.infra.db import engine  # noqa: E402

_MULTIPROC_DIR = Path(os.environ["PROMETHEUS_MULTIPROC_DIR"])
# Ensure it exists; do NOT destructively clear it here. Clearing must
# happen exactly once, before any worker starts -- see the Makefile's
# run-api target. Clearing it here, at per-worker *import* time, would
# race: a worker that finishes starting up first and begins recording real
# metrics could have its freshly-written file deleted by a sibling worker
# that is still starting up and reaches this line later.
_MULTIPROC_DIR.mkdir(parents=True, exist_ok=True)

# Prometheus's default buckets (.005 .01 .025 .05 .075 .1 .25 .5 .75 1 2.5 5
# 7.5 10) are too coarse in exactly the range that matters most here: real
# lock waits under contention mostly land between a few ms and a few
# hundred ms, and the big gaps at the low end (e.g. nothing between 0.1 and
# 0.25) mean a p99 estimate (bucket-boundary based -- see
# loadtest/run_benchmark.py's _parse_histogram_p99_seconds) can only ever
# report one of a handful of values in that window. A project about
# measuring lock contention precisely shouldn't settle for that.
_LOCK_TIMING_BUCKETS = (
    0.001,
    0.005,
    0.01,
    0.025,
    0.05,
    0.075,
    0.1,
    0.15,
    0.2,
    0.25,
    0.3,
    0.4,
    0.5,
    0.75,
    1.0,
    2.5,
    5.0,
    float("inf"),
)

lock_wait_seconds = Histogram(
    "lock_wait_seconds",
    "Time from issuing SELECT ... FOR UPDATE to the lock actually being "
    "acquired. NOT overall request duration, and NOT pool checkout time "
    "(see pool_checkout_seconds) -- conflating the two would make it "
    "impossible to tell lock contention apart from pool exhaustion.",
    buckets=_LOCK_TIMING_BUCKETS,
)

pool_checkout_seconds = Histogram(
    "pool_checkout_seconds",
    "Time spent waiting to check out a connection from the SQLAlchemy "
    "pool, measured via the pool's own 'checkout' event -- not inferred "
    "from any statement's latency, which would blend pool-wait time into "
    "whatever else that statement measures.",
    buckets=_LOCK_TIMING_BUCKETS,
)

deadlocks_total = Counter(
    "deadlocks_total",
    "Postgres deadlock (40P01) errors encountered while locking seats. "
    "In pessimistic mode (a) (specific seats, ORDER BY id), a deadlock is "
    "impossible by construction -- this incrementing is a BUG SIGNAL (the "
    "ordering guarantee broke somehow), not a normal operational event. "
    "Alert on nonzero, the same way as "
    "oversell_blocked_total{layer='database'}.",
)

lock_timeouts_total = Counter(
    "lock_timeouts_total",
    "Postgres lock_timeout (55P03) errors -- a blocked acquire that gave "
    "up cleanly rather than hanging. Expected to be nonzero under "
    "sustained high contention; unlike deadlocks_total, this is not "
    "itself a bug signal.",
)

oversell_blocked_total = Counter(
    "oversell_blocked_total",
    "An acquisition was rejected before it could oversell a seat, broken "
    "down by which layer caught it. layer='application': the domain "
    "state machine rejected the transition (SPEC.md section 11) -- "
    "expected to be nonzero constantly under contention, every 'loser' "
    "in a race is one of these. layer='database': the DB-level partial-"
    "unique-index guard on booking_seats caught something the "
    "application layer missed -- if this is EVER nonzero, application "
    "logic has a bug. Dormant for now: booking_seats isn't written until "
    "Phase 5's confirm/booking path exists, so 'database' has no firing "
    "point yet in Phase 1/2.",
    ["layer"],
)

# --- pool checkout wait, wired to the pool's own checkout event -----------
#
# SQLAlchemy's Pool exposes "checkout" (fires once a connection has been
# handed to the caller) and "checkin" (fires when one is returned) -- there
# is no separate "a caller started waiting" event, because the pool only
# knows about connections, not about who is asking for one. To measure the
# CALLER's wait, we bridge the two: set a contextvar immediately before
# asking for a connection, and read it back inside the checkout event
# handler, which fires synchronously within that same call (SQLAlchemy's
# async engine bridges to this sync event via greenlet within the same
# task, so contextvars set just before the call are visible inside the
# handler). This measures exactly [asked for a connection] -> [pool's own
# event confirming one was handed over], never conflated with whatever
# statement runs afterward.
_checkout_requested_at: contextvars.ContextVar[float] = contextvars.ContextVar(
    "checkout_requested_at"
)


@asynccontextmanager
async def timed_checkout() -> AsyncIterator[None]:
    """Wrap the first `session.execute()` (or `engine.connect()`) of a
    request -- whichever actually triggers the pool to hand out a
    connection -- so pool_checkout_seconds reflects genuine wait time.
    """
    token = _checkout_requested_at.set(time.monotonic())
    try:
        yield
    finally:
        _checkout_requested_at.reset(token)


def _on_pool_checkout(
    dbapi_connection: object, connection_record: object, connection_proxy: object
) -> None:
    requested_at = _checkout_requested_at.get(None)
    if requested_at is not None:
        pool_checkout_seconds.observe(time.monotonic() - requested_at)


event.listens_for(engine.sync_engine, "checkout")(_on_pool_checkout)


def render_metrics_text() -> bytes:
    """Aggregate every worker's per-process metric files into one
    Prometheus text-format payload. See module docstring for why this is
    necessary at all under --workers > 1.
    """
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    return generate_latest(registry)
