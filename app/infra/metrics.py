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
    Gauge,
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
    "Postgres deadlock (40P01) errors encountered while locking/updating "
    "seats. Shared across two strategies with OPPOSITE meanings for the "
    "same event -- see each strategy module for which applies: "
    "in pessimistic mode (a) (specific seats, ORDER BY id), a deadlock is "
    "impossible by construction -- this incrementing is a BUG SIGNAL (the "
    "ordering guarantee broke somehow), not a normal operational event, "
    "and should alert on nonzero the same way as "
    "oversell_blocked_total{layer='database'}. In optimistic mode "
    "(app/inventory/strategies/optimistic.py's multi-row unnest() UPDATE, "
    "which cannot express ORDER BY the way FOR UPDATE can), a deadlock is "
    "an expected-but-rare event under multi-seat contention -- it is "
    "caught and retried, not a bug signal, and should NOT alert on a "
    "single occurrence the way it would for pessimistic mode (a).",
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


optimistic_conflicts_total = Counter(
    "optimistic_conflicts_total",
    "An optimistic acquire attempt's UPDATE matched fewer rows than "
    "requested -- someone else changed at least one seat's version "
    "between our read and our write. Incremented once per conflicting "
    "attempt, including the final attempt of an exhausted acquire() call "
    "-- see optimistic_retries_total for the (slightly smaller) count of "
    "conflicts that actually triggered another attempt.",
)

optimistic_retries_total = Counter(
    "optimistic_retries_total",
    "A conflict was followed by an actual retry (attempts remained in "
    "the budget). Strictly <= optimistic_conflicts_total -- a conflict on "
    "the LAST allowed attempt increments conflicts but not this, since "
    "there is no further attempt to make.",
)

optimistic_exhausted_total = Counter(
    "optimistic_exhausted_total",
    "The retry budget (Settings.optimistic_max_attempts) was reached "
    "without a successful acquire. This is the self-inflicted-DoS guard "
    "actually firing: a clean domain failure, not an infinite loop and "
    "not a 500.",
)

# Buckets are the discrete integer attempt counts themselves (1..10), NOT
# Prometheus's default time-duration-shaped buckets (.005 .01 .025 ... 5
# 7.5 10). "Attempts until success" is a small integer, and comparing an
# integer like 2 or 3 against duration buckets whose first ten boundaries
# are all < 1.0 would dump nearly every real observation into the last two
# or three buckets (1.0, 2.5, 5.0) -- destroying exactly the resolution
# ("did most successes need 1 attempt or 3?") this histogram exists to
# capture. max_attempts defaults to 5, so 1..10 comfortably covers even a
# reconfigured, larger retry budget.
optimistic_attempts = Histogram(
    "optimistic_attempts",
    "Number of attempts (reads+UPDATE cycles) an optimistic acquire() "
    "call needed before it succeeded. Only observed on success -- an "
    "exhausted call is not 'infinity attempts,' it's optimistic_exhausted"
    "_total.",
    buckets=(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
)

# --- hold sweeper (workers/sweeper.py) ----------------------------------
#
# The sweeper is a THIRD writer, alongside whichever booking strategy is
# under test -- it takes locks (FOR UPDATE SKIP LOCKED) and contends for
# the same rows. Its own metrics are tracked separately, never folded into
# a strategy's, so a benchmark run can report what fraction of database
# work was the sweeper's rather than contention between bookers -- see
# workers/sweeper.py's module docstring and
# docs/benchmarks/phase3-crossover.md for why that fraction matters. The
# sweeper process shares this SAME PROMETHEUS_MULTIPROC_DIR with whichever
# API instance is running (set via the same env var, see
# workers/sweeper.py), so these show up in that API's own GET /metrics
# scrape automatically -- no separate scrape endpoint needed.
sweeper_seats_expired_total = Counter(
    "sweeper_seats_expired_total",
    "Seats transitioned HELD -> AVAILABLE by the sweeper because their "
    "hold had expired. Summed across every batch, every pass.",
)

sweeper_batch_duration_seconds = Histogram(
    "sweeper_batch_duration_seconds",
    "Wall-clock time for one whole sweeper pass (the SELECT ... FOR "
    "UPDATE SKIP LOCKED through COMMIT/ROLLBACK), regardless of how many "
    "seats it found. Its sum, as a fraction of a benchmark run's "
    "duration, is what 'sweeper share of DB time' means here -- an upper "
    "bound on how much of the run the sweeper was actively doing work, "
    "not a literal accounting of Postgres-side CPU time.",
    buckets=_LOCK_TIMING_BUCKETS,
)

sweeper_lock_wait_seconds = Histogram(
    "sweeper_lock_wait_seconds",
    "Time from issuing the sweeper's SELECT ... FOR UPDATE SKIP LOCKED "
    "to getting rows back. Deliberately a SEPARATE metric from "
    "lock_wait_seconds (pessimistic strategy's per-acquisition lock "
    "wait) -- conflating the two would make it impossible to tell "
    "whether observed lock contention came from bookers racing each "
    "other or from the sweeper racing bookers.",
    buckets=_LOCK_TIMING_BUCKETS,
)

sweeper_illegal_transition_total = Counter(
    "sweeper_illegal_transition_total",
    "A sweeper pass's expire() call raised IllegalTransition for a "
    "candidate row -- e.g. a concurrent booker legitimately reclaimed an "
    "expired hold (or something else changed its status) between this "
    "pass's SELECT ... FOR UPDATE SKIP LOCKED and the expire() call a few "
    "lines later. EXPECTED, not an error: SKIP LOCKED should make this "
    "rare (the row is locked for the whole window between read and "
    "write), so this is the safety net catching whatever narrow race "
    "SKIP LOCKED doesn't cover, not the routine path. Logged at debug,"
    " counted here, never raised -- one unexpected row must never abort "
    "an entire batch.",
)

# multiprocess_mode='mostrecent': there is exactly one sweeper process
# (unlike uvicorn's --workers N), so "the current value" is unambiguous --
# whichever process last measured it. 'sum'/'max' would be wrong here (this
# is a single source of truth, not readings from several workers to
# combine); 'mostrecent' is the one mode that means "the last observation
# wins," matching a single periodically-refreshed snapshot gauge exactly.
sweeper_backlog_gauge = Gauge(
    "sweeper_backlog",
    "Count of seats currently HELD with hold_expires_at already passed, "
    "not yet swept -- i.e. how far behind the sweeper is RIGHT NOW. "
    "Measured by workers/sweeper.py's measure_backlog(), called at the "
    "start of every sweep pass AND independently of whether a pass is "
    "running at all (see that function) -- this is how a stopped or "
    "overloaded sweeper is detected: this number rises with nothing "
    "sweeping it back down, rather than only being knowable the next "
    "time a pass happens to run.",
    multiprocess_mode="mostrecent",
)

# --- Redis hold-mirror cache (app/infra/hold_cache.py) ------------------
hold_cache_errors_total = Counter(
    "hold_cache_errors_total",
    "A Redis operation against the hold mirror (set/get/delete) failed --"
    " connection refused, timeout, or any other RedisError. Never raised "
    "to the caller (CLAUDE.md rule 4: Redis is a cache, Postgres is the "
    "source of truth -- a hold must still succeed in Postgres even if its "
    "mirror write fails); this counter is how that silent degrade-to-"
    "correct-but-slower is made observable instead of literally silent.",
    ["operation"],
)

# --- reconciler (workers/reconciler.py) ---------------------------------
reconciliation_divergence_total = Counter(
    "reconciliation_divergence_total",
    "Postgres/Redis divergence CONFIRMED (see reconciliation_transient_"
    "total for candidates that resolved on their own) and repaired by the "
    "reconciler, by kind. Postgres always wins -- this counts repairs "
    "made TO Redis, never the reverse. redis_key_missing_for_held_seat: "
    "Postgres says HELD, no mirror key exists (e.g. a hold succeeded but "
    "its mirror write failed -- see hold_cache_errors_total). "
    "redis_key_present_for_unheld_seat: a mirror key exists for a seat "
    "Postgres no longer considers HELD (e.g. the sweeper's Redis delete "
    "failed after its Postgres commit succeeded -- see workers/"
    "sweeper.py's ordering comment). redis_session_mismatch: a mirror key "
    "exists but names a DIFFERENT session than Postgres's "
    "held_by_session_id -- the one kind where Redis serves an actively "
    "WRONG answer (telling the wrong session it holds a seat) rather than "
    "a merely stale one; can only arise from a stale key surviving an "
    "expire-and-reacquire cycle. Worth a resume line by itself (SPEC.md "
    "section 5): it says the system assumed its own cache would drift and "
    "instrumented for it. This is the metric to alert on -- it only "
    "increments after confirm-on-second-look, so it should not need its "
    "threshold raised to absorb read-timing noise the way an immediate, "
    "unconfirmed count would.",
    ["kind"],
)

reconciliation_transient_total = Counter(
    "reconciliation_transient_total",
    "A discrepancy observed on the reconciler's first, non-atomic read "
    "across Postgres and Redis, but which had already resolved itself by "
    "the time of the confirm-on-second-look re-read -- a seat caught "
    "mid-transition, not real drift. NOT repaired (nothing to repair) and "
    "NOT counted in reconciliation_divergence_total. Same labels/kinds as "
    "that counter, tracked separately so the alerting signal stays clean: "
    "a divergence counter with false positives gets its threshold raised "
    "until it stops firing, at which point real drift becomes invisible "
    "too. A high reconciliation_transient_total alongside a low "
    "reconciliation_divergence_total is a healthy, expected pattern under "
    "real load, not a problem -- it is confirm-on-second-look doing "
    "exactly what it is for.",
    ["kind"],
)


# --- idempotency (SPEC.md section 6, app/infra/idempotency.py) ---------
idempotent_replay_total = Counter(
    "idempotent_replay_total",
    "A request arrived with an Idempotency-Key already COMPLETED with a "
    "matching fingerprint -- the stored response was returned verbatim, "
    "with no re-execution. Expected to be nonzero under normal client "
    "retry behaviour (e.g. a client that never saw the first response due "
    "to a network blip); it is not itself a bug signal.",
)

idempotency_conflict_total = Counter(
    "idempotency_conflict_total",
    "A request reused an Idempotency-Key already seen with a DIFFERENT "
    "request fingerprint -- rejected 422. This is a client bug (SPEC.md "
    "section 6): the same key must always mean the same request. Should "
    "be at or near zero in a correctly-behaving client population.",
)

idempotency_in_progress_total = Counter(
    "idempotency_in_progress_total",
    "A request reused an Idempotency-Key whose original request is still "
    "IN_PROGRESS -- rejected 409 with Retry-After. Expected under a "
    "client that retries too eagerly (before the original request could "
    "possibly have finished) or genuinely concurrent duplicate submission "
    "(e.g. a double-click); not itself a bug signal.",
)

idempotency_stale_keys_reaped_total = Counter(
    "idempotency_stale_keys_reaped_total",
    "workers/idempotency_reaper.py found an IN_PROGRESS idempotency_keys "
    "row past Settings.idempotency_stale_timeout_seconds with NO booking "
    "carrying that key -- the original request crashed before doing any "
    "durable work. Marked FAILED so a client retry is free to execute "
    "for real. See idempotency_stale_keys_recovered_total for the OTHER "
    "outcome of a stale scan (a booking DOES exist).",
)

idempotency_stale_keys_recovered_total = Counter(
    "idempotency_stale_keys_recovered_total",
    "workers/idempotency_reaper.py found an IN_PROGRESS idempotency_keys "
    "row past timeout WHERE a booking already carries that key -- the "
    "booking write itself succeeded and only the completion marker was "
    "lost (e.g. a crash between the booking committing and the key being "
    "marked COMPLETED). Recovered to COMPLETED with a response "
    "reconstructed from the booking's current state, rather than being "
    "marked FAILED -- marking it FAILED here would let a client retry "
    "re-execute a request that already succeeded and double-book. "
    "Nonzero values mean real crash recovery happened, which is the "
    "system working as designed, not a bug signal by itself -- but a "
    "sustained nonzero rate alongside no corresponding rate of actual "
    "process crashes/restarts would be worth investigating.",
)

# --- payment webhooks (SPEC.md section 7, app/payments/) ----------------
webhook_events_total = Counter(
    "webhook_events_total",
    "Every payment webhook request that reached signature verification, "
    "by event type and outcome (outcome='accepted': durably inserted; "
    "'duplicate': provider_event_id already seen, see "
    "webhook_duplicate_total; 'unresolved': inserted but booking_id did "
    "not resolve, see webhook_unresolved_total). Signature failures are "
    "NOT included here (see webhook_signature_failures_total) -- an "
    "unauthenticated request never reaches far enough to have a "
    "meaningful event_type.",
    ["type", "outcome"],
)

webhook_duplicate_total = Counter(
    "webhook_duplicate_total",
    "A webhook's provider_event_id unique-violated on INSERT -- already "
    "processed (or already durably queued). Returned 200 immediately, "
    "same as a fresh accept (SPEC.md section 7: never make a provider "
    "distinguish 'duplicate' from 'accepted' by status code, or its own "
    "retry behaviour turns into a retry storm against a 5xx). Expected "
    "to be nonzero under normal provider behaviour -- most providers "
    "retry aggressively by design.",
)

webhook_unresolved_total = Counter(
    "webhook_unresolved_total",
    "A webhook's payload did not carry a booking_id that resolves to an "
    "existing booking (missing, malformed, or referencing an id that "
    "does not exist -- e.g. a test event, or an event replayed from a "
    "different environment's data). Still durably inserted with "
    "processing_status='UNRESOLVED' and returned 200 -- rejecting it "
    "would make a legitimate provider event retry forever, the same "
    "retry-storm failure mode a 500 on a duplicate would cause.",
)

webhook_signature_failures_total = Counter(
    "webhook_signature_failures_total",
    "HMAC verification failed over the raw request body -- rejected 401, "
    "nothing inserted. Should be at or near zero from the real provider; "
    "sustained nonzero values mean either a misconfigured shared secret "
    "or a genuine forgery attempt, and this is the metric to alert on "
    "for the latter.",
)

late_payment_refund_required_total = Counter(
    "late_payment_refund_required_total",
    "A payment.succeeded event was applied to a booking whose hold had "
    "already expired and been resold to a different booking by the time "
    "the payment cleared (SPEC.md section 7's 'late-success case'). The "
    "booking moved to REFUND_REQUIRED; the seat was NOT touched -- money "
    "is reversible, a seat someone else now legitimately holds is not. "
    "Same fail-toward-the-recoverable-side principle as "
    "workers/sweeper.py's Postgres-then-Redis delete ordering and Phase "
    "4's lazy expiry. Never expected to be zero forever under real "
    "payment-gateway latency variance, but a sustained high rate would "
    "point at hold_duration_seconds being too short relative to actual "
    "payment processing time.",
)

# --- realtime seat map (SPEC.md section 9, app/realtime/) ---------------
#
# multiprocess_mode="livesum": with 4 uvicorn workers, each worker holds
# some subset of all open WebSocket connections -- unlike
# sweeper_backlog_gauge (one process is ever the source of truth) this is
# genuinely N independent live counts that should be SUMMED for the
# fleet-wide total, and "livesum" is specifically "sum across processes
# that are still alive" (a worker that crashed shouldn't keep
# contributing a stale count forever, which plain "sum" would allow).
ws_connections_gauge = Gauge(
    "ws_connections",
    "Currently open WebSocket connections, summed across all live "
    "uvicorn workers.",
    multiprocess_mode="livesum",
)

ws_messages_sent_total = Counter(
    "ws_messages_sent_total",
    "One increment per actual WebSocket send() call -- i.e. per "
    "connected client that received a given section's coalesced diff, "
    "not per underlying seat change. This is the number the coalescing "
    "buffer exists to keep independent of how many raw seat-change "
    "events occurred, not independent of how many clients are "
    "connected (see ws_events_coalesced_total for the other half of "
    "that story).",
)

ws_events_coalesced_total = Counter(
    "ws_events_coalesced_total",
    "Raw seat-change events absorbed into a combined broadcast (or into "
    "no broadcast at all, when a seat's net state across the window was "
    "unchanged) rather than sent as their own message. A high rate here "
    "relative to ws_messages_sent_total is the coalescing buffer doing "
    "its job -- see app/realtime/coalescer.py's module docstring for "
    "why this is the main scaling mechanism, not an optimisation on top "
    "of one.",
)

ws_broadcast_duration_seconds = Histogram(
    "ws_broadcast_duration_seconds",
    "Time to serialize one section's coalesced diff and send it to "
    "every locally-connected subscriber of that section, once per "
    "coalescing window per section (not per client) -- this is exactly "
    "the cost coalescing makes O(clients) per tick instead of "
    "O(events x clients).",
    buckets=_LOCK_TIMING_BUCKETS,
)


def render_metrics_text() -> bytes:
    """Aggregate every worker's per-process metric files into one
    Prometheus text-format payload. See module docstring for why this is
    necessary at all under --workers > 1.
    """
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    return generate_latest(registry)
