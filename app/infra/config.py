"""Application settings, loaded from environment variables / .env."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration. See .env.example for the full set of keys."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    env: str = "development"
    log_level: str = "INFO"

    postgres_user: str = "seatlock"
    postgres_password: str = "seatlock_local_dev"
    postgres_db: str = "seatlock"
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    database_url: str = "postgresql+asyncpg://seatlock:seatlock_local_dev@localhost:5432/seatlock"

    # Postgres session-level GUC, set on every connection this app's own
    # engine opens (app/infra/db.py, via asyncpg's server_settings) --
    # NOT a cluster-wide postgresql.conf change, deliberately: a slow but
    # legitimate Alembic migration or a testcontainers session must not
    # get killed by a timeout scoped to the API/worker connection pool.
    #
    # Phase 8a's chaos suite (scenario e, api_worker_killed) passed
    # WITHOUT this setting -- but only because this codebase's
    # transactions are all short (a row lock held for a handful of
    # milliseconds). That is not a general guarantee: a worker
    # hard-killed mid-transaction leaves Postgres holding a connection
    # that is idle in transaction FOREVER from Postgres's point of view --
    # there was no clean disconnect, the socket simply stopped responding,
    # and Postgres has no way to distinguish "client is thinking" from
    # "client is dead" except a timeout. Without one, resolution falls
    # back to the OS's TCP keepalive defaults, which are on the order of
    # TWO HOURS -- during which every row lock that transaction held
    # (e.g. every seat in a multi-seat hold) blocks every other booking
    # attempt on those exact seats.
    #
    # 5000ms: every real transaction in this codebase (SELECT ... FOR
    # UPDATE, the optimistic conditional UPDATE, a booking confirm) is
    # sub-100ms end to end -- 5s is generous headroom against real work,
    # while being a ~1400x tighter bound than the TCP keepalive fallback
    # it replaces. See loadtest/chaos/scenarios/
    # api_worker_killed_holding_lock.py for the scenario that opens a
    # transaction, holds it well past this timeout, hard-kills the
    # process holding it, and asserts the lock is released within this
    # bound rather than indefinitely.
    idle_in_transaction_session_timeout_ms: int = 5000

    redis_url: str = "redis://localhost:6379/0"
    # Bounds on every Redis command/connection this process issues.
    # UNCONFIGURED, these are unbounded -- and a paused (not killed) Redis
    # is a real difference: `docker pause` freezes the redis-server
    # process via the cgroup freezer, but the container's network
    # namespace stays up at the kernel level, so a NEW TCP connection can
    # still complete its handshake (the kernel accepts it into the listen
    # backlog independently of the frozen process ever calling accept()).
    # The result is a connection that looks alive but never answers --
    # exactly the "hang, not a crash" case Phase 8's chaos suite is built
    # to find. Both app/infra/hold_cache.py's mirror SET and
    # app/realtime/pubsub.py's PUBLISH are awaited INLINE in the booking
    # request path (create_hold/extend_hold/confirm_booking), so without
    # these timeouts a paused Redis turns directly into hung API requests
    # and, eventually, an exhausted event loop -- a cache outage becoming
    # an API outage. redis-py's TimeoutError subclasses RedisError, so
    # every existing `except RedisError` call site already catches it with
    # no changes needed there.
    #
    # 2.0s each: long enough that a real Redis round-trip (even under
    # load, even reconnecting) never trips it, short enough that a paused
    # Redis costs a bounded ~2-4s per request (two sequential Redis
    # touches on the hold path: mirror-set, then publish) instead of
    # forever. See docs/chaos-results.md (scenario b) for the measured
    # effect of this value on booking throughput during a Redis pause.
    redis_socket_timeout_seconds: float = 2.0
    redis_socket_connect_timeout_seconds: float = 2.0

    # Which SeatAcquisitionStrategy app/inventory/strategies/base.py wires
    # up. Only "naive" exists (Phase 1); "pessimistic" and "optimistic"
    # raise NotImplementedError so the selection wiring is proven correct
    # before there's anything real behind the other two branches.
    strategy: str = "naive"

    # Widens the naive strategy's read-then-write race window so the
    # oversell it causes is reproducible on demand, not just probabilistic
    # under natural scheduling jitter. 0 leaves the bug real but timing-
    # dependent. See app/inventory/strategies/naive.py.
    naive_race_window_ms: int = 0

    # How long a hold lasts once acquired (SPEC.md section 5: 8 minutes).
    # float, not int: the Phase 3 recirculating-contention benchmark
    # overrides this to sub-integer values (e.g. 1.5s) so inventory
    # cycles fast enough to observe within a short load-test burst -- a
    # benchmarking configuration, never a product one. 480 (8 minutes)
    # remains the actual product default.
    hold_duration_seconds: float = 480.0

    # Uvicorn worker count for `make run-api` (see Makefile). Each worker
    # is a separate process with its own connection pool, so the real
    # ceiling is workers * (pool_size + max_overflow), which must stay
    # comfortably below Postgres's max_connections (default 100) — leave
    # headroom for psql, Alembic, and the testcontainers suite running
    # alongside it. 4 * (10 + 5) = 60 is the default budget here.
    uvicorn_workers: int = 4
    pool_size: int = 10
    max_overflow: int = 5

    # How long a blocked SELECT ... FOR UPDATE waits before Postgres gives
    # up and raises lock_not_available (55P03), rather than hanging
    # indefinitely. See app/inventory/strategies/pessimistic.py.
    pessimistic_lock_timeout_ms: int = 5000

    # Optimistic strategy (Strategy C) retry policy. base is the full-
    # jitter backoff base (seconds); max_attempts is the retry budget --
    # unbounded retries under sustained contention are a self-inflicted
    # DoS. See app/inventory/strategies/optimistic.py.
    optimistic_backoff_base_seconds: float = 0.05
    optimistic_max_attempts: int = 5
    # Ablation switch (SPEC.md section 4 / Phase 3 plan item 5): full
    # jitter vs. fixed backoff is a MEASURED result, not an asserted one --
    # this flag is what the loadtest harness flips between the two runs.
    optimistic_full_jitter: bool = True

    # Repo-relative (not an absolute temp path) so the Makefile's run-api
    # target and this process resolve the SAME directory regardless of
    # which shell/OS temp-dir convention is in play -- see
    # app/infra/metrics.py's module docstring for why this must be cleared
    # exactly once, before any worker starts (Makefile does that; this
    # setting is just the shared path both sides agree on).
    prometheus_multiproc_dir: str = ".prometheus-multiproc"

    # Hold sweeper (SPEC.md section 5 / I3, workers/sweeper.py). 5s is
    # SPEC.md's own production guidance (5-10s) -- these are PRODUCT
    # defaults. They can be this relaxed, rather than sub-second, BECAUSE
    # lazy expiry is the actual enforcement mechanism (Phase 4): every
    # read path that reports or acts on seat availability treats a HELD
    # row whose hold_expires_at has already passed as available, whether
    # or not the sweeper has physically gotten to it yet (see
    # app/inventory/strategies/pessimistic.py's acquire_any_n and
    # app/api/routes/admin.py's seat-status-counts). The sweeper's job is
    # only to eventually make the ROW ITSELF agree with what every reader
    # already treats as true -- cleanup, not the correctness mechanism.
    # That is what makes a multi-second interval safe here at all: I3
    # ("no seat stays HELD past hold_expires_at beyond one sweeper
    # interval") is about the ROW's status column converging, not about
    # whether the seat is reclaimable in the meantime -- it always is.
    #
    # The Phase 3 recirculating-contention benchmark overrides the
    # interval to 100ms via env var (never by editing this default) for a
    # reason specific to benchmarking, not production: it needed inventory
    # to visibly, repeatedly recirculate within a short (10-20s) load-test
    # burst so contention could be observed for most of the run instead of
    # once at the start -- a load-test-duration constraint that does not
    # exist in production, where holds simply expire and get reclaimed
    # (lazily, or eventually swept) on whatever timescale users actually
    # act on. See docs/benchmarks/phase3-crossover.md for the values used
    # and how they were chosen.
    sweeper_interval_seconds: float = 5.0
    # Larger batches hold more row locks simultaneously (FOR UPDATE SKIP
    # LOCKED on up to this many rows per pass), which can queue bookers
    # trying to acquire one of those same rows; smaller batches free rows
    # faster but may not keep up with a large backlog. 100 is a starting
    # point, not a derived optimum -- tune against observed
    # sweeper_backlog_gauge in a real deployment.
    sweeper_batch_size: int = 100

    # How often the reconciler (workers/reconciler.py) compares Redis
    # hold-mirror keys against Postgres and repairs divergence. Minutes,
    # not seconds -- SPEC.md section 5: "runs every few minutes." Default
    # here is more frequent (60s) for faster feedback in development; a
    # real deployment can widen it.
    reconciler_interval_seconds: float = 60.0

    # Confirm-on-second-look (see workers/reconciler.py's module
    # docstring for the full reasoning): the reconciler cannot read
    # Postgres and Redis atomically, so a single observation of a
    # discrepancy is a CANDIDATE, not a finding -- it may just be a seat
    # mid-transition. After finding candidates, it waits this long, then
    # re-reads both stores before deciding to repair-and-count or
    # dismiss-as-transient. 500ms is enough for an in-flight
    # request/sweep pass to finish; not so long that a real divergence
    # sits unrepaired for a meaningfully longer window.
    reconciler_confirm_delay_seconds: float = 0.5

    # A HELD seat whose updated_at is within this many seconds of "now"
    # is skipped entirely for this pass -- a row changing right now is
    # likely a transition in flight, not drift, and will be caught next
    # pass if it's real. Distinct from reconciler_confirm_delay_seconds:
    # this filters out candidates before they're even considered: no
    # log line, no wasted confirm-delay wait, on the class of divergence
    # most likely to be pure timing noise.
    reconciler_recent_change_grace_seconds: float = 2.0

    # --- Phase 5: idempotency (SPEC.md section 6) ---------------------
    #
    # How long an IN_PROGRESS idempotency_keys row can sit unfinished
    # before workers/idempotency_reaper.py treats it as abandoned (a
    # crashed request, not a slow one). 60s per the Phase 5 plan --
    # comfortably longer than any booking/confirm transaction should ever
    # take, short enough that a genuinely stuck client isn't blocked for
    # long behind a 409.
    idempotency_stale_timeout_seconds: float = 60.0
    # Deliberately shorter than the timeout above, not equal to it: if the
    # reaper only ran once per timeout window, a key could sit reapable
    # for up to one full extra interval before anything noticed. Running
    # at roughly half the timeout bounds that detection lag without
    # scanning needlessly often.
    idempotency_reaper_interval_seconds: float = 30.0
    # SKIP LOCKED batch size for the reaper's scan -- same tradeoff as
    # Settings.sweeper_batch_size (workers/sweeper.py), reused here rather
    # than invented fresh: this table is expected to be far smaller and
    # far less hot than seats, so no separate tuning knob is justified
    # yet.
    idempotency_reaper_batch_size: int = 100
    # How long an idempotency_keys row is considered valid for replay
    # after COMPLETED -- SPEC.md section 3 specifies the expires_at
    # column but not a value. 24h: long enough to cover any realistic
    # client retry window (including a user closing their laptop and
    # retrying the next morning), short enough that this table doesn't
    # grow unboundedly forever with no retention story. No cleanup job
    # consumes this yet (see IdempotencyKeyRow's own docstring) -- the
    # column and this default exist so one can be added later without a
    # schema change.
    idempotency_key_ttl_seconds: float = 86400.0

    # --- Phase 5: payment webhooks (SPEC.md section 7) -----------------
    #
    # HMAC-SHA256 secret shared with the (mocked, per SPEC.md section 15's
    # declared scope cut) payment provider. INSECURE PLACEHOLDER DEFAULT --
    # every real deployment must override this via .env; it is not
    # validated as non-default here for the same reason
    # postgres_password isn't: this project's threat model stops at "the
    # signature check exists and is exercised by tests," not secret
    # management.
    webhook_hmac_secret: str = "dev-webhook-secret-change-me"
    # workers/payment_worker.py's poll interval for unprocessed
    # payment_events. Short: SPEC.md section 7's "fast ack, async
    # process" means the ack (webhook route's 200) is already fast --
    # this just controls how quickly the ASYNC effect follows it.
    payment_worker_interval_seconds: float = 5.0
    payment_worker_batch_size: int = 100

    # --- Phase 7: realtime seat map (SPEC.md section 9) ----------------
    #
    # The coalescing window (app/realtime/coalescer.py): raw seat-change
    # events for one (event, section) accumulate here, and one combined
    # diff is broadcast per window instead of one message per event. This
    # is the mechanism that keeps broadcast cost O(clients) per tick
    # rather than O(events x clients) -- see that module's own docstring.
    # 100ms is short enough that a hold/release still feels instant to a
    # human, long enough to actually absorb a burst (a flash-sale-style
    # run of holds/releases on the same seats within one tick) into a
    # single serialize-and-send per section.
    ws_coalesce_window_ms: float = 100.0

    # --- Phase 9: interactive walkthrough (SPEC.md has no section for
    # this yet -- it is a portfolio/interview artifact, not a product
    # feature) ----------------------------------------------------------
    #
    # app/api/routes/demo.py's whole router is gated behind this,
    # defaulting OFF: those endpoints let a caller choose the
    # deliberately-broken naive strategy per request (POST /api/demo/race
    # takes `strategy` in its body) and spawn arbitrary concurrent
    # acquisition load against one seat. Exposing that ungated would let
    # any caller pick the unsafe path against a real deployment -- fine
    # for a local/interview demo, never fine as a default. Every demo
    # route 404s (not 403 -- the routes should look like they don't
    # exist, not like a locked door worth trying to pick) when this is
    # false.
    demo_mode: bool = False
    # Only ever used by the demo hold-lifecycle endpoint
    # (POST /api/demo/hold) -- a walkthrough for an interviewer needs a
    # hold that visibly expires within the ~90 seconds the whole page is
    # meant to take, not the product's real 480s
    # (Settings.hold_duration_seconds, left untouched by this feature
    # entirely). 8s: long enough to read the countdown and understand
    # what's being shown, short enough that nobody sits waiting.
    demo_default_hold_duration_seconds: float = 8.0


settings = Settings()
