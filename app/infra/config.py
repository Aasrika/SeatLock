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

    redis_url: str = "redis://localhost:6379/0"

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


settings = Settings()
