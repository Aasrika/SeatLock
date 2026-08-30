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

    # Hold sweeper (SPEC.md section 5 / I3, workers/sweeper_worker.py).
    # 5-10s is SPEC.md's own production guidance -- these are PRODUCT
    # defaults, not benchmarking values. The Phase 3 contention sweep
    # overrides both via env vars to something much shorter (a
    # benchmarking configuration, never used here) so inventory
    # recirculates within a short load-test burst; see
    # docs/benchmarks/phase3-crossover.md for the actual values used and
    # how they were chosen (empirically, via a pilot run, not guessed).
    sweeper_interval_seconds: float = 5.0
    sweeper_batch_size: int = 500


settings = Settings()
