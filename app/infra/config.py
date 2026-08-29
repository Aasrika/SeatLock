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
    hold_duration_seconds: int = 480

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

    # Repo-relative (not an absolute temp path) so the Makefile's run-api
    # target and this process resolve the SAME directory regardless of
    # which shell/OS temp-dir convention is in play -- see
    # app/infra/metrics.py's module docstring for why this must be cleared
    # exactly once, before any worker starts (Makefile does that; this
    # setting is just the shared path both sides agree on).
    prometheus_multiproc_dir: str = ".prometheus-multiproc"


settings = Settings()
