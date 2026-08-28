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


settings = Settings()
