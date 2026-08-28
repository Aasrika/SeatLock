"""Async Redis client factory.

Redis is a cache and pub/sub layer only — never the source of truth for
correctness-critical state. See CLAUDE.md rule 4.
"""

from functools import lru_cache

from redis.asyncio import Redis

from app.infra.config import settings


@lru_cache
def get_redis() -> Redis:
    """Return a process-wide async Redis client, built lazily from settings."""
    return Redis.from_url(settings.redis_url, decode_responses=True)
