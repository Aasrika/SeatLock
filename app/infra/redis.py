"""Async Redis client factory.

Redis is a cache and pub/sub layer only — never the source of truth for
correctness-critical state. See CLAUDE.md rule 4.
"""

from functools import lru_cache

from redis.asyncio import Redis

from app.infra.config import settings


@lru_cache
def get_redis() -> Redis:
    """Return a process-wide async Redis client, built lazily from settings.

    socket_timeout / socket_connect_timeout are NOT optional here -- see
    Settings.redis_socket_timeout_seconds's comment for the full reasoning
    (a paused Redis hangs open connections rather than refusing them, and
    every caller on the booking hot path awaits Redis inline). Without
    these, an unbounded wait on a cache is how a cache outage becomes an
    API outage.
    """
    return Redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_timeout=settings.redis_socket_timeout_seconds,
        socket_connect_timeout=settings.redis_socket_connect_timeout_seconds,
    )
