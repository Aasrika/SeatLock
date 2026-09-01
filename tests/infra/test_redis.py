"""Phase 8: the Redis client must never wait unboundedly on a command or
a connection attempt -- see app/infra/config.py's comment on
redis_socket_timeout_seconds for the full reasoning (a paused, not just
killed, Redis hangs open connections rather than refusing them, and
every caller on the booking hot path awaits Redis inline).

This is a construction-time check, not a chaos test: it asserts the
*configuration* is wired through to the real client, not that a hang
actually gets bounded end to end (that is loadtest/chaos/scenarios/
redis_paused.py's job, against a real paused Redis).
"""

from app.infra.config import settings
from app.infra.redis import get_redis


def test_redis_client_has_socket_timeouts_configured() -> None:
    client = get_redis()
    kwargs = client.connection_pool.connection_kwargs

    assert kwargs.get("socket_timeout") == settings.redis_socket_timeout_seconds
    assert kwargs.get("socket_connect_timeout") == settings.redis_socket_connect_timeout_seconds
    # Both must be real, finite bounds -- None (redis-py's "wait forever"
    # default) defeats the entire point.
    assert kwargs.get("socket_timeout") is not None
    assert kwargs.get("socket_connect_timeout") is not None
