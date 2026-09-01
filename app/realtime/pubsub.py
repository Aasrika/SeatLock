"""Publishing seat changes to Redis, and the channel-naming convention
both the publish side (this module) and the subscribe side
(app/realtime/hub.py) share.

Called from every write path that changes a seat's status or
hold_expires_at, AFTER that write's own Postgres commit -- never before.
Same ordering principle as app/infra/hold_cache.py's mirror writes and
workers/sweeper.py's Postgres-then-Redis delete: a publish that never
arrives (Redis down, a dropped connection) leaves a client's view stale
until its next full snapshot -- a self-correcting problem. A publish that
arrives before the commit it describes has actually landed could show a
seat's new state to a viewer before Postgres would agree it's true if
asked right now -- the direction this project always fails away from.

Best-effort: a publish failure must never fail (or roll back) the booking
write it describes. The seat map going briefly stale is an acceptable,
self-correcting cost; losing a hold/confirm/refund because Redis hiccuped
is not.
"""

from __future__ import annotations

import json
from datetime import datetime

import structlog
from redis.asyncio import Redis
from redis.exceptions import RedisError

log = structlog.get_logger(__name__)


def channel_name(event_id: int, section: str) -> str:
    return f"event:{event_id}:section:{section}"


async def publish_seat_update(
    redis_client: Redis,
    *,
    event_id: int,
    section: str,
    seat_id: int,
    status: str,
    hold_expires_at: datetime | None,
    version: int,
) -> None:
    """Best-effort -- never raises. See module docstring."""
    payload = json.dumps(
        {
            "seat_id": seat_id,
            "status": status,
            "hold_expires_at": hold_expires_at.isoformat() if hold_expires_at else None,
            "version": version,
        }
    )
    try:
        await redis_client.publish(channel_name(event_id, section), payload)
    except RedisError:
        log.warning("realtime.publish_failed", event_id=event_id, section=section, seat_id=seat_id)
