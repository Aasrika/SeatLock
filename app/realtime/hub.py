"""The realtime hub: one instance per uvicorn worker process, owning
that worker's local WebSocket connections, its Redis pub/sub
subscriptions, and the coalescing flush ticker.

Deliberately NOT a cross-process registry -- with 4 uvicorn workers, a
client's WebSocket connection lives in exactly one of them. Cross-worker
fanout is Redis's job (a publish from any worker reaches every worker
subscribed to that channel); this hub's registry only needs to know
which of ITS OWN local connections care about which sections.

Subscribe/unsubscribe to Redis channels is reference-counted per
(event_id, section): the first local subscriber to a section triggers a
real Redis SUBSCRIBE, the last one leaving triggers UNSUBSCRIBE -- a
worker with no clients watching event 3's section VIP has no reason to
keep receiving its traffic.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from datetime import UTC, datetime

import structlog
from fastapi import WebSocket
from redis.asyncio import Redis
from redis.asyncio.client import PubSub
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.infra.config import settings
from app.infra.metrics import (
    ws_broadcast_duration_seconds,
    ws_connections_gauge,
    ws_events_coalesced_total,
    ws_messages_sent_total,
)
from app.infra.tables import SeatRow
from app.realtime.coalescer import SeatState, SectionCoalescer
from app.realtime.pubsub import channel_name

log = structlog.get_logger(__name__)


def _parse_channel(channel: str) -> tuple[int, str]:
    # "event:{id}:section:{section}" -- split with maxsplit=3 so a
    # section name that happened to contain ":" survives intact in the
    # last part, rather than being silently truncated.
    _, event_id_str, _, section = channel.split(":", 3)
    return int(event_id_str), section


class RealtimeHub:
    def __init__(
        self, redis_client: Redis, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        self._redis = redis_client
        self._session_factory = session_factory
        self._subscribers: dict[tuple[int, str], set[WebSocket]] = {}
        self._coalescers: dict[tuple[int, str], SectionCoalescer] = {}
        self._pubsub: PubSub | None = None
        self._listen_task: asyncio.Task[None] | None = None
        self._flush_task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        self._pubsub = self._redis.pubsub()
        self._listen_task = asyncio.create_task(self._listen_loop())
        self._flush_task = asyncio.create_task(self._flush_loop())

    async def stop(self) -> None:
        for task in (self._listen_task, self._flush_task):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        if self._pubsub is not None:
            await self._pubsub.aclose()

    async def _all_sections(self, session: AsyncSession, event_id: int) -> list[str]:
        rows = (
            (
                await session.execute(
                    select(SeatRow.section).where(SeatRow.event_id == event_id).distinct()
                )
            )
            .scalars()
            .all()
        )
        return sorted(rows)

    async def _load_snapshot_seats(
        self, session: AsyncSession, event_id: int, sections: list[str], now: datetime
    ) -> list[dict]:
        """Lazy-expiry aware, matching every other read path in this
        codebase (app/api/routes/admin.py's get_seat_status_counts,
        app/inventory/strategies/pessimistic.py's acquire_any_n): a HELD
        row past hold_expires_at is reported AVAILABLE, not HELD -- a
        viewer's snapshot must agree with what an acquire attempt would
        actually see, or the seat map would show a seat as taken that a
        click would immediately prove is free.
        """
        rows = (
            (
                await session.execute(
                    select(SeatRow)
                    .where(SeatRow.event_id == event_id, SeatRow.section.in_(sections))
                    .order_by(SeatRow.section, SeatRow.row_label, SeatRow.seat_number)
                )
            )
            .scalars()
            .all()
        )
        seats = []
        for row in rows:
            expired = (
                row.status == "HELD"
                and row.hold_expires_at is not None
                and row.hold_expires_at <= now
            )
            status = "AVAILABLE" if expired else row.status
            seats.append(
                {
                    "id": row.id,
                    "section": row.section,
                    "row_label": row.row_label,
                    "seat_number": row.seat_number,
                    "status": status,
                    "hold_expires_at": None
                    if expired
                    else (row.hold_expires_at.isoformat() if row.hold_expires_at else None),
                    "version": row.version,
                }
            )
        return seats

    async def connect(self, websocket: WebSocket, event_id: int, sections: list[str]) -> list[str]:
        """Accepts the connection, sends the initial snapshot, and
        registers the subscription. Returns the actual section list
        (resolved to "all sections" if the client didn't specify any) so
        the caller can pass it back to disconnect().
        """
        await websocket.accept()
        now = datetime.now(UTC)
        async with self._session_factory() as session:
            resolved_sections = sections or await self._all_sections(session, event_id)
            seats = await self._load_snapshot_seats(session, event_id, resolved_sections, now)

        for section in resolved_sections:
            await self._add_subscriber(event_id, section, websocket)
        ws_connections_gauge.inc()

        await websocket.send_text(
            json.dumps(
                {
                    "type": "snapshot",
                    "event_id": event_id,
                    "sections": resolved_sections,
                    "server_time": now.isoformat(),
                    "seats": seats,
                }
            )
        )
        return resolved_sections

    async def disconnect(self, websocket: WebSocket, event_id: int, sections: list[str]) -> None:
        for section in sections:
            await self._remove_subscriber(event_id, section, websocket)
        ws_connections_gauge.dec()

    async def _add_subscriber(self, event_id: int, section: str, websocket: WebSocket) -> None:
        key = (event_id, section)
        async with self._lock:
            subs = self._subscribers.setdefault(key, set())
            is_new_section = len(subs) == 0
            subs.add(websocket)
            self._coalescers.setdefault(key, SectionCoalescer())
            if is_new_section:
                assert self._pubsub is not None
                await self._pubsub.subscribe(channel_name(event_id, section))

    async def _remove_subscriber(self, event_id: int, section: str, websocket: WebSocket) -> None:
        key = (event_id, section)
        async with self._lock:
            subs = self._subscribers.get(key)
            if subs is None:
                return
            subs.discard(websocket)
            if not subs:
                del self._subscribers[key]
                self._coalescers.pop(key, None)
                assert self._pubsub is not None
                await self._pubsub.unsubscribe(channel_name(event_id, section))

    async def _listen_loop(self) -> None:
        assert self._pubsub is not None
        while True:
            try:
                message = await self._pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 -- a transient Redis hiccup must not kill the loop
                log.warning("realtime.listen_error", exc_info=True)
                await asyncio.sleep(0.1)
                continue
            if message is None or message["type"] != "message":
                continue
            event_id, section = _parse_channel(message["channel"])
            coalescer = self._coalescers.get((event_id, section))
            if coalescer is None:
                # Unsubscribed between publish and delivery -- no local
                # subscriber left to care, drop it.
                continue
            data = json.loads(message["data"])
            hold_expires_at = (
                datetime.fromisoformat(data["hold_expires_at"]) if data["hold_expires_at"] else None
            )
            coalescer.record(
                data["seat_id"],
                SeatState(data["status"], hold_expires_at, data["version"]),
            )

    async def _flush_loop(self) -> None:
        interval = settings.ws_coalesce_window_ms / 1000
        while True:
            await asyncio.sleep(interval)
            for (event_id, section), coalescer in list(self._coalescers.items()):
                if not coalescer.is_dirty:
                    continue
                start = time.monotonic()
                diffs, raw = coalescer.flush()
                # Every raw event either becomes exactly one seat entry
                # in the outbound diff, or is absorbed entirely
                # (suppressed, or superseded within the window by a
                # later update for the same seat) -- see
                # coalescer.py's own docstring.
                ws_events_coalesced_total.inc(max(0, raw - len(diffs)))
                if not diffs:
                    continue
                message = json.dumps(
                    {
                        "type": "diff",
                        "event_id": event_id,
                        "section": section,
                        "seats": [
                            {
                                "id": d.seat_id,
                                "status": d.status,
                                "hold_expires_at": d.hold_expires_at.isoformat()
                                if d.hold_expires_at
                                else None,
                                "version": d.version,
                            }
                            for d in diffs
                        ],
                    }
                )
                for websocket in list(self._subscribers.get((event_id, section), ())):
                    try:
                        await websocket.send_text(message)
                        ws_messages_sent_total.inc()
                    except Exception:  # noqa: BLE001 -- disconnect cleanup happens in the route
                        log.debug("realtime.send_failed", event_id=event_id, section=section)
                ws_broadcast_duration_seconds.observe(time.monotonic() - start)


_hub: RealtimeHub | None = None


def get_hub() -> RealtimeHub:
    if _hub is None:
        raise RuntimeError("RealtimeHub not initialized -- app startup did not run")
    return _hub


def init_hub(redis_client: Redis, session_factory: async_sessionmaker[AsyncSession]) -> RealtimeHub:
    global _hub
    _hub = RealtimeHub(redis_client, session_factory)
    return _hub
