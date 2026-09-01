"""Phase 7 item 10: fanout reaches only subscribed sections; a client
reconnecting gets a full snapshot.

Exercises app/realtime/hub.py directly against REAL Redis (the same
dev-mode client every other test in this suite uses -- see
app/infra/redis.py; Redis in this project doesn't need per-test
container isolation the way Postgres does, only explicit key cleanup)
and real Postgres (testcontainers, this file's own session_factory).

Does NOT go through app.main's FastAPI app / TestClient: app.infra.db's
module-level engine binds to Settings.database_url at IMPORT time (the
.env default, i.e. the dev database), not this file's ephemeral
testcontainers one -- exactly the reason every other integration test
in this suite builds its own session_factory rather than importing
app.main. A `_FakeWebSocket` duck-types the two methods hub.py actually
calls (accept(), send_text()) so the real coalescing + Redis pub/sub
pipeline is exercised end-to-end without needing a real ASGI transport,
which is standard Starlette machinery not worth re-testing here.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from sqlalchemy import insert, select
from sqlalchemy import update as sa_update
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from app.infra.redis import get_redis
from app.infra.tables import EventRow, SeatRow
from app.realtime.hub import RealtimeHub
from app.realtime.pubsub import publish_seat_update

NOW = datetime.now(UTC)
FUTURE = NOW + timedelta(minutes=10)

# Comfortably longer than Settings.ws_coalesce_window_ms's default
# (100ms) -- a real flush cycle must have run by the time a test checks
# what was sent.
FLUSH_WAIT_SECONDS = 0.35


class _FakeWebSocket:
    """Duck-types exactly the WebSocket surface app/realtime/hub.py
    calls -- see module docstring for why this stands in for a real
    ASGI connection.
    """

    def __init__(self) -> None:
        self.sent: list[dict] = []
        self.accepted = False

    async def accept(self) -> None:
        self.accepted = True

    async def send_text(self, data: str) -> None:
        self.sent.append(json.loads(data))


@pytest_asyncio.fixture(scope="module")
async def big_pool_engine(database_url: str, migrated_schema: None):
    eng = create_async_engine(database_url, pool_size=10, max_overflow=5)
    yield eng
    await eng.dispose()


@pytest.fixture
def session_factory(big_pool_engine: AsyncEngine):
    return async_sessionmaker(bind=big_pool_engine, expire_on_commit=False)


@pytest_asyncio.fixture
async def hub(session_factory):
    h = RealtimeHub(get_redis(), session_factory)
    await h.start()
    yield h
    await h.stop()


async def _seed_event_with_sections(session_factory, sections: dict[str, int]) -> int:
    """`sections` maps section name -> seat count."""
    total = sum(sections.values())
    async with session_factory() as session:
        event_id = (
            await session.execute(
                insert(EventRow).returning(EventRow.id),
                {
                    "name": "Realtime Test",
                    "venue": "Test Venue",
                    "starts_at": NOW,
                    "total_seats": total,
                },
            )
        ).scalar_one()
        rows = []
        for section, count in sections.items():
            rows.extend(
                {
                    "event_id": event_id,
                    "section": section,
                    "row_label": "1",
                    "seat_number": i + 1,
                    "status": "AVAILABLE",
                    "version": 0,
                }
                for i in range(count)
            )
        await session.execute(insert(SeatRow), rows)
        await session.commit()
    return event_id


async def _seat_ids_for_section(session_factory, event_id: int, section: str) -> list[int]:
    async with session_factory() as session:
        return list(
            (
                await session.execute(
                    select(SeatRow.id)
                    .where(SeatRow.event_id == event_id, SeatRow.section == section)
                    .order_by(SeatRow.id)
                )
            )
            .scalars()
            .all()
        )


class TestFanoutScoping:
    async def test_publish_to_one_section_never_reaches_another(self, session_factory, hub):
        event_id = await _seed_event_with_sections(session_factory, {"A": 2, "B": 2})
        seat_a = (await _seat_ids_for_section(session_factory, event_id, "A"))[0]

        ws_a = _FakeWebSocket()
        ws_b = _FakeWebSocket()
        await hub.connect(ws_a, event_id, ["A"])
        await hub.connect(ws_b, event_id, ["B"])

        # Each connect() already delivered its own snapshot -- clear that
        # so only what happens AFTER this point is being asserted.
        ws_a.sent.clear()
        ws_b.sent.clear()

        await publish_seat_update(
            get_redis(),
            event_id=event_id,
            section="A",
            seat_id=seat_a,
            status="HELD",
            hold_expires_at=FUTURE,
            version=1,
        )
        await asyncio.sleep(FLUSH_WAIT_SECONDS)

        assert len(ws_a.sent) == 1
        assert ws_a.sent[0]["type"] == "diff"
        assert ws_a.sent[0]["section"] == "A"
        assert ws_a.sent[0]["seats"][0]["id"] == seat_a

        assert ws_b.sent == []  # section B never saw section A's change

        await hub.disconnect(ws_a, event_id, ["A"])
        await hub.disconnect(ws_b, event_id, ["B"])


class TestReconnectSnapshot:
    async def test_reconnect_gets_a_fresh_snapshot_not_a_resumed_stream(self, session_factory, hub):
        event_id = await _seed_event_with_sections(session_factory, {"A": 1})
        seat_id = (await _seat_ids_for_section(session_factory, event_id, "A"))[0]

        ws1 = _FakeWebSocket()
        sections = await hub.connect(ws1, event_id, ["A"])
        first_snapshot = ws1.sent[0]
        assert first_snapshot["type"] == "snapshot"
        assert first_snapshot["seats"][0]["status"] == "AVAILABLE"
        await hub.disconnect(ws1, event_id, sections)

        # State changes while NO ONE is connected -- a diff stream
        # couldn't possibly have delivered this to a client that wasn't
        # there to receive it.
        async with session_factory() as session:
            await session.execute(
                sa_update(SeatRow)
                .where(SeatRow.id == seat_id)
                .values(status="HELD", held_by_session_id="s1", hold_expires_at=FUTURE, version=1)
            )
            await session.commit()

        ws2 = _FakeWebSocket()
        await hub.connect(ws2, event_id, ["A"])

        assert len(ws2.sent) == 1  # a full snapshot, not zero (no stale assumption) and not a diff
        second_snapshot = ws2.sent[0]
        assert second_snapshot["type"] == "snapshot"
        assert second_snapshot["seats"][0]["status"] == "HELD"
        assert second_snapshot["seats"][0]["version"] == 1

        await hub.disconnect(ws2, event_id, ["A"])


class TestSubscriberCleanup:
    async def test_disconnect_removes_the_subscriber_and_unsubscribes_when_last(
        self, session_factory, hub
    ):
        event_id = await _seed_event_with_sections(session_factory, {"A": 1})
        ws = _FakeWebSocket()
        await hub.connect(ws, event_id, ["A"])
        assert (event_id, "A") in hub._subscribers

        await hub.disconnect(ws, event_id, ["A"])
        assert (event_id, "A") not in hub._subscribers
        assert (event_id, "A") not in hub._coalescers
