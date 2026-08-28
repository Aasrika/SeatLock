"""Proves Strategy A (naive) oversells under real concurrent contention.

This is Phase 1's actual gate (SPEC.md section 12): "Overselling
reproduced and measured." A k6 load test can show this happening
eventually; this test proves it on every run, deterministically, by
widening NAIVE_RACE_WINDOW_MS so every concurrent attempt's SELECT
happens before any of their UPDATEs -- turning "might oversell under load"
into "will oversell," per SPEC.md section 10 Layer 3's guidance to test
concurrency with a real barrier rather than trusting asyncio.gather timing
alone.

Each concurrent attempt uses its own AsyncSession over its own connection
-- this is deliberately NOT the shared-transaction `db_conn` fixture used
elsewhere, since the whole point is genuinely independent, concurrently
committing transactions racing each other.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

import app.infra.config as infra_config
from app.infra.tables import EventRow, HoldAuditRow, SeatRow
from app.inventory.strategies.naive import NaiveStrategy

NOW = datetime(2026, 6, 1, tzinfo=UTC)
HOLD_DURATION = timedelta(minutes=8)
CONCURRENT_HOLDERS = 20
RACE_WINDOW_MS = 200


@pytest.fixture
def _widen_race_window():
    """Widen the naive strategy's TOCTOU window for the duration of this
    test only, so every concurrent SELECT below completes before any
    UPDATE does. Restored afterward -- this setting is a global singleton
    (see app/infra/config.py), not per-call.
    """
    original = infra_config.settings.naive_race_window_ms
    infra_config.settings.naive_race_window_ms = RACE_WINDOW_MS
    yield
    infra_config.settings.naive_race_window_ms = original


async def test_naive_strategy_oversells_under_concurrent_contention(
    engine: AsyncEngine, _widen_race_window: None
):
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)

    async with session_factory() as setup_session:
        event_id = (
            await setup_session.execute(
                insert(EventRow).returning(EventRow.id),
                {"name": "Race Test", "venue": "Test Venue", "starts_at": NOW, "total_seats": 1},
            )
        ).scalar_one()
        seat_id = (
            await setup_session.execute(
                insert(SeatRow).returning(SeatRow.id),
                {
                    "event_id": event_id,
                    "section": "A",
                    "row_label": "1",
                    "seat_number": 1,
                    "status": "AVAILABLE",
                    "version": 0,
                },
            )
        ).scalar_one()
        await setup_session.commit()

    strategy = NaiveStrategy()

    async def attempt(holder: str):
        async with session_factory() as session:
            return await strategy.acquire(session, [seat_id], holder, HOLD_DURATION, NOW)

    results = await asyncio.gather(*[attempt(f"session-{i}") for i in range(CONCURRENT_HOLDERS)])

    successes = [r for r in results if r.success]
    assert len(successes) > 1, (
        "expected the naive strategy's TOCTOU race to let more than one "
        "concurrent request believe it won the seat -- if this ever fails, "
        "widen RACE_WINDOW_MS, do not go make naive.py 'safer'"
    )

    async with session_factory() as session:
        holder_rows = (
            (
                await session.execute(
                    select(HoldAuditRow.session_id).where(HoldAuditRow.seat_id == seat_id)
                )
            )
            .scalars()
            .all()
        )

    # hold_audit is append-only, so it sees every winner even though the
    # seats row itself will only ever show whichever UPDATE landed last --
    # that's precisely why the oversell-report endpoint reads hold_audit
    # rather than the seats table.
    assert len(set(holder_rows)) > 1
    assert len(holder_rows) == len(successes)
