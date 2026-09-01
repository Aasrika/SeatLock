"""GET /api/admin/dashboard -- a typed, tested JSON shape, per review:
Prometheus exposition format is an interface to Prometheus, not to a
UI, and a frontend parsing it directly can't tell which multiprocess_
mode a given metric uses (see app/infra/metrics.py's own docstring --
ws_connections_gauge is "livesum", sweeper_backlog_gauge is
"mostrecent"; a bare line-parser has no way to know which is which).

Calls get_dashboard/get_invariants directly (this suite's established
convention -- see test_idempotency.py's own docstring for why: a
FastAPI route decorated with @router.get is still a plain callable).
Counter/Gauge deltas, not absolute values, throughout -- these are
module-level globals shared across the whole test session (same
reasoning as test_optimistic.py's own metric assertions).
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
import pytest_asyncio
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from app.api.routes.admin import get_dashboard, get_invariants
from app.infra.metrics import (
    deadlocks_total,
    optimistic_conflicts_total,
    reconciliation_divergence_total,
    sweeper_backlog_gauge,
)
from app.infra.tables import EventRow, SeatRow

NOW = datetime.now(UTC)


@pytest_asyncio.fixture(scope="module")
async def big_pool_engine(database_url: str, migrated_schema: None):
    eng = create_async_engine(database_url, pool_size=5, max_overflow=5)
    yield eng
    await eng.dispose()


@pytest.fixture
def session_factory(big_pool_engine: AsyncEngine):
    return async_sessionmaker(bind=big_pool_engine, expire_on_commit=False)


async def _seed_healthy_event(session_factory, seat_count: int = 3) -> int:
    async with session_factory() as session:
        event_id = (
            await session.execute(
                insert(EventRow).returning(EventRow.id),
                {
                    "name": "Dashboard Test",
                    "venue": "Test Venue",
                    "starts_at": NOW,
                    "total_seats": seat_count,
                },
            )
        ).scalar_one()
        await session.execute(
            insert(SeatRow),
            [
                {
                    "event_id": event_id,
                    "section": "A",
                    "row_label": "1",
                    "seat_number": i + 1,
                    "status": "AVAILABLE",
                    "version": 0,
                }
                for i in range(seat_count)
            ],
        )
        await session.commit()
    return event_id


class TestDashboardShapeWithoutEventId:
    async def test_no_event_id_omits_invariants_but_includes_metrics(self, session_factory):
        async with session_factory() as session:
            response = await get_dashboard(session, event_id=None)

        assert response.event_id is None
        assert response.invariants is None
        # Every field must be present and typed -- a KeyError/AttributeError
        # here would mean the endpoint's own shape is broken, which is
        # exactly the failure mode a frontend text-parser could never
        # catch at type-check time.
        assert isinstance(response.metrics.sweeper_backlog, float)
        assert isinstance(response.metrics.reconciliation_divergence_by_kind, dict)


class TestDashboardReflectsRealMetrics:
    async def test_sweeper_backlog_reflects_the_gauges_current_value(self, session_factory):
        sweeper_backlog_gauge.set(7)
        async with session_factory() as session:
            response = await get_dashboard(session, event_id=None)
        assert response.metrics.sweeper_backlog == 7

    async def test_counter_increments_are_reflected(self, session_factory):
        # Both "before" and "after" go through get_dashboard itself --
        # not counter.collect() called directly, which reads a
        # DIFFERENT (single-process-local) view than the aggregated
        # multiprocess registry the endpoint actually reads (see
        # app/api/routes/admin.py's _collect_samples docstring). Reading
        # "before" any other way risks comparing two different sources
        # of truth against each other.
        async with session_factory() as session:
            before = await get_dashboard(session, event_id=None)

        deadlocks_total.inc()
        optimistic_conflicts_total.inc(3)
        reconciliation_divergence_total.labels(kind="redis_session_mismatch").inc()

        async with session_factory() as session:
            after = await get_dashboard(session, event_id=None)

        assert after.metrics.deadlocks_total == before.metrics.deadlocks_total + 1
        assert (
            after.metrics.optimistic_conflicts_total
            == before.metrics.optimistic_conflicts_total + 3
        )
        before_divergence = before.metrics.reconciliation_divergence_by_kind.get(
            "redis_session_mismatch", 0.0
        )
        after_divergence = after.metrics.reconciliation_divergence_by_kind.get(
            "redis_session_mismatch", 0.0
        )
        assert after_divergence == before_divergence + 1


class TestDashboardInvariantsPerEvent:
    async def test_event_id_given_includes_a_healthy_events_invariant_status(self, session_factory):
        event_id = await _seed_healthy_event(session_factory)

        async with session_factory() as session:
            response = await get_dashboard(session, event_id=event_id)

        assert response.event_id == event_id
        assert response.invariants is not None
        assert all(result.passed for result in response.invariants.values())

    async def test_matches_get_invariants_for_the_same_event(self, session_factory):
        """The dashboard's invariant section and the dedicated
        /invariants endpoint share one implementation
        (_compute_invariants) -- this is the test that would catch them
        silently diverging if that sharing were ever removed.
        """
        event_id = await _seed_healthy_event(session_factory)

        async with session_factory() as session:
            dashboard = await get_dashboard(session, event_id=event_id)
        async with session_factory() as session:
            standalone = await get_invariants(event_id, session)

        assert dashboard.invariants == standalone.results
