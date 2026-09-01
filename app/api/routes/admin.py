"""GET /api/admin/invariants, GET /api/admin/oversell-report, and
GET /api/admin/seat-status-counts.

Thin router: the checks themselves are app/domain/invariants.py's pure
functions; this module's job is only to load the seats/audit rows they
need and shape the response. /invariants is what the load harness
(loadtest/run_benchmark.py) polls DURING a run, not just after -- catching
a violation that self-heals before the run ends is the whole point
(SPEC.md section 10). /seat-status-counts exists for the same
during-a-run-not-just-after reason: the Phase 3 recirculating-contention
pilot (loadtest/recirculating_pilot.py) polls it throughout a run to
confirm inventory is actually cycling AVAILABLE -> HELD -> AVAILABLE
rather than being exhausted once and staying that way -- a real
observability need, not a benchmark-only shortcut.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import and_, case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.errors import InvariantViolation
from app.domain.invariants import (
    check_booking_linkage,
    check_conservation,
    check_no_double_booking,
    check_state_coherence,
)
from app.infra.db import get_session
from app.infra.mappers import seat_to_domain

# CollectorRegistry/multiprocess via app.infra.metrics, NOT a fresh
# `from prometheus_client import ...` here -- that module sets
# PROMETHEUS_MULTIPROC_DIR before ITS OWN prometheus_client import, and
# prometheus_client decides in-process-vs-multiprocess mode once, at its
# first import in the whole process, never revisited (see that module's
# docstring). This router is the first thing app/main.py imports (`from
# app.api.routes import admin, booking, ...`); a direct prometheus_client
# import here would race that env var and could lose, silently putting
# every metric in the API -- not just this endpoint's own reads -- into
# in-process-only mode. Importing these two names FROM metrics.py instead
# forces its module body (including the env var) to run first, since
# Python must finish executing a module before any of its names are
# importable elsewhere.
from app.infra.metrics import CollectorRegistry, multiprocess
from app.infra.tables import BookingSeatRow, EventRow, HoldAuditRow, SeatRow

router = APIRouter()


class InvariantResult(BaseModel):
    passed: bool
    detail: str | None = None


class InvariantsResponse(BaseModel):
    event_id: int
    checked_at: datetime
    results: dict[str, InvariantResult]
    all_passed: bool


async def _get_event_or_404(session: AsyncSession, event_id: int) -> EventRow:
    event = await session.get(EventRow, event_id)
    if event is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="event not found")
    return event


async def _compute_invariants(session: AsyncSession, event: EventRow) -> dict[str, InvariantResult]:
    """Shared by GET /invariants and GET /dashboard -- both need exactly
    this same per-event check, and duplicating it would risk the two
    silently diverging over time.
    """
    seat_rows = (
        (await session.execute(select(SeatRow).where(SeatRow.event_id == event.id))).scalars().all()
    )
    seats = [seat_to_domain(row) for row in seat_rows]

    active_seat_ids = set(
        (
            await session.execute(
                select(BookingSeatRow.seat_id)
                .join(SeatRow, SeatRow.id == BookingSeatRow.seat_id)
                .where(SeatRow.event_id == event.id, BookingSeatRow.released_at.is_(None))
            )
        )
        .scalars()
        .all()
    )

    # Every check runs independently -- one invariant failing must never
    # prevent the others from being reported.
    results: dict[str, InvariantResult] = {}

    def run_check(name: str, check: Callable[[], None]) -> None:
        try:
            check()
            results[name] = InvariantResult(passed=True)
        except InvariantViolation as exc:
            results[name] = InvariantResult(passed=False, detail=str(exc))

    run_check("conservation", lambda: check_conservation(seats, event.total_seats))
    run_check("no_double_booking", lambda: check_no_double_booking(seats))
    run_check("state_coherence", lambda: check_state_coherence(seats))
    run_check("booking_linkage", lambda: check_booking_linkage(seats, active_seat_ids))
    return results


@router.get("/invariants", response_model=InvariantsResponse)
async def get_invariants(
    event_id: int, session: Annotated[AsyncSession, Depends(get_session)]
) -> InvariantsResponse:
    event = await _get_event_or_404(session, event_id)
    results = await _compute_invariants(session, event)

    return InvariantsResponse(
        event_id=event_id,
        checked_at=datetime.now(UTC),
        results=results,
        all_passed=all(result.passed for result in results.values()),
    )


class SeatOversell(BaseModel):
    seat_id: int
    distinct_holders: int
    holders: list[str]


class OversellReportResponse(BaseModel):
    event_id: int
    seats: list[SeatOversell]
    # Two genuinely different numbers -- collapsing them into one
    # "total_oversell_count" hid the actual signal (see loadtest/results/
    # for the investigation): oversold_seats is capped at the number of
    # seats in contention (1 for a single-seat scenario, however many are
    # in play for a multi-seat one) and cannot show a distribution by
    # itself. excess_holders (sum over seats of holders - 1) has no such
    # ceiling and is what actually varies run to run under a real race.
    oversold_seats: int
    excess_holders: int


@router.get("/oversell-report", response_model=OversellReportResponse)
async def get_oversell_report(
    event_id: int, session: Annotated[AsyncSession, Depends(get_session)]
) -> OversellReportResponse:
    await _get_event_or_404(session, event_id)

    rows = (
        await session.execute(
            select(HoldAuditRow.seat_id, HoldAuditRow.session_id)
            .join(SeatRow, SeatRow.id == HoldAuditRow.seat_id)
            .where(SeatRow.event_id == event_id)
        )
    ).all()

    holders_by_seat: dict[int, set[str]] = {}
    for seat_id, session_id in rows:
        holders_by_seat.setdefault(seat_id, set()).add(session_id)

    seats = [
        SeatOversell(seat_id=seat_id, distinct_holders=len(holders), holders=sorted(holders))
        for seat_id, holders in sorted(holders_by_seat.items())
    ]
    oversold_seats = sum(1 for seat in seats if seat.distinct_holders > 1)
    excess_holders = sum(max(0, seat.distinct_holders - 1) for seat in seats)

    return OversellReportResponse(
        event_id=event_id,
        seats=seats,
        oversold_seats=oversold_seats,
        excess_holders=excess_holders,
    )


class SeatStatusCountsResponse(BaseModel):
    event_id: int
    checked_at: datetime
    available: int
    held: int
    booked: int


@router.get("/seat-status-counts", response_model=SeatStatusCountsResponse)
async def get_seat_status_counts(
    event_id: int, session: Annotated[AsyncSession, Depends(get_session)]
) -> SeatStatusCountsResponse:
    """A single cheap GROUP BY -- not check_conservation's full snapshot-
    and-validate path, which loads every seat row and every domain object
    just to answer "how many of each status right now." Deliberately
    unauthenticated/unthrottled like every other /api/admin route in this
    phase (SPEC.md's admin auth is a later phase) -- fine for a load-test
    harness polling it every 50-100ms during a run, not fine to expose
    publicly as-is.

    Lazy-expiry aware (Phase 4): a HELD row whose hold_expires_at has
    already passed is counted as AVAILABLE, not HELD -- this endpoint's
    whole purpose is reporting seat *availability*, and reporting a
    reclaimable seat as unavailable would be exactly the query-layer/
    domain-layer disagreement Phase 4's audit exists to close (see
    app/inventory/strategies/pessimistic.py's acquire_any_n for the same
    fix applied to an acquisition path instead of a report).
    """
    await _get_event_or_404(session, event_id)
    now = datetime.now(UTC)

    effective_status = case(
        (and_(SeatRow.status == "HELD", SeatRow.hold_expires_at <= now), "AVAILABLE"),
        else_=SeatRow.status,
    )
    rows = (
        await session.execute(
            select(effective_status, func.count())
            .where(SeatRow.event_id == event_id)
            .group_by(effective_status)
        )
    ).all()
    counts = {status: count for status, count in rows}

    return SeatStatusCountsResponse(
        event_id=event_id,
        checked_at=datetime.now(UTC),
        available=counts.get("AVAILABLE", 0),
        held=counts.get("HELD", 0),
        booked=counts.get("BOOKED", 0),
    )


def _collect_samples() -> list:
    """Reads the SAME aggregated multiprocess registry GET /metrics
    exposes (app/infra/metrics.py's render_metrics_text) -- just as
    structured Sample objects instead of Prometheus text, so the values
    below are typed and shaped here, once, rather than left for the
    frontend to parse out of a text format that exists to talk to
    Prometheus, not to a UI (see the review comment this endpoint
    responds to: a metric rename or a multiprocess_mode change would
    silently break a text-parsing frontend with no type error and no
    failing test -- exactly the class of stale-definition bug this
    project has already found five times).

    Flattened across families, keyed by nothing -- deliberately, NOT a
    dict keyed by family name. Every Counter in this codebase is
    constructed with its name ALREADY ending in "_total" (e.g.
    "deadlocks_total"); prometheus_client strips that suffix from the
    FAMILY name internally and re-adds it only on the SAMPLE name (the
    same quirk this codebase's own test helpers already work around --
    see e.g. test_optimistic.py's `_counter_value`). Filtering by
    `sample.name` directly below sidesteps needing to replicate that
    stripping logic here too.
    """
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    return [sample for family in registry.collect() for sample in family.samples]


def _gauge_value(samples: list, metric_name: str) -> float:
    for sample in samples:
        if sample.name == metric_name:
            return sample.value
    return 0.0


def _counter_total(samples: list, metric_name: str) -> float:
    """`metric_name` is the counter's OWN construction name, already
    ending in "_total" -- that is also the exposed sample name (see
    _collect_samples' docstring), so no suffix manipulation happens
    here. Sums across every label combination; callers that need a
    per-label breakdown use _counter_by_label instead.
    """
    return sum(sample.value for sample in samples if sample.name == metric_name)


def _counter_by_label(samples: list, metric_name: str, label: str) -> dict[str, float]:
    result: dict[str, float] = {}
    for sample in samples:
        if sample.name == metric_name and label in sample.labels:
            key = sample.labels[label]
            result[key] = result.get(key, 0.0) + sample.value
    return result


def _histogram_count_and_sum(samples: list, metric_name: str) -> tuple[float, float]:
    count = sum(sample.value for sample in samples if sample.name == f"{metric_name}_count")
    total = sum(sample.value for sample in samples if sample.name == f"{metric_name}_sum")
    return count, total


class DashboardMetrics(BaseModel):
    sweeper_backlog: float
    lock_wait_seconds_count: float
    lock_wait_seconds_sum: float
    deadlocks_total: float
    lock_timeouts_total: float
    optimistic_conflicts_total: float
    optimistic_retries_total: float
    optimistic_exhausted_total: float
    reconciliation_divergence_by_kind: dict[str, float]
    reconciliation_transient_by_kind: dict[str, float]


class DashboardResponse(BaseModel):
    checked_at: datetime
    event_id: int | None
    invariants: dict[str, InvariantResult] | None
    metrics: DashboardMetrics


@router.get("/dashboard", response_model=DashboardResponse)
async def get_dashboard(
    session: Annotated[AsyncSession, Depends(get_session)],
    event_id: int | None = None,
) -> DashboardResponse:
    """SPEC.md section 9's admin dashboard: live invariant status,
    sweeper backlog, lock contention, retry rates, reconciliation
    divergences -- one typed, tested response, not a frontend parsing
    Prometheus text (see _collect_metric_families' docstring for why).

    `invariants` is only populated when `event_id` is given -- it is the
    one genuinely per-event piece of this response; sweeper/lock/retry/
    reconciliation metrics are system-wide regardless of which event a
    viewer happens to have open.
    """
    invariants: dict[str, InvariantResult] | None = None
    if event_id is not None:
        event = await _get_event_or_404(session, event_id)
        invariants = await _compute_invariants(session, event)

    samples = _collect_samples()
    lock_wait_count, lock_wait_sum = _histogram_count_and_sum(samples, "lock_wait_seconds")
    metrics = DashboardMetrics(
        sweeper_backlog=_gauge_value(samples, "sweeper_backlog"),
        lock_wait_seconds_count=lock_wait_count,
        lock_wait_seconds_sum=lock_wait_sum,
        deadlocks_total=_counter_total(samples, "deadlocks_total"),
        lock_timeouts_total=_counter_total(samples, "lock_timeouts_total"),
        optimistic_conflicts_total=_counter_total(samples, "optimistic_conflicts_total"),
        optimistic_retries_total=_counter_total(samples, "optimistic_retries_total"),
        optimistic_exhausted_total=_counter_total(samples, "optimistic_exhausted_total"),
        reconciliation_divergence_by_kind=_counter_by_label(
            samples, "reconciliation_divergence_total", "kind"
        ),
        reconciliation_transient_by_kind=_counter_by_label(
            samples, "reconciliation_transient_total", "kind"
        ),
    )

    return DashboardResponse(
        checked_at=datetime.now(UTC), event_id=event_id, invariants=invariants, metrics=metrics
    )
