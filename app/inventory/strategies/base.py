"""The common interface every seat-acquisition strategy implements.

SPEC.md section 4: three strategies (naive, pessimistic, optimistic) behind
one Protocol, selected via config so the load harness can run all three
against identical scenarios. Only naive exists as of Phase 1.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession


@dataclass
class AcquireResult:
    """The outcome of one acquire() call."""

    success: bool
    acquired: list[int]
    failed: list[int]
    reason: str | None = None


class StrategyUnavailable(Exception):
    """A strategy could not complete an acquire() due to an infrastructure
    condition -- a lock timeout, a deadlock, a connection failure -- not a
    business decision about seat availability.

    Deliberately NOT an AcquireResult(success=False, ...): that shape means
    "we checked, and the seat isn't available," which is a 409 as far as
    the API is concerned. This means "we couldn't even finish checking,"
    which is a 503 -- the caller should retry, the seat's actual
    availability is still unknown. Strategy-agnostic and raised here (not
    in a specific strategy module) so app/api/routes/booking.py can catch
    it without needing to know which strategy is configured.
    """


class SeatAcquisitionStrategy(Protocol):
    """A way of turning a list of seat ids into HELD seats (or not)."""

    async def acquire(
        self,
        session: AsyncSession,
        seat_ids: list[int],
        holder: str,
        hold_duration: timedelta,
        now: datetime,
    ) -> AcquireResult: ...


def get_strategy(name: str) -> SeatAcquisitionStrategy:
    """Look up a strategy by name (see Settings.strategy / STRATEGY env var).

    Deferred imports below are deliberate: they keep this module import-
    light and, more importantly, prove the wiring works end-to-end for
    strategies that don't exist yet -- selecting "pessimistic" or
    "optimistic" today raises NotImplementedError rather than an ImportError
    or AttributeError, which would leave it ambiguous whether the config
    plumbing or the strategy itself was the missing piece.
    """
    if name == "naive":
        from app.inventory.strategies.naive import NaiveStrategy

        return NaiveStrategy()
    if name == "pessimistic":
        from app.inventory.strategies.pessimistic import PessimisticStrategy

        return PessimisticStrategy()
    if name == "optimistic":
        raise NotImplementedError("Optimistic locking strategy is Phase 3.")
    raise ValueError(f"Unknown strategy: {name!r}")
