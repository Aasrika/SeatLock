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
    """The outcome of one acquire() call.

    attempts: the literal count of acquisition attempts MADE, by every
    strategy, on every return path -- never "1 meaning no retries" for
    naive/pessimistic and something else for optimistic. Those two never
    retry internally, so every return is attempts=1; optimistic's own
    retry loop sets it to whichever iteration actually returned (1 on an
    immediate win or an unretryable rejection, up to max_attempts on
    exhaustion). Defaults to 0, not 1, so a caller that forgets to set it
    gets an obviously-wrong sentinel instead of a silently-plausible
    "one attempt" -- added for the walkthrough page's race demo (Phase
    9), which reports this per attempt.
    """

    success: bool
    acquired: list[int]
    failed: list[int]
    reason: str | None = None
    attempts: int = 0


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
    light, and originally also proved the selection wiring worked
    end-to-end before "pessimistic"/"optimistic" existed (they raised
    NotImplementedError rather than an ImportError or AttributeError,
    which would have left it ambiguous whether the config plumbing or the
    strategy itself was the missing piece). All three now exist.
    """
    if name == "naive":
        from app.inventory.strategies.naive import NaiveStrategy

        return NaiveStrategy()
    if name == "pessimistic":
        from app.inventory.strategies.pessimistic import PessimisticStrategy

        return PessimisticStrategy()
    if name == "optimistic":
        from app.inventory.strategies.optimistic import OptimisticStrategy

        return OptimisticStrategy()
    raise ValueError(f"Unknown strategy: {name!r}")
