"""Seeds (or resets) a fixed demo event for the Phase 7 two-browser demo
(web/demo/two-browsers.mjs) and for manual exploration of the seat map.

Deliberately separate from scripts/seed.py (which is for benchmarking
-- always creates a fresh, potentially huge event, exactly what a
load-test run wants). This one is IDEMPOTENT instead: reuses the event
named "Realtime Demo" if it already exists, resetting every one of its
seats back to AVAILABLE (clearing any hold/booking state left over from
a previous run) rather than creating a new one every time -- so the
demo can be re-recorded repeatedly without accumulating throwaway
events in the dev database, and the script always yields the same
deterministic event_id.

    python -m scripts.seed_demo_event

Prints exactly one line to stdout: the event's id. Everything else
goes to stderr, so a caller can capture the id with `$(...)` without
picking up log noise.
"""

from __future__ import annotations

import asyncio
import sys
from datetime import UTC, datetime

from sqlalchemy import insert, select
from sqlalchemy import update as sa_update

from app.infra.db import async_session_factory
from app.infra.tables import EventRow, SeatRow

EVENT_NAME = "Realtime Demo"
SECTIONS = {"A": 2}  # section -> number of rows
SEATS_PER_ROW = 6


async def main() -> int:
    async with async_session_factory() as session:
        existing_event_id = (
            await session.execute(select(EventRow.id).where(EventRow.name == EVENT_NAME))
        ).scalar_one_or_none()

        if existing_event_id is not None:
            await session.execute(
                sa_update(SeatRow)
                .where(SeatRow.event_id == existing_event_id)
                .values(
                    status="AVAILABLE",
                    held_by_session_id=None,
                    hold_expires_at=None,
                    booking_id=None,
                )
            )
            await session.commit()
            print(f"reusing existing demo event {existing_event_id}, seats reset", file=sys.stderr)
            print(existing_event_id)
            return existing_event_id

        total_seats = sum(rows * SEATS_PER_ROW for rows in SECTIONS.values())
        event_id = (
            await session.execute(
                insert(EventRow).returning(EventRow.id),
                {
                    "name": EVENT_NAME,
                    "venue": "Demo Venue",
                    "starts_at": datetime.now(UTC),
                    "total_seats": total_seats,
                },
            )
        ).scalar_one()

        rows_to_insert = [
            {
                "event_id": event_id,
                "section": section,
                "row_label": str(row),
                "seat_number": seat_number,
                "status": "AVAILABLE",
                "version": 0,
            }
            for section, row_count in SECTIONS.items()
            for row in range(1, row_count + 1)
            for seat_number in range(1, SEATS_PER_ROW + 1)
        ]
        await session.execute(insert(SeatRow), rows_to_insert)
        await session.commit()
        print(f"created demo event {event_id} with {total_seats} seats", file=sys.stderr)
        print(event_id)
        return event_id


if __name__ == "__main__":
    asyncio.run(main())
