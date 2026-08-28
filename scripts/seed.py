"""CLI to seed an event with a realistic venue layout of seats.

Usage:
    python scripts/seed.py --event-name "Friday Night Show" --seats 5000
    python scripts/seed.py --contention   # tiny 10-seat event for load tests

Uses bulk inserts (one executemany-style INSERT for all seats), not
row-by-row session.add(), since seeding thousands of rows one at a time is
the kind of thing that looks fine in dev and falls over the first time
someone seeds a real venue.
"""

from __future__ import annotations

import argparse
import asyncio
import string
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import insert

from app.infra.db import async_session_factory
from app.infra.tables import EventRow, SeatRow

SEATS_PER_ROW = 20
ROWS_PER_SECTION = 10
SEATS_PER_SECTION = SEATS_PER_ROW * ROWS_PER_SECTION
CONTENTION_SEAT_COUNT = 10


def _section_label(index: int) -> str:
    """0 -> "A", 25 -> "Z", 26 -> "AA", 27 -> "AB", ... (spreadsheet-style),
    so layouts past 26 sections still get readable, unique labels.
    """
    label = ""
    index += 1
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        label = string.ascii_uppercase[remainder] + label
    return label


def build_seat_rows(event_id: int, total_seats: int) -> list[dict[str, Any]]:
    """Lay seats out into sections of ROWS_PER_SECTION rows of SEATS_PER_ROW
    seats each, filling however many are requested. The last section/row is
    truncated to the exact remainder rather than padded with empty seats.
    """
    rows: list[dict[str, Any]] = []
    for i in range(total_seats):
        section_index, remainder = divmod(i, SEATS_PER_SECTION)
        row_index, seat_index = divmod(remainder, SEATS_PER_ROW)
        rows.append(
            {
                "event_id": event_id,
                "section": _section_label(section_index),
                "row_label": str(row_index + 1),
                "seat_number": seat_index + 1,
                "status": "AVAILABLE",
                "version": 0,
            }
        )
    return rows


async def seed(event_name: str, total_seats: int) -> None:
    async with async_session_factory() as session:
        result = await session.execute(
            insert(EventRow).returning(EventRow.id),
            {
                "name": event_name,
                "venue": "Seatlock Test Arena",
                "starts_at": datetime.now(UTC) + timedelta(days=30),
                "total_seats": total_seats,
            },
        )
        event_id = result.scalar_one()

        await session.execute(insert(SeatRow), build_seat_rows(event_id, total_seats))
        await session.commit()

    print(f"Seeded event {event_id} ({event_name!r}) with {total_seats} seats.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seats", type=int, default=5000, help="Number of seats to create.")
    parser.add_argument(
        "--event-name", type=str, default="Seatlock Demo Event", help="Name of the event."
    )
    parser.add_argument(
        "--contention",
        action="store_true",
        help=(
            f"Create a tiny {CONTENTION_SEAT_COUNT}-seat event for "
            "high-contention load tests (overrides --seats)."
        ),
    )
    args = parser.parse_args()

    total_seats = CONTENTION_SEAT_COUNT if args.contention else args.seats
    asyncio.run(seed(args.event_name, total_seats))


if __name__ == "__main__":
    main()
