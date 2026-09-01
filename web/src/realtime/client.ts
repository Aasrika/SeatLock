// The client-side half of the ordering/coalescing contract
// app/realtime/coalescer.py's docstring establishes on the server side.
// Pure functions, no DOM/WebSocket -- deliberately, so the two rules
// that actually matter (clock-skew-correct countdowns, version-
// monotonic discard) are testable without a socket, a timer, or a
// component tree. See client.test.ts.

import type { DiffSeatEntry, RenderedSeat, SeatMap, SnapshotSeat } from "./types";

/**
 * The snapshot's own `server_time` field vs. the client's clock AT THE
 * MOMENT the snapshot arrived -- computed exactly once per connection,
 * never re-derived per tick. A hold's countdown must be measured
 * against the SERVER's clock, not the client's: a client whose clock
 * is skewed (or a backgrounded tab whose timers were throttled and
 * whose Date.now() nonetheless keeps advancing correctly) must not
 * disagree with the server about when a hold actually dies.
 */
export function computeServerTimeOffsetMs(serverTimeIso: string, clientNowMs: number): number {
  return new Date(serverTimeIso).getTime() - clientNowMs;
}

/**
 * Remaining time on a hold, in milliseconds -- NEGATIVE once expired,
 * deliberately not clamped to zero here, so a caller can distinguish
 * "just expired" from "expired a while ago" if it ever needs to.
 * `null` in, `null` out: an AVAILABLE/BOOKED seat has no countdown.
 *
 * Never `setTimeout(hold_duration_ms)`: that schedules relative to
 * "now" as the CLIENT's clock (and, in a backgrounded tab, browsers
 * throttle/clamp timers unpredictably) rather than computing against
 * an absolute server-issued deadline every time it's asked. Calling
 * this fresh on every render/tick, always from `hold_expires_at` and
 * the one offset computed above, is what makes the countdown correct
 * regardless of how many ticks were missed or how skewed the client's
 * own clock is.
 */
export function computeRemainingMs(
  holdExpiresAtIso: string | null,
  offsetMs: number,
  clientNowMs: number,
): number | null {
  if (holdExpiresAtIso === null) return null;
  const holdExpiresAtMs = new Date(holdExpiresAtIso).getTime();
  const serverEquivalentNow = clientNowMs + offsetMs;
  return holdExpiresAtMs - serverEquivalentNow;
}

export function buildSeatMapFromSnapshot(seats: SnapshotSeat[]): SeatMap {
  const map: SeatMap = new Map();
  for (const seat of seats) {
    map.set(seat.id, {
      id: seat.id,
      section: seat.section,
      rowLabel: seat.row_label,
      seatNumber: seat.seat_number,
      status: seat.status,
      holdExpiresAt: seat.hold_expires_at,
      version: seat.version,
    });
  }
  return map;
}

/**
 * Applies one diff entry, enforcing version-monotonic discard.
 *
 * Redis pub/sub gives no ordering guarantee ACROSS the 4 uvicorn
 * workers publishing independently (see app/realtime/pubsub.py's
 * module docstring) -- a client can receive an update describing an
 * OLDER write after a NEWER one already arrived. Discarding anything
 * whose version is <= what this seat already holds is what makes that
 * safe: the client's rendered state can only ever move forward.
 *
 * This is a SEPARATE concern from server-side coalescing suppression
 * (app/realtime/coalescer.py's SeatState.rendered_equal) -- the server
 * decides whether to send anything at all; this decides, once
 * something IS received, whether it's actually newer. A suppressed
 * server-side window never regresses what was last actually sent, so
 * the next real broadcast this function sees always carries a higher
 * version than anything already applied -- traced through explicitly
 * in coalescer.py's own docstring, and mirrored here by this same
 * discard rule existing independently on the client.
 *
 * Returns the SAME map reference when nothing changes (discarded, or
 * the seat is unknown) -- callers doing `setState(applySeatDiff(...))`
 * get a stable reference in that case, avoiding a spurious re-render.
 */
export function applySeatDiff(seats: SeatMap, diff: DiffSeatEntry): SeatMap {
  const existing = seats.get(diff.id);
  if (existing === undefined || diff.version <= existing.version) {
    return seats;
  }
  const next = new Map(seats);
  next.set(diff.id, {
    ...existing,
    status: diff.status,
    holdExpiresAt: diff.hold_expires_at,
    version: diff.version,
  });
  return next;
}

export function applySeatDiffs(seats: SeatMap, diffs: DiffSeatEntry[]): SeatMap {
  let next = seats;
  for (const diff of diffs) {
    next = applySeatDiff(next, diff);
  }
  return next;
}

/** Optimistic local override -- see the SeatMapPage docstring for the
 * full act-then-reconcile flow this supports. Applied on top of the
 * server-driven map, never written back into it directly, so a
 * rollback just means dropping this override and re-rendering from
 * the last known-good server state.
 */
export function withOptimisticHold(seats: SeatMap, seatId: number): SeatMap {
  const existing = seats.get(seatId);
  if (existing === undefined) return seats;
  const next = new Map(seats);
  next.set(seatId, { ...existing, status: "HELD" });
  return next;
}

export function seatById(seats: SeatMap, seatId: number): RenderedSeat | undefined {
  return seats.get(seatId);
}
