import { describe, expect, it } from "vitest";
import {
  applySeatDiff,
  buildSeatMapFromSnapshot,
  computeRemainingMs,
  computeServerTimeOffsetMs,
} from "./client";
import type { DiffSeatEntry, SnapshotSeat } from "./types";

describe("computeServerTimeOffsetMs / computeRemainingMs under clock skew", () => {
  it("computes zero offset when client and server clocks agree", () => {
    const serverTime = "2026-06-01T12:00:00.000Z";
    const clientNow = new Date(serverTime).getTime();
    expect(computeServerTimeOffsetMs(serverTime, clientNow)).toBe(0);
  });

  it("a client clock running BEHIND the server does not overstate remaining time", () => {
    // Client's Date.now() reads 30s earlier than the server's clock --
    // e.g. the client's system clock is simply slow.
    const serverTime = "2026-06-01T12:00:30.000Z";
    const clientNowAtConnect = new Date("2026-06-01T12:00:00.000Z").getTime();
    const offset = computeServerTimeOffsetMs(serverTime, clientNowAtConnect);
    expect(offset).toBe(30_000);

    // A hold expires at 12:00:40 server time. The client's own clock
    // says only 12:00:10 has passed since connect -- naively computing
    // "expiresAt - clientNow" (ignoring the offset) would wrongly
    // report 30s remaining, when the server would say only 0s is left.
    const holdExpiresAt = "2026-06-01T12:00:40.000Z";
    const clientNowLater = new Date("2026-06-01T12:00:10.000Z").getTime(); // 10s of client-perceived time passed
    const remaining = computeRemainingMs(holdExpiresAt, offset, clientNowLater);
    // Correct (offset-adjusted): server-equivalent now is 12:00:10 (client) + 30s offset = 12:00:40 -> remaining 0.
    expect(remaining).toBe(0);

    const naiveRemaining = new Date(holdExpiresAt).getTime() - clientNowLater;
    expect(naiveRemaining).toBe(30_000); // what an unadjusted client would have wrongly shown
    expect(remaining).not.toBe(naiveRemaining);
  });

  it("a client clock running AHEAD of the server does not understate remaining time", () => {
    const serverTime = "2026-06-01T12:00:00.000Z";
    const clientNowAtConnect = new Date("2026-06-01T12:00:20.000Z").getTime(); // client is 20s fast
    const offset = computeServerTimeOffsetMs(serverTime, clientNowAtConnect);
    expect(offset).toBe(-20_000);

    const holdExpiresAt = "2026-06-01T12:05:00.000Z"; // 5 minutes after server_time
    const clientNowLater = new Date("2026-06-01T12:00:20.000Z").getTime(); // no time elapsed yet
    const remaining = computeRemainingMs(holdExpiresAt, offset, clientNowLater);
    expect(remaining).toBe(5 * 60_000); // still the full 5 minutes, not 5:20
  });

  it("returns null for a seat with no hold_expires_at", () => {
    expect(computeRemainingMs(null, 0, Date.now())).toBeNull();
  });
});

describe("applySeatDiff -- version-monotonic discard", () => {
  const snapshot: SnapshotSeat[] = [
    {
      id: 1,
      section: "A",
      row_label: "1",
      seat_number: 1,
      status: "AVAILABLE",
      hold_expires_at: null,
      version: 10,
    },
  ];

  it("discards an update whose version is lower than what is already held", () => {
    const seats = buildSeatMapFromSnapshot(snapshot);
    const higherFirst: DiffSeatEntry = {
      id: 1,
      status: "HELD",
      hold_expires_at: "2026-06-01T12:08:00.000Z",
      version: 15,
    };
    const afterHigher = applySeatDiff(seats, higherFirst);
    expect(afterHigher.get(1)?.version).toBe(15);
    expect(afterHigher.get(1)?.status).toBe("HELD");

    // A lower version arrives AFTER (out of order, e.g. a different
    // uvicorn worker's publish overtaken in transit) -- must be
    // discarded, not applied.
    const lowerSecond: DiffSeatEntry = {
      id: 1,
      status: "AVAILABLE",
      hold_expires_at: null,
      version: 12,
    };
    const afterLower = applySeatDiff(afterHigher, lowerSecond);
    expect(afterLower.get(1)?.version).toBe(15);
    expect(afterLower.get(1)?.status).toBe("HELD"); // unchanged -- the stale update never applied
    expect(afterLower).toBe(afterHigher); // same reference: no-op, no re-render
  });

  it("discards an update with an EQUAL version, not just a lower one", () => {
    const seats = buildSeatMapFromSnapshot(snapshot);
    const sameVersion: DiffSeatEntry = {
      id: 1,
      status: "BOOKED",
      hold_expires_at: null,
      version: 10, // equal to the snapshot's own version
    };
    const result = applySeatDiff(seats, sameVersion);
    expect(result.get(1)?.status).toBe("AVAILABLE"); // still the snapshot's state
  });

  it("applies a genuinely newer update", () => {
    const seats = buildSeatMapFromSnapshot(snapshot);
    const newer: DiffSeatEntry = {
      id: 1,
      status: "HELD",
      hold_expires_at: "2026-06-01T12:08:00.000Z",
      version: 11,
    };
    const result = applySeatDiff(seats, newer);
    expect(result.get(1)?.status).toBe("HELD");
    expect(result.get(1)?.version).toBe(11);
  });

  it("ignores a diff for a seat not present in the snapshot", () => {
    const seats = buildSeatMapFromSnapshot(snapshot);
    const unknown: DiffSeatEntry = { id: 999, status: "HELD", hold_expires_at: null, version: 1 };
    const result = applySeatDiff(seats, unknown);
    expect(result).toBe(seats);
    expect(result.has(999)).toBe(false);
  });
});

describe("the suppression/version interaction across windows (mirrors the server-side test)", () => {
  it("a seat suppressed in one window still accepts the next window's real change", () => {
    // Mirrors tests/realtime/test_coalescer.py's
    // TestSuppressionAcrossWindowsWithClientVersionDiscard exactly, from
    // the CLIENT's side of the same sequence: window 1's held-then-
    // released nets to nothing sent (so the client never sees v11/v12
    // at all), then window 2's real hold (v13) must still be accepted.
    let seats = buildSeatMapFromSnapshot([
      {
        id: 1,
        section: "A",
        row_label: "1",
        seat_number: 1,
        status: "AVAILABLE",
        hold_expires_at: null,
        version: 10,
      },
    ]);

    // Window 1: nothing arrives at all (server suppressed it).

    // Window 2: the real change.
    seats = applySeatDiff(seats, {
      id: 1,
      status: "HELD",
      hold_expires_at: "2026-06-01T12:08:00.000Z",
      version: 13,
    });

    expect(seats.get(1)?.status).toBe("HELD");
    expect(seats.get(1)?.version).toBe(13);
  });
});
