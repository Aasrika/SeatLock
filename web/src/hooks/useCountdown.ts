import { useEffect, useState } from "react";
import { computeRemainingMs } from "../realtime/client";

const TICK_MS = 250;

/**
 * Recomputes from the ABSOLUTE deadline (`holdExpiresAt` + the one
 * server-time offset computed at connect) on every tick -- never
 * `remaining - tickInterval`, which would accumulate drift and, in a
 * backgrounded tab where timers get throttled, silently fall further
 * and further behind wall-clock time. Every tick asks "how much time
 * is ACTUALLY left, right now" from scratch.
 */
export function useCountdown(holdExpiresAt: string | null, serverTimeOffsetMs: number): number | null {
  const [remainingMs, setRemainingMs] = useState<number | null>(() =>
    computeRemainingMs(holdExpiresAt, serverTimeOffsetMs, Date.now()),
  );

  useEffect(() => {
    if (holdExpiresAt === null) {
      setRemainingMs(null);
      return;
    }
    const tick = () => setRemainingMs(computeRemainingMs(holdExpiresAt, serverTimeOffsetMs, Date.now()));
    tick();
    const interval = setInterval(tick, TICK_MS);
    return () => clearInterval(interval);
  }, [holdExpiresAt, serverTimeOffsetMs]);

  return remainingMs;
}

export function formatRemaining(remainingMs: number | null): string {
  if (remainingMs === null) return "";
  if (remainingMs <= 0) return "0:00";
  const totalSeconds = Math.ceil(remainingMs / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${minutes}:${seconds.toString().padStart(2, "0")}`;
}
