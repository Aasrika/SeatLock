import { useEffect, useRef, useState } from "react";
import { createDemoHold, getDemoState } from "../../demo-api";
import type { DemoHoldResponse, DemoSeat } from "../../demo-api";
import { type AsyncState, describeError } from "../../async-state";
import { useCountdown, formatRemaining } from "../../hooks/useCountdown";
import { newIdempotencyKey } from "../../idempotency";
import { StateMachineDiagram } from "./StateMachineDiagram";
import type { SeatMachineState } from "./StateMachineDiagram";

const POLL_MS = 1000;
const DEMO_HOLD_SECONDS = 8;

function toMachineState(status: string): SeatMachineState {
  if (status === "BOOKED") return "BOOKED";
  if (status === "HELD") return "HELD";
  return "AVAILABLE";
}

export function LifecycleSection({ eventId, seatId }: { eventId: number; seatId: number }) {
  const [holdState, setHoldState] = useState<AsyncState<DemoHoldResponse>>({ status: "idle" });
  const [seatState, setSeatState] = useState<AsyncState<DemoSeat>>({ status: "idle" });
  const sessionIdRef = useRef<string>(newIdempotencyKey());

  // serverTimeOffsetMs=0: a simplification, not the real Phase 7
  // mechanism -- this page has no live WebSocket snapshot to derive a
  // server-time offset from (see useSeatMapSocket for where that
  // normally comes from). For a local walkthrough, client/server clock
  // skew is assumed negligible; the countdown still recomputes from the
  // absolute hold_expires_at deadline every tick, never from a
  // setTimeout(duration), which is the actual property that matters.
  const holdExpiresAt = holdState.status === "success" ? holdState.data.hold_expires_at : null;
  const remainingMs = useCountdown(holdExpiresAt, 0);

  useEffect(() => {
    if (holdState.status !== "success") return;
    let cancelled = false;
    const poll = async () => {
      try {
        const state = await getDemoState(eventId);
        const seat = state.seats.find((s) => s.id === seatId);
        if (!cancelled && seat) {
          setSeatState({ status: "success", data: seat });
        }
      } catch (err) {
        if (!cancelled) {
          setSeatState({ status: "error", message: describeError(err) });
        }
      }
    };
    void poll();
    const interval = setInterval(() => void poll(), POLL_MS);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [holdState.status, eventId, seatId]);

  async function takeHold() {
    setHoldState({ status: "loading" });
    try {
      const data = await createDemoHold({
        event_id: eventId,
        seat_id: seatId,
        session_id: sessionIdRef.current,
        hold_duration_seconds: DEMO_HOLD_SECONDS,
      });
      setHoldState({ status: "success", data });
    } catch (err) {
      setHoldState({ status: "error", message: describeError(err) });
    }
  }

  const seat = seatState.status === "success" ? seatState.data : null;
  const expired = remainingMs !== null && remainingMs <= 0;

  return (
    <section className="wt-section" aria-labelledby="wt-lifecycle-heading">
      <div className="wt-section-header">
        <h2 id="wt-lifecycle-heading">2. Hold lifecycle</h2>
      </div>

      <p className="wt-prediction">
        <strong>Prediction: </strong>
        a {DEMO_HOLD_SECONDS}s hold expires, but the seat becomes reclaimable the INSTANT it
        expires, not when a background sweep process eventually gets to it — even though the raw
        status column will keep saying HELD for a little while longer.
      </p>

      <div className="wt-controls">
        <button
          type="button"
          className="wt-button"
          onClick={takeHold}
          disabled={holdState.status === "loading"}
        >
          {holdState.status === "loading" ? "Holding…" : "Take a hold"}
        </button>
        {remainingMs !== null && (
          <span className="wt-mono">
            {expired ? "expired" : `expires in ${formatRemaining(remainingMs)}`}
          </span>
        )}
      </div>

      {holdState.status === "error" && <p className="wt-error">{holdState.message}</p>}

      {seat && (
        <>
          <StateMachineDiagram current={toMachineState(seat.status)} />

          <dl className="wt-raw-vs-computed">
            <div>
              <dt>status column (raw)</dt>
              <dd>{seat.status}</dd>
            </div>
            <div>
              <dt>bookable (computed, lazy expiry)</dt>
              <dd style={{ color: seat.bookable ? "var(--status-good)" : "var(--status-critical)" }}>
                {seat.bookable ? "yes" : "no"}
              </dd>
            </div>
          </dl>

          {seat.status === "HELD" && seat.bookable && (
            <p className="wt-prediction">
              This is the Phase 4 design claim, live: <code>status</code> still reads{" "}
              <strong>HELD</strong>, but <code>bookable</code> is already <strong>true</strong>.
              The seat has been reclaimable since the instant it expired — every real acquisition
              path treats it as available already, whether or not the background sweeper (which
              runs every few seconds and only updates the status column, for bookkeeping) has
              physically reached this row yet. Watch <code>status</code> flip to AVAILABLE on its
              own, once the sweeper does.
            </p>
          )}
        </>
      )}
    </section>
  );
}
