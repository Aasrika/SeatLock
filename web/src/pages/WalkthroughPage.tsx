import { useEffect, useState } from "react";
import { ApiError } from "../api";
import { getDemoState, resetDemoEvent } from "../demo-api";
import { type AsyncState, describeError } from "../async-state";
import { RaceSection } from "../components/walkthrough/RaceSection";
import { LifecycleSection } from "../components/walkthrough/LifecycleSection";
import { IdempotencySection } from "../components/walkthrough/IdempotencySection";
import { FailureInjectionSection } from "../components/walkthrough/FailureInjectionSection";
import "../walkthrough.css";

// Gate-probe outcomes -- FastAPI's default body for a bare
// HTTPException(404) (app/api/routes/demo.py's require_demo_mode) is
// {"detail": "Not Found"}, distinct from _get_event_or_404's explicit
// {"detail": "event not found"} -- checked directly against the running
// app, not assumed. That difference is what lets this page tell "demo
// mode is off" apart from "the demo event doesn't exist yet" instead of
// collapsing both into one unhelpful message.
type GateState =
  | { status: "checking" }
  | { status: "ready"; seatIds: [number, number, number] }
  | { status: "demo_mode_off" }
  | { status: "event_missing" }
  | { status: "error"; message: string };

export function WalkthroughPage({ eventId }: { eventId: number }) {
  const [gate, setGate] = useState<GateState>({ status: "checking" });
  const [resetState, setResetState] = useState<AsyncState<number>>({ status: "idle" });

  useEffect(() => {
    let cancelled = false;
    async function probe() {
      try {
        const state = await getDemoState(eventId);
        if (cancelled) return;
        if (state.seats.length < 3) {
          setGate({
            status: "error",
            message: `Event ${eventId} has ${state.seats.length} seat(s); the walkthrough needs at least 3.`,
          });
          return;
        }
        const seatIds = state.seats.slice(0, 3).map((s) => s.id) as [number, number, number];
        setGate({ status: "ready", seatIds });
      } catch (err) {
        if (cancelled) return;
        if (err instanceof ApiError && err.status === 404) {
          const detail =
            err.body !== null && typeof err.body === "object" && "detail" in err.body
              ? (err.body as { detail: unknown }).detail
              : null;
          setGate({ status: detail === "event not found" ? "event_missing" : "demo_mode_off" });
          return;
        }
        setGate({ status: "error", message: describeError(err) });
      }
    }
    void probe();
    return () => {
      cancelled = true;
    };
  }, [eventId]);

  async function resetAll() {
    setResetState({ status: "loading" });
    try {
      const result = await resetDemoEvent(eventId);
      setResetState({ status: "success", data: result.seats_reset });
    } catch (err) {
      setResetState({ status: "error", message: describeError(err) });
    }
  }

  if (gate.status === "checking") {
    return (
      <div className="walkthrough">
        <p>Checking demo availability…</p>
      </div>
    );
  }

  if (gate.status === "demo_mode_off") {
    return (
      <div className="walkthrough">
        <h2>Demo mode is off</h2>
        <p className="wt-error">
          This page needs the backend started with <code className="wt-mono">DEMO_MODE=true</code>.
          Every route under <code className="wt-mono">/api/demo</code> 404s otherwise, deliberately
          — see <code className="wt-mono">app/api/routes/demo.py</code>.
        </p>
      </div>
    );
  }

  if (gate.status === "event_missing") {
    return (
      <div className="walkthrough">
        <h2>Demo event not found</h2>
        <p className="wt-error">
          Event {eventId} doesn&apos;t exist yet. Run{" "}
          <code className="wt-mono">python -m scripts.seed_demo_event</code> and reload.
        </p>
      </div>
    );
  }

  if (gate.status === "error") {
    return (
      <div className="walkthrough">
        <p className="wt-error">{gate.message}</p>
      </div>
    );
  }

  const [raceSeatId, lifecycleSeatId, idempotencySeatId] = gate.seatIds;

  return (
    <div className="walkthrough">
      <h2>How this system stays correct under concurrency</h2>
      <p className="wt-intro">
        Four short, click-driven demonstrations of the guarantees this project makes — each states
        its prediction first, then shows what actually happened. ~90 seconds end to end.
      </p>
      <div className="wt-controls">
        <button
          type="button"
          className="wt-button wt-button--secondary"
          onClick={resetAll}
          disabled={resetState.status === "loading"}
        >
          {resetState.status === "loading" ? "Resetting…" : "Reset all seats"}
        </button>
        {resetState.status === "success" && (
          <span className="wt-mono">{resetState.data} seat(s) reset</span>
        )}
        {resetState.status === "error" && <span className="wt-error">{resetState.message}</span>}
      </div>

      <RaceSection eventId={eventId} seatId={raceSeatId} />
      <LifecycleSection eventId={eventId} seatId={lifecycleSeatId} />
      <IdempotencySection eventId={eventId} seatId={idempotencySeatId} />
      <FailureInjectionSection eventId={eventId} />
    </div>
  );
}
