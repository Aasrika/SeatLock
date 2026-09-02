import { useMemo, useState } from "react";
import { runRace } from "../../demo-api";
import type { RaceResponse, StrategyName } from "../../demo-api";
import { type AsyncState, describeError } from "../../async-state";
import { InvariantPanel } from "./InvariantPanel";

const STRATEGIES: StrategyName[] = ["naive", "pessimistic", "optimistic"];

const PREDICTIONS: Record<StrategyName, string> = {
  naive:
    "naive reads then writes with no lock and no version check — under real " +
    "concurrent contention, expect MULTIPLE holders for one seat.",
  pessimistic:
    "pessimistic takes a row lock before deciding anything — expect EXACTLY ONE holder, always.",
  optimistic:
    "optimistic checks a version number at write time and retries on conflict — " +
    "expect EXACTLY ONE holder, always.",
};

function predictedHolders(strategy: StrategyName): "multiple" | "one" {
  return strategy === "naive" ? "multiple" : "one";
}

export function RaceSection({ eventId, seatId }: { eventId: number; seatId: number }) {
  const [strategy, setStrategy] = useState<StrategyName>("naive");
  const [concurrency, setConcurrency] = useState(20);
  const [state, setState] = useState<AsyncState<RaceResponse>>({ status: "idle" });

  async function fire() {
    setState({ status: "loading" });
    try {
      const data = await runRace({ event_id: eventId, seat_id: seatId, concurrency, strategy });
      setState({ status: "success", data });
    } catch (err) {
      setState({ status: "error", message: describeError(err) });
    }
  }

  const sortedAttempts = useMemo(() => {
    if (state.status !== "success") return [];
    // Sorted by latency, ascending -- the reveal order below then
    // matches the order attempts actually resolved in, not an
    // arbitrary array order the server happened to return.
    return [...state.data.attempts].sort((a, b) => a.latency_ms - b.latency_ms);
  }, [state]);

  return (
    <section className="wt-section" aria-labelledby="wt-race-heading">
      <div className="wt-section-header">
        <h2 id="wt-race-heading">1. The race</h2>
      </div>

      <div className="wt-controls">
        <label htmlFor="wt-strategy">Strategy</label>
        <select
          id="wt-strategy"
          className="wt-select"
          value={strategy}
          onChange={(e) => setStrategy(e.target.value as StrategyName)}
          disabled={state.status === "loading"}
        >
          {STRATEGIES.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>

        <label htmlFor="wt-concurrency">Concurrency: </label>
        <input
          id="wt-concurrency"
          type="range"
          min={2}
          max={100}
          value={concurrency}
          onChange={(e) => setConcurrency(Number(e.target.value))}
          disabled={state.status === "loading"}
        />
        <span className="wt-slider-value wt-mono">{concurrency}</span>

        <button type="button" className="wt-button" onClick={fire} disabled={state.status === "loading"}>
          {state.status === "loading" ? "Firing…" : "Fire"}
        </button>
      </div>

      <p className="wt-prediction">
        <strong>Prediction: </strong>
        {PREDICTIONS[strategy]}
      </p>

      {state.status === "error" && <p className="wt-error">{state.message}</p>}

      {state.status === "success" && (
        <>
          <ResultBanner response={state.data} predicted={predictedHolders(strategy)} />
          <div className="wt-row">
            <div className="wt-col">
              <h3>Attempts ({state.data.attempts.length})</h3>
              <div className="wt-attempts">
                {sortedAttempts.map((attempt) => (
                  <div
                    key={attempt.session_id}
                    className={`wt-attempt-row wt-attempt-row--${attempt.outcome}`}
                    style={{ animationDelay: `${Math.min(attempt.latency_ms, 400)}ms` }}
                  >
                    <span>{attempt.session_id}</span>
                    <OutcomeBadge outcome={attempt.outcome} />
                    <span>{attempt.latency_ms.toFixed(1)}ms</span>
                  </div>
                ))}
              </div>
            </div>
            <div className="wt-col">
              <InvariantPanel invariants={state.data.invariants} />
            </div>
          </div>
        </>
      )}

      <p style={{ maxWidth: "72ch", color: "var(--text-secondary)", marginTop: 12 }}>
        This is TOCTOU: time-of-check-to-time-of-use. naive reads a seat&apos;s status, decides in
        its own memory that it&apos;s available, then writes — with a real window between the read
        and the write where another request can do the exact same thing. PostgreSQL&apos;s default
        READ COMMITTED isolation does not close that window by itself: it only guarantees each
        individual statement sees committed data, not that nothing changes between two statements
        in the same transaction. Closing it takes an explicit lock (pessimistic) or a version check
        tied to the write itself (optimistic) — see the paragraph nobody reads until it&apos;s the
        reason a seat sold twice.
      </p>
    </section>
  );
}

function OutcomeBadge({ outcome }: { outcome: "acquired" | "rejected" | "error" }) {
  if (outcome === "acquired") {
    return <span className="wt-status wt-status--good">acquired</span>;
  }
  if (outcome === "error") {
    return <span className="wt-status wt-status--critical">error</span>;
  }
  // Rejected is the SYSTEM WORKING, not a failure -- most attempts under
  // real contention are supposed to lose. Neutral, not critical/red.
  return <span className="wt-status wt-status--neutral">rejected</span>;
}

function ResultBanner({
  response,
  predicted,
}: {
  response: RaceResponse;
  predicted: "multiple" | "one";
}) {
  const actual = response.successful_holders > 1 ? "multiple" : "one";
  const predictionHeld = actual === predicted;
  const oversold = response.successful_holders > 1;

  return (
    <div
      className={`wt-result-banner ${oversold ? "wt-result-banner--oversold" : "wt-result-banner--held-correctly"}`}
    >
      <strong className="wt-mono" style={{ fontSize: 16 }}>
        {response.successful_holders}
      </strong>
      <span>
        holder{response.successful_holders === 1 ? "" : "s"} for 1 seat
        {response.excess_holders > 0 && (
          <>
            {" "}
            — <strong>{response.excess_holders} oversold</strong>
          </>
        )}
      </span>
      <span className={predictionHeld ? "wt-status wt-status--good" : "wt-status wt-status--critical"}>
        prediction {predictionHeld ? "held" : "did not hold"}
      </span>
    </div>
  );
}
