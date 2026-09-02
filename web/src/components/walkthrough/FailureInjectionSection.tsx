import { useEffect, useState } from "react";
import { getDemoState } from "../../demo-api";
import type { DemoStateResponse } from "../../demo-api";
import { type AsyncState, describeError } from "../../async-state";
import { InvariantPanel } from "./InvariantPanel";

const POLL_MS = 2000;

const COMMANDS = [
  { label: "Kill Redis outright", command: "docker kill seatlock-redis-1" },
  { label: "Pause Redis (a hang, not a crash)", command: "docker pause seatlock-redis-1" },
  { label: "Bring Redis back", command: "docker unpause seatlock-redis-1 || docker start seatlock-redis-1" },
  { label: "Restart Postgres mid-load", command: "docker compose restart postgres" },
];

export function FailureInjectionSection({ eventId }: { eventId: number }) {
  const [polling, setPolling] = useState(false);
  const [state, setState] = useState<AsyncState<DemoStateResponse>>({ status: "idle" });

  useEffect(() => {
    if (!polling) return;
    let cancelled = false;
    const poll = async () => {
      try {
        const data = await getDemoState(eventId);
        if (!cancelled) setState({ status: "success", data });
      } catch (err) {
        if (!cancelled) setState({ status: "error", message: describeError(err) });
      }
    };
    void poll();
    const interval = setInterval(() => void poll(), POLL_MS);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [polling, eventId]);

  return (
    <section className="wt-section" aria-labelledby="wt-failure-heading">
      <div className="wt-section-header">
        <h2 id="wt-failure-heading">4. Failure injection</h2>
      </div>

      <p className="wt-prediction">
        <strong>Prediction: </strong>
        holds and confirms keep succeeding on Postgres alone; all four live invariants keep
        passing; availability reads that would have used Redis fall back to Postgres and slow
        down. Correct-but-slower, never incorrect — this is the same claim{" "}
        <a
          href="https://github.com/Aasrika/SeatLock/blob/main/docs/chaos-results.md"
          target="_blank"
          rel="noreferrer"
        >
          docs/chaos-results.md
        </a>{" "}
        tests with real chaos scenarios, not a browser click.
      </p>

      <p style={{ color: "var(--text-secondary)" }}>
        Redis and Postgres failures require Docker and cannot be triggered from this browser tab.
        Copy a command below into a terminal, then watch this panel while it runs:
      </p>

      {COMMANDS.map((c) => (
        <CopyCommand key={c.command} label={c.label} command={c.command} />
      ))}

      <div className="wt-controls" style={{ marginTop: 10 }}>
        <button type="button" className="wt-button" onClick={() => setPolling((p) => !p)}>
          {polling ? "Stop polling" : "Start polling /api/demo/state"}
        </button>
        {polling && <span className="wt-status wt-status--warning">live, every {POLL_MS / 1000}s</span>}
      </div>

      {state.status === "error" && <p className="wt-error">{state.message}</p>}

      {state.status === "success" && (
        <div className="wt-row">
          <div className="wt-col">
            <h3>Seats</h3>
            <div className="wt-attempts">
              {state.data.seats.map((seat) => (
                <div key={seat.id} className="wt-attempt-row">
                  <span>
                    {seat.section}
                    {seat.row_label}-{seat.seat_number}
                  </span>
                  <span className="wt-mono">{seat.status}</span>
                  <span>{seat.bookable ? "bookable" : "not bookable"}</span>
                </div>
              ))}
            </div>
          </div>
          <div className="wt-col">
            <InvariantPanel invariants={state.data.invariants} />
          </div>
        </div>
      )}

      <p style={{ maxWidth: "72ch", color: "var(--text-secondary)", marginTop: 12 }}>
        Seven more scenarios — Redis killed-and-restarted-empty, the sweeper killed outright, one
        of four API workers killed mid-transaction (with and without a held row lock), and a
        Postgres restart caught a real 500-vs-503 bug — are exercised for real, under sustained
        load, in{" "}
        <a
          href="https://github.com/Aasrika/SeatLock/blob/main/docs/chaos-results.md"
          target="_blank"
          rel="noreferrer"
        >
          docs/chaos-results.md
        </a>
        . Those cannot be demonstrated from a browser at all.
      </p>
    </section>
  );
}

function CopyCommand({ label, command }: { label: string; command: string }) {
  const [copied, setCopied] = useState(false);

  async function copy() {
    try {
      await navigator.clipboard.writeText(command);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      // Clipboard API can be unavailable (permissions, non-HTTPS
      // context) -- the command is still visible and selectable by
      // hand, so this is a degraded-but-functional fallback, not an
      // error worth surfacing.
    }
  }

  return (
    <div className="wt-copy-command">
      <span className="wt-copy-command-label">{label}</span>
      <code>{command}</code>
      <button type="button" className="wt-button wt-button--secondary" onClick={copy}>
        {copied ? "Copied" : "Copy"}
      </button>
    </div>
  );
}
