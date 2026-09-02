import { useState } from "react";
import { ApiError, createBooking } from "../../api";
import type { BookingResponse } from "../../api";
import { createDemoHold } from "../../demo-api";
import { newIdempotencyKey } from "../../idempotency";
import { type AsyncState, describeError } from "../../async-state";

type StepOutcome =
  | { kind: "booking"; label: string; response: BookingResponse }
  | { kind: "conflict"; label: string; status: number; detail: string };

export function IdempotencySection({ eventId, seatId }: { eventId: number; seatId: number }) {
  const [state, setState] = useState<AsyncState<StepOutcome[]>>({ status: "idle" });

  async function run() {
    setState({ status: "loading" });
    const steps: StepOutcome[] = [];
    try {
      const sessionId = newIdempotencyKey();
      await createDemoHold({ event_id: eventId, seat_id: seatId, session_id: sessionId });

      const key = newIdempotencyKey();
      const body = {
        event_id: eventId,
        seat_ids: [seatId],
        session_id: sessionId,
        user_id: 1,
        total_amount: "42.00",
        currency: "USD",
      };

      const first = await createBooking(body, key);
      steps.push({ kind: "booking", label: "First request", response: first });

      const second = await createBooking(body, key);
      steps.push({ kind: "booking", label: "Identical retry, same key", response: second });

      try {
        await createBooking({ ...body, total_amount: "99.00" }, key);
        steps.push({
          kind: "conflict",
          label: "Same key, different body",
          status: 0,
          detail: "expected a 422, but the request succeeded — this would be a real bug",
        });
      } catch (err) {
        if (err instanceof ApiError && err.status === 422) {
          const detail =
            err.body !== null && typeof err.body === "object" && "detail" in err.body
              ? String((err.body as { detail: unknown }).detail)
              : "422 Unprocessable Entity";
          steps.push({ kind: "conflict", label: "Same key, different body", status: 422, detail });
        } else {
          throw err;
        }
      }

      setState({ status: "success", data: steps });
    } catch (err) {
      setState({ status: "error", message: describeError(err) });
    }
  }

  return (
    <section className="wt-section" aria-labelledby="wt-idempotency-heading">
      <div className="wt-section-header">
        <h2 id="wt-idempotency-heading">3. Idempotency</h2>
      </div>

      <p className="wt-prediction">
        <strong>Prediction: </strong>
        the same Idempotency-Key with the same request body returns the identical response twice —
        one booking, not two. The same key with a DIFFERENT body is rejected outright: replaying a
        key means "this is the same request as before," and a caller sending a different body under
        it is either a bug or two attempts colliding — surfacing that loudly, as a 422, is correct;
        silently picking one body to honour would hide whichever mistake caused it.
      </p>

      <div className="wt-controls">
        <button
          type="button"
          className="wt-button"
          onClick={run}
          disabled={state.status === "loading"}
        >
          {state.status === "loading" ? "Running…" : "Run idempotency demo"}
        </button>
      </div>

      {state.status === "error" && <p className="wt-error">{state.message}</p>}

      {state.status === "success" && (
        <ol className="wt-invariant-list" style={{ display: "block" }}>
          {state.data.map((step, i) => (
            <li key={i} style={{ display: "block", marginBottom: 6 }}>
              <span className="wt-status wt-status--neutral">{step.label}</span>{" "}
              {step.kind === "booking" ? (
                <span className="wt-mono">
                  booking #{step.response.id}, status={step.response.status}
                </span>
              ) : (
                <span className={step.status === 422 ? "wt-mono" : "wt-error"}>
                  {step.status === 422 ? `422 — ${step.detail}` : step.detail}
                </span>
              )}
            </li>
          ))}
          {state.data.filter((s) => s.kind === "booking").length === 2 &&
            (state.data[0] as Extract<StepOutcome, { kind: "booking" }>).response.id ===
              (state.data[1] as Extract<StepOutcome, { kind: "booking" }>).response.id && (
              <li>
                <span className="wt-status wt-status--good">confirmed</span> both requests returned
                the same booking id — exactly one booking was created.
              </li>
            )}
        </ol>
      )}
    </section>
  );
}
