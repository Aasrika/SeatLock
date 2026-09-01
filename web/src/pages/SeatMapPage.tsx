import { useMemo, useRef, useState } from "react";
import { ApiError, confirmBooking, createBooking, createHold } from "../api";
import { ConnectionStatus } from "../components/ConnectionStatus";
import { SeatMap } from "../components/SeatMap";
import { useCountdown, formatRemaining } from "../hooks/useCountdown";
import { useSeatMapSocket } from "../hooks/useSeatMapSocket";
import { newIdempotencyKey } from "../idempotency";
import { withOptimisticHold } from "../realtime/client";
import type { SeatMap as SeatMapType } from "../realtime/types";

const SESSION_STORAGE_KEY = "seatlock.session_id";
const USER_ID = 1; // no auth in this phase's scope (CLAUDE.md/SPEC.md section 15) -- a fixed demo user id

function getOrCreateSessionId(): string {
  const existing = sessionStorage.getItem(SESSION_STORAGE_KEY);
  if (existing !== null) return existing;
  const fresh = crypto.randomUUID();
  sessionStorage.setItem(SESSION_STORAGE_KEY, fresh);
  return fresh;
}

interface CheckoutAttemptKeys {
  createKey: string;
  confirmKey: string;
}

export function SeatMapPage({ eventId }: { eventId: number }) {
  const sessionId = useMemo(() => getOrCreateSessionId(), []);
  const { seats, status, serverTimeOffsetMs } = useSeatMapSocket(eventId, "");

  // Optimistic overrides layered on top of the server-driven map -- see
  // client.ts's withOptimisticHold. Cleared for a seat as soon as the
  // WebSocket's own version-tracked diff confirms the real state,
  // whichever it turns out to be.
  const [optimisticHeld, setOptimisticHeld] = useState<ReadonlySet<number>>(() => new Set());
  const [mySeatIds, setMySeatIds] = useState<ReadonlySet<number>>(() => new Set());
  const [message, setMessage] = useState<string | null>(null);
  const [booking, setBooking] = useState<{ id: number; status: string } | null>(null);
  const checkoutKeysRef = useRef<CheckoutAttemptKeys | null>(null);

  const displaySeats: SeatMapType = useMemo(() => {
    let map = seats;
    for (const seatId of optimisticHeld) {
      map = withOptimisticHold(map, seatId);
    }
    return map;
  }, [seats, optimisticHeld]);

  const heldByMe = [...mySeatIds];
  const firstHeldExpiry = heldByMe.length > 0 ? (seats.get(heldByMe[0])?.holdExpiresAt ?? null) : null;
  const remainingMs = useCountdown(firstHeldExpiry, serverTimeOffsetMs);

  async function handleSeatClick(seatId: number) {
    setMessage(null);
    // OPTIMISTIC UI WITH ROLLBACK -- act, then reconcile. The frontend
    // mirror of backend optimistic locking (app/inventory/strategies/
    // optimistic.py): assume the click succeeds immediately, so the UI
    // never waits on a round trip, and undo cleanly the instant the
    // server disagrees.
    setOptimisticHeld((prev) => new Set(prev).add(seatId));
    try {
      await createHold({ event_id: eventId, seat_ids: [seatId], session_id: sessionId });
      setMySeatIds((prev) => new Set(prev).add(seatId));
      // The real confirmation arrives over the WebSocket as a version-
      // tracked diff; once `seats` itself reflects HELD, this override
      // is redundant and can be dropped.
      setOptimisticHeld((prev) => {
        const next = new Set(prev);
        next.delete(seatId);
        return next;
      });
    } catch (err) {
      setOptimisticHeld((prev) => {
        const next = new Set(prev);
        next.delete(seatId);
        return next;
      });
      setMessage(
        err instanceof ApiError
          ? `Could not hold that seat: ${describeApiError(err)}`
          : "Could not hold that seat.",
      );
    }
  }

  async function handleCheckout() {
    if (heldByMe.length === 0) return;
    setMessage(null);

    // Stable across retries of THIS attempt; a fresh pair only when a
    // new attempt starts (see resetAttempt below) -- create and confirm
    // are two DIFFERENT endpoints/paths, so they need two DIFFERENT
    // keys (app/infra/idempotency.py's fingerprint includes the
    // request path; reusing one key across both would make the
    // second call's fingerprint mismatch the first's and get a 422).
    if (checkoutKeysRef.current === null) {
      checkoutKeysRef.current = { createKey: newIdempotencyKey(), confirmKey: newIdempotencyKey() };
    }
    const { createKey, confirmKey } = checkoutKeysRef.current;

    try {
      const created = await createBooking(
        {
          event_id: eventId,
          seat_ids: heldByMe,
          session_id: sessionId,
          user_id: USER_ID,
          total_amount: (heldByMe.length * 42).toFixed(2),
          currency: "USD",
        },
        createKey,
      );
      setBooking({ id: created.id, status: created.status });

      const confirmed = await confirmBooking(created.id, sessionId, confirmKey);
      setBooking({ id: confirmed.id, status: confirmed.status });
      setMessage(`Booking ${confirmed.id} confirmed.`);
      resetAttempt();
    } catch (err) {
      setMessage(
        err instanceof ApiError
          ? `Checkout failed: ${describeApiError(err)}`
          : "Checkout failed.",
      );
      // Deliberately NOT resetting checkoutKeysRef here -- a retry of
      // this same failure should reuse the same keys, per SPEC.md
      // section 9. Only a genuinely new attempt (different seats)
      // regenerates them.
    }
  }

  function resetAttempt() {
    checkoutKeysRef.current = null;
    setMySeatIds(new Set());
    setBooking(null);
  }

  return (
    <div className="seat-map-page">
      <header>
        <h2>Event {eventId}</h2>
        <ConnectionStatus status={status} />
      </header>

      {message !== null && <p className="message">{message}</p>}

      {heldByMe.length > 0 && (
        <div className="checkout-bar">
          <span>
            Holding {heldByMe.length} seat(s){" "}
            {remainingMs !== null && `— expires in ${formatRemaining(remainingMs)}`}
          </span>
          <button type="button" onClick={handleCheckout} disabled={booking?.status === "CONFIRMED"}>
            Book {heldByMe.length} seat(s)
          </button>
        </div>
      )}

      <SeatMap seats={displaySeats} mySeatIds={mySeatIds} onSeatClick={handleSeatClick} />
    </div>
  );
}

function describeApiError(err: ApiError): string {
  if (err.body !== null && typeof err.body === "object" && "reason" in err.body) {
    return String((err.body as { reason: unknown }).reason);
  }
  return err.message;
}
