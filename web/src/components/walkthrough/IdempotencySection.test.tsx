import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { IdempotencySection } from "./IdempotencySection";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

const HOLD_RESPONSE = {
  event_id: 1,
  seat_id: 10,
  session_id: "s1",
  hold_expires_at: new Date(Date.now() + 60_000).toISOString(),
};

const BOOKING_RESPONSE = {
  id: 42,
  event_id: 1,
  seat_ids: [10],
  status: "PENDING",
  total_amount: "42.00",
  currency: "USD",
};

describe("IdempotencySection", () => {
  let fetchMock: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("idle: shows the prediction and has not called the network yet", () => {
    render(<IdempotencySection eventId={1} seatId={10} />);
    expect(screen.getByText(/surfacing that loudly, as a 422, is correct/)).toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("loading: disables the button and shows Running… while the sequence is in flight", async () => {
    let resolveHold!: (r: Response) => void;
    fetchMock.mockImplementation((url: string) => {
      if (url.includes("/demo/hold")) {
        return new Promise<Response>((resolve) => {
          resolveHold = resolve;
        });
      }
      return Promise.resolve(jsonResponse(BOOKING_RESPONSE));
    });

    render(<IdempotencySection eventId={1} seatId={10} />);
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Run idempotency demo" }));

    expect(screen.getByRole("button", { name: "Running…" })).toBeDisabled();
    resolveHold(jsonResponse(HOLD_RESPONSE));
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "Run idempotency demo" })).not.toBeDisabled(),
    );
  });

  it("success: replaying the same key returns the same booking id twice, and a different body under it 422s", async () => {
    let bookingCalls = 0;
    fetchMock.mockImplementation((url: string) => {
      if (url.includes("/demo/hold")) {
        return Promise.resolve(jsonResponse(HOLD_RESPONSE));
      }
      if (url.includes("/bookings")) {
        bookingCalls += 1;
        if (bookingCalls <= 2) {
          return Promise.resolve(jsonResponse(BOOKING_RESPONSE));
        }
        return Promise.resolve(
          jsonResponse({ detail: "Idempotency-Key reused with a different request body" }, 422),
        );
      }
      return Promise.reject(new Error(`unexpected fetch: ${url}`));
    });

    render(<IdempotencySection eventId={1} seatId={10} />);
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Run idempotency demo" }));

    await waitFor(() =>
      expect(screen.getByText(/exactly one booking was created/)).toBeInTheDocument(),
    );
    expect(screen.getAllByText(/booking #42/)).toHaveLength(2);
    expect(screen.getByText(/422 — Idempotency-Key reused/)).toBeInTheDocument();
    expect(bookingCalls).toBe(3);
  });

  it("error: surfaces an unexpected failure instead of silently doing nothing", async () => {
    fetchMock.mockImplementation((url: string) => {
      if (url.includes("/demo/hold")) {
        return Promise.resolve(jsonResponse({ detail: "seat already held" }, 409));
      }
      return Promise.reject(new Error(`unexpected fetch: ${url}`));
    });

    render(<IdempotencySection eventId={1} seatId={10} />);
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Run idempotency demo" }));

    await waitFor(() => expect(screen.getByText("seat already held")).toBeInTheDocument());
  });
});
