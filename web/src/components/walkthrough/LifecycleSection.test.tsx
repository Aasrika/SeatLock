import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { LifecycleSection } from "./LifecycleSection";
import type { DemoSeat, DemoStateResponse } from "../../demo-api";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function makeSeat(overrides: Partial<DemoSeat> = {}): DemoSeat {
  return {
    id: 10,
    section: "A",
    row_label: "1",
    seat_number: 1,
    status: "AVAILABLE",
    held_by_session_id: null,
    hold_expires_at: null,
    booking_id: null,
    bookable: true,
    ...overrides,
  };
}

function makeStateResponse(seat: DemoSeat): DemoStateResponse {
  return {
    event_id: 1,
    checked_at: new Date().toISOString(),
    seats: [seat],
    invariants: {
      results: { conservation: { passed: true, detail: null } },
      checked_count: 4,
      total_count: 5,
      unchecked: ["I3", "I4", "I5"],
      unchecked_note: "note",
    },
  };
}

describe("LifecycleSection", () => {
  let fetchMock: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("idle: shows the prediction and no state-machine diagram before a hold is taken", () => {
    render(<LifecycleSection eventId={1} seatId={10} />);
    expect(screen.getByText(/becomes reclaimable the INSTANT it/)).toBeInTheDocument();
    expect(screen.queryByRole("img", { name: /Seat state machine/ })).not.toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("loading: disables the button and shows Holding… while the hold request is in flight", async () => {
    let resolveHold!: (r: Response) => void;
    fetchMock.mockReturnValue(
      new Promise<Response>((resolve) => {
        resolveHold = resolve;
      }),
    );
    render(<LifecycleSection eventId={1} seatId={10} />);
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Take a hold" }));

    expect(screen.getByRole("button", { name: "Holding…" })).toBeDisabled();

    resolveHold(
      jsonResponse({
        event_id: 1,
        seat_id: 10,
        session_id: "s1",
        hold_expires_at: new Date(Date.now() + 8000).toISOString(),
      }),
    );
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "Take a hold" })).not.toBeDisabled(),
    );
  });

  it("success: shows the raw status/computed bookable side by side and the lazy-expiry paragraph when they diverge", async () => {
    const heldButBookableSeat = makeSeat({ status: "HELD", bookable: true });
    fetchMock.mockImplementation((url: string) => {
      if (url.includes("/demo/hold")) {
        return Promise.resolve(
          jsonResponse({
            event_id: 1,
            seat_id: 10,
            session_id: "s1",
            hold_expires_at: new Date(Date.now() + 8000).toISOString(),
          }),
        );
      }
      if (url.includes("/demo/state")) {
        return Promise.resolve(jsonResponse(makeStateResponse(heldButBookableSeat)));
      }
      return Promise.reject(new Error(`unexpected fetch: ${url}`));
    });

    render(<LifecycleSection eventId={1} seatId={10} />);
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Take a hold" }));

    await waitFor(() =>
      expect(screen.getByRole("img", { name: "Seat state machine, currently HELD" })).toBeInTheDocument(),
    );
    // The side-by-side raw-vs-computed display (refinement #3): status
    // still reads HELD while bookable is already true. "HELD" also
    // appears in the state-machine diagram and the explanation
    // paragraph below, so scope this to the <dd> that holds the raw
    // status value specifically.
    const rawStatusRow = screen.getByText("status column (raw)").closest("div");
    expect(rawStatusRow).not.toBeNull();
    expect(within(rawStatusRow as HTMLElement).getByText("HELD")).toBeInTheDocument();
    expect(screen.getByText("yes")).toBeInTheDocument();
    expect(screen.getByText(/Phase 4 design claim, live/)).toBeInTheDocument();
  });

  it("error: shows the failure message when taking a hold fails", async () => {
    fetchMock.mockResolvedValue(jsonResponse({ detail: "seat already held" }, 409));
    render(<LifecycleSection eventId={1} seatId={10} />);
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Take a hold" }));

    await waitFor(() => expect(screen.getByText("seat already held")).toBeInTheDocument());
    expect(screen.getByRole("button", { name: "Take a hold" })).not.toBeDisabled();
  });
});
