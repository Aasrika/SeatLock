import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { RaceSection } from "./RaceSection";
import type { RaceResponse } from "../../demo-api";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

// A representative naive-strategy race outcome: 3 acquired (oversold),
// 1 rejected, invariants deliberately showing the disclosed 4-of-5 gap
// (InvariantPanel.tsx's own reasoning) rather than a fake "all five."
function makeRaceResponse(overrides: Partial<RaceResponse> = {}): RaceResponse {
  return {
    event_id: 1,
    seat_id: 10,
    strategy: "naive",
    concurrency: 4,
    attempts: [
      { session_id: "s1", outcome: "acquired", latency_ms: 12.5, attempts: 1, reason: null },
      { session_id: "s2", outcome: "acquired", latency_ms: 15.1, attempts: 1, reason: null },
      { session_id: "s3", outcome: "acquired", latency_ms: 9.8, attempts: 1, reason: null },
      { session_id: "s4", outcome: "rejected", latency_ms: 20.0, attempts: 1, reason: "already booked" },
    ],
    successful_holders: 3,
    excess_holders: 2,
    invariants: {
      results: {
        conservation: { passed: true, detail: null },
        structural: { passed: true, detail: null },
        state_coherence: { passed: false, detail: "seat 10 has 3 concurrent holders" },
        booking_linkage: { passed: true, detail: null },
      },
      checked_count: 4,
      total_count: 5,
      unchecked: ["I3", "I4", "I5"],
      unchecked_note:
        "I3/I4/I5 are not evaluated by this live checker -- they are covered by " +
        "the project's test suite and chaos scenarios instead. See docs/chaos-results.md.",
    },
    ...overrides,
  };
}

describe("RaceSection", () => {
  let fetchMock: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("idle: shows the per-strategy prediction before firing, with nothing fired yet", () => {
    render(<RaceSection eventId={1} seatId={10} />);
    expect(screen.getByText(/expect MULTIPLE holders/)).toBeInTheDocument();
    expect(screen.queryByText(/holder.*for 1 seat/)).not.toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("loading: disables the Fire button and shows Firing… while the request is in flight", async () => {
    let resolveFetch!: (r: Response) => void;
    fetchMock.mockReturnValue(
      new Promise<Response>((resolve) => {
        resolveFetch = resolve;
      }),
    );
    render(<RaceSection eventId={1} seatId={10} />);
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Fire" }));

    const button = screen.getByRole("button", { name: "Firing…" });
    expect(button).toBeDisabled();

    resolveFetch(jsonResponse(makeRaceResponse()));
    await waitFor(() => expect(screen.getByRole("button", { name: "Fire" })).not.toBeDisabled());
  });

  it("success: renders the holder count, the disclosed 4-of-5 invariant summary, and every attempt row", async () => {
    fetchMock.mockResolvedValue(jsonResponse(makeRaceResponse()));
    render(<RaceSection eventId={1} seatId={10} />);
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Fire" }));

    await waitFor(() => expect(screen.getByText("3")).toBeInTheDocument());
    expect(screen.getByText(/2 oversold/)).toBeInTheDocument();
    expect(screen.getByText("prediction held")).toBeInTheDocument();
    // The disclosure requirement: "4 of 5," never "all five."
    expect(screen.getByText(/4 of 5 verified live/)).toBeInTheDocument();
    expect(screen.getByText(/I3\/I4\/I5 are not evaluated by this live checker/)).toBeInTheDocument();
    expect(screen.getAllByText("acquired")).toHaveLength(3);
    expect(screen.getByText("rejected")).toBeInTheDocument();
  });

  it("error: shows the server's detail message and re-enables the Fire button", async () => {
    fetchMock.mockResolvedValue(jsonResponse({ detail: "event not found" }, 404));
    render(<RaceSection eventId={1} seatId={10} />);
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Fire" }));

    await waitFor(() => expect(screen.getByText("event not found")).toBeInTheDocument());
    expect(screen.getByRole("button", { name: "Fire" })).not.toBeDisabled();
  });
});
