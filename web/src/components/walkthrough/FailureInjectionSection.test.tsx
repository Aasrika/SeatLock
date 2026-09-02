import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { FailureInjectionSection } from "./FailureInjectionSection";
import type { DemoStateResponse } from "../../demo-api";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function makeStateResponse(): DemoStateResponse {
  return {
    event_id: 1,
    checked_at: new Date().toISOString(),
    seats: [
      {
        id: 10,
        section: "A",
        row_label: "1",
        seat_number: 1,
        status: "AVAILABLE",
        held_by_session_id: null,
        hold_expires_at: null,
        booking_id: null,
        bookable: true,
      },
    ],
    invariants: {
      results: { conservation: { passed: true, detail: null } },
      checked_count: 4,
      total_count: 5,
      unchecked: ["I3", "I4", "I5"],
      unchecked_note: "note",
    },
  };
}

describe("FailureInjectionSection", () => {
  let fetchMock: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("idle: shows copyable commands and does not poll until asked to", () => {
    render(<FailureInjectionSection eventId={1} />);
    expect(screen.getByText("docker kill seatlock-redis-1")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Start polling/ })).toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("success: polling renders live seat state and the disclosed invariant panel", async () => {
    fetchMock.mockResolvedValue(jsonResponse(makeStateResponse()));
    render(<FailureInjectionSection eventId={1} />);
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: /Start polling/ }));

    await waitFor(() => expect(screen.getByText("AVAILABLE")).toBeInTheDocument());
    expect(screen.getByText(/4 of 5 verified live/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Stop polling" })).toBeInTheDocument();
  });

  it("error: shows the failure message when a poll fails (e.g. the backend is down)", async () => {
    fetchMock.mockResolvedValue(jsonResponse({ detail: "internal error" }, 500));
    render(<FailureInjectionSection eventId={1} />);
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: /Start polling/ }));

    await waitFor(() => expect(screen.getByText("internal error")).toBeInTheDocument());
  });
});
