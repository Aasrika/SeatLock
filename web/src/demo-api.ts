// Typed client for app/api/routes/demo.py -- the walkthrough page's
// backend. Every one of these 404s when DEMO_MODE is off server-side;
// callers are expected to treat a 404 from THESE specific endpoints as
// "the demo isn't enabled here," not as "this seat doesn't exist" (see
// WalkthroughPage's own top-level DEMO_MODE check).

import { ApiError } from "./api";

const API_BASE = import.meta.env.VITE_API_BASE ?? "/api";

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const response = await fetch(`${API_BASE}/demo${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const data: unknown = await response.json();
  if (!response.ok) {
    throw new ApiError(response.status, data);
  }
  return data as T;
}

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE}/demo${path}`);
  const data: unknown = await response.json();
  if (!response.ok) {
    throw new ApiError(response.status, data);
  }
  return data as T;
}

export type StrategyName = "naive" | "pessimistic" | "optimistic";

export interface InvariantResult {
  passed: boolean;
  detail: string | null;
}

// Deliberately not "all five": app/api/routes/demo.py's own
// _summarize_invariants only ever reports the four checks GET /api/
// admin/invariants actually verifies (conservation/I2, a structural I1
// check, state-coherence, booking-linkage) -- I3/I4/I5 are listed as
// UNCHECKED here on purpose, not omitted. See InvariantPanel.
export interface InvariantSummary {
  results: Record<string, InvariantResult>;
  checked_count: number;
  total_count: 5;
  unchecked: string[];
  unchecked_note: string;
}

export interface RaceRequest {
  event_id: number;
  seat_id: number;
  concurrency: number;
  strategy: StrategyName;
}

export interface AttemptResult {
  session_id: string;
  outcome: "acquired" | "rejected" | "error";
  latency_ms: number;
  attempts: number;
  reason: string | null;
}

export interface RaceResponse {
  event_id: number;
  seat_id: number;
  strategy: StrategyName;
  concurrency: number;
  attempts: AttemptResult[];
  successful_holders: number;
  excess_holders: number;
  invariants: InvariantSummary;
}

export function runRace(req: RaceRequest): Promise<RaceResponse> {
  return postJson<RaceResponse>("/race", req);
}

export interface ResetResponse {
  event_id: number;
  seats_reset: number;
}

export function resetDemoEvent(eventId: number): Promise<ResetResponse> {
  return postJson<ResetResponse>("/reset", { event_id: eventId });
}

export interface DemoSeat {
  id: number;
  section: string;
  row_label: string;
  seat_number: number;
  status: string;
  held_by_session_id: string | null;
  hold_expires_at: string | null;
  booking_id: number | null;
  bookable: boolean;
}

export interface DemoStateResponse {
  event_id: number;
  checked_at: string;
  seats: DemoSeat[];
  invariants: InvariantSummary;
}

export function getDemoState(eventId: number): Promise<DemoStateResponse> {
  return getJson<DemoStateResponse>(`/state?event_id=${eventId}`);
}

export interface DemoHoldRequest {
  event_id: number;
  seat_id: number;
  session_id: string;
  hold_duration_seconds?: number;
}

export interface DemoHoldResponse {
  event_id: number;
  seat_id: number;
  session_id: string;
  hold_expires_at: string;
}

export function createDemoHold(req: DemoHoldRequest): Promise<DemoHoldResponse> {
  return postJson<DemoHoldResponse>("/hold", req);
}
