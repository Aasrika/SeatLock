// REST calls against the FastAPI backend. Deliberately thin -- no
// business logic here, matching the backend's own "routes are thin"
// convention (app/api/routes/*.py).

const API_BASE = import.meta.env.VITE_API_BASE ?? "/api";

export class ApiError extends Error {
  status: number;
  body: unknown;

  constructor(status: number, body: unknown) {
    super(`API error ${status}`);
    this.status = status;
    this.body = body;
  }
}

async function postJson<T>(path: string, body: unknown, idempotencyKey?: string): Promise<T> {
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (idempotencyKey !== undefined) {
    headers["Idempotency-Key"] = idempotencyKey;
  }
  const response = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });
  const data: unknown = await response.json();
  if (!response.ok) {
    throw new ApiError(response.status, data);
  }
  return data as T;
}

export interface HoldRequest {
  event_id: number;
  seat_ids: number[];
  session_id: string;
}

export interface HoldResponse {
  event_id: number;
  seat_ids: number[];
  session_id: string;
  hold_expires_at: string;
}

// No Idempotency-Key -- app/api/routes/booking.py's POST /holds never
// gained one (only POST /bookings and its confirm did, in Phase 5); a
// double-click here is prevented client-side (see SeatMapPage), not by
// this endpoint's own idempotency machinery.
export function createHold(req: HoldRequest): Promise<HoldResponse> {
  return postJson<HoldResponse>("/holds", req);
}

export interface CreateBookingRequest {
  event_id: number;
  seat_ids: number[];
  session_id: string;
  user_id: number;
  total_amount: string;
  currency: string;
}

export interface BookingResponse {
  id: number;
  event_id: number;
  user_id: number;
  session_id: string;
  status: string;
  seat_ids: number[];
  total_amount: string;
  currency: string;
  created_at: string;
  confirmed_at: string | null;
}

export function createBooking(
  req: CreateBookingRequest,
  idempotencyKey: string,
): Promise<BookingResponse> {
  return postJson<BookingResponse>("/bookings", req, idempotencyKey);
}

export function confirmBooking(
  bookingId: number,
  sessionId: string,
  idempotencyKey: string,
): Promise<BookingResponse> {
  return postJson<BookingResponse>(
    `/bookings/${bookingId}/confirm`,
    { session_id: sessionId },
    idempotencyKey,
  );
}

export interface InvariantResult {
  passed: boolean;
  detail: string | null;
}

export interface DashboardMetrics {
  sweeper_backlog: number;
  lock_wait_seconds_count: number;
  lock_wait_seconds_sum: number;
  deadlocks_total: number;
  lock_timeouts_total: number;
  optimistic_conflicts_total: number;
  optimistic_retries_total: number;
  optimistic_exhausted_total: number;
  reconciliation_divergence_by_kind: Record<string, number>;
  reconciliation_transient_by_kind: Record<string, number>;
}

export interface DashboardResponse {
  checked_at: string;
  event_id: number | null;
  invariants: Record<string, InvariantResult> | null;
  metrics: DashboardMetrics;
}

export async function getDashboard(eventId?: number): Promise<DashboardResponse> {
  const query = eventId !== undefined ? `?event_id=${eventId}` : "";
  const response = await fetch(`${API_BASE}/admin/dashboard${query}`);
  const data: unknown = await response.json();
  if (!response.ok) {
    throw new ApiError(response.status, data);
  }
  return data as DashboardResponse;
}
