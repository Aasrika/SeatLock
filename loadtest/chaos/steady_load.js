// Phase 8a's chaos-suite load generator. Distinct from loadtest/*.js
// (Phase 3's benchmarks): those are short, single-endpoint bursts built
// to make oversell/throughput comparable across strategies. This one
// exercises the FULL booking hot path (hold -> create booking -> confirm)
// continuously for a long, injectable-mid-run DURATION, because every
// chaos scenario needs load running not just at the moment of injection
// but through steady state, injection, and recovery alike (SPEC.md
// section 10's discipline: steady state -> hypothesis -> inject -> assert
// throughout -> recover).
//
// Most iterations only hold (and let the hold expire -- HOLD_DURATION_
// SECONDS is overridden short by the harness, same trick as Phase 3's
// recirculating benchmark) so the seat pool actually recirculates over a
// run this long instead of monotonically draining into BOOKED. A
// configurable fraction (BOOKING_FRACTION) goes all the way through
// create-booking + confirm, because several chaos hypotheses are
// specifically about the CONFIRM path (Postgres restart: 503 not 500;
// Redis pause: booking throughput specifically, not just holds).
//
// Every request is tagged { name: 'hold' | 'booking_create' | 'confirm' }
// -- this is what loadtest/chaos/harness.py's live tailer of k6's own
// `--out json=...` stream groups by to build a timestamped, per-endpoint-
// kind outcome timeline DURING the run (not just from the end-of-run
// summary). Do not remove or rename these tags; the harness depends on
// them by exact string.
//
// Parameterised by env vars:
//   BASE_URL          API base URL             (default http://localhost:8000)
//   EVENT_ID          event to hold seats in    (default 1)
//   SEAT_IDS          comma-separated seat ids  (required)
//   VUS               concurrent VUs            (default 30)
//   DURATION          measured run length       (default 60s)
//   WARMUP_VUS        VUs during warmup         (default 10)
//   WARMUP_DURATION   warmup length             (default 5s)
//   BOOKING_FRACTION  P(a successful hold proceeds to book+confirm) (default 0.2)
//   SUMMARY_PATH      where handleSummary() writes raw JSON

import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';
const EVENT_ID = parseInt(__ENV.EVENT_ID || '1', 10);
const SEAT_IDS = (__ENV.SEAT_IDS || '')
  .split(',')
  .map((s) => parseInt(s.trim(), 10))
  .filter((n) => !Number.isNaN(n));
const VUS = parseInt(__ENV.VUS || '30', 10);
const DURATION = __ENV.DURATION || '60s';
const WARMUP_VUS = parseInt(__ENV.WARMUP_VUS || '10', 10);
const WARMUP_DURATION = __ENV.WARMUP_DURATION || '5s';
const BOOKING_FRACTION = parseFloat(__ENV.BOOKING_FRACTION || '0.2');

// Generic, kind-agnostic totals -- same six-way split for every request
// regardless of which endpoint it hit. This is what a Postgres-restart
// scenario cares about across the board ("no request returns 500,
// anywhere"), and what parse_k6_summary()-style post-run reporting wants
// for parity with loadtest/run_benchmark.py's existing categories.
const status2xx = new Counter('status_2xx');
const status409 = new Counter('status_409');
const status500 = new Counter('status_500');
const status503 = new Counter('status_503');
const statusOther = new Counter('status_other');
const statusTransportError = new Counter('status_transport_error');

// Per-kind success counters -- what "booking throughput held up" actually
// means: successful confirms per second, not "requests per second" of any
// kind. hold_2xx alone cannot tell you whether the confirm path is even
// being reached.
const holdSuccess = new Counter('hold_success');
const bookingCreateSuccess = new Counter('booking_create_success');
const confirmSuccess = new Counter('confirm_success');

export const options = {
  scenarios: {
    warmup: {
      executor: 'constant-vus',
      vus: WARMUP_VUS,
      duration: WARMUP_DURATION,
      startTime: '0s',
      exec: 'warmup',
      tags: { phase: 'warmup' },
    },
    measured: {
      executor: 'constant-vus',
      vus: VUS,
      duration: DURATION,
      startTime: WARMUP_DURATION,
      exec: 'measured',
      tags: { phase: 'measured' },
    },
  },
};

export function warmup() {
  http.get(`${BASE_URL}/health`, { tags: { phase: 'warmup', name: 'health' } });
}

function categorize(res) {
  if (res.status === 0) {
    statusTransportError.add(1);
  } else if (res.status >= 200 && res.status < 300) {
    status2xx.add(1);
  } else if (res.status === 409) {
    status409.add(1);
  } else if (res.status === 500) {
    status500.add(1);
  } else if (res.status === 503) {
    status503.add(1);
  } else {
    statusOther.add(1);
  }
}

export function measured() {
  const seatId = SEAT_IDS[Math.floor(Math.random() * SEAT_IDS.length)];
  const sessionId = `vu-${__VU}-iter-${__ITER}`;
  const jsonHeaders = { 'Content-Type': 'application/json' };

  const holdRes = http.post(
    `${BASE_URL}/api/holds`,
    JSON.stringify({ event_id: EVENT_ID, seat_ids: [seatId], session_id: sessionId }),
    { headers: jsonHeaders, tags: { phase: 'measured', name: 'hold' } },
  );
  categorize(holdRes);
  check(holdRes, { 'hold: 201 or 409': (r) => r.status === 201 || r.status === 409 });
  if (holdRes.status !== 201) {
    return;
  }
  holdSuccess.add(1);

  if (Math.random() >= BOOKING_FRACTION) {
    // Most holds just expire naturally (short HOLD_DURATION_SECONDS,
    // set by the harness) -- this is what keeps the seat pool
    // recirculating instead of monotonically draining into BOOKED
    // over a run this long.
    return;
  }

  const bookingRes = http.post(
    `${BASE_URL}/api/bookings`,
    JSON.stringify({
      event_id: EVENT_ID,
      seat_ids: [seatId],
      session_id: sessionId,
      user_id: 1,
      total_amount: '42.00',
      currency: 'USD',
    }),
    {
      headers: { ...jsonHeaders, 'Idempotency-Key': `create-${sessionId}` },
      tags: { phase: 'measured', name: 'booking_create' },
    },
  );
  categorize(bookingRes);
  if (bookingRes.status !== 201) {
    return;
  }
  bookingCreateSuccess.add(1);
  const bookingId = bookingRes.json('id');

  const confirmRes = http.post(
    `${BASE_URL}/api/bookings/${bookingId}/confirm`,
    JSON.stringify({ session_id: sessionId }),
    {
      headers: { ...jsonHeaders, 'Idempotency-Key': `confirm-${sessionId}` },
      tags: { phase: 'measured', name: 'confirm' },
    },
  );
  categorize(confirmRes);
  if (confirmRes.status === 200) {
    confirmSuccess.add(1);
  }
  check(confirmRes, { 'confirm: never 500': (r) => r.status !== 500 });
}

export function handleSummary(data) {
  const out = {};
  if (__ENV.SUMMARY_PATH) {
    out[__ENV.SUMMARY_PATH] = JSON.stringify(data);
  }
  return out;
}
