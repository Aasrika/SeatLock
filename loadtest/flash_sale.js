// Flash-sale scenario: many virtual users ramping up hard against a small
// seat pool, each requesting one random seat from that pool. Models a
// ticket drop where demand vastly exceeds a small on-sale inventory. This
// is the HEADLINE benchmark scenario: with the whole seat pool in play,
// oversold_seats can actually show a distribution across runs, unlike
// last_seat.js where it is mathematically capped at 1 (see
// loadtest/last_seat.js's docstring and loadtest/results/ for why
// last_seat.js is kept as the worst-case demonstration instead, featuring
// excess_holders).
//
// Two scenarios, run back to back -- see loadtest/last_seat.js's docstring
// for the full rationale (cold-process connection-refused bursts,
// investigated as Experiment 1, resolved by warming up first):
//   warmup   — WARMUP_VUS hitting GET /health for WARMUP_DURATION. Never
//              touches /api/holds, so it cannot pre-consume seats the
//              measured phase needs available.
//   measured — the actual ramp: VUS peak virtual users ramping up over
//              5s, holding for DURATION, ramping down over 5s. Unlike
//              last_seat.js's constant-vus burst, a ramp is the correct
//              shape for THIS scenario (a flash sale's demand curve
//              building, not everyone arriving in the same instant), so
//              it is unchanged by the Experiment 1 investigation.
//
// Only the `measured` scenario's requests are counted in the custom
// metrics below -- warmup requests never touch them.
//
// Parameterised by env vars:
//   BASE_URL         API base URL           (default http://localhost:8000)
//   EVENT_ID         event to hold seats in (default 1)
//   SEAT_IDS         comma-separated seat ids to draw from (default 1..10)
//   VUS              peak virtual users     (default 200)
//   DURATION         time to hold at peak   (default 30s)
//   WARMUP_VUS       VUs during warmup       (default 20)
//   WARMUP_DURATION  warmup length           (default 10s)
//   SUMMARY_PATH     where handleSummary() writes raw JSON

import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';
const EVENT_ID = parseInt(__ENV.EVENT_ID || '1', 10);
const SEAT_IDS = (__ENV.SEAT_IDS || '1,2,3,4,5,6,7,8,9,10')
  .split(',')
  .map((s) => parseInt(s.trim(), 10));
const VUS = parseInt(__ENV.VUS || '200', 10);
const DURATION = __ENV.DURATION || '30s';
const WARMUP_VUS = parseInt(__ENV.WARMUP_VUS || '20', 10);
const WARMUP_DURATION = __ENV.WARMUP_DURATION || '10s';

const status2xx = new Counter('status_2xx');
const status409 = new Counter('status_409');
const statusOther = new Counter('status_other');
const statusTransportError = new Counter('status_transport_error');
const measuredDuration = new Trend('measured_duration_ms', true);

export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
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
      executor: 'ramping-vus',
      startVUs: 0,
      startTime: WARMUP_DURATION,
      stages: [
        { duration: '5s', target: VUS },
        { duration: DURATION, target: VUS },
        { duration: '5s', target: 0 },
      ],
      exec: 'measured',
      tags: { phase: 'measured' },
    },
  },
};

export function warmup() {
  http.get(`${BASE_URL}/health`, { tags: { phase: 'warmup' } });
}

export function measured() {
  const seatId = SEAT_IDS[Math.floor(Math.random() * SEAT_IDS.length)];
  const payload = JSON.stringify({
    event_id: EVENT_ID,
    seat_ids: [seatId],
    session_id: `vu-${__VU}-iter-${__ITER}`,
  });
  const params = {
    headers: { 'Content-Type': 'application/json' },
    tags: { phase: 'measured' },
  };

  const res = http.post(`${BASE_URL}/api/holds`, payload, params);

  if (res.status === 0) {
    statusTransportError.add(1);
  } else if (res.status >= 200 && res.status < 300) {
    status2xx.add(1);
  } else if (res.status === 409) {
    status409.add(1);
  } else {
    statusOther.add(1);
  }
  if (res.status !== 0) {
    measuredDuration.add(res.timings.duration);
  }

  check(res, {
    'status is 201 or 409': (r) => r.status === 201 || r.status === 409,
  });
}

export function handleSummary(data) {
  const out = {};
  if (__ENV.SUMMARY_PATH) {
    out[__ENV.SUMMARY_PATH] = JSON.stringify(data);
  }
  return out;
}
