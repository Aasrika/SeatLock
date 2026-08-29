// Contention-sweep scenario: Phase 3's phase deliverable. Combines
// last_seat.js's constant-vus load shape (so every contention ratio in
// the sweep is hit with the SAME comparable load pattern, not a ramp that
// would itself vary between runs) with flash_sale.js's "many seats, pick
// one at random" mechanic (so seat count -- the thing loadtest/
// run_benchmark.py's sweep actually varies -- has somewhere to vary).
// Neither existing script alone was the right shape: last_seat.js is
// hardcoded to exactly one seat, flash_sale.js ramps.
//
// Contention ratio (requests offered per seat) is varied by changing SEAT
// COUNT, never VU count. VUs and duration together determine the offered
// load (roughly: VUs * (duration / average request latency) requests get
// attempted); if VU count also changed between sweep points, a difference
// in throughput or p99 could be caused by MORE OR FEWER REQUESTS being
// thrown at the system, not by contention per seat -- confounding the
// exact comparison the sweep exists to make. Holding VUs and duration
// fixed and only ever changing seat count isolates contention as the one
// variable that moves. See loadtest/run_benchmark.py's sweep orchestration
// for how seat count is computed from a target ratio.
//
// Two scenarios, same warmup rationale as last_seat.js/flash_sale.js
// (cold-process connection-refused bursts, see
// docs/benchmarks/phase1-connection-refused-investigation.md):
//   warmup   — WARMUP_VUS hitting GET /health for WARMUP_DURATION.
//   measured — VUS constant VUs, each picking a random seat from SEAT_IDS
//              on every iteration, for DURATION.
//
// Parameterised by env vars:
//   BASE_URL         API base URL           (default http://localhost:8000)
//   EVENT_ID         event to hold seats in (default 1)
//   SEAT_IDS         comma-separated seat ids to draw from (required --
//                    no sensible default for a sweep, unlike flash_sale.js)
//   VUS              concurrent VUs for the measured burst (default 200)
//   DURATION         measured burst length                 (default 10s)
//   WARMUP_VUS       VUs during warmup                      (default 20)
//   WARMUP_DURATION  warmup length                          (default 5s)
//   SUMMARY_PATH     where handleSummary() writes raw JSON

import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';
const EVENT_ID = parseInt(__ENV.EVENT_ID || '1', 10);
const SEAT_IDS = (__ENV.SEAT_IDS || '')
  .split(',')
  .map((s) => parseInt(s.trim(), 10))
  .filter((n) => !Number.isNaN(n));
const VUS = parseInt(__ENV.VUS || '200', 10);
const DURATION = __ENV.DURATION || '10s';
const WARMUP_VUS = parseInt(__ENV.WARMUP_VUS || '20', 10);
const WARMUP_DURATION = __ENV.WARMUP_DURATION || '5s';

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
