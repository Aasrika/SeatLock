// Last-seat scenario: every virtual user targets the SAME single seat for
// the whole run. Worst-case contention — this is the scenario most likely
// to expose Strategy A's oversell on every run, and later, the scenario
// where pessimistic locking's throughput collapse and optimistic locking's
// retry storm are most visible.
//
// Two scenarios, run back to back:
//   warmup   — WARMUP_VUS hitting GET /health for WARMUP_DURATION. Proven
//              by experiment (see loadtest/results/) that a cold process
//              (DB pool filling lazily, import paths, uvicorn's HTTP
//              parsing, FastAPI routing -- all paying first-call cost) is
//              why a sudden burst gets connection-refused at t=0, not
//              worker count or backlog size. Warmup never touches
//              /api/holds, so it cannot contaminate the measured phase's
//              seat availability.
//   measured — the actual burst: VUS constant VUs hammering SEAT_ID for
//              DURATION. This is the scenario under test -- constant-vus
//              on purpose. A flash sale is genuinely everyone arriving at
//              once; softening this into a ramp would stop measuring what
//              we're claiming to measure.
//
// Only the `measured` scenario's requests are counted in the custom
// metrics below (status_2xx / status_409 / status_other /
// status_transport_error / measured_duration_ms) -- warmup requests never
// touch these, so no tag-filtering trick is needed to exclude them.
//
// Parameterised by env vars:
//   BASE_URL         API base URL      (default http://localhost:8000)
//   EVENT_ID         event to hold in  (default 1)
//   SEAT_ID          the one seat id everyone fights over (default 1)
//   VUS              concurrent VUs for the measured burst (default 500)
//   DURATION         measured burst length                 (default 10s)
//   WARMUP_VUS       VUs during warmup                      (default 20)
//   WARMUP_DURATION  warmup length                          (default 10s)
//   SUMMARY_PATH     where handleSummary() writes raw JSON (required to
//                    get a summary file at all -- see handleSummary below)
//
// status_transport_error (res.status === 0, e.g. k6 error_code 1212
// "connection refused") is counted separately from status_other (a real
// HTTP response with an unexpected status) -- conflating them was exactly
// what made last Phase 1 benchmark run impossible to read: a genuine
// connection-refused burst and ordinary "unexpected app response" look
// identical if merged into one bucket.

import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';
const EVENT_ID = parseInt(__ENV.EVENT_ID || '1', 10);
const SEAT_ID = parseInt(__ENV.SEAT_ID || '1', 10);
const VUS = parseInt(__ENV.VUS || '500', 10);
const DURATION = __ENV.DURATION || '10s';
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
  const payload = JSON.stringify({
    event_id: EVENT_ID,
    seat_ids: [SEAT_ID],
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
  // Transport failures never reached the server, so their near-zero
  // "duration" would understate real latency -- excluded from the trend.
  if (res.status !== 0) {
    measuredDuration.add(res.timings.duration);
  }

  check(res, {
    'status is 201 or 409': (r) => r.status === 201 || r.status === 409,
  });
}

// --summary-export is deprecated and (confirmed by direct inspection) uses
// a different, flatter schema than handleSummary()'s `data` argument --
// see loadtest/run_benchmark.py's parser for the schema handleSummary
// actually produces (data.metrics.<name>.values.<stat>). Writing it
// ourselves, keyed by an env var, avoids depending on a deprecated flag at
// all.
export function handleSummary(data) {
  const out = {};
  if (__ENV.SUMMARY_PATH) {
    out[__ENV.SUMMARY_PATH] = JSON.stringify(data);
  }
  return out;
}
