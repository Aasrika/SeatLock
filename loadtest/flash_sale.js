// Flash-sale scenario: many virtual users ramping up hard against a small
// seat pool, each requesting one random seat from that pool. Models a
// ticket drop where demand vastly exceeds a small on-sale inventory.
//
// Parameterised entirely by env vars so the same script drives every
// strategy/contention-level cell of SPEC.md section 4's measurement
// matrix:
//   BASE_URL   API base URL           (default http://localhost:8000)
//   EVENT_ID   event to hold seats in (default 1)
//   SEAT_IDS   comma-separated seat ids to draw from (default 1..10)
//   VUS        peak virtual users     (default 200)
//   DURATION   time to hold at peak   (default 30s)
//
// Records, per SPEC.md section 4's table: HTTP status distribution
// (status_2xx / status_409 / status_other counters), p50/p95/p99 (k6's
// built-in http_req_duration trend, with summaryTrendStats set below so
// med/p(95)/p(99) are all present in --summary-export output), and
// throughput (k6's built-in http_reqs rate).

import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';
const EVENT_ID = parseInt(__ENV.EVENT_ID || '1', 10);
const SEAT_IDS = (__ENV.SEAT_IDS || '1,2,3,4,5,6,7,8,9,10')
  .split(',')
  .map((s) => parseInt(s.trim(), 10));
const VUS = parseInt(__ENV.VUS || '200', 10);
const DURATION = __ENV.DURATION || '30s';

const status2xx = new Counter('status_2xx');
const status409 = new Counter('status_409');
const statusOther = new Counter('status_other');

export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    flash_sale: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '5s', target: VUS },
        { duration: DURATION, target: VUS },
        { duration: '5s', target: 0 },
      ],
    },
  },
};

export default function () {
  const seatId = SEAT_IDS[Math.floor(Math.random() * SEAT_IDS.length)];
  const payload = JSON.stringify({
    event_id: EVENT_ID,
    seat_ids: [seatId],
    session_id: `vu-${__VU}-iter-${__ITER}`,
  });
  const params = { headers: { 'Content-Type': 'application/json' } };

  const res = http.post(`${BASE_URL}/api/holds`, payload, params);

  if (res.status >= 200 && res.status < 300) {
    status2xx.add(1);
  } else if (res.status === 409) {
    status409.add(1);
  } else {
    statusOther.add(1);
  }

  check(res, {
    'status is 201 or 409': (r) => r.status === 201 || r.status === 409,
  });
}
