// Last-seat scenario: every virtual user targets the SAME single seat for
// the whole run. Worst-case contention — this is the scenario most likely
// to expose Strategy A's oversell on every run, and later, the scenario
// where pessimistic locking's throughput collapse and optimistic locking's
// retry storm are most visible.
//
// Parameterised by env vars:
//   BASE_URL   API base URL      (default http://localhost:8000)
//   EVENT_ID   event to hold in  (default 1)
//   SEAT_ID    the one seat id everyone fights over (default 1)
//   VUS        concurrent virtual users for the whole run (default 500)
//   DURATION   run length                                 (default 10s)
//
// Uses 'constant-vus' rather than flash_sale.js's ramp: there is no build-
// up here, every VU starts hammering the same seat immediately, which is
// the point.
//
// Records the same three things as flash_sale.js: HTTP status
// distribution, p50/p95/p99 (via summaryTrendStats), throughput.

import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';
const EVENT_ID = parseInt(__ENV.EVENT_ID || '1', 10);
const SEAT_ID = parseInt(__ENV.SEAT_ID || '1', 10);
const VUS = parseInt(__ENV.VUS || '500', 10);
const DURATION = __ENV.DURATION || '10s';

const status2xx = new Counter('status_2xx');
const status409 = new Counter('status_409');
const statusOther = new Counter('status_other');

export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    last_seat: {
      executor: 'constant-vus',
      vus: VUS,
      duration: DURATION,
    },
  },
};

export default function () {
  const payload = JSON.stringify({
    event_id: EVENT_ID,
    seat_ids: [SEAT_ID],
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
