# Seatlock — Concurrency-Safe Ticketing System

**Project specification and phased build plan**
Stack: Python 3.11 / FastAPI / PostgreSQL / Redis / React + TypeScript / Docker

---

## 0. The one-sentence pitch

A high-contention event ticketing system that provably never oversells a seat, benchmarked against a deliberately naive baseline under 10,000 concurrent requests — with three different concurrency-control strategies implemented, measured, and compared.

Alternative names if you prefer: **HoldFast**, **LastSeat**, **Turnstile**. Keep it boring and technical. The domain is not the selling point; the guarantee is.

---

## 1. Why this project and not another one

You already have ShopFlow (microservices, FastAPI, Redis, Celery, Docker) on your pod.ai resume. A second general-purpose backend app would be redundant. This project is different in kind because it is organised around **one falsifiable claim** rather than a feature list:

> Under adversarial concurrent load, this system's inventory invariant never breaks.

Everything you build exists to support or stress-test that claim. That is the same structural pattern as RAGFail (controlled degradation study) and the Research Copilot CONTRADICTED safety net (conservative classifier that abstains rather than guesses) — but expressed in systems engineering rather than ML. It extends your existing narrative instead of competing with it.

**Critically: you keep the broken version.** The naive implementation stays on a branch as the control condition. A project that demonstrates the bug it fixes is worth five projects that merely claim to have avoided it.

---

## 2. Architectural honesty (read this before you start)

**This is a modular monolith, not microservices.** Build it that way and say so.

You will be tempted to split it into services because "microservices" sounds better on a resume. Resist. Distributed transactions across a booking service and an inventory service would make the correctness guarantee *harder*, not easier, and you would be adding a saga pattern to solve a problem you created yourself.

In an interview, "I considered microservices and chose a modular monolith because the core invariant requires a single transactional boundary over seat inventory, and splitting it would have forced me into eventual consistency for something that needs to be strongly consistent" is a **much** stronger answer than a service diagram. It shows you pick architecture for reasons.

This is the same lesson as the "multi-agent vs orchestration" flag from your Winnify write-up. Precision beats inflation, every time, because inflation collapses under one follow-up question.

**Module boundaries (enforced by directory structure and import rules, not network calls):**

```
app/
  domain/        # entities, state machine, invariants — zero I/O, pure Python
  inventory/     # seat state transitions, concurrency control strategies
  booking/       # orchestration: hold -> confirm -> book
  payments/      # webhook ingestion, event dedup, reconciliation
  admission/     # rate limiting, virtual waiting room
  realtime/      # WebSocket fanout, Redis pub/sub
  platform/      # db session, redis client, idempotency, observability
  api/           # FastAPI routers — thin, no business logic
workers/         # background jobs: hold sweeper, reconciler
loadtest/        # k6 / Locust scenarios
```

Keep `domain/` free of database and Redis imports. If you can unit-test the state machine with no containers running, your boundaries are correct.

---

## 3. The domain model

### Seat state machine

```
AVAILABLE ──hold──> HELD ──confirm──> BOOKED
    ^                 │                  │
    │                 │                  │
    └────expire───────┘                  │
    └────────────release─────────────────┘  (cancellation / refund)
```

**Legal transitions only.** Every transition is a function in `domain/state_machine.py` that takes the current state and returns either the new state or raises `IllegalTransition`. No code anywhere else is allowed to set a seat's status directly.

This single design decision is what makes the whole system defensible. When a late webhook arrives for a seat that already expired, you don't have an `if` statement somewhere — you have a state machine that rejects the transition and routes to the refund path.

### Core tables

```sql
-- events
id, name, venue, starts_at, total_seats, created_at

-- seats
id, event_id, section, row_label, seat_number,
status,              -- AVAILABLE | HELD | BOOKED
version,             -- integer, for optimistic locking
held_by_session_id,  -- nullable
hold_expires_at,     -- nullable timestamptz — SOURCE OF TRUTH for expiry
booking_id,          -- nullable FK
updated_at
UNIQUE (event_id, section, row_label, seat_number)

-- bookings
id, event_id, user_id, session_id,
status,              -- PENDING | CONFIRMED | CANCELLED | REFUNDED
total_amount, currency,
idempotency_key,     -- nullable, indexed
created_at, confirmed_at

-- booking_seats
booking_id, seat_id
UNIQUE (seat_id) WHERE booking active  -- partial unique index, DB-level oversell guard

-- idempotency_keys
key TEXT PRIMARY KEY,
user_id, request_fingerprint,   -- hash of method + path + body
status,                          -- IN_PROGRESS | COMPLETED
response_status, response_body,
created_at, expires_at

-- payment_events
provider_event_id TEXT PRIMARY KEY,   -- dedup guarantee lives here
booking_id, event_type, payload JSONB,
received_at, processed_at, processing_status

-- outbox                  (Phase 5+)
id, aggregate_id, event_type, payload, created_at, published_at
```

### The invariants (write these down, test them relentlessly)

1. **I1 — No oversell.** For any seat, at most one active booking exists. Enforced at three levels: application logic, transaction isolation, and a partial unique index as the last line of defence.
2. **I2 — Conservation.** `count(AVAILABLE) + count(HELD) + count(BOOKED) == total_seats`, always, at any instant.
3. **I3 — No stale holds.** No seat is in `HELD` with `hold_expires_at < now()` for longer than the sweeper interval.
4. **I4 — Idempotency.** The same `Idempotency-Key` with the same request fingerprint always returns the same response and creates at most one booking.
5. **I5 — Webhook exactly-once effect.** Processing the same `provider_event_id` N times has the same effect as processing it once.

Every one of these becomes an automated test. Invariants I1 and I2 also get checked continuously by the load harness while traffic is running, not just at the end.

---

## 4. The concurrency spine — the actual heart of the project

You will implement **three** strategies behind a common interface and measure all three. This comparison is the intellectual core of the project and the thing you will spend most of your interview time discussing.

```python
class SeatAcquisitionStrategy(Protocol):
    async def acquire(self, session, seat_ids: list[int], holder: str) -> AcquireResult: ...
```

Select via config so the load harness can run all three against identical scenarios.

### Strategy A — Naive (the control condition, deliberately broken)

```
1. SELECT status FROM seats WHERE id = ANY(...)
2. if all AVAILABLE:
3.     UPDATE seats SET status = 'HELD' WHERE id = ANY(...)
```

Read-then-write with no locking. Under READ COMMITTED, two transactions both read `AVAILABLE`, both write. This is a **time-of-check-to-time-of-use** race, and it is the same class of bug as a TOCTOU vulnerability in filesystem code.

**Deliverable:** measured oversell count under load. Do not skip this. Prove the problem exists before you solve it.

### Strategy B — Pessimistic locking

```sql
BEGIN;
SELECT id, status FROM seats
  WHERE id = ANY($1)
  ORDER BY id                    -- deterministic ordering prevents deadlock
  FOR UPDATE;
-- validate all AVAILABLE, else ROLLBACK
UPDATE seats SET status='HELD', held_by_session_id=$2, hold_expires_at=now()+interval '8 minutes'
  WHERE id = ANY($1);
COMMIT;
```

**Things you must understand and be able to explain:**

- Why `ORDER BY id` matters: two transactions locking seats {5,9} and {9,5} in opposite order deadlock. Consistent lock ordering is the standard fix and it is a classic interview question.
- `FOR UPDATE` vs `FOR UPDATE SKIP LOCKED` vs `FOR UPDATE NOWAIT`. For *specific* seat selection you want to block or fail fast. For *any-N-seats* selection, `SKIP LOCKED` is dramatically better because contenders grab different rows instead of queueing on the same one. Implement both selection modes — it demonstrates the distinction concretely.
- Lock duration is held for the whole transaction. Never do I/O (payment gateway call, HTTP request) inside a `FOR UPDATE` transaction. This is why holds exist as a separate phase at all.
- Throughput collapses under high contention because locks serialise. You will measure this.

### Strategy C — Optimistic locking

```sql
UPDATE seats
   SET status='HELD', version = version + 1, held_by_session_id=$2, hold_expires_at=$3
 WHERE id = ANY($1) AND status='AVAILABLE' AND version = $4;
-- if rowcount != len(seat_ids): conflict -> rollback, retry with jittered backoff
```

**Things you must understand and be able to explain:**

- Retry with **exponential backoff and full jitter**. Without jitter, retrying clients resynchronise and collide again — a thundering herd you created yourself.
- A retry budget with a hard cap. Unbounded retries under sustained contention are a self-inflicted DoS.
- Optimistic wins under **low** contention (no lock overhead, no blocking). Pessimistic wins under **high** contention (retries waste work; one queue is cheaper than N failed attempts). Your benchmark will show the crossover point. Being able to say "the crossover in my system was around 40 concurrent requests per seat" is a genuinely rare thing for a fresher to say.

### The measurement matrix — your headline result

Run every strategy against every contention level. Fixed duration, fixed seat pool, identical harness.

| Strategy | Contention | Oversells | Throughput (bookings/s) | p50 | p95 | p99 | Retry rate | Error rate |
|---|---|---|---|---|---|---|---|---|
| Naive | Low (10 req/seat) | | | | | | — | |
| Naive | High (500 req/seat) | **>0** | | | | | — | |
| Pessimistic | Low | 0 | | | | | — | |
| Pessimistic | High | 0 | | | | | — | |
| Optimistic | Low | 0 | | | | | | |
| Optimistic | High | 0 | | | | | | |

This table goes at the top of your README. It is the single most valuable artifact in the entire project.

---

## 5. Holds, expiry, and the reconciliation problem

Two-phase booking: **hold** (free, instant, TTL-bounded) then **confirm** (payment). This is what real ticketing systems do, and it is where the genuinely subtle bugs live.

### The rule that matters

**PostgreSQL `hold_expires_at` is the source of truth. Redis TTL is only an optimisation.**

Redis can restart, evict keys under memory pressure, or fail over and lose recent writes. If your expiry logic depends solely on a Redis key vanishing, seats leak into permanent `HELD` limbo — invariant I3 broken, and your event silently sells 200 fewer tickets than it has.

So:
- Every read path filters with `AND (status != 'HELD' OR hold_expires_at < now())`
- Redis holds a mirrored key with TTL purely to serve fast availability reads and drive WebSocket expiry notifications
- A **sweeper worker** runs every 5–10 seconds: `UPDATE seats SET status='AVAILABLE', held_by_session_id=NULL, hold_expires_at=NULL WHERE status='HELD' AND hold_expires_at < now()` — batched, with `SKIP LOCKED`, and it publishes release events
- A **reconciler** runs every few minutes and repairs Redis/Postgres divergence, logging every discrepancy it finds as a counter

**The counter `reconciliation_divergence_total` is worth a resume line by itself.** It says you assumed your own cache would drift and instrumented for it.

### Hold extension and the race at the boundary

User clicks "extend" at T+7:59 while the sweeper fires at T+8:00. Extension must be a conditional update: `WHERE status='HELD' AND held_by_session_id=$1 AND hold_expires_at > now()`. If rowcount is 0, the hold is already gone — return a clean 409 and let the frontend re-acquire. Do not paper over it.

---

## 6. Idempotency

Any endpoint that creates or mutates money-adjacent state requires an `Idempotency-Key` header.

**Flow:**
1. `INSERT INTO idempotency_keys (key, user_id, request_fingerprint, status) VALUES (..., 'IN_PROGRESS')`
2. On unique-violation → key already exists. Fetch it.
   - `COMPLETED` and fingerprint matches → return the stored response verbatim. Do not re-execute.
   - `COMPLETED` and fingerprint **differs** → `422`. Same key, different payload is a client bug and must be surfaced loudly, not silently accepted.
   - `IN_PROGRESS` → `409 Conflict` with `Retry-After`. The original request is still running.
3. Execute the operation.
4. Store the response body and status, mark `COMPLETED`, in the **same transaction** as the booking write. If these are separate transactions you can crash between them and lose idempotency.

**The interview question you will get:** "What if the server crashes after step 3 but before step 4?" Answer: the key stays `IN_PROGRESS`, a retry gets 409, and a stale-key reaper marks rows older than a timeout as failed so the client can safely retry. You will have thought about this because it is written here; almost no candidate has.

---

## 7. Payment webhooks — out-of-order, duplicated, late

Webhooks are unreliable by nature. Providers retry aggressively and deliver out of order.

- **Dedup:** `provider_event_id` is the primary key of `payment_events`. Insert first, process second. Unique violation means already seen — return 200 immediately (never 500, or the provider retries forever and you build a retry storm).
- **Signature verification:** HMAC over the raw body. Verify *before* parsing JSON. Constant-time comparison.
- **Out-of-order:** `payment.succeeded` can arrive after `payment.refunded`. Guard every effect through the state machine. Illegal transition → log, record, do not apply.
- **The late-success case:** payment succeeds *after* the hold expired and the seat was resold. This is the hard one and it is real. Correct handling: booking → `REFUND_REQUIRED`, seat untouched, alert raised. **Do not** try to reclaim the seat. Explaining this tradeoff — that you chose to refund rather than double-allocate, because money is reversible and a seat is not — is a genuinely senior-sounding answer.
- **Fast ack, async process:** return 200 immediately after durable insert, process in a worker. Webhook endpoints must be fast or providers time out and retry.

---

## 8. Admission control (the flash-sale layer)

### Rate limiting
Sliding-window counter in Redis, implemented as a **Lua script** so check-and-increment is atomic. Per-IP and per-user, different limits. Return `429` with `Retry-After` and `X-RateLimit-*` headers.

Write the Lua script yourself rather than importing a library. It's 15 lines and it demonstrates you understand why the operation must be atomic — a GET-then-SET rate limiter has the same race condition as the naive booking path, which is a nice symmetry to point out in an interview.

### Virtual waiting room
For flash sales: only N users are admitted to the booking page concurrently.

- Arrival → Redis sorted set with timestamp score, receives a queue token
- WebSocket pushes live position and an ETA
- Admission worker moves the head of the queue into an "active" set with a TTL, publishes admission events
- Abandoned sessions expire out of the active set and free their slot

This is the feature that makes the project *look* like a real product rather than an exercise, and it costs maybe three days.

---

## 9. Frontend (React + TypeScript)

Real frontend, since it's your weakest demonstrated area.

- **Live seat map** — SVG grid, seats change colour in real time as others hold and book them. Driven by WebSocket, fanned out via Redis pub/sub so it works with multiple API replicas.
- **Optimistic UI with rollback** — seat greys instantly on click, reverts with a clear message if the server rejects. Nice parallel to optimistic locking on the backend; mention it, interviewers enjoy that.
- **Hold countdown timer** — server-authoritative. Send `hold_expires_at` and compute remaining time client-side against a server-time offset, never `setTimeout(480000)`. Clock skew and backgrounded tabs will otherwise lie to your user.
- **Idempotency-Key generation** — UUID per checkout attempt, held stable across retries. Reuse it on retry; regenerate only on a genuinely new attempt.
- **Reconnect handling** — WebSocket drops, client refetches full state on reconnect rather than assuming its cached view survived.
- **Admin dashboard** — live invariant status, lock contention, retry rates, hold expiry counts, reconciliation divergences.

---

## 10. Testing — this is what separates you from everyone else

You have done rigorous evaluation before (RAGFail, the Research Copilot validation run). Apply the same standard here.

**Layer 1 — Unit.** State machine, pure. No containers. Every legal and illegal transition.

**Layer 2 — Integration.** Real Postgres and Redis via `testcontainers`. No mocks for the database — mocking a database in a project about database concurrency defeats the point entirely.

**Layer 3 — Deterministic concurrency tests.** In CI, not manual:
```python
async def test_only_one_wins():
    results = await asyncio.gather(*[book_seat(seat_id=1) for _ in range(50)],
                                   return_exceptions=True)
    assert sum(r.success for r in results if not isinstance(r, Exception)) == 1
```
Plus a barrier-synchronised variant so all 50 requests hit the critical section in the same window. Run each concurrency test 20× in CI — race conditions are probabilistic and a single green run proves nothing.

**Layer 4 — Property-based (Hypothesis).** Generate random valid operation sequences (hold, confirm, expire, cancel, in any order) and assert I2 conservation holds after every step. This finds sequences you would never write by hand.

**Layer 5 — Chaos.** Under sustained load, then:
- `docker kill redis` → assert no invariant violated, assert degraded-but-correct behaviour
- Kill a worker mid-transaction → assert Postgres rolls back cleanly, no partial state
- `pg_sleep` injection to force lock timeouts → assert graceful 503, not a hang
- Network partition between API and Redis (`tc netem`) → assert fallback to Postgres-only availability reads

**Layer 6 — Load (k6 or Locust).** Scenarios: flash sale (10k virtual users, 500 seats, ramp over 30s), thundering herd (all users target the single last seat), steady state, and a sustained soak to catch connection-pool leaks.

**Invariant checking runs continuously during load tests**, not just at the end. A poller asserts I1 and I2 every 500ms while traffic is live. Catching a violation that self-heals before the test finishes is exactly the kind of bug that reaches production otherwise.

---

## 11. Observability

You already tracked latency percentiles and token costs in Research Copilot, so this will be familiar ground.

- Structured JSON logs, request ID propagated through every layer including workers
- Prometheus metrics:
  - `booking_attempts_total{strategy, outcome}`
  - `oversell_blocked_total{layer}` — application vs DB-constraint. If the DB constraint ever fires, your application logic has a bug. That counter should be zero and you should alert on it.
  - `optimistic_retries_total`, `lock_wait_seconds`
  - `hold_expired_total`, `reconciliation_divergence_total`
  - `idempotent_replay_total`
  - `webhook_events_total{type, outcome}`
  - Request latency histograms with proper buckets
- Grafana dashboard, screenshotted in the README
- OpenTelemetry tracing across API → DB → worker (bonus: this was on your Cognizant assessment syllabus)

---

## 12. Phased build order

Do not reorder these. Phase 1 before Phase 2 specifically — **prove the bug before you fix it**, or you'll never have the baseline numbers and the whole project loses its spine.

| Phase | Deliverable | Gate to pass before moving on |
|---|---|---|
| 0 | Docker Compose, schema, domain model, state machine, unit tests | State machine tests green with no containers running |
| 1 | Naive strategy + load harness | **Overselling reproduced and measured.** Screenshot it. |
| 2 | Pessimistic locking + deadlock ordering + SKIP LOCKED variant | 0 oversells at high contention; throughput recorded |
| 3 | Optimistic locking + jittered backoff + retry budget | Full comparison matrix populated |
| 4 | Holds, TTL, sweeper, reconciler | I3 holds under chaos; divergence counter wired |
| 5 | Idempotency + webhooks + outbox | I4 and I5 tested including crash-between-steps |
| 6 | Rate limiting (Lua) + virtual waiting room | Flash-sale scenario passes at 10k VUs |
| 7 | React frontend, live seat map, admin dashboard | Two browsers, one seat, correct behaviour visible |
| 8 | Observability, chaos suite, README with results | Every number in the README reproducible via one command |

**Phase 8 discipline:** a `make benchmark` command that regenerates the entire results table from scratch. If a reviewer can reproduce your numbers, your numbers are evidence. If they can't, they're decoration.

---

## 13. Resume bullets (draft — update with your real numbers)

> **Seatlock — Concurrency-Safe Ticketing System** · FastAPI, PostgreSQL, Redis, React, Docker · [GitHub] [Live]
>
> - Built a high-contention ticketing system guaranteeing zero overselling under 10,000 concurrent requests; a controlled naive baseline produced N phantom bookings under identical load, isolating read-then-write races as the failure mode.
> - Implemented and benchmarked three concurrency-control strategies (naive, pessimistic `SELECT FOR UPDATE`, optimistic versioning with jittered backoff) across contention levels, identifying the throughput crossover point at ~X concurrent requests per seat.
> - Designed two-phase booking with TTL-bounded holds, idempotency-keyed writes, and deduplicated payment webhooks; enforced a five-invariant correctness contract via property-based and chaos tests that kill Redis and workers mid-transaction.
> - Instrumented p50/p95/p99 latency, lock contention, retry rate, and cache-divergence counters in Prometheus/Grafana; full benchmark suite reproducible with a single command.

Four bullets, every one carrying a number or a named technique. No adjectives.

---

## 14. Interview questions this project earns you

You will be able to answer all of these from experience rather than from theory. That difference is audible.

1. Walk me through a race condition you've personally caused and fixed.
2. Optimistic vs pessimistic locking — when would you choose each?
3. What isolation level were you running, and what anomalies does it permit?
4. How do you prevent deadlocks when locking multiple rows?
5. What is `SKIP LOCKED` for and when is it wrong to use it?
6. How do you make a POST endpoint idempotent? What if the server crashes mid-write?
7. Your cache and your database disagree. What do you do?
8. How do you handle a webhook arriving twice? Out of order? Six hours late?
9. Why not microservices here?
10. How would you scale this to 100× the load? What breaks first?
11. Why jitter in the backoff?
12. How do you test something that only fails 1% of the time?

Rehearse 9 and 10 especially. Question 9 is where most candidates overclaim and lose credibility; you'll have a real reason. Question 10 is where you say: the database write path is the bottleneck, and the next step is partitioning inventory by event so contention is sharded — but the naive first instinct, adding read replicas, wouldn't help because the constraint is on writes.

---

## 15. Deliberate scope cuts (say these out loud in interviews)

Knowing what you left out, and why, reads as judgement rather than as gaps.

- **No real payment provider.** A mock gateway with configurable latency, failure, and duplicate-webhook injection. Better for testing than a sandbox account, and you control the failure modes.
- **No horizontal DB scaling.** Single Postgres. Correct choice for a single-event inventory boundary; sharding by event is the documented next step.
- **No auth beyond JWT sessions.** Auth is solved and boring; it isn't what this project is demonstrating.
- **No mobile app.** Responsive web only.

---

## 16. Things that will actually bite you

- **Connection pool exhaustion under load.** Your pool size, worker count, and Postgres `max_connections` must be reasoned about together. Undersize it and you'll misread queueing as lock contention. Use PgBouncer if you go past a few API replicas.
- **`asyncio.gather` in tests doesn't guarantee simultaneity.** Tasks may still serialise. Use an explicit `asyncio.Barrier` or threads with a real barrier for true simultaneous arrival.
- **Timezones.** Everything in UTC, `timestamptz`, always. Hold expiry is the single worst place for a naive datetime to hide.
- **WebSocket fanout across replicas.** In-memory connection registries break the moment you scale past one process. Redis pub/sub from day one.
- **Load-generating from one laptop.** Your client will bottleneck before your server does. Check client-side CPU and file descriptor limits before believing any throughput number.
- **Green CI on concurrency tests is weak evidence.** Run them repeatedly, and vary timing.

---

## 17. README structure (write this last, but plan for it now)

1. One-line description
2. **The results table** — above the fold, before installation instructions
3. Demo GIF: two browsers, one seat, live seat map updating
4. Architecture diagram
5. The five invariants and how each is enforced
6. The three strategies, with the crossover analysis
7. Chaos test results
8. How to reproduce every number (`make benchmark`)
9. Deliberate scope cuts

Lead with evidence. Most READMEs open with an install guide, which tells a reviewer nothing about whether the project is any good.