# Seatlock

A concurrency-safe event ticketing system: three seat-acquisition
strategies, one of them deliberately broken, built behind a common
interface and measured against each other under real load.

With no locking and no injected delay (`NAIVE_RACE_WINDOW_MS=0` — the
race happens at whatever speed the scheduler produces on its own),
**the naive strategy oversold in 10 of 10 runs, up to 47 excess holders
on one contested seat** ([Phase 1](docs/benchmarks/phase1-naive.md),
`last_seat`, run 3). That is the baseline this project's zero has to
beat, and it stays unlocked in the codebase — more on why below.

**Pessimistic locking oversold 0 seats across all 10 of those same
runs** ([Phase 2](docs/benchmarks/phase2-naive-vs-pessimistic.md)).

Optimistic locking was measured later, under a different workload
(sustained recirculating contention rather than a single acquire-once
burst — the original oversell metric didn't survive that redesign; see
[Measurement failures found and corrected](#measurement-failures-found-and-corrected)
below). Across 36 cells × 3 repetitions, it shows 0–3 apparent overlaps,
attributed to individual requests' own latency exceeding the
measurement's tolerance window rather than genuine double-holds, and
cross-checked against the naive control's 20–60 overlaps at the same
ratios — confirming the detection mechanism finds real oversell when
it's actually there. This is an inference from the mechanism, not a
proof, exactly as the finding itself states
([Phase 3, Part 4](docs/benchmarks/phase3-crossover.md#part-4--the-oversell-metric-definition-failure)).

The naive strategy is never "fixed" — [CLAUDE.md](CLAUDE.md) rule 6 —
because a correctness claim is worthless without a control that fails.

---

## Results

The comparison below uses the corrected, production-configuration
dataset: [Phase 3's Appendix](docs/benchmarks/phase3-crossover.md#appendix--phase-4-item-7-re-running-this-sweep-at-the-production-sweeper-interval),
re-run at the production 5s sweeper interval after fixing a measurement
bug (see below) — a wider valid range than the original 100ms-interval
benchmark it superseded. 3 repetitions per cell; every cell's mean is
shown with its min/max across those repetitions.

| Strategy | Ratio | p99 (ms) mean/min/max | Throughput (req/s) mean/min/max |
|---|---|---|---|
| naive | 2 | 1269.5 / 943.2 / 1586.6 | 816.7 / 737.6 / 919.5 |
| pessimistic | 2 | 1172.3 / 1048.4 / 1345.4 | 621.7 / 567.4 / 667.1 |
| optimistic | 2 | 1175.0 / 1063.3 / 1326.9 | 709.1 / 604.0 / 875.6 |
| naive | 5 | 944.5 / 911.7 / 982.8 | 689.2 / 608.6 / 748.3 |
| pessimistic | 5 | 1172.7 / 1122.2 / 1214.7 | 652.3 / 564.1 / 787.7 |
| optimistic | 5 | 1329.7 / 822.6 / 2198.8 | 658.3 / 603.4 / 759.7 |
| naive | 10 | 1080.8 / 985.1 / 1172.0 | 716.6 / 654.9 / 838.2 |
| pessimistic | 10 | 1108.3 / 1027.8 / 1196.4 | 550.2 / 469.4 / 591.8 |
| optimistic | 10 | 1140.4 / 1020.3 / 1311.5 | 752.8 / 674.8 / 880.1 |

**No crossover between pessimistic and optimistic locking was observed
within the valid range (contention ratios 2–10, per the production-
interval re-run), and the valid range was too narrow to locate one if
it exists.** At ratios 2 and 5, neither p99 nor throughput
survives an overlap check across the 3 repetitions — the honest
statement is no measurable difference, not a percentage lead in either
direction.

At ratio 10, throughput ranges do *not* overlap — pessimistic's three
reps (469.4–591.8 req/s) sit entirely below optimistic's (674.8–880.1
req/s), the first non-overlapping result at any ratio in either version
of this sweep. This is reported, not claimed as a finding: it is a
3-rep, boundary-adjacent observation (optimistic's own validity measure
at ratio 10 sits right at the threshold), warranting a dedicated
follow-up with more repetitions before it's treated as evidence of a
real mechanism.

Full tables, variance methodology, and the earlier (narrower, 100ms-
interval) benchmark this superseded: [docs/benchmarks/phase3-crossover.md](docs/benchmarks/phase3-crossover.md).

---

## Demo

> **TODO (Phase 7):** GIF here — two browser windows on the same event,
> one seat, live seat-map update showing the hold/expiry/confirm cycle
> propagate to both in real time.

---

## The five invariants

Every seat-status change goes through exactly one place —
[`app/domain/state_machine.py`](app/domain/state_machine.py) — so each
invariant below can be checked against a pure function's output, not
scattered call sites.

- **I1 — At most one active booking per seat.** Enforced at three
  independent layers: application logic (the domain state machine
  rejects an illegal transition before anything is written), a
  concurrency-control layer specific to each strategy (pessimistic's
  `SELECT ... FOR UPDATE`; optimistic's version-checked conditional
  `UPDATE`), and a database constraint as the last line of defence — a
  partial unique index on `booking_seats(seat_id) WHERE released_at IS
  NULL`. That DB-level guard has its own metric,
  `oversell_blocked_total{layer="database"}`, with a dedicated test
  that forces it to fire — but in every other test and every benchmark
  run in this project, it has stayed at zero. **A nonzero value in
  production means application logic already let two bookings collide
  before the database caught it — a bug report, not a safety net doing
  its job.**
- **I2 — Conservation.** `count(AVAILABLE) + count(HELD) + count(BOOKED)
  == total_seats`, always. Checked via `check_conservation`
  ([`app/domain/invariants.py`](app/domain/invariants.py)) against a
  real seat snapshot after every test and continuously by the load
  harness while traffic is running, not just once at the end.
- **I3 — No stale holds.** No seat stays `HELD` with `hold_expires_at`
  in the past beyond one sweeper interval. The sweeper
  ([`workers/sweeper.py`](workers/sweeper.py)) is cleanup, not the
  correctness mechanism — every read/write path that reports or acts on
  availability treats an expired `HELD` row as already reclaimable,
  independently of whether the sweeper has physically reached it. That
  is what makes a 5-second production sweeper interval safe instead of
  requiring sub-second polling — confirmed empirically, not assumed
  (see the item-7 re-run below).
- **I4 — Idempotency.** The same `Idempotency-Key` with the same
  request fingerprint always returns the same response and creates at
  most one booking — scoped to the authenticated user, not the key
  alone (see [Design decisions](#key-design-decisions)).
- **I5 — Webhook exactly-once effect.** Processing the same
  `provider_event_id` N times has the same effect as processing it
  once — the id is the row's own primary key, so "already seen" is a
  database-level unique violation, not a racy application check.

---

## Measurement failures found and corrected

Five cases where this project's own instrumentation returned a
plausible-looking number that was wrong, and how each was caught. The
shared pattern across all five: **instrumentation that returns
plausible numbers while being wrong is worse than instrumentation that
fails loudly** — every one of these passed as a well-formed result
before it was interrogated.

1. **The acquire-only sweep was measuring rejection cost, not
   contention.** The first version of the three-strategy sweep found
   optimistic leading pessimistic at every contention ratio, with
   non-overlapping ranges — a clean, confident result. Splitting each
   run at its last successful acquisition showed the contested phase
   was only 0.1–9.3% of the measured window; the rest was
   post-exhaustion rejection cost, and the two strategies reject
   differently (optimistic: no lock; pessimistic: acquire the lock,
   *then* discover it's too late). The gap was real but measured the
   wrong thing. [Phase 3, Parts 1–2](docs/benchmarks/phase3-crossover.md#part-1--v1-the-acquire-only-coarse-sweep).
2. **`oversold_seats` became meaningless the moment seats recirculated.**
   An early smoke test reported `pessimistic oversold_seats = 30` — a
   strategy that cannot oversell by construction. The metric's
   definition ("flag any seat with more than one distinct holder,
   ever") was correct for an acquire-once workload and silently wrong
   for a recirculating one, where the same seat legitimately has many
   sequential holders. Replaced with a time-aware overlap check against
   `hold_audit`. [Phase 3, Part 4](docs/benchmarks/phase3-crossover.md#part-4--the-oversell-metric-definition-failure).
3. **`fraction_available` kept polling raw seat status after lazy
   expiry changed what "available" meant.** Re-running the sweep at the
   production 5-second sweeper interval, this validity metric collapsed
   to 0.032–0.205 everywhere — including at the least contested ratio,
   which had measured 1.000 before. The poll counted the raw `status`
   column; under lazy expiry, a `HELD` row can be reclaimable well
   before the sweeper physically flips it. The metric never raised an
   error — it just stopped measuring what its name said it measured.
   [Phase 3, Appendix](docs/benchmarks/phase3-crossover.md#the-invalid-measurement-fraction_available-under-lazy-expiry).
4. **A real lazy-expiry bug survived a targeted audit because it was
   written in raw SQL.** The same production-interval re-run also
   revealed that optimistic locking's conditional `UPDATE` still
   required `status = 'AVAILABLE'` literally — every expired-but-
   unswept hold was misread as a live conflict and retried to
   exhaustion, dropping throughput to roughly a third, with no error
   anywhere. An almost-identical bug in the pessimistic strategy had
   already been caught by a grep-style audit for the ORM's
   `SeatRow.status ==` pattern; this one was invisible to that search
   because the rule was spelled `status = 'AVAILABLE'` inside a string.
   An audit is only as good as its ability to see every place a rule is
   expressed. [Phase 3, Appendix](docs/benchmarks/phase3-crossover.md#two-lazy-expiry-bugs-found-two-different-ways).
5. **The reconciler's own non-atomic read was inflating the metric it
   exists to keep meaningful.** Postgres and Redis can't be read
   atomically together, so a single comparison pass can catch a seat
   mid-transition and report it as diverged when nothing is actually
   wrong. Left unfixed, that noise would have gotten
   `reconciliation_divergence_total`'s alert threshold raised until real
   drift stopped being visible too. Fixed with confirm-on-second-look:
   a candidate is re-read after a short delay before it counts as a
   real divergence. [`workers/reconciler.py`](workers/reconciler.py).

---

## Architecture

Modular monolith. Never microservices — [CLAUDE.md](CLAUDE.md) rule 1.
Seat inventory needs a single transactional boundary: I1 and I2 are
cross-seat, cross-booking invariants that have to be checked and
enforced inside one ACID transaction. Splitting inventory across
services would force eventual consistency onto exactly the part of the
system that has to be strongly consistent — trading a database
transaction for a distributed one, and reintroducing at the network
layer the same race the naive strategy demonstrates at the row layer.

```
app/domain/        pure Python, zero I/O — the state machines and invariants
app/inventory/      the three seat-acquisition strategies behind one interface
app/booking/         orchestration: hold -> create -> confirm
app/payments/         webhook signature verification + ingestion
app/infra/             SQLAlchemy tables, Redis cache, idempotency, metrics
app/api/routes/          thin FastAPI routers — no business logic
workers/                  sweeper, reconciler, idempotency reaper, payment worker
```

Background jobs (`workers/`) are separate OS processes sharing the same
database, not separate services — CLAUDE.md rule 1 again, applied to
deployment topology, not just code layout.

---

## Key design decisions

**Lazy expiry, sweeper as cleanup.** Every read and write path that
checks seat availability treats an expired `HELD` row as reclaimable on
its own — the sweeper's job is only to eventually make the row agree
with what every reader already believes, which is what lets it run
every 5–10 seconds instead of sub-second. [SPEC.md §5](docs/SPEC.md).

**Postgres-then-Redis ordering.** The sweeper deletes a seat's Redis
mirror key strictly *after* its Postgres commit, never before. A crash
in between leaves Redis stale-unavailable, which the reconciler repairs
— a lost sale, but a safe one. The reverse ordering could tell a second
customer a seat is free while Postgres still disagrees.
[`workers/sweeper.py`](workers/sweeper.py).

**Idempotency completion in the same transaction as the work it
guards.** The response is stored and the key marked `COMPLETED` in the
identical database transaction as the booking write it describes — a
split would let a crash leave a booking committed with its key still
`IN_PROGRESS`, and a naive retry would double-book.
[`app/infra/idempotency.py`](app/infra/idempotency.py).

**Idempotency keys scoped by `(user_id, key)`, not `key` alone.** The
`Idempotency-Key` header is client-supplied, untrusted input; two
different users can submit the same value. A lookup scoped by key alone
would let one user's request find — and on a fingerprint match, be
served — a different user's stored response. Same class of bug as a
missing ownership check on `GET /bookings/{id}`.

**The late-payment refund path.** A `payment.succeeded` webhook arriving
after a hold expired and the seat was resold moves the booking to
`REFUND_REQUIRED` and leaves the seat untouched — reclaiming it would be
a second, different oversell. [SPEC.md §7](docs/SPEC.md).

The principle underneath all five: **fail toward the recoverable side.**
Money is reversible; a resold seat, a stale cache entry read as
available, and a client that can't tell "succeeded" from "lost" are
not — every decision above resolves its ambiguity toward the option
that can still be undone.

---

## Reproduce every number

```bash
# Phase 1 — naive control
make run-api
python -m loadtest.run_benchmark --scenario flash_sale --runs 5
python -m loadtest.run_benchmark --scenario last_seat --runs 5

# Phase 2 — naive vs. pessimistic
python -m loadtest.run_benchmark --strategies naive,pessimistic --scenario flash_sale --runs 5
python -m loadtest.run_benchmark --strategies naive,pessimistic --scenario last_seat --runs 5

# Phase 3 — the crossover investigation, in order
python -m loadtest.run_benchmark --sweep                              # v1, superseded (see Part 1)
python -m loadtest.diagnose_exhaustion                                 # the diagnostic that falsified v1
python -m loadtest.recirculating_pilot --ratios 2,5,10,20               # parameter selection
python -m loadtest.recirculating_sweep                                   # v2, 100ms-interval benchmark
python -m loadtest.recirculating_sweep --sweeper-interval-seconds 5.0     # production-interval re-run (Results table above)

# Full test suite + lint
make test
make lint
```

Per-run raw k6 output and JSON land in `loadtest/results/` (gitignored —
generated, not source); the committed, human-readable summaries with
full configuration are in `docs/benchmarks/`.

---

## Scope cuts and limitations

- **Single, shared development machine** for every benchmark above —
  running Docker Desktop, testcontainers, and everything else on it
  concurrently. Not an isolated benchmark host.
- **3 repetitions per cell**, the minimum this project treats as
  acceptable — and, as the results above show, not enough to
  distinguish the observed mean differences from noise at most ratios.
- **Valid contention range is only ratios 2–10.** Ratio 20 and above
  never cleared the recirculation-validity threshold in this design;
  the range is too narrow to say whether a pessimistic/optimistic
  crossover exists at higher contention.
- **A client-side transport-failure ceiling exists between 500 and
  1000 VUs** on this machine (confirmed: 0 failures at 500 VUs,
  1817–2425 at 1000) — ratios requiring more VUs than that at the
  design's seat floor can't be measured here at all.
- **No real payment provider.** Webhook signature verification and
  ingestion are real; the payment gateway itself is out of scope —
  `payment.succeeded`/`payment.refunded` are injected directly.
- **No auth beyond JWT sessions**, and that's assumed rather than
  built. `user_id`/`session_id` are caller-supplied identifiers, not
  verified against a real authentication layer — deliberately out of
  scope for what this project demonstrates.

---

## Install and run

Requires Docker, Python 3.11+, and `k6` for load testing.

```bash
git clone <this repo> && cd seatlock
cp .env.example .env

make up                    # Postgres + Redis
python -m pip install -e ".[dev]"
alembic upgrade head

make run-api                # API, 4 uvicorn workers
make sweeper                # background: expired-hold cleanup
make reconciler              # background: Redis/Postgres divergence repair
make idempotency-reaper        # background: stale idempotency-key recovery
make payment-worker              # background: webhook effect application

make test                          # full suite against real Postgres + Redis (testcontainers)
make lint                           # ruff + mypy
```

Each background worker in the last block is a separate process sharing
the API's database — start whichever ones a given workflow needs
alongside `run-api`, not instead of it.
