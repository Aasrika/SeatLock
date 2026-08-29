# Phase 2 benchmark: naive vs. pessimistic locking

SPEC.md section 4's headline comparison, started: Strategy A (naive,
Phase 1) against Strategy B (pessimistic locking). Reproduce with:

```
make run-api    # not needed for this — see below
python -m loadtest.run_benchmark --strategies naive,pessimistic --scenario flash_sale --runs 5
python -m loadtest.run_benchmark --strategies naive,pessimistic --scenario last_seat --runs 5
```

Unlike Phase 1's single-strategy runs, **comparison mode starts and stops
the API itself**, once per strategy, so `STRATEGY` is the only thing that
varies between them — every other setting (scenario, VUs, workers,
pool_size, `NAIVE_RACE_WINDOW_MS=0`) is forced identical and asserted
identical (`_assert_identical_configuration`) before a comparison table is
even produced. Per-run raw JSON stays in the gitignored
`loadtest/results/`; this file is the committed summary.

`lock_wait_seconds` p99 comes from pessimistic's own `/metrics` scrape at
the end of its runs (Prometheus histogram, bucket-boundary estimate — see
`_parse_histogram_p99_seconds`), not from k6. It's absent for naive, which
never takes a row lock.

---

## flash_sale — headline scenario

**Configuration**: workers=4, pool_size=10, max_overflow=5,
`NAIVE_RACE_WINDOW_MS=0`, VUs=200, duration=30s, warmup=20 VUs for 10s,
seats=10.

### naive

| Run | Successes | Expected 409s | Oversold seats | Excess holders | Throughput (req/s) | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---|---|---|---|---|---|---|---|
| 1 | 10 | 23623 | 0 | 0 | 787.8 | 279.1 | 735.6 | 1136.5 |
| 2 | 12 | 22211 | 2 | 2 | 740.8 | 301.8 | 772.1 | 1217.8 |
| 3 | 12 | 22325 | 2 | 2 | 744.6 | 272.4 | 797.0 | 1284.4 |
| 4 | 13 | 22342 | 3 | 3 | 745.2 | 307.8 | 783.6 | 1138.5 |
| 5 | 11 | 22264 | 1 | 1 | 742.5 | 307.4 | 789.9 | 1136.0 |
| **Mean** | — | — | — | — | **752.2** | **293.7** | **775.6** | **1182.6** |

Transport failures: 0 in all 5 runs (200 VUs is well under the
connection-refused threshold documented in
[the Phase 1 investigation](../phase1-connection-refused-investigation.md)).

### pessimistic

| Run | Successes | Expected 409s | Oversold seats | Excess holders | Throughput (req/s) | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---|---|---|---|---|---|---|---|
| 1 | 10 | 18155 | 0 | 0 | 605.5 | 372.7 | 916.2 | 1349.5 |
| 2 | 10 | 17878 | 0 | 0 | 596.3 | 395.1 | 934.2 | 1286.1 |
| 3 | 10 | 18528 | 0 | 0 | 617.9 | 358.4 | 897.8 | 1345.5 |
| 4 | 10 | 17987 | 0 | 0 | 599.9 | 378.2 | 953.9 | 1368.4 |
| 5 | 10 | 18097 | 0 | 0 | 603.6 | 384.0 | 908.2 | 1331.3 |
| **Mean** | — | — | — | — | **604.6** | **377.7** | **922.1** | **1336.1** |

`lock_wait_seconds` p99: **150ms**. Successes are **exactly 10 in every
run** — one winner per seat, every seat, every run. Not "close to 10" —
exactly 10.

### Comparison

| Strategy | Oversold seats (total) | Excess holders (total) | Throughput (req/s, mean) | p99 (ms, mean) | lock_wait p99 (ms) |
|---|---|---|---|---|---|
| naive | 8 | 8 | 752.2 | 1182.6 | — |
| pessimistic | **0** | **0** | 604.6 | 1336.1 | 150 |

Pessimistic trades ~20% throughput for zero oversell at this contention
level — exactly SPEC.md section 4's predicted shape, not assumed.

---

## last_seat — worst-case demonstration

**Configuration**: workers=4, pool_size=10, max_overflow=5,
`NAIVE_RACE_WINDOW_MS=0`, VUs=500, duration=10s, warmup=20 VUs for 10s,
seats=10 (1 contended).

### naive

| Run | Successes | Transport failures | Oversold seats | Excess holders | Throughput (req/s) | p99 (ms) |
|---|---|---|---|---|---|---|
| 1 | 43 | 0 | 1 | 42 | 563.3 | 4198.1 |
| 2 | 55 | 0 | 1 | 54 | 579.4 | 3323.7 |
| 3 | 35 | 95 | 1 | 34 | 591.2 | 3462.0 |
| 4 | 55 | 2895 | 1 | 54 | 825.4 | 3630.5 |
| 5 | 38 | 2330 | 1 | 37 | 767.7 | 3967.3 |
| **Mean** | — | — | — | — | **665.4** | **3716.3** |

`oversold_seats` is constant at 1 (as Phase 1 found — mathematically
capped, one seat in contention). `excess_holders` varies 34–54 per run —
**under natural timing, one naive seat oversells by dozens of holders
every single run.**

### pessimistic

| Run | Successes | Transport failures | Oversold seats | Excess holders | Throughput (req/s) | p99 (ms) |
|---|---|---|---|---|---|---|
| 1 | 1 | 2643 | 0 | 0 | 571.3 | 3709.7 |
| 2 | 1 | 0 | 0 | 0 | 307.8 | 3742.6 |
| 3 | 1 | 573 | 0 | 0 | 258.0 | 7631.8 |
| 4 | 1 | 756 | 0 | 0 | 384.3 | 3531.5 |
| 5 | 1 | 446 | 0 | 0 | 351.6 | 3481.9 |
| **Mean** | — | — | — | — | **374.6** | **4419.5** |

`lock_wait_seconds` p99: **400ms**. Successes are **exactly 1 in every
run** — the whole point of a row lock on one row.

### Comparison

| Strategy | Oversold seats (total) | Excess holders (total) | Throughput (req/s, mean) | p99 (ms, mean) | lock_wait p99 (ms) | Error rate (mean) |
|---|---|---|---|---|---|---|
| naive | 5 | 221 | 665.4 | 3716.3 | — | 0.160 |
| pessimistic | **0** | **0** | 374.6 | 4419.5 | 400 | 0.236 |

This is the scenario where "throughput collapses under high contention
because locks serialise" (SPEC.md section 4) is visible directly:
pessimistic's throughput drops by ~44% relative to naive here, and its
error rate is *higher* than naive's, not lower — under 500 concurrent
requests genuinely serialising on one row, more of them hit
`lock_timeout` (503) than naive's TOCTOU race ever produces failures for.
Naive is fast and wrong; pessimistic is correct and, at this specific
contention extreme, slower and less available. Neither number makes the
other one wrong — they're measuring different things, and that tradeoff
*is* the finding.

---

## Reading these numbers

- **Pessimistic locking oversold zero seats across both scenarios, ten
  total runs, at `NAIVE_RACE_WINDOW_MS=0`.** Naive oversold in 9 of those
  10 runs. This isn't a claim about what pessimistic locking *should* do
  — it's what it *did*, under the exact same load, seat pool, and
  hardware naive was measured against.
- **The tradeoff is real and directional, not just "pessimistic is
  slower."** At moderate contention (flash_sale, 200 VUs across 10
  seats), pessimistic costs ~20% throughput for zero oversell — a
  reasonable trade. At extreme single-seat contention (last_seat, 500 VUs
  on 1 seat), the same strategy costs ~44% throughput *and* a higher
  error rate. Strategy C (optimistic, Phase 3) exists precisely because
  neither strategy is right at every contention level — that crossover is
  Phase 3's headline result.
- `lock_wait_seconds` p99 (150ms moderate contention, 400ms worst-case)
  is measured from Postgres's own lock manager via SQLAlchemy's pool
  `checkout` event, never inferred from a statement's total latency — see
  `app/infra/metrics.py`.
