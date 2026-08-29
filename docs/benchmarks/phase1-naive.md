# Phase 1 benchmark: naive strategy (control condition)

This is the project's headline artifact for Phase 1 — SPEC.md's gate for
this phase is "overselling reproduced and measured." These are the
measurements. Reproduce with:

```
make run-api                                   # start the API (4 workers)
python -m loadtest.run_benchmark --scenario flash_sale --runs 5
python -m loadtest.run_benchmark --scenario last_seat --runs 5
```

Per-run raw k6 summaries and JSON are written to `loadtest/results/`
(gitignored — generated, not source); this file is the committed,
human-readable summary. Full run configuration is recorded here and in
every JSON result, per the principle that a benchmark whose configuration
isn't recorded alongside its numbers is not reproducible.

Related: [docs/phase1-connection-refused-investigation.md](../phase1-connection-refused-investigation.md)
documents the `transport_failures` category seen in the `last_seat`
results below and why it's reported rather than hidden.

---

## flash_sale — headline scenario

The full 10-seat pool is in contention, so `oversold_seats` can show a
real distribution across runs (unlike `last_seat`, where it's
mathematically capped at 1).

**Configuration**: strategy=naive, workers=4, pool_size=10, max_overflow=5,
NAIVE_RACE_WINDOW_MS=0, VUs=200, duration=30s, warmup=20 VUs for 10s,
seats=10.

| Run | Successes | Expected 409s | Unexpected errors | Transport failures | Oversold seats | Excess holders | Contention ratio | Throughput (req/s) | p50 (ms) | p95 (ms) | p99 (ms) | Invariant violations |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 11 | 36212 | 0 | 0 | 1 | 1 | 3622.3 | 1207.4 | 186.0 | 485.4 | 713.7 | 0 |
| 2 | 14 | 28638 | 0 | 0 | 3 | 4 | 2865.2 | 955.1 | 222.5 | 618.7 | 941.4 | 0 |
| 3 | 11 | 27802 | 0 | 0 | 1 | 1 | 2781.3 | 927.1 | 245.4 | 628.8 | 946.5 | 0 |
| 4 | 11 | 28340 | 0 | 0 | 1 | 1 | 2835.1 | 945.0 | 204.0 | 631.0 | 966.8 | 0 |
| 5 | 14 | 28028 | 0 | 0 | 4 | 4 | 2804.2 | 934.7 | 230.8 | 623.7 | 990.4 | 0 |
| **Mean** | — | — | — | — | — | — | **2981.6** | **993.9** | **217.8** | **597.5** | **911.7** | — |

5/5 runs produced at least one oversold seat; 5/5 runs produced at least
one excess holder; 0/5 runs recorded an invariant violation while the load
was running. `oversold_seats` varies 1–4 across identical runs —
**oversold_seats_fraction mean 0.2** (2 of 10 seats, on average). Total
excess_holders across all 5 runs: **11**.

## last_seat — worst-case demonstration

Every VU targets the same single seat, so `oversold_seats` is capped at 1
by construction and cannot show a distribution — **excess_holders** (sum
over seats of holders − 1) is the headline figure here instead.

**Configuration**: strategy=naive, workers=4, pool_size=10, max_overflow=5,
NAIVE_RACE_WINDOW_MS=0, VUs=500, duration=10s, warmup=20 VUs for 10s,
seats=10 (1 contended).

| Run | Successes | Expected 409s | Unexpected errors | Transport failures | Oversold seats | Excess holders | Contention ratio | Throughput (req/s) | p50 (ms) | p95 (ms) | p99 (ms) | Invariant violations |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 19 | 9254 | 0 | 34 | 1 | 18 | 930.7 | 930.7 | 503.8 | 1597.3 | 2434.4 | 0 |
| 2 | 45 | 7073 | 0 | 101 | 1 | 44 | 721.9 | 721.9 | 668.1 | 2015.5 | 3030.4 | 0 |
| 3 | 48 | 7030 | 0 | 0 | 1 | 47 | 707.8 | 707.8 | 699.3 | 1999.2 | 2860.0 | 0 |
| 4 | 36 | 6915 | 0 | 0 | 1 | 35 | 695.1 | 695.1 | 759.0 | 2131.0 | 3128.5 | 0 |
| 5 | 33 | 7003 | 0 | 0 | 1 | 32 | 703.6 | 703.6 | 712.5 | 2168.3 | 3397.7 | 0 |
| **Mean** | — | — | — | — | — | — | **751.8** | **751.8** | **668.5** | **1982.2** | **2970.2** | — |

5/5 runs produced at least one oversold seat (constant at 1, as
predicted); 5/5 runs produced excess holders, **varying 18–47 per run
under natural timing** (`NAIVE_RACE_WINDOW_MS=0` — this is not an
artificially widened race). Total excess_holders across all 5 runs:
**176**. `transport_failures` varies 0–101 across runs — this is the
connection-acceptance-burst phenomenon documented in
[the investigation writeup](../phase1-connection-refused-investigation.md),
not an application bug; reported here rather than merged into any other
category.

---

## Reading these numbers

- **The naive strategy oversells under natural timing, repeatably.** Every
  run of both scenarios produced at least one oversold seat and at least
  one excess holder, with `NAIVE_RACE_WINDOW_MS=0` throughout — this is
  the bug happening on its own, not a bug we forced.
- **A single seat under extreme contention (`last_seat`) doesn't just
  oversell once — it oversells by dozens.** 18 to 47 excess holders per
  run for one seat is the strongest single number from Phase 1.
- **Latency and throughput are real, not placeholder values** — p50/p95/
  p99 come from k6's `handleSummary()` payload for the measured phase
  only (warmup excluded by construction), not the deprecated
  `--summary-export` flag, which used a different schema and previously
  produced silent `None`s here.
- This table is Strategy A's row in SPEC.md section 4's measurement
  matrix. Phases 2 and 3 add Strategy B (pessimistic) and Strategy C
  (optimistic) alongside it for the full three-way comparison.
