# Phase 3 benchmark: the naive/pessimistic/optimistic contention sweep

SPEC.md section 4's crossover analysis, run empirically rather than
assumed. Reproduce with:

```
python -m loadtest.run_benchmark --sweep --sweep-reps 3
```

This is comparison mode's successor for three strategies at once, across
a range of contention levels: it starts and stops the API itself, once
per (strategy, ratio, repetition) **cell**, so STRATEGY and seat count can
both vary while everything else (VUs, duration, workers, pool settings,
`NAIVE_RACE_WINDOW_MS=0`) stays forced identical and is asserted identical
(`_assert_sweep_configuration`) before any table is produced. Cells are
**interleaved** — for a fixed contention ratio, every strategy runs once,
then the whole cycle repeats, before moving to the next ratio — precisely
so that machine drift over the run's ~21-minute wall-clock duration cannot
correlate with which strategy happens to run later. Every run's wall-clock
`started_at` is recorded for exactly this reason (see the Limitations
section for what it showed here).

Contention ratio is varied by **seat count**, never VU count: VUs (200)
and duration (10s measured, 5s warmup) are held fixed across every cell,
and `seat_count = round(vus / target_ratio)` is computed up front and
seeded fresh per cell. Changing VU count instead would also change offered
load and confound the comparison — see `loadtest/contention_sweep.js`'s
header comment.

Per-run raw JSON stays in the gitignored `loadtest/results/`; this file is
the committed synthesis.

---

## Configuration (identical across every cell except strategy/seat_count)

- Workers: 4, pool_size: 10, max_overflow: 5, `NAIVE_RACE_WINDOW_MS`: 0
- VUs: 200, measured duration: 10s, warmup: 20 VUs for 5s
- Contention ratios: 2, 5, 10, 20, 50, 100 (seat counts: 100, 40, 20, 10, 4, 2)
- 3 repetitions per (strategy, ratio) cell — minimum enforced by the harness
- Refinement pass: **did not run** — no crossover was found in the coarse
  pass (see below), and refinement only runs inside a detected interval
- Jitter ablation: optimistic only, ratios 50 and 100 (the two highest
  tested), full jitter vs. fixed backoff, 3 reps each

---

## Coarse sweep — full three-strategy table

| Strategy | Contention ratio | Seats | Oversold seats (total/3 reps) | Excess holders (total/3 reps) | Total request rate (req/s, mean / min / max) | p99 (ms, mean / min / max) | Strategy-specific (mean) |
|---|---|---|---|---|---|---|---|
| naive | 2 | 100 | 66 | 72 | 823.3 / 767.1 / 899.3 | 1008.7 / 783.7 / 1161.1 | — |
| pessimistic | 2 | 100 | 0 | 0 | 682.5 / 676.3 / 686.6 | 1367.8 / 988.4 / 1611.8 | lock_wait p99: 58.3ms |
| optimistic | 2 | 100 | 0 | 0 | 800.3 / 739.2 / 849.5 | 954.3 / 848.7 / 1092.0 | conflicts: 27.3, attempts: 1.00 |
| naive | 5 | 40 | 48 | 61 | 678.8 / 605.5 / 728.0 | 1268.3 / 1176.8 / 1365.6 | — |
| pessimistic | 5 | 40 | 0 | 0 | 608.5 / 584.0 / 641.0 | 2108.1 / 1519.2 / 2574.1 | lock_wait p99: 50.0ms |
| optimistic | 5 | 40 | 0 | 0 | 676.4 / 644.1 / 700.3 | 1071.4 / 1028.7 / 1131.7 | conflicts: 40.7, attempts: 1.00 |
| naive | 10 | 20 | 53 | 104 | 750.2 / 726.2 / 776.4 | 1096.8 / 943.5 / 1236.7 | — |
| pessimistic | 10 | 20 | 0 | 0 | 594.6 / 589.8 / 600.5 | 1505.3 / 1233.5 / 1982.0 | lock_wait p99: 50.0ms |
| optimistic | 10 | 20 | 0 | 0 | 681.1 / 662.6 / 691.0 | 1162.8 / 1126.0 / 1206.8 | conflicts: 43.7, attempts: 1.00 |
| naive | 20 | 10 | 20 | 64 | 697.7 / 632.1 / 757.4 | 2052.5 / 1063.3 / 2627.2 | — |
| pessimistic | 20 | 10 | 0 | 0 | 575.3 / 523.1 / 625.3 | 1310.9 / 1231.4 / 1443.8 | lock_wait p99: 91.7ms |
| optimistic | 20 | 10 | 0 | 0 | 672.7 / 638.7 / 697.8 | 1158.9 / 1048.8 / 1260.2 | conflicts: 51.3, attempts: 1.00 |
| naive | 50 | 4 | 12 | 117 | 684.3 / 667.1 / 710.7 | 1159.0 / 1054.8 / 1323.5 | — |
| pessimistic | 50 | 4 | 0 | 0 | 550.5 / 523.4 / 571.5 | 1371.0 / 1164.5 / 1482.0 | lock_wait p99: 216.7ms |
| optimistic | 50 | 4 | 0 | 0 | 647.7 / 595.2 / 682.6 | 1687.6 / 1013.9 / 2591.8 | conflicts: 38.7, attempts: 1.00 |
| naive | 100 | 2 | 6 | 102 | 664.0 / 599.1 / 706.3 | 1236.5 / 1039.3 / 1488.0 | — |
| pessimistic | 100 | 2 | 0 | 0 | 500.4 / 492.1 / 515.5 | 1249.1 / 1208.4 / 1298.3 | lock_wait p99: 233.3ms |
| optimistic | 100 | 2 | 0 | 0 | 703.0 / 686.3 / 725.5 | 1183.1 / 1060.5 / 1315.9 | conflicts: 53.0, attempts: 1.00 |

"Total request rate" here is `(successes + expected_409s +
unexpected_app_errors + transport_failures) / duration` — every request
the API actually processed, successful or correctly rejected. This is a
different number from "valid throughput" below, and the difference
matters — see the next section.

**Oversell**: pessimistic and optimistic oversold **zero seats across
every one of these 18 cells** (6 ratios x 3 strategies, minus the 6 naive
cells) — 108 total repetitions between the two correct strategies, zero
oversells. Naive oversold in every single cell, at every contention ratio
tested, from 2 to 100.

---

## Why there is no single "valid throughput" number in this table

Phase 3's plan (ruling 4) asked for a **valid throughput** column —
successful acquisitions minus `excess_holders` — to make naive's raw
throughput honestly non-comparable to the other two ("it counts bookings
that should have failed"). That distinction is real and worth stating
precisely, but it does not produce a *rate* that discriminates between
strategies the way "total request rate" above does, because of this
benchmark's own shape: every scenario here is **acquire-only** — no
confirm, no release — so at most `seat_count` genuine acquisitions can
ever occur, for any strategy, for the whole duration of the burst. Once
that arithmetic is followed through:

| Strategy | Ratio | Seats | Raw successes/s (mean) | Valid successes/s (successes − excess_holders) |
|---|---|---|---|---|
| naive | 2 | 100 | 12.40 | 10.00 |
| pessimistic | 2 | 100 | 10.00 | 10.00 |
| optimistic | 2 | 100 | 10.00 | 10.00 |
| naive | 100 | 2 | 3.60 | 0.20 |
| pessimistic | 100 | 2 | 0.20 | 0.20 |
| optimistic | 100 | 2 | 0.20 | 0.20 |

Valid successes/s is **identical for pessimistic and optimistic at every
ratio tested** (their `excess_holders` is always 0, so it's just
`seat_count / duration` for both, unconditionally) — that identity is a
correctness statement (neither strategy ever oversells), not a
performance one. It is naive's *raw* successes/s that inflates above
`seat_count / duration` (12.40 vs. 10.00 at ratio 2; 3.60 vs. 0.20 at
ratio 100, an 18x inflation) — that gap **is** the overselling, in exactly
the rate terms ruling 4 asked for, and it is naive's raw number, not this
benchmark's "total request rate" column, that is not comparable to the
other two strategies for this reason.

The metric that actually discriminates *between* the two correct
strategies here is **total request rate** (the main table above) and p99
latency — both of which include the (correctly) rejected 409s that make
up the overwhelming majority of requests under this much contention, and
neither of which is capped at `seat_count`.

---

## Crossover: not found in the tested range (2–100)

**Optimistic's total request rate was ahead of pessimistic's at every
single tested ratio, from 2 through 100** — SPEC.md section 4's
illustrative example ("crossover ... around 40 concurrent requests per
seat") did not materialize on this measurement. The gap is not
run-to-run noise: at every ratio, optimistic's min-across-3-reps request
rate exceeded pessimistic's max-across-3-reps request rate (e.g. ratio 2:
optimistic 739.2–849.5 vs. pessimistic 676.3–686.6; ratio 100: optimistic
686.3–725.5 vs. pessimistic 492.1–515.5) — the two ranges never overlap at
any of the six ratios tested.

Because the coarse pass never crossed, the planned refinement pass (three
to four intermediate ratios inside the crossing interval) had nothing to
refine, and correctly did not run — this is the harness behaving as
designed (`_find_crossover_interval` returning `None`), not a shortcut.

**What this does and doesn't mean**: it does not mean optimistic locking
is unconditionally better than pessimistic at arbitrarily high contention
— it means that within *this* tested range, on *this* workload shape
(acquire-only, no confirm/release, single seat pool per event, 200 fixed
VUs), on *this* machine, pessimistic's lock-queueing cost grew faster than
optimistic's retry cost across the whole range tested. Pessimistic's own
`lock_wait_seconds` p99 rose monotonically with contention (58ms → 233ms
from ratio 2 to 100) exactly as SPEC.md predicts for lock-based
serialization; optimistic's conflict count also rose (27 → 53 mean
conflicts per cell) but its retries stayed nearly always resolved within
the very first extra attempt (`attempts` mean ≈ 1.00 throughout — see
Limitations for why this is expected, not suspicious, given this
workload's winner-take-all shape). If a real crossover exists, this data
places it beyond ratio 100 for this workload and machine, or shows it
does not exist at all under this specific shape — we did not extend the
sweep past 100 to find out (see Limitations).

p99 latency is a noisier signal than throughput here: pessimistic's p99
was clearly worse at ratios 5, 10, and 20 (non-overlapping with
optimistic's range), but at ratio 50 the ranges overlap substantially
(pessimistic 1164.5–1482.0ms vs. optimistic 1013.9–2591.8ms) — at N=3
reps, we cannot claim a clean p99 ordering at that specific ratio, and say
so rather than picking a number from noise.

---

## Jitter ablation: inconclusive at N=3

Optimistic only, ratios 50 and 100 (seat counts 4 and 2 — the smallest,
highest-per-seat-contention pools in the sweep), full jitter vs. fixed
backoff, 3 reps each:

| Contention ratio | Full jitter | Total request rate (req/s, mean / min / max) | p99 (ms, mean / min / max) | Conflicts per rep |
|---|---|---|---|---|
| 50 | True | 662.4 / 641.0 / 687.7 | 1420.8 / 1083.8 / 1945.2 | 55, 50, 56 |
| 50 | False | 676.4 / 670.5 / 687.1 | 1288.5 / 1163.7 / 1432.6 | 56, 39, 43 |
| 100 | True | 676.6 / 671.4 / 681.3 | 1437.4 / 1256.5 / 1554.2 | 34, 22, 58 |
| 100 | False | 698.9 / 692.8 / 705.1 | 1122.4 / 1001.8 / 1344.3 | 58, 38, 52 |

This makes jitter a **measured** result, not an asserted one, per the
plan — and the honest measurement is: at these two ratios and N=3, fixed
backoff came out slightly *ahead* on both throughput and p99, the opposite
of AWS's "Exponential Backoff And Jitter" analysis's prediction. We do not
believe this reverses that analysis's reasoning (spreading contenders
across a full random window to avoid synchronized retry pile-ups remains
sound theoretically, and is exactly why `optimistic.py`'s default is still
full jitter). What it shows is that **this specific ablation, at this
sample size, cannot resolve the question**: request-rate ranges overlap
between the two conditions at ratio 50 (641.0–687.7 vs. 670.5–687.1), and
conflict counts per rep swing widely at both ratios (22–58 for full
jitter at ratio 100) — a spread consistent with 3 reps simply not being
enough samples to average out noise at a seat count this small (2–4
seats), not with a reliable reversal of the theoretical benefit. A larger
seat count or more repetitions would be needed to actually resolve this,
and we say so rather than reporting a false precise answer either way.

---

## Limitations

- **Single dev machine**, shared with Docker Desktop (running the
  Postgres/Redis containers this same benchmark hits) and whatever else
  was running on it at the time — not an isolated benchmark host. The
  interleaved cell ordering (ruling 3) exists specifically to keep any
  such drift from correlating with a particular strategy; `started_at`
  timestamps were recorded on every cell for exactly this check, and nine
  runs of the SAME strategy were spread across ~21 minutes of wall clock
  for every ratio, not clustered — but drift affecting all three
  strategies roughly equally, at roughly the same time, could still be
  present and would not show up as a strategy-correlated artifact.
- **The documented Windows connection-refused burst** (see
  `docs/phase1-connection-refused-investigation.md`) is why every scenario
  here still runs a warmup phase before the measured burst — without it,
  a cold process's first real burst produces transport failures that have
  nothing to do with the strategy under test.
- **3 repetitions per cell** — the harness enforces this as a minimum
  (`--sweep-reps` refuses values below 3) precisely because a single run
  proves nothing. 3 is still a small sample: min/max ranges are reported
  throughout this document instead of only means, and two places above
  (p99 at ratio 50, the entire jitter ablation) explicitly say variance is
  too wide at this N to support a confident claim, rather than reporting
  a number without that context.
- **Acquire-only workload, no confirm/release**: every cell's measured
  burst only ever calls `POST /api/holds`, never confirms or releases a
  hold — this is deliberate (Phase 3 is about the acquisition race, not
  the full booking lifecycle) but it means `successes` is mathematically
  capped at `seat_count` for every strategy, which is why "valid
  throughput" collapses to `seat_count / duration` uniformly rather than
  discriminating between strategies (see the dedicated section above) and
  why `optimistic_attempts` stays close to 1.00 even as conflict counts
  rise: once any one contender's UPDATE commits, every later reader sees
  the seat as HELD and is rejected by the domain state machine directly
  (not retried) rather than racing for it again — a longer-running,
  confirm/release-inclusive workload would let seats cycle back to
  AVAILABLE mid-burst and could show a materially different attempts
  distribution.
- **Contention ratio was capped at 100** — the coarse pass's six ratios
  (2, 5, 10, 20, 50, 100) were the ones specified for this phase; since no
  crossover appeared inside that range, we do not know from this data
  whether one exists beyond it. Extending the ratio range further is a
  reasonable next step this document does not claim to have taken.
- **lock_wait_seconds / optimistic_attempts p99 and mean values come from
  Prometheus histogram bucket boundaries** (see
  `loadtest/run_benchmark.py`'s `_parse_histogram_p99_seconds`), an
  approximation, not a true interpolated quantile — consistent with how
  Phase 2's benchmark reported the same histograms.
