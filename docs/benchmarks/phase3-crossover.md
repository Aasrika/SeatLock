# Phase 3 benchmark: from acquire-only to recirculating contention

This document is as much about a methodology failure and its correction as
about the numbers it ends with. Read it in order — the numbers at the end
only mean what they mean because of what came before them.

Reproduce with:

```
python -m loadtest.run_benchmark --sweep                    # v1, acquire-only (superseded, see Part 1)
python -m loadtest.diagnose_exhaustion                       # the diagnostic that falsified v1
python -m loadtest.recirculating_pilot --ratios 2,5,10,20     # parameter selection
python -m loadtest.recirculating_sweep                        # v2, the sweep this document reports
```

---

## Part 1 — v1: the acquire-only coarse sweep

The first version of this sweep ran all three strategies at contention
ratios 2, 5, 10, 20, 50, 100 (seat count = VUs / ratio, 200 fixed VUs), each
run a single acquire-only burst: seed N seats, fire 200 VUs at them for
10s, measure. No confirm, no release, no expiry.

**Headline finding, as it would have been reported:** optimistic's total
request rate led pessimistic's at every single tested ratio, from 2 through
100, with non-overlapping min/max ranges across 3 repetitions at every one
of the six ratios. No crossover. This looked like a clean, confident
result — SPEC.md section 4's illustrative "crossover around 40 requests per
seat" simply didn't happen on this measurement, and the effect size was
large enough that ordinary run-to-run noise couldn't plausibly explain it.

**This finding would have been written up as-is had it not been
interrogated further.** That is the single most important fact this phase
produced, and it is stated here plainly before the diagnosis, not after,
so it isn't read as something we already knew going in.

---

## Part 2 — the diagnostic that falsified it

The interrogation (`loadtest/diagnose_exhaustion.py`) split each run's
measured window at the timestamp of the *last* successful acquisition
(neither correct strategy ever oversells, so the last success is exactly
the moment the last seat filled) and computed throughput/latency
separately for the CONTESTED phase (before that point) and the EXHAUSTED
phase (after).

Result: **the contested phase was 0.1%–9.3% of every run's measured
window.** At the *lowest* tested ratio, over 90% of the run was
post-exhaustion; at ratio 100 it was 99.9%. Every number the coarse sweep
reported was overwhelmingly a measurement of what happens after every seat
is already gone, not of contention.

That distinction matters mechanistically, not just statistically:
post-exhaustion, the two strategies do different amounts of work per
*rejection*:

- **optimistic**: an unlocked read sees the seat HELD/BOOKED → the domain
  state machine rejects it. No lock taken, no UPDATE issued.
- **pessimistic**: `SELECT ... FOR UPDATE` acquires the row lock FIRST,
  THEN discovers the seat is unavailable, THEN rolls back and releases it.

Isolating the contested-phase numbers at ratio 2 — the one ratio with a
large enough contested-phase sample (450–650 requests) to say anything —
the "optimistic ahead" gap that dominated the full-window numbers largely
disappeared. **v1 was measuring rejection cost, not concurrency-control
cost**, and doing so with enough statistical confidence (non-overlapping
ranges) to look like a real result.

---

## Part 3 — v2: redesigning for genuine, persistent contention

Fixing this required a workload where contention *persists* for the whole
measured window, not just at the start. Two options were considered:
(a) short hold durations plus a sweeper reclaiming expired holds, so
inventory recirculates, or (b) a much larger seat pool at the same VU
count, so exhaustion never occurs within the run. (b) was rejected: making
the exhaustion point recede requires scaling total inventory *and* VUs
together to hold the ratio constant, and doing that for the higher ratios
would need thousands of VUs — reintroducing the client-side bottleneck
class documented in the Phase 1 connection-refused investigation. (a) was
built instead, including a genuine hold sweeper (`app/inventory/sweeper.py`,
`workers/sweeper_worker.py`) — pulled forward from its originally planned
phase specifically because this benchmark needed it, shipped as real
production code with its own tests, not a benchmarking-only hack.

### Parameter selection (the pilot)

A short pilot (`loadtest/recirculating_pilot.py`) measured actual
recirculation before committing to a full sweep:

- **Hold duration / sweeper interval**: started at 2.0s/0.2s (53–59%
  fraction of the run with ≥1 seat available at ratio 5); tightened to
  **1.0s/0.1s** (85–91% at ratio 5) and kept there.
- **Seat floor**: below ~10 seats, fraction-available stayed near 20%
  regardless of tuning (confirmed at ratio 100, 2 seats: 17.7%–23.2%
  across both parameter sets) — with only 2 units of inventory,
  "available" is inherently too narrow a target under 200-VU load. Ratios
  that would otherwise need fewer than 10 seats instead get MORE VUs,
  preserving the exact target ratio at 10 seats
  (`compute_seat_count_and_vus`).
- **Client-side ceiling**: tested ratio 50 (10 seats / 500 VUs) — zero
  transport failures. Tested ratio 100 (10 seats / 1000 VUs) — **1817–2425
  transport failures**, confirming the ceiling on this machine sits
  between 500 and 1000 VUs, the same artifact class as Phase 1's
  connection-refused investigation. This is a measurement limit, not a
  result, and ratio 100 is excluded from the analysis on this basis alone,
  independent of its fraction-available.
- **Startup transient**: the pilot's fine-grained (50ms) available-count
  samples showed the initial, fully-stocked seat pool gets consumed for
  the first time 0.80–0.94s into the measured phase at ratios 5 and 10
  (ratio 2's pool never fully emptied in 15s at all — consistent with
  near-total availability throughout, and with there being no distinct
  transient to discard there either). **1.0s**, a small margin above that
  observed range, is used as a single, ratio-independent cutoff applied to
  every cell — not a value picked per ratio after seeing that ratio's own
  data, which would reopen exactly the "fitted after the fact" problem a
  pre-declared cutoff exists to avoid.

### Validity threshold

Decided in the code before this sweep ran, not fitted to the results: a
(strategy, ratio) cell counts toward the crossover comparison only if its
mean fraction_available across repetitions is **≥ 0.6**, AND it saw zero
transport failures. Every cell's measured fraction is reported below
regardless of whether it clears the bar.

---

## Part 4 — the oversell-metric definition failure

Partway through building the v2 harness, an early smoke test reported
**pessimistic `oversold_seats = 30`** — a strategy that cannot oversell by
construction. This was not a wrong number from a working metric; it was a
*metric answering a question that no longer applied*, and it had been
returning plausible-looking numbers and would have kept doing so
silently.

`GET /api/admin/oversell-report`'s definition — "flag any seat with more
than one distinct holder, ever" — is correct for an acquire-once workload,
where a seat, once won, is never released again (true of every benchmark
before this one). In a recirculating workload, the *same* seat is
legitimately held by many different sessions over one run, sequentially,
as holds expire and get reclaimed. That endpoint's definition flags every
one of those legitimate handoffs identically to a real double-hold. This
is a subtler failure than a wrong value: the metric kept producing
numbers, kept "passing" in the sense of returning a well-formed response,
and the numbers were simply measuring something else.

**Replacement**: `check_recirculating_oversell` (`loadtest/
recirculating_sweep.py`) computes genuine, time-aware overlaps from
`hold_audit`: hold *i*'s window is `[acquired_at_i, acquired_at_i +
hold_duration)`, and it overlaps the next hold on the same seat when the
next `acquired_at` falls before that window ends. A fixed-window
comparison alone still produced false positives — `acquired_at` is
captured in `app/api/routes/booking.py` *before* `strategy.acquire()` does
any database I/O (confirmed by reading the code, not assumed), so a
request's recorded timestamp can trail the actual moment its row write
committed by however long its own processing took. A first attempt with a
fixed comparison found pessimistic "overlaps" with gaps of 0.975–0.993s
against a 1.0s hold — i.e., normal processing latency, not a race. The fix
uses each cell's own observed **steady-state p99 latency** as the
tolerance, since a fixed constant would either miss this artifact under
heavy contention or mask a genuine overlap under light contention.

After that fix, pessimistic and optimistic show **0–3 overlaps** across
the full sweep (vs. naive's 20–60 overlapping seats and 323–726 overlap
events at the same ratios — a useful cross-check that the mechanism finds
real oversell when it's actually present). **The residual 0–3 count is
attributed to individual requests whose own latency exceeded their cell's
p99 — by definition, roughly 1% of requests do — not to genuine I1
violations. This attribution is an inference from the mechanism, not a
proof**: there is no independent confirmation that these specific
overlaps are processing-latency artifacts rather than something else. It
is the most plausible explanation given the magnitude (single digits, at
the tolerance boundary) and given pessimistic's `FOR UPDATE` construction
makes an actual double-hold structurally very hard to produce, but it has
not been separately verified.

---

## Part 5 — results

### Validity per cell

| Strategy | Ratio | Seats | VUs | Mean fraction_available (per rep) | Included |
|---|---|---|---|---|---|
| naive | 2 | 100 | 200 | 1.000, 1.000, 1.000 | YES |
| pessimistic | 2 | 100 | 200 | 1.000, 1.000, 1.000 | YES |
| optimistic | 2 | 100 | 200 | 1.000, 1.000, 1.000 | YES |
| naive | 5 | 40 | 200 | 0.935, 0.929, 0.793 | YES |
| pessimistic | 5 | 40 | 200 | 0.962, 0.933, 0.790 | YES |
| optimistic | 5 | 40 | 200 | 0.966, 0.964, 0.900 | YES |
| naive | 10 | 20 | 200 | 0.510, 0.452, 0.494 | NO (0.485 < 0.6) |
| pessimistic | 10 | 20 | 200 | 0.475, 0.399, 0.466 | NO (0.447 < 0.6) |
| optimistic | 10 | 20 | 200 | 0.570, 0.607, 0.634 | **YES (0.604 — barely)** |
| naive | 20 | 10 | 200 | 0.328, 0.335, 0.283 | NO |
| pessimistic | 20 | 10 | 200 | 0.169, 0.132, 0.149 | NO |
| optimistic | 20 | 10 | 200 | 0.477, 0.462, 0.394 | NO |
| pessimistic | 50 | 10 | 500 | 0.271 (1 rep, pilot) | NO |
| optimistic | 50 | 10 | 500 | 0.565 (1 rep, pilot) | NO |
| pessimistic | 100 | 10 | 1000 | 0.316 (1 rep, pilot) | NO — 1817 transport failures |
| optimistic | 100 | 10 | 1000 | 0.637 (1 rep, pilot) | NO — 2425 transport failures |

Ratio 10 is **asymmetric**: optimistic clears the bar (barely), pessimistic
doesn't. There is no valid crossover comparison at ratio 10 — reporting
optimistic's number there without pessimistic's would not be a comparison.
**The only ratios with both strategies included are 2 and 5.**

### Steady-state latency and throughput, with variance (3 reps each)

| Strategy | Ratio | p50 (ms) mean/min/max | p95 (ms) mean/min/max | **p99 (ms) mean/min/max** | Throughput (req/s) mean/min/max |
|---|---|---|---|---|---|
| naive | 2 | 336.7 / 315.8 / 373.9 | 962.1 / 913.9 / 1005.4 | 1426.5 / 1249.6 / 1526.5 | 485.9 / 437.4 / 512.8 |
| pessimistic | 2 | 357.9 / 227.0 / 434.7 | 1276.6 / 988.1 / 1810.4 | **1901.4 / 1455.8 / 2720.5** | 446.2 / 433.2 / 463.2 |
| optimistic | 2 | 278.3 / 248.2 / 330.3 | 919.4 / 682.0 / 1073.4 | **1530.6 / 1070.4 / 2004.9** | 463.0 / 360.0 / 533.6 |
| naive | 5 | 267.6 / 173.1 / 315.3 | 687.4 / 457.0 / 803.2 | 1034.1 / 685.7 / 1216.5 | 664.2 / 592.2 / 800.9 |
| pessimistic | 5 | 307.7 / 232.6 / 381.5 | 898.1 / 618.9 / 1050.6 | **1397.2 / 952.5 / 1643.6** | 550.5 / 377.1 / 761.1 |
| optimistic | 5 | 288.2 / 196.6 / 346.4 | 877.4 / 821.7 / 933.2 | **1380.8 / 1262.6 / 1566.5** | 578.3 / 512.3 / 706.7 |

**Leading with p99, per the plan for this document**: at ratio 2, mean p99
differs by 24% (1901 vs. 1530ms) — but pessimistic's per-rep range
(1455.8–2720.5) and optimistic's (1070.4–2004.9) **overlap** (the
overlapping region is roughly 1456–2005ms). At ratio 5, pessimistic's
range (952.5–1643.6) fully contains optimistic's (1262.6–1566.5). Applying
the same overlap test to throughput: pessimistic's range at ratio 2
(433.2–463.2) sits entirely inside optimistic's (360.0–533.6); at ratio 5
the ranges overlap substantially (pessimistic 377.1–761.1, optimistic
512.3–706.7).

**With only 3 repetitions per cell, neither p99 nor throughput survives
this variance check at either valid ratio.** The honest statement is: no
measurable difference between pessimistic and optimistic in the valid
range — not "optimistic leads by 4–5%," and not "pessimistic has 24% worse
tail latency." Both of those would have been defensible-*sounding*
headlines from the mean values alone; neither is defensible once the
per-rep spread is accounted for.

### The p99s are larger than the hold duration itself, and why is not fully known

p99 latency in every cell (1.0–2.7s) exceeds the 1.0s hold duration — worth
stating plainly rather than passing over. Partial, not complete,
attribution data:

- **`lock_wait_seconds` p99 (pessimistic) was small: 50–100ms** (bucket-
  boundary estimate) at both valid ratios — the row lock itself is not
  where pessimistic's multi-second p99 comes from.
- **optimistic's `attempts` mean was 1.02–1.04** despite 244–652 recorded
  conflicts per cell — successful acquisitions almost always land on the
  first attempt; the retry loop itself is not consuming much of
  optimistic's latency either.
- **Sweeper share of run time was 16–52%** across these cells (see next
  paragraph) — a substantial, if partial, candidate explanation.
- The DB connection pool (60 total: 4 workers × (10 + 5)) is shared across
  200 VUs regardless of strategy or seat contention — client-side pool
  wait was not separately instrumented in this sweep and is a plausible
  contributor that was not measured directly.

**"Not determined" is the honest answer for how these factors combine** —
this sweep did not capture `pool_checkout_seconds` per cell, which would
be needed to separate pool-wait from whatever remains. What can be said is
that the row lock itself (for pessimistic) and the retry loop itself (for
optimistic) are both individually small, so most of the multi-second tail
is coming from somewhere else in the system common to both strategies —
consistent with, but not proof of, pool contention and/or sweeper
contention rather than a strategy-specific mechanism.

### Sweeper share of database time

| Strategy | Ratio | Sweeper share of run (3 reps) |
|---|---|---|
| naive | 2 | 0.361, 0.502, 0.357 |
| pessimistic | 2 | 0.315, 0.323, 0.262 |
| optimistic | 2 | 0.271, 0.458, 0.303 |
| naive | 5 | 0.277, 0.266, 0.394 |
| pessimistic | 5 | 0.523, 0.263, 0.163 |
| optimistic | 5 | 0.363, 0.332, 0.225 |

The sweeper is configured identically regardless of strategy (same
interval, batch size, query shape — asserted in the harness), and its
share of run time is comparable in magnitude across all three strategies
at a given ratio, which is the right cross-check for "is the sweeper
distorting the comparison unevenly" — it does not appear to be uneven. But
16–52% of a run's database activity being the sweeper itself, not
booker-vs-booker contention, means **part of what this sweep measures is
each strategy's interaction with the sweeper, not purely bookers
contending with each other** — flagged here as a real property of this
measurement, not resolved by it.

---

## Part 6 — limitations, stated without softening

- **Single, shared dev machine** running Docker Desktop, testcontainers,
  and everything else on it concurrently with these runs — not an
  isolated benchmark host.
- **3 repetitions per cell** — the minimum this project treats as
  acceptable, and, as shown above, not enough to distinguish the observed
  mean differences from noise at either valid ratio.
- **The validity threshold (0.6) was chosen before the run, which is the
  right practice, but the number itself was not independently derived or
  justified** beyond "clearly separates the ratios that showed strong
  recirculation from the ones that didn't" in the pilot. A different
  threshold (0.5, or 0.7) could plausibly shift which ratios qualify,
  particularly ratio 10, which sits right at the boundary.
- **The valid range is only ratios 2 and 5**, and ratio 10 is asymmetric
  (optimistic included, pessimistic excluded) — meaningfully narrower than
  the six ratios (2–100) the original plan intended to cover.
- **A client-side transport-failure ceiling exists between 500 and 1000
  VUs** on this machine (confirmed: 0 failures at 500, 1817–2425 at
  1000) — ratios requiring more VUs than that ceiling allows, at the
  seat floor this design requires, cannot be measured here at all.
- **Hold duration of 1.0s is a benchmarking configuration, not a product
  decision** — SPEC.md's actual guidance is minutes; `Settings.
  hold_duration_seconds` defaults to 480.0 (8 minutes), and only this
  benchmark's harness overrides it.
- **The ratio-100 pessimistic-vs-optimistic cycle-count asymmetry
  observed during the pilot (0–2 vs. 12–14 cycles) remains an untested
  hypothesis and is not a finding of this document.** A 10ms-resolution
  probe at ratio 20 showed the gap persisting at finer sampling (29 vs.
  87 cycles) rather than disappearing as pure sampling noise, but that is
  evidence the gap is not *purely* a sampling artifact — it is not
  evidence for any specific mechanism, and none is claimed here.
- **Sweeper share of database time (16–52%) means part of every cell's
  measurement is strategy-vs-sweeper interaction**, not purely
  strategy-vs-strategy contention (see Part 5).
- **p99 attribution is incomplete**: `pool_checkout_seconds` was not
  captured per cell in this sweep, so the gap between "lock wait is
  small" / "retry overhead is small" and "p99 is 1.4–2.7s" is not fully
  explained by this data.

---

## Conclusion

**No crossover between pessimistic and optimistic locking was observed
within the valid range (contention ratios 2–5), and the valid range
produced by this design was too narrow to locate a crossover if one
exists at higher contention.** This is different from, and weaker than,
"no crossover exists" — the experiment as run cannot support that
stronger claim, and does not make it.

The result that *is* supported: the original acquire-only sweep's large,
statistically clean-looking gap was substantially — plausibly almost
entirely — an artifact of measuring post-exhaustion rejection cost rather
than genuine lock contention. Once the workload was redesigned to sustain
real contention, that gap collapsed to something indistinguishable from
noise at 3 repetitions, at the only ratios where a valid comparison could
be made at all. Interrogating a clean-looking result before trusting it —
not the number that result eventually produced — is the actual output of
this phase.
