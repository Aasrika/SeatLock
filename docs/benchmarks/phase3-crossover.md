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
python -m loadtest.recirculating_sweep --sweeper-interval-seconds 5.0  # Phase 4 item-7, see Appendix
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

---

## Appendix — Phase 4 item-7: re-running this sweep at the production sweeper interval

Phase 4 made the sweeper production-grade and moved its default interval
from this benchmark's 100ms to SPEC.md's actual production guidance,
5s — safe only because lazy expiry, not sweeper frequency, is supposed to
be the correctness mechanism (every read/write path independently treats
a HELD-but-expired row as available; the sweeper just eventually makes the
row agree). Item 7 of the Phase 4 plan was to re-run this sweep at 5s and
confirm that claim empirically rather than take it on the strength of the
audit alone. It did — and, in doing so, found two things the audit missed
and one thing the sweep itself could no longer validly measure.

### Two lazy-expiry bugs, found two different ways

The Phase 4 audit itself caught one gap directly, by inspection:
`pessimistic.py`'s `acquire_any_n` filtered candidate seats with
`SeatRow.status == "AVAILABLE"`, missing the HELD-but-expired case the
domain layer already treats as reclaimable. Fixed in `7895dc3`, before
this benchmark ever ran.

Re-running this sweep at 5s surfaced a **second, structurally identical**
gap the same audit had missed: `optimistic.py`'s conditional UPDATE
required `status = 'AVAILABLE'` literally, in its raw `text()` SQL. The
reason the audit caught one and not the other is not that one bug was
subtler than the other — they are the same bug, in the same shape, in two
different strategies. It is that **the two rules were expressed in two
different vocabularies**. The pessimistic fix was found by a search built
around the ORM's `SeatRow.status ==` construct; the optimistic bug was
invisible to that search because its own equivalent rule was spelled
`status = 'AVAILABLE'` inside a string, not a piece of Python syntax any
AST-or-grep-based tool was looking for. **An audit is only as good as its
ability to see every place a rule is expressed, and each raw-SQL escape
hatch removes a construct from every future automated search** — not
because raw SQL is inherently unsafe, but because it is invisible to
tooling built around the ORM layer's shapes. Notably, **Phase 3's decision
to use `unnest()` for the optimistic strategy's per-seat expected-version
UPDATE — itself the correct call, for the deadlock-avoidance reasons
documented in that module's docstring — is exactly what put a business
rule inside raw SQL text in the first place** and is what made this
particular blind spot possible. Getting one design decision right
(`unnest()`) created the precondition for a completely unrelated audit
gap; neither decision was wrong in isolation, and the interaction between
them is the actual lesson.

### The failure mode

With the fix not yet in place, re-running at the 5s production interval
(rather than the benchmark's 100ms) made the gap large enough to see:
`optimistic.py`'s domain validation (step (b), against a freshly-read
snapshot) correctly said "HELD but expired — reclaimable." Its own write
(step (c)) then disagreed with that same read, because its WHERE clause
never accepted the expired-but-still-`HELD` case it had just validated.
The UPDATE's zero-rows-affected result was indistinguishable, from inside
the retry loop, from "someone else changed this row" — a genuine
optimistic-locking conflict — so it retried, using the full backoff
budget, against a seat nobody else was contending for at all. Throughput
fell to roughly a third of its 100ms-interval baseline, and p99 latency
rose sharply, with **no error raised anywhere**: every individual request
either succeeded or returned an ordinary, well-formed 409. Nothing in the
system's own error reporting distinguished "genuine contention" from
"the write and the read it was based on disagree about what counts as
available." **This is the same silent-wrongness shape as the TOCTOU bug
this project exists to demonstrate** — not an oversell this time, but the
same underlying failure: a component acting on a rule that silently
diverges from the rule everything else believes is in force, with the
system reporting success/failure at every step and being wrong about what
those outcomes meant.

### Sweeper database-time share: 16–52% → 1.5–3.6%

Part 5 measured the 100ms-interval benchmark's sweeper consuming
**16–52%** of each cell's total database time — a substantial fraction of
what that sweep measured was strategy-vs-sweeper interaction, not
booker-vs-booker contention (see "Sweeper share of database time" above).
Re-running at the 5s production interval, sweeper share fell to
**1.5–3.6%** across every cell
(`loadtest/results/20260830T091437Z-recirc-summary.md`). This is the
expected direction and is not itself surprising — a sweeper running 50x
less often does 50x less work — but the *size* of the drop is the point:
going from "over half of a run's database time" to "under 4%" without any
corresponding collapse in how well inventory actually recirculated
confirms that **lazy expiry, not sweeper frequency, is the dominant
reclaim path**. The sweeper's physical row updates are cleanup lagging
behind a mechanism that has already made every expired hold reclaimable
and reportable-as-available; a production deployment gets to run the
sweeper rarely specifically because the sweeper was never the thing doing
the real work.

### The invalid measurement: `fraction_available` under lazy expiry

That same 5s-interval re-run's `fraction_available` metric — the sweep's
own validity gate, ≥0.6 mean fraction of the window with at least one seat
AVAILABLE — collapsed to **0.032–0.205 across every cell**, failing the
threshold everywhere, including at ratio 2, the least contested cell in
the entire matrix, where the 100ms-interval sweep had measured 1.000. This
is not evidence that contention got worse, or that recirculation stopped
happening — pessimistic and optimistic's overlap counts, throughput, and
p99 all point the other way, back toward parity with each other. The
cause is `loadtest/recirculating_pilot.py`'s `poll_available_count_async`:
it counted seats by their raw `status` column, grouped and summed
directly. **`fraction_available` polls the raw status column, and under
lazy expiry that column no longer represents availability** — a seat can
sit at `status = 'HELD'` for up to a full sweeper interval after it
becomes reclaimable, and at a 5s interval (vs. 100ms) that window is 50x
wider, relative to the same 1.0s hold duration, than what this metric was
built and validated against. **The metric did not break — its DEFINITION
silently stopped matching reality when the mechanism changed.** It kept
returning well-formed, in-range numbers the entire time; nothing about
its shape or its call sites signalled that it needed re-examining once
Phase 4 changed what "available" actually means at the database level.

This is **the second stale-definition failure in this project**, after
Part 4's `oversold_seats`/`hold_audit`-based oversell detection, which
kept flagging legitimate sequential recirculation as double-selling after
the recirculating-workload redesign changed what "held twice" could
mean. Both failures have the identical shape: a metric whose *definition*
was correct for the system as it existed when the metric was written, and
which silently stopped being correct the moment an unrelated, correct
change was made to the mechanism it was observing — with no test failure,
no type error, and no exception anywhere to mark the moment it happened.
**The lesson is the same in both cases: when you change a mechanism,
audit the metrics that observe it, because metrics have dependencies on
system behaviour that nothing type-checks.** A metric's correctness is
coupled to an assumption about the system, and that coupling is invisible
to every tool that checks whether the code still compiles, still lints,
still passes its own tests — the metric's *tests*, if it has any, are
usually written against the same stale assumption that needs revisiting.

Not claimed here: no comparison between pessimistic and optimistic is
drawn from this run's numbers. The 0.032–0.205 fraction_available figures
above are cited only as evidence that the measurement was invalid, never
as a measurement of the system itself.

### Follow-up: fixing the measurement and re-running

`poll_available_count_async` was changed to the same lazy-expiry-aware
predicate already used by the strategies and by
`GET /api/admin/seat-status-counts` — `status = 'AVAILABLE' OR (status =
'HELD' AND hold_expires_at <= now)` — via the same `CASE`-expression
pattern as `app/api/routes/admin.py`'s `get_seat_status_counts`, and the
full sweep was re-run at the production 5s interval with the fix in
place (`loadtest/results/20260830T100647Z-recirc.json`/`-summary.md`).

**Cells now clear the validity threshold.** With the measurement itself
fixed, mean fraction_available at the 5s production interval is
comparable to (in most cells, higher than) the original 100ms-interval
benchmark's own numbers — confirming Part 5's fraction_available
collapse under the raw-status query was purely a measurement artifact,
not a real change in recirculation:

| Strategy | Ratio | Seats | VUs | Mean fraction_available (per rep) | Included |
|---|---|---|---|---|---|
| naive | 2 | 100 | 200 | 1.000, 1.000, 1.000 | YES |
| pessimistic | 2 | 100 | 200 | 1.000, 0.996, 1.000 | YES |
| optimistic | 2 | 100 | 200 | 1.000, 1.000, 1.000 | YES |
| naive | 5 | 40 | 200 | 0.878, 0.885, 0.833 | YES |
| pessimistic | 5 | 40 | 200 | 0.893, 0.842, 0.909 | YES |
| optimistic | 5 | 40 | 200 | 0.972, 0.897, 0.950 | YES |
| naive | 10 | 20 | 200 | 0.598, 0.749, 0.717 | YES |
| pessimistic | 10 | 20 | 200 | 0.759, 0.738, 0.862 | YES |
| optimistic | 10 | 20 | 200 | 0.724, 0.627, 0.450 | **YES (0.601 — barely)** |
| naive | 20 | 10 | 200 | 0.430 (mean) | NO |
| pessimistic | 20 | 10 | 200 | 0.441 (mean) | NO |
| optimistic | 20 | 10 | 200 | 0.385 (mean) | NO |

**Ratio 10 is newly, symmetrically valid** at the production interval —
unlike the original 100ms sweep (Part 5), where ratio 10 was asymmetric
(optimistic barely in, pessimistic and naive out), all three strategies
clear the bar here, giving a third valid ratio to compare that the
original sweep did not have.

### Steady-state latency and throughput, with variance (3 reps each)

| Strategy | Ratio | p50 (ms) mean/min/max | p95 (ms) mean/min/max | **p99 (ms) mean/min/max** | Throughput (req/s) mean/min/max |
|---|---|---|---|---|---|
| naive | 2 | 168.9 / 123.2 / 251.7 | 817.8 / 657.3 / 987.7 | 1269.5 / 943.2 / 1586.6 | 816.7 / 737.6 / 919.5 |
| pessimistic | 2 | 255.0 / 205.5 / 285.7 | 768.4 / 661.2 / 867.0 | **1172.3 / 1048.4 / 1345.4** | 621.7 / 567.4 / 667.1 |
| optimistic | 2 | 235.8 / 129.6 / 304.4 | 772.9 / 658.7 / 880.2 | **1175.0 / 1063.3 / 1326.9** | 709.1 / 604.0 / 875.6 |
| naive | 5 | 237.9 / 203.0 / 259.3 | 638.0 / 600.6 / 670.7 | 944.5 / 911.7 / 982.8 | 689.2 / 608.6 / 748.3 |
| pessimistic | 5 | 269.0 / 176.5 / 331.6 | 772.1 / 683.1 / 822.5 | **1172.7 / 1122.2 / 1214.7** | 652.3 / 564.1 / 787.7 |
| optimistic | 5 | 199.6 / 160.8 / 224.1 | 878.0 / 569.0 / 1417.0 | **1329.7 / 822.6 / 2198.8** | 658.3 / 603.4 / 759.7 |
| naive | 10 | 246.0 / 186.8 / 294.5 | 721.8 / 645.1 / 763.8 | 1080.8 / 985.1 / 1172.0 | 716.6 / 654.9 / 838.2 |
| pessimistic | 10 | 308.4 / 286.7 / 326.2 | 774.5 / 731.5 / 812.5 | **1108.3 / 1027.8 / 1196.4** | 550.2 / 469.4 / 591.8 |
| optimistic | 10 | 215.8 / 145.3 / 265.9 | 685.8 / 669.4 / 709.1 | **1140.4 / 1020.3 / 1311.5** | 752.8 / 674.8 / 880.1 |

**Ratios 2 and 5 reproduce Part 5's finding exactly: no measurable
difference survives the overlap check.** At ratio 2, optimistic's p99
range (1063.3–1326.9) sits entirely inside pessimistic's (1048.4–1345.4);
throughput ranges overlap substantially (pessimistic 567.4–667.1,
optimistic 604.0–875.6). At ratio 5, pessimistic's p99 range
(1122.2–1214.7) sits entirely inside optimistic's much wider one
(822.6–2198.8, one rep's tail alone reaching 2198.8ms); throughput ranges
again overlap (pessimistic 564.1–787.7, optimistic 603.4–759.7).

**Ratio 10 — the newly-valid comparison — behaves differently.** p99
ranges still overlap (pessimistic 1027.8–1196.4 sits entirely inside
optimistic's 1020.3–1311.5), but **throughput ranges do not**:
pessimistic's 3 reps (469.4–591.8 req/s) sit entirely below optimistic's
3 reps (674.8–880.1 req/s) — the first non-overlapping throughput result
at any ratio in either version of this sweep. This is reported, not
claimed as a finding: it is 3 reps, at a ratio where optimistic's own
fraction_available (0.601) sits right at the validity boundary, with no
independent replication yet. It is the kind of result that would justify
a dedicated follow-up (more reps, specifically at ratio 10) before being
treated as evidence of a real mechanism — not evidence on its own that
one exists.

Oversold/overlap counts remain the same useful cross-check as Part 4:
zero for pessimistic and optimistic at every ratio in this table, nonzero
and ratio-scaling for naive (19–113 oversold seats, 102–398 overlap
events) — the detection mechanism still finds real oversell when it's
actually present, at the production sweeper interval as much as at the
benchmark one.

**This does not change the document's Conclusion.** It extends the valid
range by one ratio (10, now symmetric) beyond what the original 100ms
sweep could support, and confirms that ratio 10's asymmetry there was a
property of the 100ms configuration's own noise, not of the underlying
system. Ratios 2–10 still show no crossover; ratio 10's throughput gap is
flagged for further investigation, not folded into that conclusion.
