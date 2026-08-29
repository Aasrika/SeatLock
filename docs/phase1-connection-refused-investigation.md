# Phase 1 benchmark: the connection-refused investigation

This documents the diagnosis and two experiments run against the early
Phase 1 benchmark (`loadtest/last_seat.js` / `flash_sale.js` +
`loadtest/run_benchmark.py`), which showed `http_req_failed ≈ 99%` and
`"connectex: ... actively refused it"` errors during high-VU runs. It's
kept here (not in `loadtest/results/`, which is gitignored generated
output) because the finding is durable and worth being able to cite later.

## Bisection: is this workers, backlog, or something else?

`last_seat.js`, 6s measured runs, real uvicorn instances, worker PIDs
tracked continuously through every run (`Get-Process -Id`, not just
before/after):

| Workers | VUs | Connection-refused lines | Workers alive throughout |
|---|---|---|---|
| 1 | 10–200 | 0 | yes |
| 1 | **250** | **1,481** | yes |
| 1 | 500 | 15,886 | yes |
| 4 | 10–250 | 0 | yes |
| 4 | **350** | **168** | yes |
| 4 | 500 | 226 | yes |

Ruled out with direct evidence, not assumption:
- **Worker crashes/restarts** — same PIDs alive before, during, and after
  every single run.
- **Port exhaustion** — Windows dynamic range is 49152–65535 (16,384
  ports, `netsh int ipv4 show dynamicport tcp`, unmodified default).
  TIME_WAIT peaked at 80 system-wide, 0 on :8000 specifically.
- **Backlog too low as configured** — uvicorn requests backlog=2048;
  `socket.SOMAXCONN` reports the modern-Windows dynamic-backlog sentinel,
  not a hard cap.

Every failure, in every run, was k6 `error_code: 1212` /
`"connectex: ... actively refused it"` (confirmed directly by pointing a
probe script at a closed port and reading `res.status`/`res.error_code`).
100% of the refusals in any given run shared one or two identical
timestamps at the very start of the run, never recurring afterward.
Throughput recovered immediately after. More workers measurably raised the
threshold. This is consistent with a connection-acceptance burst at the
instant `constant-vus` starts every VU simultaneously — not a worker-count
bug, not resource exhaustion, not a naive-strategy bug.

## Correction received and accepted

Uvicorn's workers share a single listening socket/accept queue; worker
count changes *accept rate*, not queue *depth*. That the failure threshold
moved with worker count therefore points at accept-rate/process-warmth,
not backlog depth — consistent with backlog already being large and
uncapped.

## Experiment 1 — warmup + DB pool pre-fill

Hypothesis: the accept loop is slow at t=0 because the process is cold
(DB pool filling lazily, import paths, per-request machinery). Changes:

- `app/main.py`: a `lifespan` startup hook opens `pool_size` connections
  concurrently and releases them, so the pool is warm before the first
  real request.
- `loadtest/last_seat.js` / `flash_sale.js`: a `warmup` k6 scenario
  (~20 VUs hitting `GET /health` — never `/api/holds`, so it cannot
  pre-consume seats) runs before the measured burst via `startTime`.

Result at 500 VUs / 4 workers, three repeats, same configuration:
**42, 23, 256** connection-refused lines (one repeat also showed a
transient full stall unrelated to connection refusal — investigated
separately below, did not reproduce on a second identical run).

**Conclusion: inconclusive.** Variance (23 to 256) under identical
configuration swamps any consistent improvement over the un-warmed
baseline (226, single measurement). Warmup did not reliably resolve it.

## Experiment 2 — raised backlog

Restarted uvicorn with `--backlog 8192` (warmup still applied). Three
repeats at 500 VUs / 4 workers: **364, 54, 0**.

**Conclusion: also inconclusive.** Same order of variance (0 to 364) as
Experiment 1, no consistent improvement over either the baseline or the
warmup-only condition.

## What we're keeping, and why

- **DB pool pre-fill (`app/main.py` lifespan) — kept.** Independently
  justified regardless of the burst-refusal outcome: it removes
  connection-setup latency from whichever request happens to be first,
  which matters for not contaminating p95/p99 with cold-start cost.
- **k6 warmup scenario — kept**, same reasoning, client side: the
  *measured* scenario's numbers shouldn't include k6/VU startup cost
  either.
- **Backlog — not changed from uvicorn's default (2048).** No evidence it
  reliably helps; hardcoding a "fix" that doesn't reproducibly work would
  be worse than reporting the failures honestly.

## Standing conclusion

At very high constant-vus VU counts (≥ ~350-500 on this machine, a shared
development laptop, not a dedicated benchmark host — see SPEC.md section
16: *"Load-generating from one laptop... check client-side CPU and file
descriptor limits before believing any throughput number"*), a fraction of
connections get refused at the exact instant of the burst. This fraction
is highly variable run to run and was not reliably reduced by either
warmup or a larger backlog. It is reported as its own `transport_failures`
category in every benchmark run, never merged into application-level error
rates, so its presence (or absence) is always visible rather than hidden
inside an aggregate number.

`flash_sale.js` (the headline scenario, `ramping-vus`, peak 200 VUs) does
not exhibit this at all in five repeated runs — the instantaneous-burst
character of `last_seat.js`'s `constant-vus` at 500 VUs is what triggers
it, consistent with the diagnosis above.
