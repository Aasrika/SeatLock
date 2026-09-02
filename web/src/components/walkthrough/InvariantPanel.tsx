import type { InvariantSummary } from "../../demo-api";

/**
 * "4 of 5 verified live," never "all five invariants: PASS."
 *
 * app/api/routes/demo.py's _summarize_invariants (Phase 8a's own
 * REPEATABLE READ fix, reused here, not re-implemented) only ever
 * checks conservation/I2, a structural I1 check, state-coherence, and
 * booking-linkage -- never I3 (sweeper-interval staleness), I4
 * (idempotency), or I5 (webhook exactly-once). Showing "PASS" for all
 * five while verifying four would be exactly the kind of label-
 * overstates-the-measurement bug this project has repeatedly found and
 * fixed elsewhere (see docs/chaos-results.md) -- the first instance to
 * reach a UI, if this panel didn't disclose the gap. A reviewer who
 * sees a disclosed limit trusts everything else on the page more; a
 * silent one destroys that trust the moment it's noticed.
 */
export function InvariantPanel({ invariants }: { invariants: InvariantSummary }) {
  const entries = Object.entries(invariants.results);
  const allPassed = entries.every(([, r]) => r.passed);

  return (
    <div>
      <h3>
        Invariants — {invariants.checked_count} of {invariants.total_count} verified live
      </h3>
      <ul className="wt-invariant-list">
        {entries.map(([name, result]) => (
          <li key={name}>
            <span className="wt-mono">{name}</span>
            {result.passed ? (
              <span className="wt-status wt-status--good">pass</span>
            ) : (
              <span className="wt-status wt-status--critical" title={result.detail ?? undefined}>
                fail
              </span>
            )}
          </li>
        ))}
      </ul>
      {!allPassed && (
        <p className="wt-error" role="alert">
          An invariant failed — see the detail on hover above.
        </p>
      )}
      <div className="wt-invariant-disclosure">
        {/* unchecked_note (app/api/routes/demo.py) is the single source
            of truth for WHICH invariants this checker skips and why --
            rendered verbatim, not paraphrased, so the two can't drift
            out of sync. The link below is additive (a place to go),
            not a restatement of what the note already said. */}
        {invariants.unchecked_note}
        <br />
        <a
          href="https://github.com/Aasrika/SeatLock/blob/main/docs/chaos-results.md"
          target="_blank"
          rel="noreferrer"
        >
          See docs/chaos-results.md for how those are verified.
        </a>
      </div>
    </div>
  );
}
