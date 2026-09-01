import { useEffect, useState } from "react";
import { getDashboard, type DashboardResponse } from "../api";

const POLL_INTERVAL_MS = 3000;

/**
 * Reads GET /api/admin/dashboard -- a typed JSON endpoint, NOT
 * /metrics text parsed here. See app/api/routes/admin.py's own
 * docstring for why: this project has already found five metrics
 * whose definition silently stopped matching what they measured
 * (docs/benchmarks/phase3-crossover.md), and a frontend Prometheus
 * text-parser, unable to tell one metric's multiprocess_mode from
 * another's, would have been a sixth instance of exactly that failure
 * shape.
 */
export function AdminDashboardPage({ eventId }: { eventId?: number }) {
  const [dashboard, setDashboard] = useState<DashboardResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function poll() {
      try {
        const data = await getDashboard(eventId);
        if (!cancelled) {
          setDashboard(data);
          setError(null);
        }
      } catch {
        if (!cancelled) setError("Could not reach the dashboard endpoint.");
      }
    }

    poll();
    const interval = setInterval(poll, POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [eventId]);

  if (error !== null) return <p className="message">{error}</p>;
  if (dashboard === null) return <p>Loading…</p>;

  const { metrics, invariants } = dashboard;
  const meanLockWaitMs =
    metrics.lock_wait_seconds_count > 0
      ? (metrics.lock_wait_seconds_sum / metrics.lock_wait_seconds_count) * 1000
      : null;

  return (
    <div className="admin-dashboard">
      <h2>Admin dashboard</h2>
      <p className="checked-at">as of {new Date(dashboard.checked_at).toLocaleTimeString()}</p>

      {invariants !== null && (
        <section>
          <h3>Invariants — event {dashboard.event_id}</h3>
          <ul>
            {Object.entries(invariants).map(([name, result]) => (
              <li key={name} className={result.passed ? "invariant-ok" : "invariant-fail"}>
                {result.passed ? "✓" : "✗"} {name}
                {result.detail !== null && `: ${result.detail}`}
              </li>
            ))}
          </ul>
        </section>
      )}

      <section>
        <h3>Sweeper</h3>
        <p>Backlog: {metrics.sweeper_backlog} expired-but-unswept holds</p>
      </section>

      <section>
        <h3>Lock contention</h3>
        <p>Deadlocks: {metrics.deadlocks_total}</p>
        <p>Lock timeouts: {metrics.lock_timeouts_total}</p>
        <p>
          Mean lock wait: {meanLockWaitMs !== null ? `${meanLockWaitMs.toFixed(1)}ms` : "no data yet"} (
          {metrics.lock_wait_seconds_count} observations)
        </p>
      </section>

      <section>
        <h3>Optimistic retry rates</h3>
        <p>Conflicts: {metrics.optimistic_conflicts_total}</p>
        <p>Retries: {metrics.optimistic_retries_total}</p>
        <p>Budget exhausted: {metrics.optimistic_exhausted_total}</p>
      </section>

      <section>
        <h3>Reconciliation</h3>
        <h4>Divergences (confirmed, repaired)</h4>
        <KindTable kinds={metrics.reconciliation_divergence_by_kind} emptyLabel="none" />
        <h4>Transient (resolved on second look)</h4>
        <KindTable kinds={metrics.reconciliation_transient_by_kind} emptyLabel="none" />
      </section>
    </div>
  );
}

function KindTable({ kinds, emptyLabel }: { kinds: Record<string, number>; emptyLabel: string }) {
  const entries = Object.entries(kinds);
  if (entries.length === 0) return <p>{emptyLabel}</p>;
  return (
    <ul>
      {entries.map(([kind, count]) => (
        <li key={kind}>
          {kind}: {count}
        </li>
      ))}
    </ul>
  );
}
