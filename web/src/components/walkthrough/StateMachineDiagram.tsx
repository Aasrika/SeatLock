export type SeatMachineState = "AVAILABLE" | "HELD" | "BOOKED";

/**
 * app/domain/state_machine.py's own transition diagram (hold / confirm /
 * expire / release), rendered live. Animation here is functional, not
 * decorative: highlighting the CURRENT state as it changes is what
 * makes "the seat just moved from HELD to AVAILABLE via lazy expiry, no
 * sweeper involved" legible in real time instead of requiring the
 * viewer to re-read a status field.
 */
export function StateMachineDiagram({ current }: { current: SeatMachineState }) {
  return (
    <div className="wt-state-machine" role="img" aria-label={`Seat state machine, currently ${current}`}>
      <Node label="AVAILABLE" active={current === "AVAILABLE"} />
      <span className="wt-sm-arrow">→ hold →</span>
      <Node label="HELD" active={current === "HELD"} />
      <span className="wt-sm-arrow">→ confirm →</span>
      <Node label="BOOKED" active={current === "BOOKED"} />
    </div>
  );
}

function Node({ label, active }: { label: string; active: boolean }) {
  return <div className={`wt-sm-node ${active ? "wt-sm-node--active" : ""}`}>{label}</div>;
}
