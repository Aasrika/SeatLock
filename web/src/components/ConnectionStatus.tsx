import type { ConnectionStatus as Status } from "../hooks/useSeatMapSocket";

const LABEL: Record<Status, string> = {
  connecting: "Connecting…",
  open: "Live",
  reconnecting: "Reconnecting…",
  closed: "Disconnected",
};

const COLOR: Record<Status, string> = {
  connecting: "#f5a623",
  open: "#2ecc71",
  reconnecting: "#f5a623",
  closed: "#e74c3c",
};

export function ConnectionStatus({ status }: { status: Status }) {
  return (
    <div className="connection-status" title={`WebSocket: ${status}`}>
      <span className="connection-dot" style={{ backgroundColor: COLOR[status] }} />
      {LABEL[status]}
    </div>
  );
}
