import { useEffect, useRef, useState } from "react";
import { applySeatDiffs, buildSeatMapFromSnapshot, computeServerTimeOffsetMs } from "../realtime/client";
import type { SeatMap, ServerMessage } from "../realtime/types";

export type ConnectionStatus = "connecting" | "open" | "reconnecting" | "closed";

interface UseSeatMapSocketResult {
  seats: SeatMap;
  status: ConnectionStatus;
  serverTimeOffsetMs: number;
}

const BASE_RECONNECT_DELAY_MS = 500;
const MAX_RECONNECT_DELAY_MS = 15_000;

/**
 * Full jitter, same shape as the backend's own optimistic-locking
 * retry backoff (SPEC.md section 4 / app/inventory/strategies/
 * optimistic.py) -- without jitter, every client that dropped
 * connection at roughly the same moment (a server restart, a network
 * blip) would reconnect in near-lockstep and hit the API with a
 * synchronized burst, the same thundering-herd failure jittered
 * backoff exists to prevent on the booking side.
 */
function backoffWithJitterMs(attempt: number): number {
  const cap = Math.min(MAX_RECONNECT_DELAY_MS, BASE_RECONNECT_DELAY_MS * 2 ** attempt);
  return Math.random() * cap;
}

function buildWsUrl(eventId: number, sections: string[]): string {
  const proto = window.location.protocol === "https:" ? "wss" : "ws";
  const host = window.location.host;
  const query = sections.length > 0 ? `?sections=${sections.join(",")}` : "";
  return `${proto}://${host}/ws/events/${eventId}${query}`;
}

/**
 * On EVERY connect (first connect or any reconnect), the server sends a
 * full snapshot first -- this hook always replaces its seat map
 * wholesale on that message, never merges it into whatever was there
 * before. A reconnect after a drop must never assume the cached view
 * survived the gap (SPEC.md section 9): the seats a client held in
 * memory during a disconnection could be arbitrarily stale by the time
 * the socket comes back.
 */
export function useSeatMapSocket(eventId: number, sectionsKey: string): UseSeatMapSocketResult {
  const [seats, setSeats] = useState<SeatMap>(() => new Map());
  const [status, setStatus] = useState<ConnectionStatus>("connecting");
  const [serverTimeOffsetMs, setServerTimeOffsetMs] = useState(0);

  const attemptRef = useRef(0);
  const closedByUsRef = useRef(false);
  const wsRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    closedByUsRef.current = false;
    attemptRef.current = 0;
    let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
    const sections = sectionsKey.length > 0 ? sectionsKey.split(",") : [];

    function connect(): void {
      setStatus(attemptRef.current === 0 ? "connecting" : "reconnecting");
      const ws = new WebSocket(buildWsUrl(eventId, sections));
      wsRef.current = ws;

      ws.onopen = () => {
        attemptRef.current = 0;
        setStatus("open");
      };

      ws.onmessage = (event: MessageEvent<string>) => {
        const message = JSON.parse(event.data) as ServerMessage;
        if (message.type === "snapshot") {
          setSeats(buildSeatMapFromSnapshot(message.seats));
          setServerTimeOffsetMs(computeServerTimeOffsetMs(message.server_time, Date.now()));
        } else {
          setSeats((prev) => applySeatDiffs(prev, message.seats));
        }
      };

      ws.onclose = () => {
        wsRef.current = null;
        if (closedByUsRef.current) return;
        setStatus("reconnecting");
        const attempt = attemptRef.current;
        attemptRef.current += 1;
        reconnectTimer = setTimeout(connect, backoffWithJitterMs(attempt));
      };

      ws.onerror = () => {
        ws.close();
      };
    }

    connect();

    return () => {
      closedByUsRef.current = true;
      clearTimeout(reconnectTimer);
      wsRef.current?.close();
      setStatus("closed");
    };
  }, [eventId, sectionsKey]);

  return { seats, status, serverTimeOffsetMs };
}
