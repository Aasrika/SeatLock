import { useMemo, useState } from "react";
import { AdminDashboardPage } from "./pages/AdminDashboardPage";
import { SeatMapPage } from "./pages/SeatMapPage";

function readEventIdFromUrl(): number {
  const params = new URLSearchParams(window.location.search);
  const raw = params.get("event");
  const parsed = raw !== null ? Number.parseInt(raw, 10) : Number.NaN;
  return Number.isFinite(parsed) ? parsed : 1;
}

export function App() {
  const eventId = useMemo(() => readEventIdFromUrl(), []);
  const [page, setPage] = useState<"seats" | "admin">("seats");

  return (
    <div className="app">
      <nav className="app-nav">
        <button type="button" onClick={() => setPage("seats")} disabled={page === "seats"}>
          Seat map
        </button>
        <button type="button" onClick={() => setPage("admin")} disabled={page === "admin"}>
          Admin
        </button>
      </nav>
      {page === "seats" ? (
        <SeatMapPage eventId={eventId} />
      ) : (
        <AdminDashboardPage eventId={eventId} />
      )}
    </div>
  );
}
