import { useMemo, useState } from "react";
import { AdminDashboardPage } from "./pages/AdminDashboardPage";
import { SeatMapPage } from "./pages/SeatMapPage";
import { WalkthroughPage } from "./pages/WalkthroughPage";

function readEventIdFromUrl(): number {
  const params = new URLSearchParams(window.location.search);
  const raw = params.get("event");
  const parsed = raw !== null ? Number.parseInt(raw, 10) : Number.NaN;
  return Number.isFinite(parsed) ? parsed : 1;
}

export function App() {
  const eventId = useMemo(() => readEventIdFromUrl(), []);
  const [page, setPage] = useState<"seats" | "admin" | "walkthrough">("seats");

  return (
    <div className="app">
      <nav className="app-nav">
        <button type="button" onClick={() => setPage("seats")} disabled={page === "seats"}>
          Seat map
        </button>
        <button type="button" onClick={() => setPage("admin")} disabled={page === "admin"}>
          Admin
        </button>
        <button
          type="button"
          onClick={() => setPage("walkthrough")}
          disabled={page === "walkthrough"}
        >
          Walkthrough
        </button>
      </nav>
      {page === "seats" && <SeatMapPage eventId={eventId} />}
      {page === "admin" && <AdminDashboardPage eventId={eventId} />}
      {page === "walkthrough" && <WalkthroughPage eventId={eventId} />}
    </div>
  );
}
