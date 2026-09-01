import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// Proxies /api and /ws to the FastAPI backend (make run-api, port 8000)
// during `npm run dev` -- so the browser only ever talks to Vite's own
// origin, avoiding CORS entirely rather than configuring it on the
// backend for a dev-only concern.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true,
      },
      "/ws": {
        target: "ws://localhost:8000",
        ws: true,
      },
    },
  },
});
