/// <reference types="vitest/config" />
import react from "@vitejs/plugin-react";
// Plain vite's defineConfig, not vitest/config's -- vitest bundles its
// OWN internal copy of vite, whose Plugin type structurally clashes
// with this project's own vite package (confirmed directly: importing
// defineConfig from "vitest/config" made `plugins: [react()]` fail to
// type-check against two incompatible Plugin<any> types). The triple-
// slash reference above merges vitest's `test` config key onto vite's
// own UserConfig type without pulling in its bundled vite at all.
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
  test: {
    // happy-dom, not jsdom: jsdom's own CSS-color-parsing dependency
    // chain (@asamuzakjp/css-color) ships an ESM-only build that fails
    // under this machine's Node 20.18.2 ("ERR_REQUIRE_ESM") -- confirmed
    // directly, not assumed. happy-dom provides the same DOM environment
    // React Testing Library needs without that dependency. Pure-logic
    // tests (client.test.ts) don't need a DOM at all but run fine under
    // it too.
    environment: "happy-dom",
    setupFiles: ["./src/test-setup.ts"],
  },
});
