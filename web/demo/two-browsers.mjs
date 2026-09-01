// Phase 7 item 11 -- the demo the README's GIF placeholder is waiting
// on: two browser windows on the same event, one clicks a seat, the
// other greys it out live, driven entirely by the real backend
// (Postgres + Redis pub/sub + the coalescing hub), not a mock.
//
// PREREQUISITES (this script does not manage any of these -- same
// convention as loadtest/'s own scripts, which assume `make run-api`
// is already running externally):
//   1. `make up`                      (Postgres + Redis)
//   2. `alembic upgrade head`
//   3. `make run-api`                 (port 8000)
//   4. `npm run dev` inside web/      (port 5173)
//   5. `python -m scripts.seed_demo_event` -- prints the demo event's
//      id on its own stdout line; pass it as DEMO_EVENT_ID below.
//
// USAGE:
//   DEMO_EVENT_ID=$(python -m scripts.seed_demo_event 2>/dev/null) \
//     node web/demo/two-browsers.mjs
//
// OUTPUT: two .webm recordings under web/demo/recordings/ (Playwright's
// own video format -- there is no direct "record to GIF" option). The
// two recordings are separate files by construction (two independent
// BrowserContexts = two independent windows) and are not the same
// length (window A does the click + checkout flow, window B just
// observes, so its recording ends sooner) -- combine them side by side
// with ffmpeg, padding the shorter one so it holds its last frame
// instead of the combined GIF ending asymmetrically:
//
//   ffmpeg -i web/demo/recordings/<A>.webm -i web/demo/recordings/<B>.webm \
//     -filter_complex \
//       "[0:v]tpad=stop_mode=clone:stop_duration=3[a]; \
//        [1:v]tpad=stop_mode=clone:stop_duration=3[b]; \
//        [a][b]hstack=inputs=2,fps=12,scale=1280:-1:flags=lanczos" \
//     -loop 0 docs/demo.gif
//
// NOTE: Playwright bundles a minimal ffmpeg (no GIF encoder, no hstack
// filter) -- use a full ffmpeg build for the conversion, not the one
// under ms-playwright/.

import { chromium } from "playwright";
import { mkdir } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import path from "node:path";

const eventId = process.env.DEMO_EVENT_ID;
if (!eventId) {
  console.error("Set DEMO_EVENT_ID -- see this script's own header comment for how to seed one.");
  process.exit(1);
}

const baseUrl = process.env.DEMO_BASE_URL ?? "http://localhost:5173";
const recordingsDir = path.join(path.dirname(fileURLToPath(import.meta.url)), "recordings");
await mkdir(recordingsDir, { recursive: true });

const browser = await chromium.launch();

async function newWindow(label) {
  const context = await browser.newContext({
    viewport: { width: 640, height: 480 },
    recordVideo: { dir: recordingsDir, size: { width: 640, height: 480 } },
  });
  const page = await context.newPage();
  await page.goto(`${baseUrl}/?event=${eventId}`);
  await page.waitForSelector("text=Live", { timeout: 15000 });
  await page.waitForTimeout(400); // let the initial snapshot's seats settle
  console.log(`[${label}] connected`);
  return { context, page };
}

const windowA = await newWindow("A");
const windowB = await newWindow("B");

// Both windows must agree on the starting state before the demo click
// means anything -- this is the "two browsers watching the same seat"
// setup, verified, not assumed.
const firstSeatFillA = await windowA.page.locator("svg rect").first().getAttribute("fill");
const firstSeatFillB = await windowB.page.locator("svg rect").first().getAttribute("fill");
console.log("starting fills:", { firstSeatFillA, firstSeatFillB });
if (firstSeatFillA !== firstSeatFillB) {
  throw new Error(
    "Windows disagree on starting seat state -- re-run scripts.seed_demo_event and try again.",
  );
}

console.log("[A] clicking the first seat…");
await windowA.page.locator("svg rect").first().click();

console.log("[B] waiting for it to grey out, live, with no reload…");
await windowB.page.waitForFunction(
  () => document.querySelectorAll("svg rect")[0]?.getAttribute("fill") === "#f5a623",
  { timeout: 5000 },
);
console.log("[B] confirmed: the seat clicked in window A is now HELD in window B.");

await windowA.page.waitForTimeout(1200); // hold the final frame briefly for the recording
await windowB.page.waitForTimeout(1200);

await windowA.context.close();
await windowB.context.close();
await browser.close();

console.log(`Recordings written to ${recordingsDir}`);
