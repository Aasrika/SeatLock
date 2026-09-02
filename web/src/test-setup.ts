// Extends Vitest's `expect` with jest-dom's DOM-specific matchers
// (toBeInTheDocument, toHaveTextContent, ...) -- imported once, here,
// for every test file rather than per-file.
import "@testing-library/jest-dom/vitest";

import { afterEach } from "vitest";
import { cleanup } from "@testing-library/react";

// React Testing Library auto-unmounts after each test ONLY when it
// detects a global `afterEach` (Jest-style globals). This project runs
// Vitest without `test.globals: true` (each file imports its own
// `afterEach` from "vitest" explicitly instead), so that
// auto-detection never fires -- without this, a component rendered in
// one test would stay mounted into the next, and two tests asserting
// the same text (e.g. two `render(<RaceSection .../>)` calls) would
// fail with "found multiple elements" for a reason that has nothing to
// do with either test.
afterEach(() => {
  cleanup();
});
