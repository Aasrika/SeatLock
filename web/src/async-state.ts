// A discriminated union for "a thing the user triggered that talks to
// the network," used by every section of the walkthrough page. No
// `any`, no implicit "undefined means still loading" -- every consumer
// must switch on `status` and TypeScript enforces that all four cases
// are handled.
export type AsyncState<T> =
  | { status: "idle" }
  | { status: "loading" }
  | { status: "success"; data: T }
  | { status: "error"; message: string };

export const idle: AsyncState<never> = { status: "idle" };
export const loading: AsyncState<never> = { status: "loading" };

export function success<T>(data: T): AsyncState<T> {
  return { status: "success", data };
}

export function failure(message: string): AsyncState<never> {
  return { status: "error", message };
}

// Every network call in this app throws ApiError (api.ts, demo-api.ts)
// on a non-2xx response, or a plain Error/TypeError (e.g. the network
// itself failing) otherwise -- this turns either into one readable
// string instead of leaking `[object Object]` or an unhandled shape
// into the UI. ApiError's own .message is just "API error 422"; FastAPI
// puts the actually-useful text in the response body's `detail` or
// `reason` field, so this reaches into `body` (typed as `unknown`,
// narrowed here rather than trusted) for it when present.
export function describeError(err: unknown): string {
  if (isApiErrorLike(err)) {
    const body = err.body;
    if (body !== null && typeof body === "object") {
      const record = body as Record<string, unknown>;
      const detail = record.detail ?? record.reason;
      if (typeof detail === "string") {
        return detail;
      }
    }
    return err.message;
  }
  if (err instanceof Error) {
    return err.message;
  }
  return String(err);
}

function isApiErrorLike(err: unknown): err is { message: string; body: unknown } {
  return (
    err instanceof Error &&
    "body" in err &&
    typeof (err as { body?: unknown }).body !== "undefined"
  );
}
