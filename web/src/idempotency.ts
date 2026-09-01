/**
 * One Idempotency-Key PER CHECKOUT ATTEMPT, held stable across retries
 * of that same attempt, regenerated only when the user starts a
 * genuinely new one (SPEC.md section 9 / CLAUDE.md I4).
 *
 * "A checkout attempt" here means: from the moment a user commits to
 * booking a specific set of seats, through however many network
 * retries that same request needs, until it either succeeds or the
 * user abandons it and picks different seats -- see
 * SeatMapPage.tsx's checkoutKey state, which is exactly this: created
 * once when a checkout begins, reused for every retry of THAT attempt,
 * and only replaced by a fresh call to this function when a new
 * attempt starts.
 */
export function newIdempotencyKey(): string {
  return crypto.randomUUID();
}
