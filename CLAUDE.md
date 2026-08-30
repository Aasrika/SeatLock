# Seatlock — Project Rules for Claude Code

## What this project is
A concurrency-safe event ticketing system. The ENTIRE point is a provable
correctness guarantee under concurrent load. Features are secondary.
Full spec: docs/SPEC.md — read the relevant section before implementing.

## Non-negotiable architectural rules

1. **Modular monolith. Never microservices.** Seat inventory needs a single
   transactional boundary. Do not suggest splitting into services.
2. **`app/domain/` is pure Python.** Zero imports of SQLAlchemy, Redis, FastAPI,
   or anything I/O. If you need I/O in the domain layer, the design is wrong.
3. **All seat status changes go through the state machine.** No code outside
   `app/domain/state_machine.py` may set a seat's status directly.
4. **PostgreSQL is the source of truth for hold expiry.** Redis TTL is a cache
   optimisation only. Never make correctness depend on a Redis key existing.
5. **No mocking the database.** Integration tests use testcontainers with real
   Postgres and Redis.
6. **The naive/broken implementation is intentional.** Do not "fix" Strategy A.
   It is the control condition and it must oversell.

## The five invariants — never break these
- I1: At most one active booking per seat.
- I2: count(AVAILABLE) + count(HELD) + count(BOOKED) == total_seats, always.
- I3: No seat stays HELD past hold_expires_at beyond one sweeper interval.
- I4: Same Idempotency-Key + same fingerprint = same response, one booking.
- I5: Processing the same provider_event_id N times == processing it once.

## Conventions
- Python 3.11+, async throughout (asyncpg, SQLAlchemy 2.0 async, redis.asyncio)
- All datetimes UTC, `timestamptz` in Postgres, never naive datetime objects
- Type hints everywhere; mypy strict on `app/domain/`
- Ruff for lint + format
- Conventional commits

## Working style
- Before implementing anything non-trivial, state your plan and wait for approval.
- Small commits, one concern each.
- Write tests alongside code, never as an afterthought.
- If unsure whether something violates a rule above, ask. Do not guess.

## Deviations from docs/SPEC.md
- SPEC.md section 2 originally named this package `app/platform/`. It is
  `app/infra/` instead — `platform` shadows a Python stdlib module. SPEC.md
  has been updated to match; this note is the record of why.
- SPEC.md section 5 originally wrote the expiry filter as
  `hold_expires_at < now()`. It is `<=` instead, matching
  `app/domain/state_machine.py`'s `is_hold_expired` exactly — `<` would leave
  a one-instant window where the domain layer considers a hold expired and
  the query layer does not agree. SPEC.md has been updated to match.