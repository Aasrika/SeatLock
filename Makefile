.PHONY: up down logs test lint fmt run-api run-dev benchmark

UVICORN_WORKERS ?= 4

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

test:
	pytest

lint:
	ruff check .
	mypy app/domain

fmt:
	ruff format .

# The API runs on the host (not in docker-compose) through Phases 1-3:
# containerising it would put network overhead on top of the p50/p95/p99
# numbers we're trying to attribute to concurrency control, not to Docker's
# network stack. Containerising the API is deferred to Phase 8's chaos
# suite, where that overhead stops being a confound and starts being part
# of what's under test.
#
# Connection budget: UVICORN_WORKERS * (POOL_SIZE + MAX_OVERFLOW) must stay
# below postgres max_connections (default 100) -- leave headroom for psql,
# Alembic, and the testcontainers suite running alongside it. Defaults
# (config.py): pool_size=10, max_overflow=5, so 4 workers * 15 = 60.
# Tune POOL_SIZE/MAX_OVERFLOW (env vars, not hardcoded) when diagnosing
# whether observed latency is lock contention or pool exhaustion.
run-api:
	uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers $(UVICORN_WORKERS)

# --reload does not work correctly with multiple workers -- always 1 here.
run-dev:
	uvicorn app.main:app --reload --workers 1

benchmark:
	python -m loadtest.run_benchmark
