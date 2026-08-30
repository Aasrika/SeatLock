.PHONY: up down logs test lint fmt run-api run-dev benchmark benchmark-sweep sweeper reconciler

UVICORN_WORKERS ?= 4
PROMETHEUS_MULTIPROC_DIR ?= .prometheus-multiproc

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
# Prometheus multiprocess mode (app/infra/metrics.py) needs its directory
# cleared exactly once, before any worker starts -- clearing it inside the
# app itself would race between workers starting up at slightly different
# times (see that module's docstring). This Makefile target is the one
# place that happens once, before uvicorn's master process even exists.
run-api:
	mkdir -p "$(PROMETHEUS_MULTIPROC_DIR)"
	rm -f "$(PROMETHEUS_MULTIPROC_DIR)"/*.db
	PROMETHEUS_MULTIPROC_DIR="$(PROMETHEUS_MULTIPROC_DIR)" \
		uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers $(UVICORN_WORKERS)

# --reload does not work correctly with multiple workers -- always 1 here.
# Single worker also means multiprocess mode isn't strictly needed, but
# clearing stale files is harmless and keeps behaviour consistent with
# run-api.
run-dev:
	mkdir -p "$(PROMETHEUS_MULTIPROC_DIR)"
	rm -f "$(PROMETHEUS_MULTIPROC_DIR)"/*.db
	PROMETHEUS_MULTIPROC_DIR="$(PROMETHEUS_MULTIPROC_DIR)" \
		uvicorn app.main:app --reload --workers 1

benchmark:
	python -m loadtest.run_benchmark

# Phase 3's phase deliverable -- starts and stops the API itself per cell
# (see loadtest/run_benchmark.py's run_sweep docstring), so `make run-api`
# must NOT already be running against the same --base-url.
benchmark-sweep:
	python -m loadtest.run_benchmark --sweep

# Background jobs (Phase 4, SPEC.md section 5) -- share the same database
# as `make run-api` but are genuinely separate OS processes (CLAUDE.md
# rule 1: background jobs within the monolith, not separate services).
# Run alongside run-api, not instead of it.
sweeper:
	python -m workers.sweeper

reconciler:
	python -m workers.reconciler
