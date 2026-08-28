.PHONY: up down logs test lint fmt

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
