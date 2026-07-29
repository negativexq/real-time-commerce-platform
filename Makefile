.PHONY: lint format format-check type-check test check compose-config

lint:
	python -m ruff check .

format:
	python -m ruff format .

format-check:
	python -m ruff format --check .

type-check:
	python -m mypy .

test:
	python -m pytest

check: lint format-check type-check test

compose-config:
	docker compose config
