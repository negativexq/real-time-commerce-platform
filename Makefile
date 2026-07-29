PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)

.PHONY: lint format format-check type-check test check compose-config up down logs ps \
	kafka-topics kafka-describe kafka-smoke postgres-shell postgres-tables redis-cli \
	storage-smoke storage-status storage-logs generator-build generator-up \
	generator-down generator-logs generator-run generator-sample generator-status \
	generator-smoke generator-personas generator-normal generator-suspicious \
	generator-bot generator-takeover generator-anomalies generator-persona-smoke \
	generator-anomaly-smoke clean clean-volumes

lint:
	$(PYTHON) -m ruff check .

format:
	$(PYTHON) -m ruff format .

format-check:
	$(PYTHON) -m ruff format --check .

type-check:
	$(PYTHON) -m mypy .

test:
	$(PYTHON) -m pytest

check: lint format-check type-check test

compose-config:
	docker compose config

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f kafka kafka-init kafka-ui

ps:
	docker compose ps

kafka-topics:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka:9092 --list

kafka-describe:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka:9092 --describe

kafka-smoke:
	docker compose run --rm --no-deps \
		-v "$(CURDIR)/scripts/kafka-smoke.sh:/opt/commerce/kafka-smoke.sh:ro" \
		--entrypoint /bin/bash kafka-init /opt/commerce/kafka-smoke.sh

postgres-shell:
	docker compose exec postgres sh -c \
		'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

postgres-tables:
	docker compose exec postgres sh -c \
		'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c "\dt+"'

redis-cli:
	docker compose exec redis redis-cli

storage-smoke:
	./scripts/storage-smoke.sh

storage-status:
	docker compose ps postgres redis

storage-logs:
	docker compose logs -f postgres redis

generator-build:
	docker compose --profile generator build event-generator

generator-up:
	docker compose --profile generator up -d --build event-generator

generator-down:
	docker compose --profile generator rm -sf event-generator

generator-logs:
	docker compose --profile generator logs -f event-generator

generator-run:
	docker compose --profile generator run --rm event-generator

generator-sample:
	docker compose --profile generator run --rm event-generator \
		--journeys 5 --seed 42

generator-status:
	docker compose --profile generator ps event-generator

generator-smoke:
	docker compose --profile generator run --rm \
		--entrypoint python event-generator /app/scripts/generator-smoke.py

generator-personas:
	@printf '%s\n' \
		'Supported personas: normal, indecisive, discount_hunter, suspicious, bot, account_takeover' \
		'Configured weights: $(or $(GENERATOR_PERSONA_WEIGHTS),normal=0.50,indecisive=0.15,discount_hunter=0.15,suspicious=0.10,bot=0.05,account_takeover=0.05)'

generator-normal:
	docker compose --profile generator run --rm event-generator \
		--journeys 5 --persona normal --seed 42

generator-suspicious:
	docker compose --profile generator run --rm event-generator \
		--journeys 5 --persona suspicious --seed 43

generator-bot:
	docker compose --profile generator run --rm event-generator \
		--journeys 2 --persona bot --seed 44

generator-takeover:
	docker compose --profile generator run --rm event-generator \
		--journeys 2 --persona account_takeover --seed 45

generator-anomalies:
	docker compose --profile generator run --rm \
		-e GENERATOR_DUPLICATE_EVENT_PROBABILITY=1 \
		-e GENERATOR_MALFORMED_JSON_PROBABILITY=1 \
		-e GENERATOR_MISSING_FIELD_PROBABILITY=1 \
		-e GENERATOR_UNKNOWN_EVENT_TYPE_PROBABILITY=1 \
		-e GENERATOR_LATE_EVENT_PROBABILITY=1 \
		-e GENERATOR_OUT_OF_ORDER_PROBABILITY=1 \
		-e GENERATOR_PAYLOAD_MISMATCH_PROBABILITY=1 \
		-e GENERATOR_MAX_ANOMALIES_PER_JOURNEY=7 \
		event-generator --journeys 1 --persona normal --anomalies --seed 5150

generator-persona-smoke:
	docker compose --profile generator run --rm \
		--entrypoint python event-generator /app/scripts/generator-persona-smoke.py

generator-anomaly-smoke:
	docker compose --profile generator run --rm \
		--entrypoint python event-generator /app/scripts/generator-anomaly-smoke.py

clean:
	docker compose down

clean-volumes:
	@printf '%s\n' \
		'WARNING: deleting all persisted Kafka, PostgreSQL, and Redis data.'
	docker compose down --volumes
