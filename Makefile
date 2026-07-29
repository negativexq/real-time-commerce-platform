PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)

.PHONY: lint format format-check type-check test check compose-config up down logs ps \
	kafka-topics kafka-describe kafka-smoke postgres-shell postgres-tables redis-cli \
	storage-smoke storage-status storage-logs generator-build generator-up \
	generator-down generator-logs generator-run generator-sample generator-status \
	generator-smoke generator-personas generator-normal generator-suspicious \
	generator-bot generator-takeover generator-anomalies generator-persona-smoke \
	generator-anomaly-smoke processor-build processor-up processor-down \
	processor-run processor-logs processor-status processor-sample processor-smoke \
	processor-duplicate-smoke processor-dlq-smoke processor-retry-smoke \
	processor-idempotency-status processor-clear-test-state clean clean-volumes \
	db-migrate db-migration-status db-schema-check db-tables db-counts \
	db-reset-test-data persistence-sample persistence-smoke \
	persistence-duplicate-smoke persistence-recovery-smoke \
	persistence-dependency-smoke persistence-refund-smoke \
	fraud-rules fraud-config-check fraud-db-status fraud-sample-normal \
	fraud-sample-suspicious fraud-sample-bot fraud-sample-takeover fraud-smoke \
	fraud-score-smoke fraud-alert-smoke fraud-idempotency-smoke \
	fraud-outbox-build fraud-outbox-up fraud-outbox-down fraud-outbox-logs \
	fraud-outbox-status fraud-outbox-smoke fraud-outbox-recovery-smoke \
	fraud-counts fraud-clear-test-data

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

db-migrate:
	docker compose --profile processor run --rm postgres-migrate

db-migration-status:
	docker compose --profile processor run --rm --entrypoint python \
		postgres-migrate -m services.event_processor.persistence.migrations status

db-schema-check:
	docker compose --profile processor run --rm --entrypoint python \
		postgres-migrate -m services.event_processor.persistence.migrations check

db-tables: postgres-tables

db-counts:
	docker compose exec -T postgres sh -c \
		'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c \
		"SELECT relname AS table_name, n_live_tup AS estimated_rows FROM pg_stat_user_tables ORDER BY relname;"'

db-reset-test-data:
	@test -n "$(TEST_RUN_ID)" || (echo "TEST_RUN_ID is required" >&2; exit 2)
	docker compose exec -T postgres sh -c \
		'psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" \
		-v run_id="$(TEST_RUN_ID)" -f /dev/stdin' < scripts/reset-persistence-test-data.sql

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

processor-build:
	docker compose --profile processor build event-processor

processor-up:
	docker compose --profile processor up -d --build event-processor

processor-down:
	docker compose --profile processor rm -sf event-processor

processor-run:
	docker compose --profile processor run --rm event-processor

processor-logs:
	docker compose --profile processor logs -f event-processor

processor-status:
	docker compose --profile processor ps event-processor

processor-sample:
	docker compose --profile processor run --rm \
		--entrypoint python event-processor /app/scripts/processor-smoke.py normal

processor-smoke:
	docker compose --profile processor run --rm \
		--entrypoint python event-processor /app/scripts/processor-smoke.py normal

processor-duplicate-smoke:
	docker compose --profile processor run --rm \
		--entrypoint python event-processor /app/scripts/processor-smoke.py duplicate

processor-dlq-smoke:
	docker compose --profile processor run --rm \
		--entrypoint python event-processor /app/scripts/processor-smoke.py dlq

processor-retry-smoke:
	docker compose --profile processor run --rm \
		--entrypoint python event-processor /app/scripts/processor-smoke.py retry

persistence-sample:
	docker compose --profile processor run --rm \
		--entrypoint python event-processor /app/scripts/persistence-smoke.py sample

persistence-smoke:
	docker compose --profile processor run --rm \
		--entrypoint python event-processor /app/scripts/persistence-smoke.py normal

persistence-duplicate-smoke:
	docker compose --profile processor run --rm \
		--entrypoint python event-processor /app/scripts/persistence-smoke.py duplicate

persistence-recovery-smoke:
	docker compose --profile processor run --rm \
		--entrypoint python event-processor /app/scripts/persistence-smoke.py recovery

persistence-dependency-smoke:
	docker compose --profile processor run --rm \
		--entrypoint python event-processor /app/scripts/persistence-smoke.py dependency

persistence-refund-smoke:
	docker compose --profile processor run --rm \
		--entrypoint python event-processor /app/scripts/persistence-smoke.py refund

fraud-rules:
	$(PYTHON) scripts/fraud-admin.py rules

fraud-config-check:
	$(PYTHON) scripts/fraud-admin.py config

fraud-db-status fraud-counts:
	docker compose exec -T postgres sh -c \
		'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c \
		"SELECT (SELECT COUNT(*) FROM fraud_evaluations) AS evaluations, \
		(SELECT COUNT(*) FROM fraud_alerts WHERE status = '"'"'OPEN'"'"') AS open_alerts, \
		(SELECT COUNT(*) FROM fraud_outbox WHERE status = '"'"'PENDING'"'"') AS pending, \
		(SELECT COUNT(*) FROM fraud_outbox WHERE status = '"'"'PUBLISHED'"'"') AS published;"'

fraud-sample-normal:
	$(MAKE) generator-normal

fraud-sample-suspicious:
	$(MAKE) generator-suspicious

fraud-sample-bot:
	$(MAKE) generator-bot

fraud-sample-takeover:
	$(MAKE) generator-takeover

fraud-smoke fraud-score-smoke fraud-alert-smoke fraud-idempotency-smoke:
	$(PYTHON) -m pytest tests/unit/test_fraud_engine.py

fraud-outbox-build:
	docker compose --profile fraud build fraud-outbox-publisher

fraud-outbox-up:
	docker compose --profile fraud up -d --build fraud-outbox-publisher

fraud-outbox-down:
	docker compose --profile fraud rm -sf fraud-outbox-publisher

fraud-outbox-logs:
	docker compose --profile fraud logs -f fraud-outbox-publisher

fraud-outbox-status:
	docker compose --profile fraud ps fraud-outbox-publisher

fraud-outbox-smoke:
	$(PYTHON) -m pytest tests/unit/test_fraud_engine.py \
		-k alert_event_is_deterministic

fraud-outbox-recovery-smoke:
	$(PYTHON) -m pytest tests/unit/test_fraud_outbox.py

fraud-clear-test-data:
	docker compose exec -T postgres sh -c \
		'psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c \
		"WITH test_events AS (SELECT event_id FROM processed_events \
		WHERE source LIKE '"'"'fraud-smoke:%'"'"') \
		DELETE FROM fraud_outbox WHERE aggregate_id IN \
		(SELECT alert_id FROM fraud_alerts WHERE source_event_id IN \
		(SELECT event_id FROM test_events)); \
		WITH test_events AS (SELECT event_id FROM processed_events \
		WHERE source LIKE '"'"'fraud-smoke:%'"'"') DELETE FROM fraud_alerts \
		WHERE source_event_id IN (SELECT event_id FROM test_events); \
		WITH test_events AS (SELECT event_id FROM processed_events \
		WHERE source LIKE '"'"'fraud-smoke:%'"'"') DELETE FROM fraud_evaluations \
		WHERE source_event_id IN (SELECT event_id FROM test_events);"'

processor-idempotency-status:
	docker compose exec -T redis redis-cli --scan \
		--pattern 'commerce:processor:*' | head -100

processor-clear-test-state:
	@docker compose exec -T redis sh -c \
		'redis-cli --scan --pattern "commerce:processor:test:*" | \
		xargs -r redis-cli DEL >/dev/null'
	@printf '%s\n' 'Deleted only commerce:processor:test:* Redis keys.'

clean:
	docker compose down

clean-volumes:
	@printf '%s\n' \
		'WARNING: deleting all persisted Kafka, PostgreSQL, and Redis data.'
	docker compose down --volumes
